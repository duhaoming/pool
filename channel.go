package pool

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"
)

var (
	// ErrDBClosed 连接池关闭
	ErrDBClosed = errors.New("connection pool is closed")
	// ErrBadConn 无效的连接
	ErrBadConn = errors.New("bad connection")
)

// Connect 连接接口
type Connect func(context.Context) (io.Closer, error)

// Conn 连接接口
type Driver interface {
	Conn() io.Closer
	Close() error
}

// Pool 连接池
type Pool interface {
	Get() (Driver, error)
	Close() error
}

// DB 生成一个DB池
type DB struct {
	sync.Mutex

	freeConn     []*driverConn               //空闲连接队列
	connRequests map[uint64]chan connRequest //连接等待队列
	openerCh     chan struct{}               //创建新连接信号
	cleanerCh    chan struct{}               //清理连接信号

	ctx         context.Context
	maxLifetime time.Duration //活跃时间
	timeOut     time.Duration //超时时间
	maxOpen     int           //最大打开连接数
	numOpen     int           //打开连接数
	maxIdle     int           //最大空闲连接数
	nextRequest uint64        //下一个等待连接key

	connector Connect
	stop      func() //关闭触发函数，context的
	closed    bool   //连接池是否关闭
}

type connRequest struct {
	conn *driverConn
	err  error
}

const connectionRequestQueueSize = 50
const (
	alwaysNewConn uint8 = iota
	cachedOrNewConn
)

// OpenCustom 可配置连接
func OpenCustom(c Connect, maxLifetime, timeOut time.Duration, maxIdle, maxOpen int) Pool {
	ctx, cancel := context.WithCancel(context.Background())
	db := &DB{
		connector:    c,
		openerCh:     make(chan struct{}, connectionRequestQueueSize),
		stop:         cancel,
		maxLifetime:  maxLifetime,
		timeOut:      timeOut,
		maxIdle:      maxIdle,
		maxOpen:      maxOpen,
		freeConn:     make([]*driverConn, 0, maxOpen),
		connRequests: make(map[uint64]chan connRequest),
		ctx:          ctx,
	}

	// 监控
	go db.connectionOpener(ctx)

	return db
}

// 创建新的连接
// 等待maybeOpenNewConnections释放阻塞
func (db *DB) connectionOpener(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-db.openerCh:
			go db.openNewConnection(ctx)
		}
	}
}

// 创建新的连接放入空闲连接队列，或直接返回在等待获取连接的终端
func (db *DB) openNewConnection(ctx context.Context) {
	ci, err := db.connector(ctx)
	db.Lock()
	if db.closed {
		if err == nil {
			ci.Close()
		}
		db.numOpen--
		db.Unlock()
		return
	}
	if err != nil {
		db.numOpen--
		db.recovery(nil, err)
		db.maybeOpenNewConnections()
		db.Unlock()
		return
	}
	dc := &driverConn{
		db:        db,
		createdAt: time.Now(),
		ci:        ci,
	}
	if !db.recovery(dc, nil) {
		db.numOpen--
		ci.Close()
		db.Unlock()
		return
	}
	db.Unlock()
	dc.clearConn = time.AfterFunc(db.maxLifetime, dc.cleanDriver)
}

// 连接异常时, 判断是否有还在等待的请求, 有就创建新的资源 db.openerCh <- struct{}{}
func (db *DB) maybeOpenNewConnections() {
	numRequests := len(db.connRequests)
	if db.maxOpen > 0 {
		numCanOpen := db.maxOpen - db.numOpen
		if numRequests > numCanOpen {
			numRequests = numCanOpen
		}
	}
	for numRequests > 0 {
		db.numOpen++
		numRequests--
		if db.closed {
			return
		}
		db.openerCh <- struct{}{}
	}
}

// 等待连接map的key
func (db *DB) nextConnRequestsKey() uint64 {
	next := db.nextRequest
	db.nextRequest++
	return next
}

// 获取资源
func (db *DB) conn(ctx context.Context, strategy uint8) (*driverConn, error) {
	db.Lock()
	if db.closed {
		db.Unlock()
		return nil, ErrDBClosed
	}

	select {
	default:
	case <-ctx.Done():
		db.Unlock()
		return nil, ctx.Err()
	}

	lifetime := db.maxLifetime
	numFree := len(db.freeConn)
	if strategy == cachedOrNewConn && numFree > 0 {
		conn := db.freeConn[0]
		copy(db.freeConn, db.freeConn[1:])
		db.freeConn = db.freeConn[:numFree-1]
		if conn.expired(lifetime) {
			db.Unlock()
			conn.close()
			return nil, ErrBadConn
		}
		db.Unlock()

		return conn, nil
	}

	if db.maxOpen > 0 && db.numOpen >= db.maxOpen {
		reqkey := db.nextConnRequestsKey()
		req := make(chan connRequest, 1)
		db.connRequests[reqkey] = req
		db.Unlock()

		select {
		case <-ctx.Done():
			db.Lock()
			delete(db.connRequests, reqkey)
			db.Unlock()

			select {
			default:
			case ret, ok := <-req:
				if ok && ret.conn != nil {
					if !db.putConn(ret.conn, ret.err) {
						ret.conn.close()
					}
				}
			}
			return nil, ctx.Err()
		case ret, ok := <-req:
			if !ok {
				return nil, ErrDBClosed
			}
			return ret.conn, ret.err
		}
	}

	db.numOpen++
	db.Unlock()

	conn, err := db.connector(db.ctx)
	if err != nil {
		db.Lock()
		db.numOpen--
		db.maybeOpenNewConnections()
		db.Unlock()
		return nil, err
	}
	dc := &driverConn{
		db:        db,
		ci:        conn,
		createdAt: time.Now(),
	}
	dc.clearConn = time.AfterFunc(lifetime, dc.cleanDriver)
	return dc, nil
}

// 资源回收
// 连接活跃时间到期，则判断是否还有在等待的终端获取连接 maybeOpenNewConnections()
func (db *DB) putConn(dc *driverConn, err error) bool {
	db.Lock()
	if dc.expired(db.maxLifetime) {
		db.maybeOpenNewConnections()
		db.Unlock()
		return false
	}
	isRecovery := db.recovery(dc, err)
	db.Unlock()
	return isRecovery
}

// 判断打开连接是否大于最大连接数，大于关闭此链接、小于则继续
// 判断是否又在等待获取连接的终端，有直接返回此链接
// 既没有错误，池也没有关闭，放入空闲连接队列中
func (db *DB) recovery(dc *driverConn, err error) bool {
	if db.closed {
		return false
	}

	if db.maxOpen > 0 && db.numOpen > db.maxOpen {
		return false
	}
	if len(db.connRequests) > 0 {
		var req chan connRequest
		var reqkey uint64
		for reqkey, req = range db.connRequests {
			break
		}

		delete(db.connRequests, reqkey)
		req <- connRequest{
			conn: dc,
			err:  err,
		}
		close(req)

		return true
	} else if db.maxIdle > len(db.freeConn) && err == nil {
		db.freeConn = append(db.freeConn, dc)
		return true
	}
	return false
}

// 关闭连接池
func (db *DB) Close() (err error) {
	db.Lock()
	if db.closed {
		db.Unlock()
		return ErrDBClosed
	}
	fns := make([]func() error, 0, len(db.freeConn))
	for _, dc := range db.freeConn {
		fns = append(fns, dc.close)
	}
	db.freeConn = nil
	db.closed = true

	for _, req := range db.connRequests {
		close(req)
	}
	db.connRequests = nil

	db.Unlock()

	for _, fn := range fns {
		fnerr := fn()
		if fnerr != nil {
			err = fnerr
		}
	}
	db.stop()
	return err
}

// 获取资源
// 重试机制：2次
// 重试2次之后还是异常（ErrBadConn）连接，则重新获取新的连接，而不是从空闲连接队列中获取
func (db *DB) Get() (dc Driver, err error) {
	ctx, cancel := context.WithCancel(db.ctx)
	timer := time.AfterFunc(db.timeOut, cancel)
	for i := 0; i < 2; i++ {
		dc, err = db.conn(ctx, cachedOrNewConn)
		if err != ErrBadConn {
			break
		}
	}
	if err == ErrBadConn {
		dc, err = db.conn(ctx, alwaysNewConn)
	}
	if timer.Stop() {
		cancel()
	}
	return
}
