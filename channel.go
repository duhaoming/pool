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
	ErrDBClosed = errors.New("database is closed")
	// ErrBadConn 无效的连接
	ErrBadConn = errors.New("bad connection")
	// ErrTimeOut 等待超时
	ErrTimeOut = errors.New("wait timeout")

	defContext = context.Background()
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
	Get(context.Context) (Driver, error)
	Close() error
}

// DB 生成一个DB池
type DB struct {
	sync.Mutex

	freeConn     []*driverConn               //空闲连接队列
	connRequests map[uint64]chan *driverConn //连接等待队列
	openerCh     chan struct{}               //创建新连接信号
	cleanerCh    chan struct{}               //清理连接信号

	connector   Connect
	maxLifetime time.Duration //活跃时间
	timeOut     time.Duration //超时时间
	maxOpen     int           //最大打开连接数
	numOpen     int           //打开连接数
	maxIdle     int           //最大空闲连接数
	nextRequest uint64        //下一个等待连接key

	stop   func() //关闭触发函数，context的
	closed bool   //连接池是否关闭
}

const connectionRequestQueueSize = 50

// OpenCustom 可配置连接
func OpenCustom(c Connect, maxLifetime, timeOut time.Duration, maxIdle, maxOpen int) Pool {
	ctx, cancel := context.WithCancel(defContext)
	db := &DB{
		connector:    c,
		openerCh:     make(chan struct{}, connectionRequestQueueSize),
		stop:         cancel,
		maxLifetime:  maxLifetime,
		timeOut:      timeOut,
		maxIdle:      maxIdle,
		maxOpen:      maxOpen,
		freeConn:     make([]*driverConn, 0, maxOpen),
		connRequests: make(map[uint64]chan *driverConn),
	}

	// 监控
	go db.connectionOpener(ctx)

	return db
}

// Open 默认配置连接
func Open(c Connect) Pool {
	ctx, cancel := context.WithCancel(defContext)
	db := &DB{
		connector:    c,
		openerCh:     make(chan struct{}, connectionRequestQueueSize),
		stop:         cancel,
		maxLifetime:  3 * time.Minute,
		timeOut:      15 * time.Second,
		maxIdle:      5,
		maxOpen:      10,
		freeConn:     make([]*driverConn, 0, 10),
		connRequests: make(map[uint64]chan *driverConn),
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
			db.openNewConnection(ctx)
		}
	}
}

func (db *DB) openNewConnection(ctx context.Context) {
	ci, err := db.connector(ctx)
	db.Lock()
	defer db.Unlock()
	if db.closed {
		if err == nil {
			ci.Close()
		}
		db.numOpen--
		return
	}
	if err != nil {
		db.numOpen--
		db.maybeOpenNewConnections()
		return
	}
	dc := &driverConn{
		db:        db,
		createdAt: time.Now(),
		ci:        ci,
	}
	if !db.recovery(dc) {
		db.numOpen--
		ci.Close()
		return
	}
	go dc.cleanDriver()
}

// 资源创建失败时, 判断是否有还在等待的请求, 有就创建新的资源
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

// 释放map,delete只能删除键,不能释放内存
func (db *DB) releaseConnRequests() {
	if db.nextRequest != 0 && len(db.connRequests) == 0 {
		db.connRequests = nil
		db.nextRequest = 0
		db.connRequests = make(map[uint64]chan *driverConn)
	}
}

// 获取资源
func (db *DB) conn(ctx context.Context) (*driverConn, error) {
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

	numFree := len(db.freeConn)
	if numFree > 0 {
		db.releaseConnRequests()
		conn := db.freeConn[0]
		copy(db.freeConn, db.freeConn[1:])
		db.freeConn = db.freeConn[:numFree-1]
		conn.inUse = true
		db.Unlock()
		if conn.expired(db.maxLifetime) {
			conn.close()
			return nil, ErrBadConn
		}

		return conn, nil
	}

	if db.maxOpen > 0 && db.numOpen >= db.maxOpen {
		reqkey := db.nextConnRequestsKey()
		req := make(chan *driverConn, 1)
		db.connRequests[reqkey] = req
		db.Unlock()

		select {
		case <-ctx.Done():
			db.Lock()
			delete(db.connRequests, reqkey)
			db.Unlock()

			select {
			default:
			case conn, ok := <-req:
				if ok && conn != nil {
					if !db.putConn(conn) {
						conn.close()
					}
				}
			}
			return nil, ErrTimeOut
		case conn, ok := <-req:
			if !ok || conn == nil {
				return nil, ErrBadConn
			}
			if conn.expired(db.maxLifetime) {
				conn.close()
				return nil, ErrBadConn
			}
			return conn, nil
		}
	}

	db.numOpen++
	db.Unlock()

	conn, err := db.connector(ctx)
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
		inUse:     true,
	}
	go dc.cleanDriver()
	return dc, nil
}

// 资源回收
func (db *DB) putConn(dc *driverConn) bool {
	db.Lock()
	if dc.expired(db.maxLifetime) {
		db.maybeOpenNewConnections()
		db.Unlock()
		return false
	}
	dc.inUse = false
	isRecovery := db.recovery(dc)
	db.Unlock()
	return isRecovery
}

func (db *DB) recovery(dc *driverConn) bool {
	if db.closed {
		return false
	}

	if db.maxOpen > 0 && db.numOpen > db.maxOpen {
		return false
	}
	if c := len(db.connRequests); c > 0 {
		var req chan *driverConn
		var reqkey uint64
		for reqkey, req = range db.connRequests {
			break
		}

		delete(db.connRequests, reqkey)
		req <- dc
		dc.inUse = true
		close(req)

		return true
	} else if db.maxIdle > len(db.freeConn) {
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
func (db *DB) Get(ctx context.Context) (dc Driver, err error) {
	if ctx == nil {
		ctx = defContext
	}
	ctext, cancel := context.WithTimeout(ctx, db.timeOut)
	for i := 0; i < 2; i++ {
		dc, err = db.conn(ctext)
		if err != ErrBadConn {
			break
		}
	}
	cancel()
	return
}
