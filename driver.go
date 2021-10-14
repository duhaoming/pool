package pool

import (
	"io"
	"sync"
	"time"
)

type driverConn struct {
	db *DB
	mu sync.Mutex

	clearConn *time.Timer
	createdAt time.Time //连接创建时间
	ci        io.Closer //连接接口
	closed    bool
}

// 判断活跃时间是否到期
func (dc driverConn) expired(timeout time.Duration) bool {
	if timeout <= 0 {
		return false
	}
	return dc.createdAt.Add(timeout).Before(time.Now())
}

// 关闭资源
func (dc *driverConn) close() (err error) {
	dc.mu.Lock()
	if dc.closed {
		dc.mu.Unlock()
		return nil
	}
	dc.closed = true
	if dc.clearConn != nil {
		dc.clearConn.Stop()
	}
	if dc.ci != nil {
		err = dc.ci.Close()
	}
	dc.mu.Unlock()

	dc.db.Lock()
	dc.db.numOpen--
	dc.db.maybeOpenNewConnections()
	dc.db.Unlock()

	return err
}

// 获取真实连接Connect返回的值，连接池通用性
func (dc driverConn) Conn() io.Closer {
	return dc.ci
}

// 回收资源
func (dc *driverConn) Close() error {
	if !dc.db.putConn(dc, nil) {
		return dc.close()
	}
	return nil
}

func (dc *driverConn) cleanDriver() {
	dc.db.Lock()
	if dc.db.closed || dc.db.numOpen == 0 {
		dc.db.Unlock()
		return
	}

	var closing []*driverConn = make([]*driverConn, 0, 1)
	for i, c := range dc.db.freeConn {
		if c == dc {
			closing = append(closing, c)
			last := len(dc.db.freeConn) - 1
			dc.db.freeConn[i] = dc.db.freeConn[last]
			dc.db.freeConn[last] = nil
			dc.db.freeConn = dc.db.freeConn[:last]
			break
		}
	}
	dc.db.Unlock()

	for _, c := range closing {
		c.close()
	}
}
