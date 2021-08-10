package pool

import (
	"io"
	"sync"
	"time"
)

type driverConn struct {
	db *DB
	mu sync.Mutex

	createdAt time.Time //连接创建时间
	ci        io.Closer //连接接口
	done      chan struct{}
	isClosed  bool
	inUse     bool //是否被使用
}

// 判断活跃时间是否到期
func (dc *driverConn) expired(timeout time.Duration) bool {
	if timeout <= 0 {
		return false
	}
	return dc.createdAt.Add(timeout).Before(time.Now())
}

// 关闭资源
func (dc *driverConn) close() (err error) {
	dc.mu.Lock()
	if dc.ci == nil || dc.isClosed {
		dc.mu.Unlock()
		return nil
	}
	dc.isClosed = true
	if dc.done == nil {
		closedchan := make(chan struct{})
		close(closedchan)
		dc.done = closedchan
	} else {
		close(dc.done)
	}
	dc.mu.Unlock()
	err = dc.ci.Close()

	dc.db.Lock()
	dc.db.numOpen--
	dc.db.maybeOpenNewConnections()
	dc.db.Unlock()

	return err
}

// 获取真实连接Connect返回的值，连接池通用性
func (dc *driverConn) Conn() io.Closer {
	return dc.ci
}

// 回收资源
func (dc *driverConn) Close() error {
	if !dc.db.putConn(dc) {
		return dc.close()
	}
	return nil
}

func (dc *driverConn) cleanDriver() {

	t := time.NewTimer(dc.db.maxLifetime)
	defer t.Stop()
	for {
		select {
		case <-t.C:
		case <-dc.Done():
			return
		}
		dc.db.Lock()
		if dc.db.closed || dc.db.numOpen == 0 {
			dc.db.releaseConnRequests()
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
			return
		}
		t.Reset(dc.db.maxLifetime)
	}
}

func (dc *driverConn) Done() <-chan struct{} {
	dc.mu.Lock()
	if dc.done == nil {
		dc.done = make(chan struct{})
	}
	d := dc.done
	dc.mu.Unlock()
	return d
}
