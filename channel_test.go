// 用作多的是数据库的连接池

package pool

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var addr = "127.0.0.1:8061"

func TestPool(t *testing.T) {
	cancel := server()
	defer cancel()

	b := append([]byte("duhaoming"), '\n') // 数据包的区分\n
	tp := testPool()
	defer tp.Close()
	c, err := tp.Get()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	conn := c.Conn().(net.Conn)
	if _, err := conn.Write(b); err != nil {
		t.Fatal(err)
	}
	read := make([]byte, 2)
	if _, err := conn.Read(read); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkGetPool(b *testing.B) {
	cancel := server()
	defer cancel()
	atomic.SwapUint64(&count, 0)
	atomic.SwapUint64(&writeCount, 0)

	tp := testPool()
	sendB := append([]byte("duhaoming"), '\n')

	var n sync.WaitGroup
	n.Add(b.N)
	for i := 0; i < b.N; i++ {
		go func(i int, n *sync.WaitGroup) {
			defer n.Done()
			c, err := tp.Get()
			if err != nil {
				fmt.Println("Error client", err.Error())
				return
			}
			defer c.Close()
			if conn, ok := c.Conn().(net.Conn); ok {
				// conn.Write(append([]byte(fmt.Sprintf("%d", i)), sendB...))
				conn.Write(sendB)
				atomic.AddUint64(&writeCount, 1)
				_, err := bufio.NewReader(conn).ReadBytes('\n')
				if err != nil {
					fmt.Println("Error response: ", err)
					return
				}
				// fmt.Printf("response: %s\n", read)
			}
		}(i, &n)
	}
	n.Wait()
	tp.Close()
	// fmt.Println(atomic.LoadUint64(&count), atomic.LoadUint64(&writeCount))
}

// 关闭池之后连接获取是否正常
func TestColse(t *testing.T) {
	tp := testPool()
	tp.Close()
	c, err := tp.Get()
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
}

func server() func() error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Println("Error listening", err.Error())
		return func() error { return nil } //终止程序
	}
	go accept(listener)
	return listener.Close
}

func accept(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if _, ok := err.(*net.OpError); ok {
			return
		}
		if err != nil {
			fmt.Println("Error accepting", err.Error())
			return
		}
		go read(conn)
	}
}

var count, writeCount uint64

func read(conn net.Conn) {
	for {
		b, err := bufio.NewReader(conn).ReadBytes('\n')
		if err == io.EOF {
			return
		}
		if err != nil {
			fmt.Println("Error reading", err.Error())
			return
		}
		// fmt.Printf("Received data: %s\n", b)
		conn.Write(b)
		atomic.AddUint64(&count, 1)
	}
	return
}

func testPool() Pool {
	return OpenCustom(func(ctx context.Context) (io.Closer, error) {
		dialer := &net.Dialer{
			KeepAlive: time.Minute * 5,
		}
		return dialer.DialContext(ctx, "tcp", addr)
	}, time.Minute*10, time.Second*15, 100, 100)
}
