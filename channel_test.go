// 一般不用做tcp连接池, 会发生粘包丢包情况
// 用作多的是数据库的连接池
// 或者文件打开的池、http连接池

package pool

import (
	"bytes"
	"context"
	"google.golang.org/grpc"
	"io"
	"log"
	"net"
	"sync"
	"testing"
)

var addr = "127.0.0.1:8061"

func Connects(_ context.Context) (io.Closer, error) {
	return net.Dial("tcp", addr)
}

func grpcConn(_ context.Context) (io.Closer, error) {
	return grpc.Dial("localhost:9080", grpc.WithInsecure())
}

func testListen() (func(), error) {
	listen, err := net.Listen("tcp", addr)
	if err != nil {
		return func() {}, err
	}

	go Accetp(listen)
	return func() {
		listen.Close()
	}, nil
}

func Accetp(listen net.Listener) {
	for {
		conn, err := listen.Accept()

		if err != nil {
			println(err.Error(), "------")
			return
		}
		defer conn.Close()

		go func() {
			for {
				read := make([]byte, 10)
				var rsp bytes.Buffer
				rsp.WriteString("hello ")
				if _, err := conn.Read(read); err != nil {
					//log.Printf("server read: %v\n", err)
					return
				}
				rsp.Write(read)
				if _, err := conn.Write(rsp.Bytes()); err != nil {
					//log.Printf("server write: %v\n", err)
					return
				}
			}
		}()
	}

}

func TestPool(t *testing.T) {
	cancel, err := testListen()
	if err != nil {
		t.Fatal(err)
	}

	poolConn := Open(Connects)
	c, err := poolConn.Get(nil)
	if err != nil {
		t.Fatal(err)
	}
	co := c.Conn().(net.Conn)
	if _, err := co.Write([]byte("world")); err != nil {
		t.Fatal(err)
	}
	read := make([]byte, 12)
	if _, err := co.Read(read); err != nil {
		t.Fatal(err)
	}
	println(string(read))
	c.Close()
	poolConn.Close()
	cancel()
}

func BenchmarkGetPool(b *testing.B) {
	b.StopTimer()
	cancel, err := testListen()
	if err != nil {
		b.Fatal(err)
	}

	poolConn := Open(Connects)
	b.StartTimer()

	var n sync.WaitGroup
	println(b.N)
	for i := 0; i < b.N; i++ {
		n.Add(1)
		go func(n *sync.WaitGroup) {
			defer n.Done()
			c, err := poolConn.Get(nil)
			if err != nil {
				b.Fatal(err)
			}

			co := c.Conn().(net.Conn)

			if _, err := co.Write([]byte("world")); err != nil {
				b.Fatal("client write: ", err)
			}
			read := make([]byte, 13)
			if _, err := co.Read(read); err != nil {
				b.Fatal("client read: ", err)
			}
			log.Printf("%s\n", read)
			log.Printf("%s\n", co.LocalAddr().String())

			c.Close()
		}(&n)
	}
	n.Wait()
	poolConn.Close()

	cancel()
	b.StopTimer()
}

func BenchmarkGetGrpcPool(b *testing.B) {
	b.StopTimer()

	poolConn := Open(Connects)
	b.StartTimer()

	var n sync.WaitGroup
	println(b.N)
	for i := 0; i < b.N; i++ {
		//n.Add(1)
		go func(n *sync.WaitGroup) {
			//defer n.Done()
			c, err := poolConn.Get(nil)
			if err != nil {
				b.Fatal(err)
			}

			//co := c.Conn().(*grpc.ClientConn)

			c.Close()
		}(&n)
	}
	//n.Wait()
	poolConn.Close()

	b.StopTimer()
}
