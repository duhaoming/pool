# pool
golang 实现通用资源池

# 功能
+ 资源池中资源类型为io.Closer接口类型
+ 资源有活跃时间、空闲时间，连接超时时间，可保持资源的有效性
+ 资源可回收
+ 定时清理空闲资源
+ 支持等待资源队列

# 基本用法
``` goalng
// 这里用grpc当做实例
// 创建资源函数
func Connects(_ context.Context) (io.Closer, error) {
	return grpc.Dial(addr, grpc.WithInsecure())
}
// 默认配置池
poolConn := pool.Open(Connects)
// 可配置池
cusConn := pool.OpenCustom(
        Connects,
        5 * time.Minute,  // 活跃时间
        5 * time.Second, // 超时时间
        5,    // 最大空闲资源
        10, // 最大打开的资源
)

// 获取资源
c, err := poolConn.Get(nil)

// 真实客户端
co := c.Conn().(*grpc.ClientConn)

// 回收资源
err = c.Close()

// 关闭连接池
poolConn.Close()
```

**注：**
资源池参考database/sql实现，简化了许多功能
