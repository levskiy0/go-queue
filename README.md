# go-queue

Fork from [Goravel](https://github.com/goravel/framework) for single use by necessary.

### Install

```shell
go get guthub.com/levskiy0/go-queue 
```

### Usage

```go 
func main() {
    conns := NewConnections()
    conns.Add("default", &Connection{Driver: DriverSync})
    conns.Add("redis", &Connection{Driver: DriverRedis, Redis: &RedisConfig{
        Database: 1,
        Host:     "127.0.0.1",
        Port:     "6379",
        Password: "",
    }})

    q := NewQueue(conns, slog.Default(), false)
    q.Register([]contract.Job{
        &TestAsyncJob{},
    })

    go func(ctx context.Context) {
        err := q.Worker(contract.Args{
            Connection: "redis",
            Queue:      "custom",
            Concurrent: 2,
        }).Run()
    
        if err != nil {
            return;
        }
        
        for range ctx.Done() {
            return
        }
    }(ctx)
    
    q.Job(&TestAsyncJob{}, []contract.Arg{
        {Type: "string", Value: "TestAsyncQueue"},
        {Type: "int", Value: 1},
    }).OnConnection("redis").OnQueue("custom").Dispatch()
}
```