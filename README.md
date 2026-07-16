# go-queue

Version: v1.3.0

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

### Retries

Chain `.Retries(count)` and, optionally, `.RetryAfter(initial)` onto a task before dispatching it:

```go
q.Job(job, args).OnQueue("notify:email").Retries(3).Dispatch()

q.Job(job, args).OnQueue("notify:telegram").Retries(5).RetryAfter(5 * time.Second).Dispatch()
```

`Retries(count)` is the number of retries after the first failed attempt (`count=3` means up to 4 executions total). Failed attempts are republished to the queue with a Fibonacci backoff (`1s, 2s, 3s, 5s, 8s, ...` from the default seed of `0`). `RetryAfter(initial)` seeds that progression instead of starting from `0` — the actual first delay is `FibonacciNext(initial)`, so `RetryAfter(5 * time.Second)` produces `8s, 13s, 21s, ...`. Both apply to every signature of a chain.

`DispatchSync` (and `Dispatch` on a `sync` connection) ignores both — see "Sync driver" below.

To stop retrying on a specific error, have your job implement `contract.JobWithNoRetry`:

```go
func (j *sendJob) NoRetry(err error) bool { return isPermanent(err) }
```

When `Handle` fails and `NoRetry(err)` returns `true`, the worker logs a warning and reports the task as successful — no further retries. This check only applies to the error returned by `Handle`; it has no effect on rate-limit retries (see below), which happen before `Handle` is even called.

### RateLimit

Pass a `RateLimit` in `contract.Args` to throttle a worker's execution rate:

```go
q.Worker(contract.Args{
    Queue:      "notify:telegram",
    Concurrent: 1,
    RateLimit: &contract.RateLimit{
        Limit: 24,
        Per:   time.Second,
        Key:   telegramBotKey,
    },
}).Run()
```

`Limit` events are allowed per `Per` duration, with a burst equal to `Limit`. If `Key` is `nil`, all jobs on the queue share a single bucket; otherwise each distinct `Key(args)` gets its own bucket (`queue + ":" + key`). When no token is available, the task is not executed — it is republished with `tasks.ErrRetryTaskLater` and an ETA in the near future, so the worker immediately picks up the next task instead of blocking. This does not consume any of the task's `Retries` budget: rate-limit backoffs and retry-on-error backoffs are fully independent.

For direct users of `NewWorker` (bypassing `Queue.Worker`), call `worker.WithRateLimit(rl)` explicitly.

Known limitations:
- The limiter is **per-process and per-worker**: multiple processes, or multiple workers consuming the same queue in one process, do not share a rate budget.
- The granularity of returning a rate-limited task to execution is bounded by the broker's delayed-tasks poller (`Redis.DelayedTasksPollPeriod`, set to `100ms` by this library).
- The keyed-limiter map evicts idle entries opportunistically (on access, based on `lastSeen`), not via a background goroutine — there is no lifecycle hook to stop one on `Worker.Run()`.

### Sync driver

On a `sync` connection, dispatch is fully fire-once: `Retries`/`RetryAfter` and `RateLimit` are both ignored, and `Handle`'s error is returned directly to the caller. This is intentional — sync is typically used inline on a request path (e.g. an HTTP handler), where blocking on a pacer or swallowing an error would be wrong.

### Known limitations (v1)

- The Redis broker consumes tasks with a destructive `BLPop`; if the worker process crashes between taking a task and finishing it, the task is lost. Retries protect against handler errors, not against the process dying mid-task.
- Rate limiting is per-process/per-worker, not distributed (see above).