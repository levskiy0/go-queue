package go_queue

import (
	"context"
	"github.com/levskiy0/go-queue/contract"
	"log/slog"
	"sync"
	"testing"
	"time"
)

var testSyncJob = 0
var testAsyncJob = 0
var mu sync.Mutex

type TestSyncJob struct {
}

func (receiver *TestSyncJob) Signature() string {
	return "test_sync_job"
}

func (receiver *TestSyncJob) Handle(args ...any) error {
	testSyncJob++

	return nil
}

type TestAsyncJob struct {
}

func (receiver *TestAsyncJob) Signature() string {
	return "test_async_job"
}

func (receiver *TestAsyncJob) Handle(args ...any) error {
	mu.Lock()
	defer mu.Unlock()

	testAsyncJob++

	return nil
}

func TestSync(t *testing.T) {
	conns := NewConnections()
	conns.Add("default", &Connection{Driver: DriverSync})

	q := NewQueue(conns, nil, false)
	err := q.Job(&TestSyncJob{}, []contract.Arg{
		{Type: "string", Value: "TestSyncQueue"},
		{Type: "int", Value: 1},
	}).Dispatch()

	if err != nil {
		t.Error(err)
	}
}

func TestAsyncQueue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

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
			t.Error(err)
		}

		for range ctx.Done() {
			return
		}
	}(ctx)

	q.Job(&TestAsyncJob{}, []contract.Arg{
		{Type: "string", Value: "TestAsyncQueue"},
		{Type: "int", Value: 1},
	}).OnConnection("redis").OnQueue("custom").Dispatch()

	q.Job(&TestAsyncJob{}, []contract.Arg{
		{Type: "string", Value: "TestAsyncQueue"},
		{Type: "int", Value: 2},
	}).OnConnection("redis").OnQueue("custom").Dispatch()

	time.Sleep(2 * time.Second)

	if testAsyncJob != 2 {
		t.Fail()
	}
}

func TestChainQueue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

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
			t.Error(err)
		}

		for range ctx.Done() {
			return
		}
	}(ctx)

	q.Chain([]contract.Jobs{
		{
			Job: &TestAsyncJob{},
			Args: []contract.Arg{
				{Type: "string", Value: "TestChainAsyncQueue"},
				{Type: "int", Value: 1},
			},
		},
		{
			Job: &TestAsyncJob{},
			Args: []contract.Arg{
				{Type: "string", Value: "TestChainAsyncQueue"},
				{Type: "int", Value: 2},
			},
		},
	}).OnConnection("redis").OnQueue("custom").Dispatch()

	time.Sleep(2 * time.Second)

	if testAsyncJob != 2 {
		t.Fail()
	}
}
