package go_queue

import (
	"bytes"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/levskiy0/go-queue/contract"
)

func newRedisTestConnections() *Connections {
	connections := NewConnections()
	connections.Add("redis", &Connection{
		Driver: DriverRedis,
		Redis: &RedisConfig{
			Host: "127.0.0.1",
			Port: "6379",
		},
	})
	return connections
}

func TestQueueTasksShareProducer(t *testing.T) {
	queue := NewQueue(newRedisTestConnections(), nil, true)
	job := &TestAsyncJob{}

	first := queue.Job(job, nil).(*Task)
	for range 1000 {
		next := queue.Job(job, nil).(*Task)
		if next.machinery != first.machinery {
			t.Fatal("queue created a new producer for a task")
		}
	}
}

func TestMachineryProducerIsReusedConcurrently(t *testing.T) {
	connections := newRedisTestConnections()
	machinery := NewMachinery(connections, nil)
	const total = 1000

	servers := make(chan any, total)
	var workers sync.WaitGroup
	workers.Add(total)
	for range total {
		go func() {
			defer workers.Done()
			server, err := machinery.Producer("redis")
			if err != nil {
				t.Errorf("Producer: %v", err)
				return
			}
			servers <- server
		}()
	}
	workers.Wait()
	close(servers)

	var first any
	for server := range servers {
		if first == nil {
			first = server
			continue
		}
		if server != first {
			t.Fatal("concurrent producer initialization returned different servers")
		}
	}
	if len(connections.producers().servers) != 1 {
		t.Fatalf("producers = %d, want 1", len(connections.producers().servers))
	}
}

func TestMachineryProducerCreatesOneScheduler(t *testing.T) {
	before := schedulerGoroutines()
	machinery := NewMachinery(newRedisTestConnections(), nil)

	for range 1000 {
		if _, err := machinery.Producer("redis"); err != nil {
			t.Fatal(err)
		}
	}

	deadline := time.Now().Add(time.Second)
	for schedulerGoroutines() < before+1 && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if after := schedulerGoroutines(); after != before+1 {
		t.Fatalf("scheduler goroutines = %d, want %d", after, before+1)
	}
}

func schedulerGoroutines() int {
	size := 1 << 20
	for {
		stacks := make([]byte, size)
		written := runtime.Stack(stacks, true)
		if written < len(stacks) {
			return bytes.Count(stacks[:written], []byte("github.com/robfig/cron/v3.(*Cron).run"))
		}
		size *= 2
	}
}

func TestExportedTaskConstructorsShareProducer(t *testing.T) {
	connections := newRedisTestConnections()
	job := &TestAsyncJob{}

	first, err := NewTask(connections, nil, job, nil).machinery.Producer("redis")
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewChainTask(connections, nil, []contract.Jobs{{Job: job}}).machinery.Producer("redis")
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatal("exported task constructors use different producers")
	}
}

func TestTaskRoutingKey(t *testing.T) {
	connections := newRedisTestConnections()
	task := newTask(connections, NewMachinery(connections, nil), &TestAsyncJob{}, nil)

	if task.routingKey() != "default" {
		t.Fatalf("default routing key = %q", task.routingKey())
	}
	task.OnQueue("custom")
	if task.routingKey() != "custom" {
		t.Fatalf("custom routing key = %q", task.routingKey())
	}
}

func TestQueueChainSharesProducer(t *testing.T) {
	queue := NewQueue(newRedisTestConnections(), nil, true)
	job := &TestAsyncJob{}

	chain := queue.Chain([]contract.Jobs{{Job: job}}).(*Task)
	task := queue.Job(job, nil).(*Task)
	if chain.machinery != task.machinery {
		t.Fatal("chain and task use different producers")
	}
}
