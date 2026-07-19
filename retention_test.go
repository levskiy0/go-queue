package go_queue

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/levskiy0/go-queue/contract"
)

func TestMachineryResultRetention(t *testing.T) {
	tests := []struct {
		name      string
		retention time.Duration
		seconds   int
	}{
		{name: "default", seconds: 0},
		{name: "minutes", retention: 10 * time.Minute, seconds: 600},
		{name: "rounds up", retention: 1500 * time.Millisecond, seconds: 2},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			connections := NewConnections()
			connections.Add("redis", &Connection{
				Driver: DriverRedis,
				Redis: &RedisConfig{
					Host:            "127.0.0.1",
					Port:            "6379",
					ResultRetention: test.retention,
				},
			})

			server, err := NewMachinery(connections, slog.Default()).Server("redis", "retention")
			if err != nil {
				t.Fatal(err)
			}
			if got := server.GetConfig().ResultsExpireIn; got != test.seconds {
				t.Fatalf("ResultsExpireIn = %d, want %d", got, test.seconds)
			}
		})
	}
}

func TestRedisResultRetentionTTL(t *testing.T) {
	redis := miniredis.RunT(t)
	connections := NewConnections()
	connections.Add("redis", &Connection{
		Driver: DriverRedis,
		Redis: &RedisConfig{
			Host:            redis.Host(),
			Port:            redis.Port(),
			ResultRetention: 90 * time.Second,
		},
	})

	queue := NewQueue(connections, slog.Default(), false)
	job := &TestAsyncJob{}
	queue.Register([]contract.Job{job})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runRetryWorker(t, ctx, queue, contract.Args{
		Connection: "redis",
		Queue:      "retention",
		Concurrent: 1,
	})
	time.Sleep(100 * time.Millisecond)

	if err := queue.Job(job, nil).OnConnection("redis").OnQueue("retention").Dispatch(); err != nil {
		t.Fatal(err)
	}
	if !waitFor(t, 3*time.Second, func() bool { return job.Calls() == 1 }) {
		t.Fatalf("job calls = %d, want 1", job.Calls())
	}

	var resultKey string
	if !waitFor(t, time.Second, func() bool {
		for _, key := range redis.Keys() {
			if strings.HasPrefix(key, "task_") {
				resultKey = key
				return true
			}
		}
		return false
	}) {
		t.Fatal("task result key was not created")
	}
	if ttl := redis.TTL(resultKey); ttl != 90*time.Second {
		t.Fatalf("task result TTL = %s, want 1m30s", ttl)
	}
}
