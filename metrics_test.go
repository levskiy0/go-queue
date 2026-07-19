package go_queue

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/alicebob/miniredis/v2"
)

func TestStatsAggregatesRedisWorkersAcrossProcesses(t *testing.T) {
	server := miniredis.RunT(t)
	host, port := splitTestRedisAddress(t, server.Addr())
	firstConnections := metricsTestConnections(host, port)
	secondConnections := metricsTestConnections(host, port)
	readerConnections := metricsTestConnections(host, port)
	firstRegistry := newMetricsRegistry(firstConnections, nil)
	secondRegistry := newMetricsRegistry(secondConnections, nil)
	reader := NewQueue(readerConnections, nil, false)
	t.Cleanup(func() {
		_ = firstRegistry.close()
		_ = secondRegistry.close()
		_ = reader.Close()
	})

	first := firstRegistry.worker("redis", "emails", 2)
	second := secondRegistry.worker("redis", "emails", 3)
	first.start()
	second.start()
	t.Cleanup(first.close)
	t.Cleanup(second.close)

	first.begin()
	first.finish(nil)
	first.begin()
	first.finish(nil)
	second.begin()
	second.finish(errors.New("failed"))
	first.publish()
	second.publish()

	client, err := reader.metrics.client("redis")
	if err != nil {
		t.Fatal(err)
	}
	if err := client.LPush(context.Background(), "emails", "first", "second").Err(); err != nil {
		t.Fatal(err)
	}

	stats, err := reader.Stats(context.Background(), "redis")
	if err != nil {
		t.Fatal(err)
	}
	if stats.WorkersTotal != 5 || stats.WorkersActive != 0 {
		t.Fatalf("workers = %d/%d", stats.WorkersActive, stats.WorkersTotal)
	}
	if stats.Processed != 3 || stats.Succeeded != 2 || stats.Failed != 1 {
		t.Fatalf("attempts = processed:%d succeeded:%d failed:%d", stats.Processed, stats.Succeeded, stats.Failed)
	}
	if stats.Pending != 2 {
		t.Fatalf("pending = %d", stats.Pending)
	}
	if len(stats.Queues) != 1 || stats.Queues[0].Name != "emails" || stats.Queues[0].Pending != 2 {
		t.Fatalf("queues = %+v", stats.Queues)
	}
	if stats.StartedAt.IsZero() {
		t.Fatal("started at is zero")
	}
}

func TestStatsReportsActiveWorkersAndRemovesStoppedWorker(t *testing.T) {
	server := miniredis.RunT(t)
	host, port := splitTestRedisAddress(t, server.Addr())
	connections := metricsTestConnections(host, port)
	queue := NewQueue(connections, nil, false)
	t.Cleanup(func() { _ = queue.Close() })
	worker := queue.metrics.worker("redis", "jobs", 4)
	worker.start()

	worker.begin()
	worker.publish()
	stats, err := queue.Stats(context.Background(), "redis")
	if err != nil {
		t.Fatal(err)
	}
	if stats.WorkersActive != 1 || stats.WorkersTotal != 4 || stats.Processed != 1 {
		t.Fatalf("active stats = %+v", stats)
	}

	worker.finish(nil)
	worker.close()
	stats, err = queue.Stats(context.Background(), "redis")
	if err != nil {
		t.Fatal(err)
	}
	if stats.WorkersActive != 0 || stats.WorkersTotal != 0 || len(stats.Queues) != 0 {
		t.Fatalf("stopped stats = %+v", stats)
	}
}

func TestStatsIgnoresExpiredWorkerSnapshot(t *testing.T) {
	server := miniredis.RunT(t)
	host, port := splitTestRedisAddress(t, server.Addr())
	connections := metricsTestConnections(host, port)
	queue := NewQueue(connections, nil, false)
	t.Cleanup(func() { _ = queue.Close() })
	worker := queue.metrics.worker("redis", "jobs", 1)
	worker.publish()

	server.Del(metricsWorkerKey(worker.id))
	stats, err := queue.Stats(context.Background(), "redis")
	if err != nil {
		t.Fatal(err)
	}
	if stats.WorkersTotal != 0 || len(stats.Queues) != 0 {
		t.Fatalf("expired stats = %+v", stats)
	}
}

func TestStatsTracksSyncDriverLocally(t *testing.T) {
	connections := NewConnections()
	connections.Add("sync", &Connection{Driver: DriverSync})
	queue := NewQueue(connections, nil, false)
	t.Cleanup(func() { _ = queue.Close() })

	if err := queue.Job(&TestSyncJob{}, nil).OnQueue("inline").Dispatch(); err != nil {
		t.Fatal(err)
	}
	stats, err := queue.Stats(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Processed != 1 || stats.Succeeded != 1 || stats.Failed != 0 {
		t.Fatalf("stats = %+v", stats)
	}
	if len(stats.Queues) != 1 || stats.Queues[0].Name != "inline" {
		t.Fatalf("queues = %+v", stats.Queues)
	}
}

func TestQueueCloseRemovesAllWorkerSnapshots(t *testing.T) {
	server := miniredis.RunT(t)
	host, port := splitTestRedisAddress(t, server.Addr())
	queue := NewQueue(metricsTestConnections(host, port), nil, false)
	first := queue.metrics.worker("redis", "first", 1)
	second := queue.metrics.worker("redis", "second", 1)
	first.start()
	second.start()

	members, err := server.ZMembers(metricsWorkersKey())
	if err != nil || len(members) != 2 {
		t.Fatalf("worker index = %v, err = %v", members, err)
	}
	if err := queue.Close(); err != nil {
		t.Fatal(err)
	}
	members, err = server.ZMembers(metricsWorkersKey())
	if err == nil && len(members) != 0 {
		t.Fatalf("worker index after close = %v", members)
	}
	if server.Exists(metricsWorkerKey(first.id)) || server.Exists(metricsWorkerKey(second.id)) {
		t.Fatal("worker snapshot remains after close")
	}
}

func metricsTestConnections(host, port string) *Connections {
	connections := NewConnections()
	connections.Add("redis", &Connection{
		Driver: DriverRedis,
		Redis:  &RedisConfig{Host: host, Port: port},
	})
	return connections
}

func splitTestRedisAddress(t *testing.T, address string) (string, string) {
	t.Helper()
	for index := len(address) - 1; index >= 0; index-- {
		if address[index] == ':' {
			if _, err := strconv.Atoi(address[index+1:]); err != nil {
				t.Fatal(err)
			}
			return address[:index], address[index+1:]
		}
	}
	t.Fatalf("invalid redis address %q", address)
	return "", ""
}
