package go_queue

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/levskiy0/go-queue/contract"
	"github.com/redis/go-redis/v9"
)

const (
	metricsPrefix            = "go_queue:metrics:v1"
	metricsHeartbeatInterval = 2 * time.Second
	metricsHeartbeatTTL      = 10 * time.Second
	metricsIOTimeout         = time.Second
)

type metricsRegistry struct {
	connections *Connections
	log         *slog.Logger
	mu          sync.Mutex
	clients     map[string]*redis.Client
	closed      bool
	workers     map[*workerMetrics]struct{}
	startedAt   time.Time
	localMu     sync.RWMutex
	local       map[string]*localQueueMetrics
}

type localQueueMetrics struct {
	active    atomic.Int64
	processed atomic.Uint64
	succeeded atomic.Uint64
	failed    atomic.Uint64
}

type workerMetrics struct {
	registry   *metricsRegistry
	connection string
	queue      string
	id         string
	startedAt  time.Time
	workers    int64
	active     atomic.Int64
	processed  atomic.Uint64
	succeeded  atomic.Uint64
	failed     atomic.Uint64
	state      atomic.Int32
	stop       chan struct{}
	done       chan struct{}
}

func newMetricsRegistry(connections *Connections, log *slog.Logger) *metricsRegistry {
	if log == nil {
		log = slog.Default()
	}
	return &metricsRegistry{
		connections: connections,
		log:         log,
		clients:     make(map[string]*redis.Client),
		workers:     make(map[*workerMetrics]struct{}),
		startedAt:   time.Now().UTC(),
		local:       make(map[string]*localQueueMetrics),
	}
}

func (m *metricsRegistry) worker(connection, queue string, workers int) *workerMetrics {
	if workers < 1 {
		workers = 1
	}
	worker := &workerMetrics{
		registry:   m,
		connection: connection,
		queue:      normalizeQueue(queue),
		id:         newMetricsID(),
		startedAt:  time.Now().UTC(),
		workers:    int64(workers),
		stop:       make(chan struct{}),
		done:       make(chan struct{}),
	}
	m.mu.Lock()
	if !m.closed {
		m.workers[worker] = struct{}{}
	}
	m.mu.Unlock()
	return worker
}

func (m *metricsRegistry) client(connection string) (*redis.Client, error) {
	if m == nil || m.connections == nil {
		return nil, fmt.Errorf("queue metrics: no connections found")
	}
	if connection == "" {
		connection = m.connections.GetDefault()
	}
	conn := m.connections.Get(connection)
	if conn == nil {
		return nil, fmt.Errorf("queue metrics: no connection %s found", connection)
	}
	if conn.Driver != DriverRedis || conn.Redis == nil {
		return nil, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, fmt.Errorf("queue metrics: registry is closed")
	}
	if client := m.clients[connection]; client != nil {
		return client, nil
	}
	client := redis.NewClient(&redis.Options{
		Addr:     net.JoinHostPort(conn.Redis.Host, conn.Redis.Port),
		Password: conn.Redis.Password,
		DB:       conn.Redis.Database,
	})
	m.clients[connection] = client
	return client, nil
}

func (m *metricsRegistry) stats(ctx context.Context, connection string) (contract.Stats, error) {
	if connection == "" {
		connection = m.connections.GetDefault()
	}
	conn := m.connections.Get(connection)
	if conn == nil {
		return contract.Stats{}, fmt.Errorf("queue metrics: no connection %s found", connection)
	}
	if conn.Driver == DriverSync {
		return m.localStats(), nil
	}
	client, err := m.client(connection)
	if err != nil || client == nil {
		return contract.Stats{}, err
	}

	now := time.Now().UTC()
	pipe := client.TxPipeline()
	pipe.ZRemRangeByScore(ctx, metricsWorkersKey(), "-inf", strconv.FormatInt(now.UnixMilli()-1, 10))
	workersCmd := pipe.ZRangeByScore(ctx, metricsWorkersKey(), &redis.ZRangeBy{
		Min: strconv.FormatInt(now.UnixMilli(), 10),
		Max: "+inf",
	})
	if _, err := pipe.Exec(ctx); err != nil {
		return contract.Stats{}, err
	}

	workerIDs, err := workersCmd.Result()
	if err != nil {
		return contract.Stats{}, err
	}
	workerCmds := make([]*redis.MapStringStringCmd, 0, len(workerIDs))
	pipe = client.Pipeline()
	for _, id := range workerIDs {
		workerCmds = append(workerCmds, pipe.HGetAll(ctx, metricsWorkerKey(id)))
	}
	if len(workerCmds) > 0 {
		if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
			return contract.Stats{}, err
		}
	}

	byQueue := make(map[string]*contract.QueueStats)
	result := contract.Stats{}
	for _, cmd := range workerCmds {
		fields, err := cmd.Result()
		if err != nil || len(fields) == 0 {
			continue
		}
		queue := normalizeQueue(fields["queue"])
		item := byQueue[queue]
		if item == nil {
			item = &contract.QueueStats{Name: queue}
			byQueue[queue] = item
		}
		item.WorkersActive += parseInt64(fields["workers_active"])
		item.WorkersTotal += parseInt64(fields["workers_total"])
		item.Processed += parseUint64(fields["processed"])
		item.Succeeded += parseUint64(fields["succeeded"])
		item.Failed += parseUint64(fields["failed"])
		startedAt := time.UnixMilli(parseInt64(fields["started_at"])).UTC()
		if result.StartedAt.IsZero() || startedAt.Before(result.StartedAt) {
			result.StartedAt = startedAt
		}
	}

	queueNames := make([]string, 0, len(byQueue))
	for name := range byQueue {
		queueNames = append(queueNames, name)
	}
	sort.Strings(queueNames)
	pendingCmds := make(map[string]*redis.IntCmd, len(queueNames))
	pipe = client.Pipeline()
	for _, name := range queueNames {
		pendingCmds[name] = pipe.LLen(ctx, name)
	}
	if len(pendingCmds) > 0 {
		if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
			return contract.Stats{}, err
		}
	}

	result.Queues = make([]contract.QueueStats, 0, len(queueNames))
	for _, name := range queueNames {
		item := byQueue[name]
		if pending, err := pendingCmds[name].Result(); err == nil {
			item.Pending = pending
		}
		result.WorkersActive += item.WorkersActive
		result.WorkersTotal += item.WorkersTotal
		result.Processed += item.Processed
		result.Succeeded += item.Succeeded
		result.Failed += item.Failed
		result.Pending += item.Pending
		result.Queues = append(result.Queues, *item)
	}
	return result, nil
}

func (m *metricsRegistry) beginLocal(queue string) *localQueueMetrics {
	queue = normalizeQueue(queue)
	m.localMu.RLock()
	metrics := m.local[queue]
	m.localMu.RUnlock()
	if metrics == nil {
		m.localMu.Lock()
		metrics = m.local[queue]
		if metrics == nil {
			metrics = new(localQueueMetrics)
			m.local[queue] = metrics
		}
		m.localMu.Unlock()
	}
	metrics.active.Add(1)
	metrics.processed.Add(1)
	return metrics
}

func (m *metricsRegistry) finishLocal(metrics *localQueueMetrics, err error) {
	if metrics == nil {
		return
	}
	metrics.active.Add(-1)
	if err != nil {
		metrics.failed.Add(1)
		return
	}
	metrics.succeeded.Add(1)
}

func (m *metricsRegistry) localStats() contract.Stats {
	result := contract.Stats{StartedAt: m.startedAt}
	m.localMu.RLock()
	names := make([]string, 0, len(m.local))
	for name := range m.local {
		names = append(names, name)
	}
	sort.Strings(names)
	result.Queues = make([]contract.QueueStats, 0, len(names))
	for _, name := range names {
		metrics := m.local[name]
		item := contract.QueueStats{
			Name:          name,
			WorkersActive: metrics.active.Load(),
			Processed:     metrics.processed.Load(),
			Succeeded:     metrics.succeeded.Load(),
			Failed:        metrics.failed.Load(),
		}
		result.WorkersActive += item.WorkersActive
		result.Processed += item.Processed
		result.Succeeded += item.Succeeded
		result.Failed += item.Failed
		result.Queues = append(result.Queues, item)
	}
	m.localMu.RUnlock()
	return result
}

func (m *metricsRegistry) close() error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	workers := make([]*workerMetrics, 0, len(m.workers))
	for worker := range m.workers {
		workers = append(workers, worker)
	}
	m.mu.Unlock()
	for _, worker := range workers {
		worker.close()
	}

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	clients := make([]*redis.Client, 0, len(m.clients))
	for _, client := range m.clients {
		clients = append(clients, client)
	}
	m.clients = make(map[string]*redis.Client)
	m.mu.Unlock()

	var firstErr error
	for _, client := range clients {
		if err := client.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (w *workerMetrics) start() {
	if w == nil || !w.state.CompareAndSwap(0, 1) {
		return
	}
	w.publish()
	go func() {
		defer close(w.done)
		ticker := time.NewTicker(metricsHeartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				w.publish()
			case <-w.stop:
				w.remove()
				return
			}
		}
	}()
}

func (w *workerMetrics) close() {
	if w == nil {
		return
	}
	previous := w.state.Swap(2)
	if previous == 1 {
		close(w.stop)
		<-w.done
	}
	w.registry.unregister(w)
}

func (m *metricsRegistry) unregister(worker *workerMetrics) {
	if m == nil {
		return
	}
	m.mu.Lock()
	delete(m.workers, worker)
	m.mu.Unlock()
}

func (w *workerMetrics) begin() {
	if w == nil {
		return
	}
	w.active.Add(1)
	w.processed.Add(1)
}

func (w *workerMetrics) finish(err error) {
	if w == nil {
		return
	}
	w.active.Add(-1)
	if err != nil {
		w.failed.Add(1)
		return
	}
	w.succeeded.Add(1)
}

func (w *workerMetrics) publish() {
	client, err := w.registry.client(w.connection)
	if err != nil || client == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), metricsIOTimeout)
	defer cancel()
	expiresAt := time.Now().Add(metricsHeartbeatTTL)
	pipe := client.TxPipeline()
	pipe.HSet(ctx, metricsWorkerKey(w.id), map[string]any{
		"queue":          w.queue,
		"workers_active": w.active.Load(),
		"workers_total":  w.workers,
		"processed":      w.processed.Load(),
		"succeeded":      w.succeeded.Load(),
		"failed":         w.failed.Load(),
		"started_at":     w.startedAt.UnixMilli(),
	})
	pipe.Expire(ctx, metricsWorkerKey(w.id), metricsHeartbeatTTL)
	pipe.ZAdd(ctx, metricsWorkersKey(), redis.Z{Score: float64(expiresAt.UnixMilli()), Member: w.id})
	pipe.ZRemRangeByScore(ctx, metricsWorkersKey(), "-inf", strconv.FormatInt(time.Now().UnixMilli()-1, 10))
	if _, err := pipe.Exec(ctx); err != nil {
		w.registry.log.Debug("queue metrics heartbeat failed", "queue", w.queue, "error", err)
	}
}

func (w *workerMetrics) remove() {
	client, err := w.registry.client(w.connection)
	if err != nil || client == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), metricsIOTimeout)
	defer cancel()
	pipe := client.TxPipeline()
	pipe.Del(ctx, metricsWorkerKey(w.id))
	pipe.ZRem(ctx, metricsWorkersKey(), w.id)
	_, _ = pipe.Exec(ctx)
}

func metricsWorkersKey() string {
	return metricsPrefix + ":workers"
}

func metricsWorkerKey(id string) string {
	return metricsPrefix + ":worker:" + id
}

func normalizeQueue(queue string) string {
	if queue == "" {
		return "default"
	}
	return queue
}

func newMetricsID() string {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err == nil {
		return hex.EncodeToString(value)
	}
	return strconv.FormatInt(time.Now().UnixNano(), 36)
}

func parseInt64(value string) int64 {
	parsed, _ := strconv.ParseInt(value, 10, 64)
	return parsed
}

func parseUint64(value string) uint64 {
	parsed, _ := strconv.ParseUint(value, 10, 64)
	return parsed
}
