package go_queue

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/levskiy0/go-queue/contract"
)

type countingJob struct {
	mu        sync.Mutex
	calls     int
	failUntil int
	err       error
}

func (j *countingJob) Signature() string { return "counting_job" }

func (j *countingJob) Handle(args ...any) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	j.calls++
	if j.calls <= j.failUntil {
		if j.err != nil {
			return j.err
		}

		return errors.New("transient failure")
	}

	return nil
}

func (j *countingJob) Calls() int {
	j.mu.Lock()
	defer j.mu.Unlock()

	return j.calls
}

type alwaysFailJob struct {
	mu    sync.Mutex
	calls int
}

func (j *alwaysFailJob) Signature() string { return "always_fail_job" }

func (j *alwaysFailJob) Handle(args ...any) error {
	j.mu.Lock()
	j.calls++
	j.mu.Unlock()

	return errors.New("permanent failure")
}

func (j *alwaysFailJob) Calls() int {
	j.mu.Lock()
	defer j.mu.Unlock()

	return j.calls
}

type noRetryJob struct {
	mu    sync.Mutex
	calls int
}

func (j *noRetryJob) Signature() string { return "no_retry_job" }

func (j *noRetryJob) Handle(args ...any) error {
	j.mu.Lock()
	j.calls++
	j.mu.Unlock()

	return errors.New("do not retry me")
}

func (j *noRetryJob) NoRetry(err error) bool { return true }

func (j *noRetryJob) Calls() int {
	j.mu.Lock()
	defer j.mu.Unlock()

	return j.calls
}

type noRetrySucceedsEventuallyJob struct {
	mu    sync.Mutex
	calls int
}

func (j *noRetrySucceedsEventuallyJob) Signature() string { return "no_retry_rate_limited_job" }

func (j *noRetrySucceedsEventuallyJob) Handle(args ...any) error {
	j.mu.Lock()
	j.calls++
	j.mu.Unlock()

	return nil
}

func (j *noRetrySucceedsEventuallyJob) NoRetry(err error) bool { return true }

func (j *noRetrySucceedsEventuallyJob) Calls() int {
	j.mu.Lock()
	defer j.mu.Unlock()

	return j.calls
}

func newMiniredisConnections(t *testing.T) *Connections {
	t.Helper()

	srv := miniredis.RunT(t)

	conns := NewConnections()
	conns.Add("default", &Connection{Driver: DriverSync})
	conns.Add("redis", &Connection{Driver: DriverRedis, Redis: &RedisConfig{
		Host: srv.Host(),
		Port: srv.Port(),
	}})

	return conns
}

func runRetryWorker(t *testing.T, ctx context.Context, q *Queue, args contract.Args) {
	t.Helper()

	go func() {
		if err := q.Worker(args).Run(); err != nil {
			select {
			case <-ctx.Done():
			default:
				t.Error(err)
			}
		}
	}()
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) bool {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}

	return cond()
}

func TestRetriesEventuallySucceeds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	conns := newMiniredisConnections(t)
	q := NewQueue(conns, slog.Default(), true)

	job := &countingJob{failUntil: 2}
	q.Register([]contract.Job{job})

	runRetryWorker(t, ctx, q, contract.Args{Connection: "redis", Queue: "retries_succeed", Concurrent: 1})
	time.Sleep(300 * time.Millisecond)

	err := q.Job(job, nil).OnConnection("redis").OnQueue("retries_succeed").Retries(2).Dispatch()
	if err != nil {
		t.Fatalf("dispatch failed: %v", err)
	}

	if !waitFor(t, 12*time.Second, func() bool { return job.Calls() == 3 }) {
		t.Fatalf("expected 3 executions, got %d", job.Calls())
	}

	time.Sleep(3 * time.Second)
	if job.Calls() != 3 {
		t.Fatalf("expected exactly 3 executions after success, got %d", job.Calls())
	}
}

func TestRetriesExhausted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()

	conns := newMiniredisConnections(t)
	q := NewQueue(conns, slog.Default(), true)

	job := &alwaysFailJob{}
	q.Register([]contract.Job{job})

	runRetryWorker(t, ctx, q, contract.Args{Connection: "redis", Queue: "retries_exhausted", Concurrent: 1})
	time.Sleep(300 * time.Millisecond)

	err := q.Job(job, nil).OnConnection("redis").OnQueue("retries_exhausted").Retries(1).Dispatch()
	if err != nil {
		t.Fatalf("dispatch failed: %v", err)
	}

	if !waitFor(t, 6*time.Second, func() bool { return job.Calls() == 2 }) {
		t.Fatalf("expected 2 executions, got %d", job.Calls())
	}

	time.Sleep(3 * time.Second)
	if job.Calls() != 2 {
		t.Fatalf("expected exactly 2 executions (retry budget exhausted), got %d", job.Calls())
	}
}

func TestNoRetrySwallowsError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conns := newMiniredisConnections(t)
	q := NewQueue(conns, slog.Default(), true)

	job := &noRetryJob{}
	q.Register([]contract.Job{job})

	runRetryWorker(t, ctx, q, contract.Args{Connection: "redis", Queue: "no_retry", Concurrent: 1})
	time.Sleep(300 * time.Millisecond)

	err := q.Job(job, nil).OnConnection("redis").OnQueue("no_retry").Retries(3).Dispatch()
	if err != nil {
		t.Fatalf("dispatch failed: %v", err)
	}

	if !waitFor(t, 3*time.Second, func() bool { return job.Calls() == 1 }) {
		t.Fatalf("expected exactly 1 execution, got %d", job.Calls())
	}

	time.Sleep(2 * time.Second)
	if job.Calls() != 1 {
		t.Fatalf("expected no retry to happen, got %d executions", job.Calls())
	}
}

func TestNoRetryDoesNotAffectRetryLater(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()

	conns := newMiniredisConnections(t)
	q := NewQueue(conns, slog.Default(), true)

	job := &noRetrySucceedsEventuallyJob{}
	q.Register([]contract.Job{job})

	runRetryWorker(t, ctx, q, contract.Args{
		Connection: "redis",
		Queue:      "no_retry_rate_limited",
		Concurrent: 1,
		RateLimit: &contract.RateLimit{
			Limit: 1,
			Per:   2 * time.Second,
		},
	})
	time.Sleep(300 * time.Millisecond)

	for i := 0; i < 2; i++ {
		if err := q.Job(job, nil).OnConnection("redis").OnQueue("no_retry_rate_limited").Dispatch(); err != nil {
			t.Fatalf("dispatch %d failed: %v", i, err)
		}
	}

	if !waitFor(t, 8*time.Second, func() bool { return job.Calls() == 2 }) {
		t.Fatalf("expected both rate-limited jobs to eventually execute, got %d", job.Calls())
	}
}

func TestSyncIgnoresRetriesAndRateLimit(t *testing.T) {
	conns := NewConnections()
	conns.Add("default", &Connection{Driver: DriverSync})

	q := NewQueue(conns, nil, true)

	job := &alwaysFailJob{}

	err := q.Job(job, nil).Retries(3).Dispatch()
	if err == nil {
		t.Fatal("expected sync dispatch to propagate the job error")
	}

	if job.Calls() != 1 {
		t.Fatalf("expected exactly 1 execution on sync driver, got %d", job.Calls())
	}
}
