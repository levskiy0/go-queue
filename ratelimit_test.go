package go_queue

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/levskiy0/go-queue/contract"
)

type pacingJob struct {
	mu    sync.Mutex
	calls int
}

func (j *pacingJob) Signature() string { return "pacing_job" }

func (j *pacingJob) Handle(args ...any) error {
	j.mu.Lock()
	j.calls++
	j.mu.Unlock()

	return nil
}

func (j *pacingJob) Calls() int {
	j.mu.Lock()
	defer j.mu.Unlock()

	return j.calls
}

type keyedPacingJob struct {
	mu    sync.Mutex
	calls map[string]int
}

func newKeyedPacingJob() *keyedPacingJob {
	return &keyedPacingJob{calls: make(map[string]int)}
}

func (j *keyedPacingJob) Signature() string { return "keyed_pacing_job" }

func (j *keyedPacingJob) Handle(args ...any) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	key, _ := args[0].(string)
	j.calls[key]++

	return nil
}

func (j *keyedPacingJob) Total() int {
	j.mu.Lock()
	defer j.mu.Unlock()

	total := 0
	for _, calls := range j.calls {
		total += calls
	}

	return total
}

type onceSucceedsJob struct {
	mu    sync.Mutex
	calls int
}

func (j *onceSucceedsJob) Signature() string { return "burn_budget_job" }

func (j *onceSucceedsJob) Handle(args ...any) error {
	j.mu.Lock()
	j.calls++
	j.mu.Unlock()

	return nil
}

func (j *onceSucceedsJob) Calls() int {
	j.mu.Lock()
	defer j.mu.Unlock()

	return j.calls
}

func TestRateLimitPacing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	conns := newMiniredisConnections(t)
	q := NewQueue(conns, slog.Default(), true)

	job := &pacingJob{}
	q.Register([]contract.Job{job})

	runRetryWorker(t, ctx, q, contract.Args{
		Connection: "redis",
		Queue:      "rate_pacing",
		Concurrent: 2,
		RateLimit:  &contract.RateLimit{Limit: 2, Per: time.Second},
	})
	time.Sleep(300 * time.Millisecond)

	start := time.Now()
	for i := 0; i < 6; i++ {
		if err := q.Job(job, nil).OnConnection("redis").OnQueue("rate_pacing").Dispatch(); err != nil {
			t.Fatalf("dispatch %d failed: %v", i, err)
		}
	}

	if !waitFor(t, 12*time.Second, func() bool { return job.Calls() == 6 }) {
		t.Fatalf("expected 6 executions, got %d", job.Calls())
	}
	elapsed := time.Since(start)

	if elapsed < 1500*time.Millisecond {
		t.Fatalf("expected rate limiting to pace execution over at least 1.5s, took %s", elapsed)
	}

	time.Sleep(500 * time.Millisecond)
	if job.Calls() != 6 {
		t.Fatalf("expected exactly 6 executions, got %d", job.Calls())
	}
}

func TestRateLimitPerKey(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	conns := newMiniredisConnections(t)
	q := NewQueue(conns, slog.Default(), true)

	job := newKeyedPacingJob()
	q.Register([]contract.Job{job})

	runRetryWorker(t, ctx, q, contract.Args{
		Connection: "redis",
		Queue:      "rate_per_key",
		Concurrent: 2,
		RateLimit: &contract.RateLimit{
			Limit: 1,
			Per:   time.Second,
			Key: func(args []any) string {
				if len(args) == 0 {
					return ""
				}
				key, _ := args[0].(string)

				return key
			},
		},
	})
	time.Sleep(300 * time.Millisecond)

	start := time.Now()
	for _, key := range []string{"a", "b"} {
		for i := 0; i < 3; i++ {
			err := q.Job(job, []contract.Arg{{Type: "string", Value: key}}).
				OnConnection("redis").OnQueue("rate_per_key").Dispatch()
			if err != nil {
				t.Fatalf("dispatch key=%s failed: %v", key, err)
			}
		}
	}

	if !waitFor(t, 12*time.Second, func() bool { return job.Total() == 6 }) {
		t.Fatalf("expected 6 executions across both keys, got %d", job.Total())
	}
	elapsed := time.Since(start)

	if elapsed > 4*time.Second {
		t.Fatalf("expected independent keys to not block each other, took %s", elapsed)
	}
}

func TestRateLimitDoesNotBurnRetryBudget(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()

	conns := newMiniredisConnections(t)
	q := NewQueue(conns, slog.Default(), true)

	job := &onceSucceedsJob{}
	q.Register([]contract.Job{job})

	runRetryWorker(t, ctx, q, contract.Args{
		Connection: "redis",
		Queue:      "rate_no_burn",
		Concurrent: 1,
		RateLimit:  &contract.RateLimit{Limit: 1, Per: 2 * time.Second},
	})
	time.Sleep(300 * time.Millisecond)

	if err := q.Job(job, nil).OnConnection("redis").OnQueue("rate_no_burn").Dispatch(); err != nil {
		t.Fatalf("decoy dispatch failed: %v", err)
	}
	if err := q.Job(job, nil).OnConnection("redis").OnQueue("rate_no_burn").Retries(0).Dispatch(); err != nil {
		t.Fatalf("dispatch failed: %v", err)
	}

	if !waitFor(t, 8*time.Second, func() bool { return job.Calls() == 2 }) {
		t.Fatalf("expected both jobs to eventually succeed despite zero retry budget, got %d", job.Calls())
	}
}

func TestRateLimitZeroConfigDisablesLimiting(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conns := newMiniredisConnections(t)
	q := NewQueue(conns, slog.Default(), true)

	job := &pacingJob{}
	q.Register([]contract.Job{job})

	runRetryWorker(t, ctx, q, contract.Args{
		Connection: "redis",
		Queue:      "rate_zero_config",
		Concurrent: 1,
		RateLimit:  &contract.RateLimit{Limit: 0, Per: time.Second},
	})
	time.Sleep(300 * time.Millisecond)

	if err := q.Job(job, nil).OnConnection("redis").OnQueue("rate_zero_config").Dispatch(); err != nil {
		t.Fatalf("dispatch failed: %v", err)
	}

	if !waitFor(t, 2*time.Second, func() bool { return job.Calls() == 1 }) {
		t.Fatalf("expected job to run immediately when RateLimit is misconfigured, got %d executions", job.Calls())
	}
}
