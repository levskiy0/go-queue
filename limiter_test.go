package go_queue

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/levskiy0/go-queue/contract"
	"golang.org/x/time/rate"
)

func TestKeyedLimitersReserveAndCancel(t *testing.T) {
	kl := newKeyedLimiters(&contract.RateLimit{Limit: 1, Per: time.Second})

	if delay := kl.reserveDelay("a"); delay != 0 {
		t.Fatalf("expected first reservation to be immediate, got delay %s", delay)
	}

	delay1 := kl.reserveDelay("a")
	if delay1 <= 0 {
		t.Fatalf("expected second reservation to be delayed, got %s", delay1)
	}

	delay2 := kl.reserveDelay("a")
	if delay2 <= 0 {
		t.Fatalf("expected third reservation to be delayed, got %s", delay2)
	}

	diff := delay1 - delay2
	if diff < 0 {
		diff = -diff
	}
	if diff > 100*time.Millisecond {
		t.Fatalf("expected cancelled reservations to not compound delay, delay1=%s delay2=%s", delay1, delay2)
	}
}

func TestKeyedLimitersPerKeyIsolation(t *testing.T) {
	kl := newKeyedLimiters(&contract.RateLimit{Limit: 1, Per: time.Second})

	if delay := kl.reserveDelay("a"); delay != 0 {
		t.Fatalf("expected key a first reservation immediate, got %s", delay)
	}

	if delay := kl.reserveDelay("b"); delay != 0 {
		t.Fatalf("expected key b to be unaffected by key a, got delay %s", delay)
	}
}

func TestKeyedLimitersSweepByIdleTTL(t *testing.T) {
	kl := &keyedLimiters{
		entries:   make(map[string]*limiterEntry),
		limit:     1,
		burst:     1,
		idleTTL:   50 * time.Millisecond,
		lastSweep: time.Now().Add(-time.Hour),
	}

	old := time.Now().Add(-time.Hour)
	for i := 0; i < 5; i++ {
		kl.entries[fmt.Sprintf("stale-%d", i)] = &limiterEntry{limiter: rate.NewLimiter(1, 1), lastSeen: old}
	}

	kl.reserveDelay("fresh")

	if _, ok := kl.entries["fresh"]; !ok {
		t.Fatal("expected freshly reserved key to remain")
	}
	if len(kl.entries) != 1 {
		t.Fatalf("expected stale entries to be evicted, got %d entries left", len(kl.entries))
	}
}

func TestKeyedLimitersSweepByEntryCount(t *testing.T) {
	kl := &keyedLimiters{
		entries:   make(map[string]*limiterEntry),
		limit:     1,
		burst:     1,
		idleTTL:   time.Hour,
		lastSweep: time.Now(),
	}

	now := time.Now()
	for i := 0; i < 1025; i++ {
		lastSeen := now
		if i%2 == 0 {
			lastSeen = now.Add(-2 * time.Hour)
		}
		kl.entries[fmt.Sprintf("k-%d", i)] = &limiterEntry{limiter: rate.NewLimiter(1, 1), lastSeen: lastSeen}
	}

	kl.reserveDelay("trigger")

	for key, entry := range kl.entries {
		if key == "trigger" {
			continue
		}
		if now.Sub(entry.lastSeen) > kl.idleTTL {
			t.Fatalf("expected stale entry %s to be evicted once sweep triggered by size", key)
		}
	}
}

func TestKeyedLimitersConcurrentAccess(t *testing.T) {
	kl := newKeyedLimiters(&contract.RateLimit{Limit: 100, Per: time.Second})

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()

			key := fmt.Sprintf("key-%d", n%5)
			kl.reserveDelay(key)
		}(i)
	}
	wg.Wait()
}
