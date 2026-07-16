package go_queue

import (
	"sync"
	"time"

	"github.com/levskiy0/go-queue/contract"
	"golang.org/x/time/rate"
)

const minKeyedLimitersEntriesBeforeSweep = 1024

type limiterEntry struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

type keyedLimiters struct {
	mu        sync.Mutex
	entries   map[string]*limiterEntry
	limit     rate.Limit
	burst     int
	idleTTL   time.Duration
	lastSweep time.Time
}

func newKeyedLimiters(cfg *contract.RateLimit) *keyedLimiters {
	idleTTL := 10 * cfg.Per
	if idleTTL < time.Minute {
		idleTTL = time.Minute
	}

	return &keyedLimiters{
		entries:   make(map[string]*limiterEntry),
		limit:     rate.Limit(float64(cfg.Limit) / cfg.Per.Seconds()),
		burst:     cfg.Limit,
		idleTTL:   idleTTL,
		lastSweep: time.Now(),
	}
}

func (k *keyedLimiters) reserveDelay(key string) time.Duration {
	k.mu.Lock()
	defer k.mu.Unlock()

	now := time.Now()
	k.sweepLocked(now)

	entry, ok := k.entries[key]
	if !ok {
		entry = &limiterEntry{limiter: rate.NewLimiter(k.limit, k.burst)}
		k.entries[key] = entry
	}
	entry.lastSeen = now

	reservation := entry.limiter.ReserveN(now, 1)
	delay := reservation.DelayFrom(now)
	if delay > 0 {
		reservation.Cancel()
	}

	return delay
}

func (k *keyedLimiters) sweepLocked(now time.Time) {
	if len(k.entries) <= minKeyedLimitersEntriesBeforeSweep && now.Sub(k.lastSweep) <= k.idleTTL {
		return
	}

	for key, entry := range k.entries {
		if now.Sub(entry.lastSeen) > k.idleTTL {
			delete(k.entries, key)
		}
	}
	k.lastSweep = now
}
