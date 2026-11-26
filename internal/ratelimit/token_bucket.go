package ratelimit

import (
	"sync"
	"time"
)

type bucket struct {
	tokens     float64
	lastRefill time.Time
}

// Limiter implements a thread-safe per-key token bucket rate limiter.
type Limiter struct {
	mu         sync.Mutex
	ratePerSec float64
	burst      float64
	buckets    map[string]*bucket
	nowFn      func() time.Time
}

// NewLimiter creates a token bucket limiter with the specified refill rate and burst capacity.
func NewLimiter(ratePerSec, burst float64) *Limiter {
	return &Limiter{
		ratePerSec: ratePerSec,
		burst:      burst,
		buckets:    make(map[string]*bucket),
		nowFn:      time.Now,
	}
}

// Allow consumes 1 token for the given key if available.
func (l *Limiter) Allow(key string) bool {
	now := l.nowFn()
	l.mu.Lock()
	defer l.mu.Unlock()

	b, ok := l.buckets[key]
	if !ok {
		l.buckets[key] = &bucket{
			tokens:     l.burst - 1,
			lastRefill: now,
		}
		return l.burst >= 1
	}

	elapsed := now.Sub(b.lastRefill).Seconds()
	if elapsed > 0 {
		b.tokens += elapsed * l.ratePerSec
		if b.tokens > l.burst {
			b.tokens = l.burst
		}
		b.lastRefill = now
	}

	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// EvictStale removes idle buckets older than maxIdle to prevent unbounded map growth.
func (l *Limiter) EvictStale(maxIdle time.Duration) int {
	now := l.nowFn()
	l.mu.Lock()
	defer l.mu.Unlock()
	removed := 0
	for k, b := range l.buckets {
		if now.Sub(b.lastRefill) > maxIdle {
			delete(l.buckets, k)
			removed++
		}
	}
	return removed
}
