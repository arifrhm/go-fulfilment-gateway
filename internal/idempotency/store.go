package idempotency

import (
	"errors"
	"sync"
	"time"
)

var (
	// ErrInFlight indicates another goroutine is currently executing the same idempotency key.
	ErrInFlight = errors.New("request with this idempotency key is already in flight")
	// ErrEmptyKey indicates a missing idempotency key.
	ErrEmptyKey = errors.New("idempotency key is required")
)

// Record holds the cached response for a completed idempotent operation.
type Record struct {
	StatusCode int
	Body       []byte
	CreatedAt  time.Time
	InFlight   bool
}

// Store provides thread-safe idempotency key deduplication with TTL eviction.
type Store struct {
	mu      sync.Mutex
	records map[string]Record
	ttl     time.Duration
	nowFn   func() time.Time
}

// NewStore creates a new idempotency store with the given TTL.
func NewStore(ttl time.Duration) *Store {
	return &Store{
		records: make(map[string]Record),
		ttl:     ttl,
		nowFn:   time.Now,
	}
}

// Acquire checks if a key already exists or is in-flight.
// If not present (or expired), it marks the key as in-flight and returns (nil, true, nil).
// If completed, it returns the cached Record and (record, false, nil).
func (s *Store) Acquire(key string) (*Record, bool, error) {
	if key == "" {
		return nil, false, ErrEmptyKey
	}

	now := s.nowFn()
	s.mu.Lock()
	defer s.mu.Unlock()

	if rec, ok := s.records[key]; ok {
		if now.Sub(rec.CreatedAt) <= s.ttl {
			if rec.InFlight {
				return nil, false, ErrInFlight
			}
			cp := rec
			cp.Body = append([]byte(nil), rec.Body...)
			return &cp, false, nil
		}
		delete(s.records, key)
	}

	s.records[key] = Record{
		CreatedAt: now,
		InFlight:  true,
	}
	return nil, true, nil
}

// Complete stores the final response for an in-flight idempotency key.
func (s *Store) Complete(key string, statusCode int, body []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records[key] = Record{
		StatusCode: statusCode,
		Body:       append([]byte(nil), body...),
		CreatedAt:  s.nowFn(),
		InFlight:   false,
	}
}

// Release removes an in-flight key when processing fails before a deterministic outcome.
func (s *Store) Release(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if rec, ok := s.records[key]; ok && rec.InFlight {
		delete(s.records, key)
	}
}

// SweepExpired removes expired records to bound memory growth.
func (s *Store) SweepExpired() int {
	now := s.nowFn()
	s.mu.Lock()
	defer s.mu.Unlock()
	removed := 0
	for k, rec := range s.records {
		if !rec.InFlight && now.Sub(rec.CreatedAt) > s.ttl {
			delete(s.records, k)
			removed++
		}
	}
	return removed
}
