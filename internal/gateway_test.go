package internal_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arifrhm/go-fulfilment-gateway/internal/idempotency"
	"github.com/arifrhm/go-fulfilment-gateway/internal/metrics"
	"github.com/arifrhm/go-fulfilment-gateway/internal/outbox"
	"github.com/arifrhm/go-fulfilment-gateway/internal/ratelimit"
)

func TestIdempotencyConcurrentDeduplication(t *testing.T) {
	store := idempotency.NewStore(5 * time.Minute)
	const workers = 32
	var wg sync.WaitGroup
	var acquiredCount atomic.Int32
	var inFlightConflicts atomic.Int32

	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			_, acq, err := store.Acquire("order-key-123")
			if err != nil {
				if errors.Is(err, idempotency.ErrInFlight) {
					inFlightConflicts.Add(1)
				}
				return
			}
			if acq {
				acquiredCount.Add(1)
				time.Sleep(15 * time.Millisecond)
				store.Complete("order-key-123", 202, []byte(`{"status":"ACCEPTED"}`))
			}
		}()
	}
	wg.Wait()

	if acquiredCount.Load() != 1 {
		t.Fatalf("expected exactly 1 acquisition, got %d", acquiredCount.Load())
	}

	rec, acq, err := store.Acquire("order-key-123")
	if err != nil || acq || rec == nil || rec.StatusCode != 202 {
		t.Fatalf("expected cached 202 replay, got rec=%v acq=%v err=%v", rec, acq, err)
	}
}

func TestRateLimiterBurstAndThrottle(t *testing.T) {
	lim := ratelimit.NewLimiter(10, 5)
	allowed := 0
	for i := 0; i < 10; i++ {
		if lim.Allow("merchant-a") {
			allowed++
		}
	}
	if allowed != 5 {
		t.Fatalf("expected 5 burst tokens allowed, got %d", allowed)
	}
}

func TestOutboxRetryAndDLQ(t *testing.T) {
	reg := &metrics.Registry{}
	var attempts atomic.Int32

	disp := outbox.NewDispatcher(16, 2, 2, 2*time.Millisecond, func(ctx context.Context, ev outbox.Event) error {
		attempts.Add(1)
		if ev.OrderID == "fail-order" {
			return errors.New("downstream timeout")
		}
		return nil
	}, reg, nil)

	if err := disp.Enqueue(outbox.Event{ID: "1", OrderID: "ok-order"}); err != nil {
		t.Fatalf("unexpected enqueue error: %v", err)
	}
	if err := disp.Enqueue(outbox.Event{ID: "2", OrderID: "fail-order"}); err != nil {
		t.Fatalf("unexpected enqueue error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := disp.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown error: %v", err)
	}

	if reg.OutboxProcessed.Load() != 1 {
		t.Fatalf("expected 1 processed event, got %d", reg.OutboxProcessed.Load())
	}
	if reg.OutboxDeadLettered.Load() != 1 {
		t.Fatalf("expected 1 DLQ event, got %d", reg.OutboxDeadLettered.Load())
	}
	if len(disp.DeadLetters()) != 1 || disp.DeadLetters()[0].OrderID != "fail-order" {
		t.Fatalf("unexpected DLQ contents: %+v", disp.DeadLetters())
	}
}

func BenchmarkIdempotencyAcquireAndComplete(b *testing.B) {
	store := idempotency.NewStore(10 * time.Minute)
	payload := []byte(`{"order_id":"ord-1","status":"ACCEPTED"}`)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i%1024)
		_, acq, _ := store.Acquire(key)
		if acq {
			store.Complete(key, 202, payload)
		}
	}
}

func BenchmarkRateLimiterAllow(b *testing.B) {
	lim := ratelimit.NewLimiter(1_000_000, 1_000_000)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			lim.Allow("bench-client")
		}
	})
}
