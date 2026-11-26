package internal_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arifrhm/go-fulfilment-gateway/internal/circuitbreaker"
	"github.com/arifrhm/go-fulfilment-gateway/internal/coalesce"
	"github.com/arifrhm/go-fulfilment-gateway/internal/geodispatch"
	"github.com/arifrhm/go-fulfilment-gateway/internal/hedging"
)

func TestSingleflightCollapsesThunderingHerd(t *testing.T) {
	g := coalesce.NewGroup[string]()
	var backendCalls atomic.Int32
	const callers = 100

	var wg sync.WaitGroup
	wg.Add(callers)
	for i := 0; i < callers; i++ {
		go func() {
			defer wg.Done()
			val, _, err := g.Do(context.Background(), "hot-merchant-1", func(ctx context.Context) (string, error) {
				backendCalls.Add(1)
				time.Sleep(20 * time.Millisecond)
				return "merchant-menu-v1", nil
			})
			if err != nil || val != "merchant-menu-v1" {
				t.Errorf("unexpected result: val=%q err=%v", val, err)
			}
		}()
	}
	wg.Wait()

	if backendCalls.Load() != 1 {
		t.Fatalf("expected 1 backend call for 100 concurrent readers, got %d", backendCalls.Load())
	}
}

func TestHedgingCutsTailLatency(t *testing.T) {
	h := hedging.NewHedger(5 * time.Millisecond)
	start := time.Now()
	res, err := hedging.Execute(context.Background(), h, 2, func(ctx context.Context, replicaIdx int) (string, error) {
		if replicaIdx == 1 {
			// Simulate degraded primary experiencing GC/IO stall
			select {
			case <-time.After(150 * time.Millisecond):
				return "slow", nil
			case <-ctx.Done():
				return "", ctx.Err()
			}
		}
		return "fast", nil
	})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Value != "fast" || !res.Hedged {
		t.Fatalf("expected hedged fast response, got %+v", res)
	}
	if elapsed > 40*time.Millisecond {
		t.Fatalf("hedging took too long: %v", elapsed)
	}
}

func TestCircuitBreakerTripAndRecovery(t *testing.T) {
	cb := circuitbreaker.NewBreaker(3, 25*time.Millisecond)
	boom := errors.New("downstream 503")

	for i := 0; i < 3; i++ {
		_ = cb.Execute(func() error { return boom })
	}
	if cb.State() != circuitbreaker.StateOpen {
		t.Fatalf("expected circuit open after 3 failures, got %v", cb.State())
	}
	if err := cb.Execute(func() error { return nil }); !errors.Is(err, circuitbreaker.ErrCircuitOpen) {
		t.Fatalf("expected ErrCircuitOpen, got %v", err)
	}

	time.Sleep(30 * time.Millisecond)
	if err := cb.Execute(func() error { return nil }); err != nil {
		t.Fatalf("expected half-open recovery to succeed, got %v", err)
	}
	if cb.State() != circuitbreaker.StateClosed {
		t.Fatalf("expected circuit closed after recovery, got %v", cb.State())
	}
}

func TestGeoDispatchZeroDoubleAllocation(t *testing.T) {
	m := geodispatch.NewMatcher(64)
	const totalDrivers = 50
	for i := 0; i < totalDrivers; i++ {
		m.RegisterDriver(fmt.Sprintf("drv-%d", i), "h3-jkt-sudirman")
	}

	const orders = 200
	var wg sync.WaitGroup
	var mu sync.Mutex
	claimed := make(map[string]int)

	wg.Add(orders)
	for i := 0; i < orders; i++ {
		go func() {
			defer wg.Done()
			id, err := m.Allocate("h3-jkt-sudirman")
			if err == nil {
				mu.Lock()
				claimed[id]++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(claimed) != totalDrivers {
		t.Fatalf("expected %d unique drivers claimed, got %d", totalDrivers, len(claimed))
	}
	for id, count := range claimed {
		if count != 1 {
			t.Fatalf("double-dispatch detected for driver %s: claimed %d times", id, count)
		}
	}
}

func BenchmarkGeoDispatchSharded(b *testing.B) {
	m := geodispatch.NewMatcher(64)
	for i := 0; i < 64; i++ {
		cell := fmt.Sprintf("cell-%d", i)
		for d := 0; d < 10; d++ {
			m.RegisterDriver(fmt.Sprintf("drv-%d-%d", i, d), cell)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			cell := fmt.Sprintf("cell-%d", i%64)
			if id, err := m.Allocate(cell); err == nil {
				m.Release(cell, id)
			}
			i++
		}
	})
}
