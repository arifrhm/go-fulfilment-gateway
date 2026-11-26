package hedging

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
)

var ErrNoUpstreams = errors.New("no upstreams configured")

// UpstreamFunc represents a call to a specific replica index.
type UpstreamFunc[T any] func(ctx context.Context, replicaIdx int) (T, error)

// Result wraps the winning hedged response and whether hedging was triggered.
type Result[T any] struct {
	Value      T
	ReplicaIdx int
	Hedged     bool
}

// Hedger mitigates tail-latency amplification (P99/P999) by issuing a backup request
// to a secondary replica if the primary does not respond within hedgeDelay.
type Hedger struct {
	hedgeDelay time.Duration
	rrCounter  atomic.Uint64
	HedgedSent atomic.Uint64
}

// NewHedger creates a tail-latency hedger with the given delay threshold.
func NewHedger(hedgeDelay time.Duration) *Hedger {
	return &Hedger{hedgeDelay: hedgeDelay}
}

// Execute runs fn against a primary replica and, if hedgeDelay elapses without a response,
// races a backup request against a different replica, cancelling the slower call.
func Execute[T any](ctx context.Context, h *Hedger, replicaCount int, fn UpstreamFunc[T]) (Result[T], error) {
	if replicaCount <= 0 {
		var zero Result[T]
		return zero, ErrNoUpstreams
	}
	if replicaCount == 1 {
		val, err := fn(ctx, 0)
		return Result[T]{Value: val, ReplicaIdx: 0}, err
	}

	primary := int(h.rrCounter.Add(1) % uint64(replicaCount))
	secondary := (primary + 1) % replicaCount

	type outcome struct {
		res Result[T]
		err error
	}
	ch := make(chan outcome, 2)

	callCtx, cancelAll := context.WithCancel(ctx)
	defer cancelAll()

	go func() {
		val, err := fn(callCtx, primary)
		ch <- outcome{res: Result[T]{Value: val, ReplicaIdx: primary, Hedged: false}, err: err}
	}()

	timer := time.NewTimer(h.hedgeDelay)
	defer timer.Stop()

	select {
	case out := <-ch:
		return out.res, out.err
	case <-ctx.Done():
		var zero Result[T]
		return zero, ctx.Err()
	case <-timer.C:
		h.HedgedSent.Add(1)
		go func() {
			val, err := fn(callCtx, secondary)
			ch <- outcome{res: Result[T]{Value: val, ReplicaIdx: secondary, Hedged: true}, err: err}
		}()
	}

	var firstErr error
	for i := 0; i < 2; i++ {
		select {
		case out := <-ch:
			if out.err == nil {
				return out.res, nil
			}
			firstErr = out.err
		case <-ctx.Done():
			var zero Result[T]
			return zero, ctx.Err()
		}
	}
	var zero Result[T]
	return zero, firstErr
}
