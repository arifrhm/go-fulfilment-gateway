package coalesce

import (
	"context"
	"sync"
)

type call[T any] struct {
	wg  sync.WaitGroup
	val T
	err error
}

// Group collapses duplicate concurrent calls for the same key into a single execution
// to protect databases and caches from thundering-herd traffic spikes.
type Group[T any] struct {
	mu    sync.Mutex
	calls map[string]*call[T]
}

// NewGroup creates a new generic singleflight Group.
func NewGroup[T any]() *Group[T] {
	return &Group[T]{
		calls: make(map[string]*call[T]),
	}
}

// Do executes fn once for all concurrent callers sharing the same key.
// Each caller respects its own ctx deadline while waiting for the shared result.
func (g *Group[T]) Do(ctx context.Context, key string, fn func(context.Context) (T, error)) (T, bool, error) {
	g.mu.Lock()
	if c, ok := g.calls[key]; ok {
		g.mu.Unlock()
		done := make(chan struct{})
		go func() {
			c.wg.Wait()
			close(done)
		}()
		select {
		case <-ctx.Done():
			var zero T
			return zero, true, ctx.Err()
		case <-done:
			return c.val, true, c.err
		}
	}

	c := &call[T]{}
	c.wg.Add(1)
	g.calls[key] = c
	g.mu.Unlock()

	defer func() {
		g.mu.Lock()
		delete(g.calls, key)
		g.mu.Unlock()
		c.wg.Done()
	}()

	c.val, c.err = fn(ctx)
	return c.val, false, c.err
}
