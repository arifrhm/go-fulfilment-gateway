package outbox

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/arifrhm/go-fulfilment-gateway/internal/metrics"
)

var ErrQueueFull = errors.New("outbox queue is full (backpressure)")

// Event represents an asynchronous fulfilment domain event.
type Event struct {
	ID        string
	OrderID   string
	Payload   []byte
	Attempts  int
	CreatedAt time.Time
}

// Handler processes an outbox event. Returning an error triggers retry up to MaxRetries.
type Handler func(ctx context.Context, ev Event) error

// Dispatcher manages a bounded worker pool with exponential backoff retries and a DLQ.
type Dispatcher struct {
	queue      chan Event
	dlqMu      sync.Mutex
	dlq        []Event
	handler    Handler
	maxRetries int
	baseDelay  time.Duration
	metrics    *metrics.Registry
	logger     *slog.Logger
	wg         sync.WaitGroup
}

// NewDispatcher creates a bounded outbox worker pool.
func NewDispatcher(queueSize, workers, maxRetries int, baseDelay time.Duration, handler Handler, m *metrics.Registry, logger *slog.Logger) *Dispatcher {
	if logger == nil {
		logger = slog.Default()
	}
	d := &Dispatcher{
		queue:      make(chan Event, queueSize),
		handler:    handler,
		maxRetries: maxRetries,
		baseDelay:  baseDelay,
		metrics:    m,
		logger:     logger,
	}
	for i := 0; i < workers; i++ {
		d.wg.Add(1)
		go d.workerLoop(i)
	}
	return d
}

// Enqueue submits an event without blocking; returns ErrQueueFull if the buffer is saturated.
func (d *Dispatcher) Enqueue(ev Event) error {
	select {
	case d.queue <- ev:
		if d.metrics != nil {
			d.metrics.OutboxEnqueued.Add(1)
		}
		return nil
	default:
		return ErrQueueFull
	}
}

// Shutdown closes the queue and waits for all workers to drain remaining events.
func (d *Dispatcher) Shutdown(ctx context.Context) error {
	close(d.queue)
	done := make(chan struct{})
	go func() {
		d.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// DeadLetters returns a snapshot of events that exhausted retries.
func (d *Dispatcher) DeadLetters() []Event {
	d.dlqMu.Lock()
	defer d.dlqMu.Unlock()
	out := make([]Event, len(d.dlq))
	copy(out, d.dlq)
	return out
}

func (d *Dispatcher) workerLoop(workerID int) {
	defer d.wg.Done()
	for ev := range d.queue {
		d.processWithRetry(workerID, ev)
	}
}

func (d *Dispatcher) processWithRetry(workerID int, ev Event) {
	for attempt := 0; attempt <= d.maxRetries; attempt++ {
		ev.Attempts = attempt + 1
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := d.handler(ctx, ev)
		cancel()

		if err == nil {
			if d.metrics != nil {
				d.metrics.OutboxProcessed.Add(1)
			}
			return
		}

		if attempt < d.maxRetries {
			if d.metrics != nil {
				d.metrics.OutboxRetried.Add(1)
			}
			backoff := d.baseDelay * time.Duration(1<<attempt)
			time.Sleep(backoff)
			continue
		}

		d.dlqMu.Lock()
		d.dlq = append(d.dlq, ev)
		d.dlqMu.Unlock()
		if d.metrics != nil {
			d.metrics.OutboxDeadLettered.Add(1)
		}
		d.logger.Error("event moved to dead-letter queue",
			"worker_id", workerID,
			"event_id", ev.ID,
			"order_id", ev.OrderID,
			"attempts", ev.Attempts,
			"error", err,
		)
	}
}
