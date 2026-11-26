package metrics

import (
	"fmt"
	"io"
	"sync/atomic"
	"time"
)

// Registry holds lock-free atomic counters for Prometheus exposition.
type Registry struct {
	RequestsTotal      atomic.Uint64
	IdempotencyHits    atomic.Uint64
	RateLimitedTotal   atomic.Uint64
	OutboxEnqueued     atomic.Uint64
	OutboxProcessed    atomic.Uint64
	OutboxRetried      atomic.Uint64
	OutboxDeadLettered atomic.Uint64
	LatencySumMicros   atomic.Uint64
	InFlightRequests   atomic.Int64
}

// ObserveLatency records request latency in microseconds.
func (r *Registry) ObserveLatency(d time.Duration) {
	r.LatencySumMicros.Add(uint64(d.Microseconds()))
}

// WritePrometheus writes metrics in Prometheus text exposition format.
func (r *Registry) WritePrometheus(w io.Writer) {
	fmt.Fprintf(w, "# TYPE gateway_requests_total counter\ngateway_requests_total %d\n", r.RequestsTotal.Load())
	fmt.Fprintf(w, "# TYPE gateway_idempotency_hits_total counter\ngateway_idempotency_hits_total %d\n", r.IdempotencyHits.Load())
	fmt.Fprintf(w, "# TYPE gateway_rate_limited_total counter\ngateway_rate_limited_total %d\n", r.RateLimitedTotal.Load())
	fmt.Fprintf(w, "# TYPE gateway_in_flight_requests gauge\ngateway_in_flight_requests %d\n", r.InFlightRequests.Load())
	fmt.Fprintf(w, "# TYPE gateway_request_latency_micros_sum counter\ngateway_request_latency_micros_sum %d\n", r.LatencySumMicros.Load())
	fmt.Fprintf(w, "# TYPE outbox_enqueued_total counter\noutbox_enqueued_total %d\n", r.OutboxEnqueued.Load())
	fmt.Fprintf(w, "# TYPE outbox_processed_total counter\noutbox_processed_total %d\n", r.OutboxProcessed.Load())
	fmt.Fprintf(w, "# TYPE outbox_retried_total counter\noutbox_retried_total %d\n", r.OutboxRetried.Load())
	fmt.Fprintf(w, "# TYPE outbox_dlq_total counter\noutbox_dlq_total %d\n", r.OutboxDeadLettered.Load())
}
