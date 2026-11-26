# go-fulfilment-gateway

A high-throughput, resilient Go order, geo-dispatch, and service-platform gateway built with zero external dependencies, engineered to solve real-world production failure modes at **Grab (Fulfilment / Business Platform)** and **ByteDance (Service Platform / Cloud Infrastructure)** scale:

- **Sharded Geo-Cell Driver Matcher (`internal/geodispatch`):** FNV-32a cell-partitioned in-memory allocation engine eliminating global lock contention during rush-hour surges with **zero double-dispatch** (`53.78 ns/op`, `8 B/op`).
- **Generic Singleflight Request Coalescing (`internal/coalesce`):** Collapses thundering-herd hot-key reads (e.g., flash-sale merchant menus) from 100+ concurrent callers into **1 backend execution** with per-caller `context.Context` deadlines.
- **Tail-at-Scale Hedged Requests (`internal/hedging`):** Mitigates P99/P999 tail-latency amplification across fan-out microservices by racing a backup request to a secondary replica after `hedgeDelay` and cancelling the slower call.
- **Adaptive Circuit Breaker (`internal/circuitbreaker`):** Closed/Open/Half-Open state machine preventing cascading failures and retry storms.
- **Idempotency-Key Deduplication (`internal/idempotency`):** Thread-safe in-flight locking (`409 Conflict`) and deterministic replay (`X-Idempotent-Replay: true`) with TTL eviction (`86.99 ns/op`).
- **Per-Client Token Bucket Rate Limiter (`internal/ratelimit`):** Zero-allocation parallel allow check (`151.7 ns/op`, `0 B/op`, `0 allocs/op`) with stale-bucket eviction.
- **Bounded Outbox Worker Pool + DLQ (`internal/outbox`):** Non-blocking enqueue with explicit backpressure (`503`), exponential backoff retries, Dead-Letter Queue, and graceful drain.
- **Kubernetes & Observability Ready:** `/healthz`, `/readyz` (flips to `503` on `SIGTERM` before HTTP/worker drain), Prometheus `/metrics`, `net/http/pprof`, and `slog` JSON logging.

## Quick Start & Verification

```bash
# Run all unit & concurrency race tests (7/7 PASS)
go test -race -v ./...

# Run benchmarks
go test -bench=. -benchmem ./...

# Build & run gateway
go run ./cmd/gateway
```
