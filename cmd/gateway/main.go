package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/arifrhm/go-fulfilment-gateway/internal/idempotency"
	"github.com/arifrhm/go-fulfilment-gateway/internal/metrics"
	"github.com/arifrhm/go-fulfilment-gateway/internal/outbox"
	"github.com/arifrhm/go-fulfilment-gateway/internal/ratelimit"
)

type OrderRequest struct {
	OrderID    string `json:"order_id"`
	MerchantID string `json:"merchant_id"`
	AmountIDR  int64  `json:"amount_idr"`
}

type OrderResponse struct {
	OrderID string `json:"order_id"`
	Status  string `json:"status"`
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	reg := &metrics.Registry{}
	idem := idempotency.NewStore(10 * time.Minute)
	limiter := ratelimit.NewLimiter(500, 1000)

	var ready atomic.Bool
	ready.Store(true)

	dispatcher := outbox.NewDispatcher(4096, 8, 3, 25*time.Millisecond, func(ctx context.Context, ev outbox.Event) error {
		// Simulated downstream fulfilment dispatch
		return nil
	}, reg, logger)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if !ready.Load() {
			http.Error(w, "draining", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		reg.WritePrometheus(w)
	})
	mux.HandleFunc("POST /v1/orders", newOrderHandler(idem, limiter, dispatcher, reg, logger))

	srv := &http.Server{
		Addr:              ":8080",
		Handler:           mux,
		ReadHeaderTimeout: 3 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		logger.Info("fulfilment gateway listening", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	sig := <-sigCh

	logger.Info("shutdown signal received, draining connections", "signal", sig.String())
	ready.Store(false)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		logger.Error("http shutdown error", "error", err)
	}
	if err := dispatcher.Shutdown(ctx); err != nil {
		logger.Error("outbox drain error", "error", err)
	}
	logger.Info("graceful shutdown complete")
}

func newOrderHandler(
	idem *idempotency.Store,
	limiter *ratelimit.Limiter,
	dispatcher *outbox.Dispatcher,
	reg *metrics.Registry,
	logger *slog.Logger,
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		reg.RequestsTotal.Add(1)
		reg.InFlightRequests.Add(1)
		defer func() {
			reg.InFlightRequests.Add(-1)
			reg.ObserveLatency(time.Since(start))
		}()

		clientKey := r.Header.Get("X-Client-ID")
		if clientKey == "" {
			clientKey = r.RemoteAddr
		}
		if !limiter.Allow(clientKey) {
			reg.RateLimitedTotal.Add(1)
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}

		idemKey := r.Header.Get("Idempotency-Key")
		cached, acquired, err := idem.Acquire(idemKey)
		if err != nil {
			status := http.StatusBadRequest
			if errors.Is(err, idempotency.ErrInFlight) {
				status = http.StatusConflict
			}
			http.Error(w, err.Error(), status)
			return
		}
		if !acquired && cached != nil {
			reg.IdempotencyHits.Add(1)
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Idempotent-Replay", "true")
			w.WriteHeader(cached.StatusCode)
			_, _ = w.Write(cached.Body)
			return
		}

		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			idem.Release(idemKey)
			http.Error(w, "failed to read body", http.StatusBadRequest)
			return
		}
		var req OrderRequest
		if err := json.Unmarshal(body, &req); err != nil || req.OrderID == "" || req.AmountIDR <= 0 {
			idem.Release(idemKey)
			http.Error(w, "invalid order payload", http.StatusBadRequest)
			return
		}

		ev := outbox.Event{
			ID:        fmt.Sprintf("evt-%s", req.OrderID),
			OrderID:   req.OrderID,
			Payload:   body,
			CreatedAt: time.Now(),
		}
		if err := dispatcher.Enqueue(ev); err != nil {
			idem.Release(idemKey)
			http.Error(w, "service overloaded, retry later", http.StatusServiceUnavailable)
			return
		}

		respBytes, _ := json.Marshal(OrderResponse{
			OrderID: req.OrderID,
			Status:  "ACCEPTED",
		})
		idem.Complete(idemKey, http.StatusAccepted, respBytes)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write(respBytes)
	}
}
