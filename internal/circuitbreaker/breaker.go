package circuitbreaker

import (
	"errors"
	"sync"
	"time"
)

type State int

const (
	StateClosed State = iota
	StateOpen
	StateHalfOpen
)

var ErrCircuitOpen = errors.New("circuit breaker is open")

// Breaker protects downstream services from cascading failure and retry storms.
type Breaker struct {
	mu               sync.Mutex
	state            State
	failures         int
	failureThreshold int
	openTimeout      time.Duration
	openedAt         time.Time
	halfOpenInFlight bool
	nowFn            func() time.Time
}

// NewBreaker creates a circuit breaker with a failure threshold and cooldown timeout.
func NewBreaker(failureThreshold int, openTimeout time.Duration) *Breaker {
	return &Breaker{
		state:            StateClosed,
		failureThreshold: failureThreshold,
		openTimeout:      openTimeout,
		nowFn:            time.Now,
	}
}

// State returns the current breaker state.
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

// Execute runs fn if the circuit allows it, updating failure/recovery state.
func (b *Breaker) Execute(fn func() error) error {
	if err := b.beforeCall(); err != nil {
		return err
	}
	err := fn()
	b.afterCall(err)
	return err
}

func (b *Breaker) beforeCall() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case StateClosed:
		return nil
	case StateOpen:
		if b.nowFn().Sub(b.openedAt) >= b.openTimeout {
			b.state = StateHalfOpen
			b.halfOpenInFlight = true
			return nil
		}
		return ErrCircuitOpen
	case StateHalfOpen:
		if b.halfOpenInFlight {
			return ErrCircuitOpen
		}
		b.halfOpenInFlight = true
		return nil
	default:
		return ErrCircuitOpen
	}
}

func (b *Breaker) afterCall(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if err == nil {
		b.failures = 0
		b.state = StateClosed
		b.halfOpenInFlight = false
		return
	}

	b.halfOpenInFlight = false
	if b.state == StateHalfOpen {
		b.state = StateOpen
		b.openedAt = b.nowFn()
		return
	}

	b.failures++
	if b.failures >= b.failureThreshold {
		b.state = StateOpen
		b.openedAt = b.nowFn()
	}
}
