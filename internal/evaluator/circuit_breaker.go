package evaluator

import (
	"fmt"
	"sync"
	"time"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// CircuitBreakerState represents the state of a circuit breaker.
type CircuitBreakerState int

const (
	CircuitClosed   CircuitBreakerState = iota // normal operation
	CircuitOpen                                // failing fast
	CircuitHalfOpen                            // probing
)

// CircuitBreakerConfig holds circuit breaker settings.
type CircuitBreakerConfig struct {
	FailThreshold int           // consecutive failures before opening (default 5)
	Cooldown      time.Duration // how long to stay open before half-open (default 30s)
	FailWindow    time.Duration // reset consecutive counter if last failure is older than this (default 60s)
}

// DefaultCircuitBreakerConfig returns the default config.
func DefaultCircuitBreakerConfig() CircuitBreakerConfig {
	return CircuitBreakerConfig{
		FailThreshold: 5,
		Cooldown:      30 * time.Second,
		FailWindow:    60 * time.Second,
	}
}

type evaluatorCircuit struct {
	consecutiveFails int
	lastFailTime     time.Time
	openedAt         time.Time
	state            CircuitBreakerState
}

// CircuitBreaker tracks per-evaluator failure state for circuit breaking.
type CircuitBreaker struct {
	mu       sync.Mutex
	config   CircuitBreakerConfig
	circuits map[string]*evaluatorCircuit // key: evaluator name
}

// NewCircuitBreaker creates a new CircuitBreaker with the given config.
func NewCircuitBreaker(config CircuitBreakerConfig) *CircuitBreaker {
	if config.FailThreshold <= 0 {
		config.FailThreshold = 5
	}
	if config.Cooldown <= 0 {
		config.Cooldown = 30 * time.Second
	}
	if config.FailWindow <= 0 {
		config.FailWindow = 60 * time.Second
	}
	return &CircuitBreaker{
		config:   config,
		circuits: make(map[string]*evaluatorCircuit),
	}
}

// Allow checks if an evaluator call should proceed.
// Returns true if the circuit is closed or half-open (probe).
// Returns false if the circuit is open (fail fast).
func (cb *CircuitBreaker) Allow(name string) bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	c, ok := cb.circuits[name]
	if !ok {
		return true // no circuit = closed
	}

	now := time.Now()
	switch c.state {
	case CircuitClosed:
		return true
	case CircuitOpen:
		if now.Sub(c.openedAt) >= cb.config.Cooldown {
			c.state = CircuitHalfOpen
			return true // allow probe
		}
		return false
	case CircuitHalfOpen:
		return true // allow probe
	}
	return true
}

// RecordSuccess records a successful evaluator call.
func (cb *CircuitBreaker) RecordSuccess(name string) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	c, ok := cb.circuits[name]
	if !ok {
		return
	}
	c.consecutiveFails = 0
	c.state = CircuitClosed
}

// RecordFailure records a failed evaluator call. Only transient/timeout failures
// count toward the threshold — permanent config errors do not trip the breaker.
func (cb *CircuitBreaker) RecordFailure(name string, category types.FailureCategory) {
	// Only count transient-like failures.
	if category == types.FailurePermanent {
		return
	}

	cb.mu.Lock()
	defer cb.mu.Unlock()

	c, ok := cb.circuits[name]
	if !ok {
		c = &evaluatorCircuit{}
		cb.circuits[name] = c
	}

	now := time.Now()

	// Reset counter if last failure is outside the window.
	if !c.lastFailTime.IsZero() && now.Sub(c.lastFailTime) > cb.config.FailWindow {
		c.consecutiveFails = 0
	}

	c.consecutiveFails++
	c.lastFailTime = now

	if c.consecutiveFails >= cb.config.FailThreshold {
		c.state = CircuitOpen
		c.openedAt = now
	}
}

// State returns the current state of an evaluator's circuit.
func (cb *CircuitBreaker) State(name string) CircuitBreakerState {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	c, ok := cb.circuits[name]
	if !ok {
		return CircuitClosed
	}
	return c.state
}

// String returns a human-readable state name.
func (s CircuitBreakerState) String() string {
	switch s {
	case CircuitClosed:
		return "CLOSED"
	case CircuitOpen:
		return "OPEN"
	case CircuitHalfOpen:
		return "HALF_OPEN"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", int(s))
	}
}
