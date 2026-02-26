package evaluator

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/dwsmith1983/interlock/pkg/types"
)

func TestCircuitBreaker_StartsInClosedState(t *testing.T) {
	cb := NewCircuitBreaker(DefaultCircuitBreakerConfig())

	assert.Equal(t, CircuitClosed, cb.State("eval-1"))
	assert.True(t, cb.Allow("eval-1"))
}

func TestCircuitBreaker_OpensAfterThreshold(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailThreshold: 5,
		Cooldown:      30 * time.Second,
		FailWindow:    60 * time.Second,
	})

	for i := 0; i < 5; i++ {
		cb.RecordFailure("eval-1", types.FailureTransient)
	}

	assert.Equal(t, CircuitOpen, cb.State("eval-1"))
	assert.False(t, cb.Allow("eval-1"))
}

func TestCircuitBreaker_PermanentFailureIgnored(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailThreshold: 3,
		Cooldown:      30 * time.Second,
		FailWindow:    60 * time.Second,
	})

	// Record many permanent failures — should NOT open the circuit.
	for i := 0; i < 10; i++ {
		cb.RecordFailure("eval-1", types.FailurePermanent)
	}

	assert.Equal(t, CircuitClosed, cb.State("eval-1"))
	assert.True(t, cb.Allow("eval-1"))
}

func TestCircuitBreaker_CooldownTransitionsToHalfOpen(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailThreshold: 1,
		Cooldown:      1 * time.Millisecond, // very short for testing
		FailWindow:    60 * time.Second,
	})

	cb.RecordFailure("eval-1", types.FailureTransient)
	assert.Equal(t, CircuitOpen, cb.State("eval-1"))

	// Wait for cooldown to elapse.
	time.Sleep(5 * time.Millisecond)

	// Allow should transition to half-open and return true.
	assert.True(t, cb.Allow("eval-1"))
	assert.Equal(t, CircuitHalfOpen, cb.State("eval-1"))
}

func TestCircuitBreaker_HalfOpenSuccess_ResetsToClosed(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailThreshold: 1,
		Cooldown:      1 * time.Millisecond,
		FailWindow:    60 * time.Second,
	})

	cb.RecordFailure("eval-1", types.FailureTransient)
	time.Sleep(5 * time.Millisecond)
	cb.Allow("eval-1") // transitions to half-open

	cb.RecordSuccess("eval-1")
	assert.Equal(t, CircuitClosed, cb.State("eval-1"))
	assert.True(t, cb.Allow("eval-1"))
}

func TestCircuitBreaker_HalfOpenFailure_ReOpens(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailThreshold: 1,
		Cooldown:      1 * time.Millisecond,
		FailWindow:    60 * time.Second,
	})

	cb.RecordFailure("eval-1", types.FailureTransient)
	time.Sleep(5 * time.Millisecond)
	cb.Allow("eval-1") // transitions to half-open

	cb.RecordFailure("eval-1", types.FailureTransient)
	assert.Equal(t, CircuitOpen, cb.State("eval-1"))
	assert.False(t, cb.Allow("eval-1"))
}

func TestCircuitBreaker_FailWindowResetsCounter(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailThreshold: 3,
		Cooldown:      30 * time.Second,
		FailWindow:    1 * time.Millisecond, // very short for testing
	})

	// Record 2 failures.
	cb.RecordFailure("eval-1", types.FailureTransient)
	cb.RecordFailure("eval-1", types.FailureTransient)

	// Wait for fail window to expire.
	time.Sleep(5 * time.Millisecond)

	// Next failure should reset counter to 1 (not accumulate to 3).
	cb.RecordFailure("eval-1", types.FailureTransient)
	assert.Equal(t, CircuitClosed, cb.State("eval-1"))
	assert.True(t, cb.Allow("eval-1"))
}

func TestCircuitBreaker_DefaultConfig(t *testing.T) {
	// Zero-value config should get proper defaults.
	cb := NewCircuitBreaker(CircuitBreakerConfig{})

	// Verify defaults by tripping the breaker at exactly 5 failures.
	for i := 0; i < 4; i++ {
		cb.RecordFailure("eval-1", types.FailureTransient)
	}
	assert.Equal(t, CircuitClosed, cb.State("eval-1"))

	cb.RecordFailure("eval-1", types.FailureTransient)
	assert.Equal(t, CircuitOpen, cb.State("eval-1"))
}
