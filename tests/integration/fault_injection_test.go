package integration_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dwsmith1983/interlock/internal/client"
	"github.com/dwsmith1983/interlock/internal/resilience"
	"github.com/sony/gobreaker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Fault-injection test doubles ---

// countingHTTPDoer tracks call count atomically and always fails.
type countingHTTPDoer struct {
	calls atomic.Int32
	err   error
}

func (c *countingHTTPDoer) Do(_ *http.Request) (*http.Response, error) {
	c.calls.Add(1)
	return nil, c.err
}

// --- Tests ---

func TestFaultInjection_CircuitBreakerOpens(t *testing.T) {
	// Configure breaker to trip after 3 consecutive failures.
	tripAfter := uint32(3)
	inner := &countingHTTPDoer{err: errors.New("connection refused")}
	cfg := client.BreakerConfig{
		Name:        "test-breaker",
		MaxRequests: 1,
		Interval:    0,
		Timeout:     1 * time.Second,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= tripAfter
		},
	}
	bc := client.NewBreakerClient(inner, cfg)

	// Drive the breaker to open state with tripAfter failures.
	req, _ := http.NewRequest(http.MethodGet, "http://localhost/test", http.NoBody)
	for range tripAfter {
		_, err := bc.Do(req)
		require.Error(t, err)
	}

	// Breaker should now be open.
	assert.Equal(t, gobreaker.StateOpen, bc.State(), "breaker should be open after consecutive failures")

	// Subsequent request should get ErrOpenState without hitting the inner client.
	callsBefore := inner.calls.Load()
	_, err := bc.Do(req)
	require.Error(t, err)
	assert.ErrorIs(t, err, gobreaker.ErrOpenState,
		"requests through an open breaker should return ErrOpenState")
	assert.Equal(t, callsBefore, inner.calls.Load(),
		"open breaker should not forward requests to the inner client")
}

func TestFaultInjection_RetryExhaustion(t *testing.T) {
	maxRetries := 4
	cfg := resilience.RetryConfig{
		MaxRetries:   maxRetries,
		BaseDelay:    1 * time.Millisecond, // tiny delays for test speed
		MaxDelay:     5 * time.Millisecond,
		JitterFactor: 0,
	}

	attempts := 0
	sentinel := errors.New("persistent failure")
	fn := func() error {
		attempts++
		return sentinel
	}

	err := resilience.Retry(context.Background(), cfg, fn)

	require.Error(t, err)
	assert.ErrorIs(t, err, sentinel, "should return the last error after exhausting retries")
	assert.Equal(t, maxRetries+1, attempts,
		fmt.Sprintf("should attempt exactly MaxRetries+1 (%d) times", maxRetries+1))
}

func TestFaultInjection_ContextTimeoutMidRetry(t *testing.T) {
	cfg := resilience.RetryConfig{
		MaxRetries:   10,
		BaseDelay:    500 * time.Millisecond, // long enough to be interrupted
		MaxDelay:     2 * time.Second,
		JitterFactor: 0,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	attempts := 0
	fn := func() error {
		attempts++
		return errors.New("always fails")
	}

	err := resilience.Retry(ctx, cfg, fn)

	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded,
		"should return context.DeadlineExceeded when context times out mid-retry")
	assert.Less(t, attempts, cfg.MaxRetries+1,
		"should cancel early without exhausting all retries")
}

func TestFaultInjection_ContextCanceledMidRetry(t *testing.T) {
	cfg := resilience.RetryConfig{
		MaxRetries:   10,
		BaseDelay:    500 * time.Millisecond,
		MaxDelay:     2 * time.Second,
		JitterFactor: 0,
	}

	ctx, cancel := context.WithCancel(context.Background())

	attempts := 0
	fn := func() error {
		attempts++
		if attempts >= 2 {
			cancel() // cancel after second attempt
		}
		return errors.New("always fails")
	}

	err := resilience.Retry(ctx, cfg, fn)

	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled,
		"should return context.Canceled when context is canceled mid-retry")
	assert.Less(t, attempts, cfg.MaxRetries+1,
		"should cancel early without exhausting all retries")
}
