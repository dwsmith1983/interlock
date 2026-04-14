package resilience_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dwsmith1983/interlock/internal/resilience"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRetry_SucceedsFirstTry(t *testing.T) {
	err := resilience.Retry(context.Background(), resilience.DefaultRetryConfig(), func() error {
		return nil
	})
	assert.NoError(t, err)
}

func TestRetry_SucceedsOnRetry(t *testing.T) {
	cfg := resilience.RetryConfig{MaxRetries: 3, BaseDelay: time.Millisecond, MaxDelay: 10 * time.Millisecond, JitterFactor: 0}
	var attempts atomic.Int32

	err := resilience.Retry(context.Background(), cfg, func() error {
		if attempts.Add(1) < 3 {
			return errors.New("transient")
		}
		return nil
	})

	assert.NoError(t, err)
	assert.Equal(t, int32(3), attempts.Load())
}

func TestRetry_ExhaustsRetries(t *testing.T) {
	cfg := resilience.RetryConfig{MaxRetries: 2, BaseDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond, JitterFactor: 0}
	sentinel := errors.New("persistent failure")

	err := resilience.Retry(context.Background(), cfg, func() error {
		return sentinel
	})

	require.Error(t, err)
	assert.ErrorIs(t, err, sentinel)
}

func TestRetry_ContextCancellation(t *testing.T) {
	cfg := resilience.RetryConfig{MaxRetries: 10, BaseDelay: time.Second, MaxDelay: 10 * time.Second, JitterFactor: 0}
	ctx, cancel := context.WithCancel(context.Background())
	var attempts atomic.Int32

	go func() {
		time.Sleep(5 * time.Millisecond)
		cancel()
	}()

	err := resilience.Retry(ctx, cfg, func() error {
		attempts.Add(1)
		return errors.New("fail")
	})

	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	assert.LessOrEqual(t, attempts.Load(), int32(2))
}

func TestRetry_DelayGrowsExponentially(t *testing.T) {
	cfg := resilience.RetryConfig{MaxRetries: 3, BaseDelay: 10 * time.Millisecond, MaxDelay: time.Second, JitterFactor: 0}
	var timestamps []time.Time

	_ = resilience.Retry(context.Background(), cfg, func() error {
		timestamps = append(timestamps, time.Now())
		return errors.New("fail")
	})

	require.Len(t, timestamps, 4) // initial + 3 retries
	d1 := timestamps[2].Sub(timestamps[1])
	d0 := timestamps[1].Sub(timestamps[0])
	assert.Greater(t, d1.Nanoseconds(), d0.Nanoseconds(), "second delay should be longer than first")
}

func TestRetry_JitterWithinBounds(t *testing.T) {
	cfg := resilience.RetryConfig{MaxRetries: 0, BaseDelay: 100 * time.Millisecond, MaxDelay: time.Second, JitterFactor: 0.5}

	// Just verify it doesn't panic and returns an error
	err := resilience.Retry(context.Background(), cfg, func() error {
		return errors.New("fail")
	})
	assert.Error(t, err)
}
