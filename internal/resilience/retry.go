package resilience

import (
	"context"
	"math"
	"math/rand/v2"
	"time"
)

// RetryConfig controls the exponential backoff retry behavior.
type RetryConfig struct {
	MaxRetries   int
	BaseDelay    time.Duration
	MaxDelay     time.Duration
	JitterFactor float64 // 0.0 = no jitter, 0.5 = ±50% jitter
}

// DefaultRetryConfig returns sensible defaults for retrying throttled writes.
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxRetries:   3,
		BaseDelay:    100 * time.Millisecond,
		MaxDelay:     5 * time.Second,
		JitterFactor: 0.5,
	}
}

// Retry calls fn up to cfg.MaxRetries+1 times with exponential backoff and jitter.
// It respects context cancellation between attempts.
func Retry(ctx context.Context, cfg RetryConfig, fn func() error) error {
	var lastErr error
	for attempt := range cfg.MaxRetries + 1 {
		lastErr = fn()
		if lastErr == nil {
			return nil
		}

		if attempt == cfg.MaxRetries {
			break
		}

		delay := computeDelay(cfg, attempt)

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return lastErr
}

func computeDelay(cfg RetryConfig, attempt int) time.Duration {
	delay := float64(cfg.BaseDelay) * math.Pow(2, float64(attempt))
	if delay > float64(cfg.MaxDelay) {
		delay = float64(cfg.MaxDelay)
	}

	jf := max(0.0, min(cfg.JitterFactor, 1.0))
	if jf > 0 {
		jitter := 1.0 - jf + rand.Float64()*2*jf
		delay *= jitter
	}

	return time.Duration(delay)
}
