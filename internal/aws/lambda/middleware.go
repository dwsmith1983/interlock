// Package lambda provides middleware utilities for AWS Lambda handlers,
// including context timeout management with safety buffers.
package lambda

import (
	"context"
	"time"
)

const (
	// DefaultSafetyBuffer is the default time reserved before the Lambda
	// deadline for cleanup work (flushing logs, closing connections, etc.).
	DefaultSafetyBuffer = 500 * time.Millisecond

	// DefaultMaxTimeout is applied when the parent context has no deadline,
	// matching the maximum Lambda execution time.
	DefaultMaxTimeout = 15 * time.Minute

	// minTimeout is the floor applied when the safety buffer would consume
	// all remaining time. This prevents a zero or negative timeout.
	minTimeout = 50 * time.Millisecond
)

// timeoutConfig holds the resolved options for WithTimeout.
type timeoutConfig struct {
	safetyBuffer time.Duration
	maxTimeout   time.Duration
}

// TimeoutOption configures the behavior of WithTimeout.
type TimeoutOption func(*timeoutConfig)

// WithSafetyBuffer overrides the default safety buffer subtracted from the
// parent context's remaining time.
func WithSafetyBuffer(d time.Duration) TimeoutOption {
	return func(c *timeoutConfig) {
		c.safetyBuffer = d
	}
}

// WithMaxTimeout overrides the default maximum timeout applied when the
// parent context has no deadline.
func WithMaxTimeout(d time.Duration) TimeoutOption {
	return func(c *timeoutConfig) {
		c.maxTimeout = d
	}
}

// WithTimeout returns a derived context with a timeout based on the remaining
// time from the parent context (typically Lambda's context) minus a safety
// buffer for cleanup. If the parent has no deadline, a max timeout is applied.
//
// The returned CancelFunc must be called to release resources.
func WithTimeout(parent context.Context, opts ...TimeoutOption) (context.Context, context.CancelFunc) {
	cfg := timeoutConfig{
		safetyBuffer: DefaultSafetyBuffer,
		maxTimeout:   DefaultMaxTimeout,
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	deadline, ok := parent.Deadline()
	if !ok {
		return context.WithTimeout(parent, cfg.maxTimeout)
	}

	remaining := time.Until(deadline) - cfg.safetyBuffer
	if remaining < minTimeout {
		remaining = minTimeout
	}

	return context.WithTimeout(parent, remaining)
}
