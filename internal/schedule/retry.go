package schedule

import (
	"math"
	"time"

	"github.com/interlock-systems/interlock/pkg/types"
)

const maxBackoffSeconds = 3600

// DefaultRetryPolicy returns the default retry configuration.
func DefaultRetryPolicy() types.RetryPolicy {
	return types.RetryPolicy{
		MaxAttempts:       3,
		BackoffSeconds:    30,
		BackoffMultiplier: 2.0,
		RetryableFailures: []types.FailureCategory{
			types.FailureTransient,
			types.FailureTimeout,
		},
	}
}

// CalculateBackoff returns the wait duration for a given attempt number.
// Uses exponential backoff: base * multiplier^(attempt-1).
func CalculateBackoff(policy types.RetryPolicy, attempt int) time.Duration {
	if attempt <= 1 {
		return time.Duration(policy.BackoffSeconds) * time.Second
	}
	multiplier := policy.BackoffMultiplier
	if multiplier <= 0 {
		multiplier = 2.0
	}
	backoff := float64(policy.BackoffSeconds) * math.Pow(multiplier, float64(attempt-1))
	if backoff > maxBackoffSeconds {
		backoff = maxBackoffSeconds
	}
	return time.Duration(backoff) * time.Second
}

// IsRetryable returns whether a failure category should be retried.
func IsRetryable(policy types.RetryPolicy, category types.FailureCategory) bool {
	if category == types.FailurePermanent {
		return false
	}
	if len(policy.RetryableFailures) == 0 {
		// Default: retry transient and timeout
		return category == types.FailureTransient || category == types.FailureTimeout
	}
	for _, fc := range policy.RetryableFailures {
		if fc == category {
			return true
		}
	}
	return false
}
