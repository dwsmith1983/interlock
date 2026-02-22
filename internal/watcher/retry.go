package watcher

import (
	"time"

	"github.com/dwsmith1983/interlock/internal/schedule"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// DefaultRetryPolicy returns the default retry configuration.
func DefaultRetryPolicy() types.RetryPolicy {
	return schedule.DefaultRetryPolicy()
}

// CalculateBackoff returns the wait duration for a given attempt number.
func CalculateBackoff(policy types.RetryPolicy, attempt int) time.Duration {
	return schedule.CalculateBackoff(policy, attempt)
}

// IsRetryable returns whether a failure category should be retried.
func IsRetryable(policy types.RetryPolicy, category types.FailureCategory) bool {
	return schedule.IsRetryable(policy, category)
}
