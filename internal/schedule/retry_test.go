package schedule

import (
	"testing"
	"time"

	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/stretchr/testify/assert"
)

func TestCalculateBackoff(t *testing.T) {
	policy := types.RetryPolicy{
		BackoffSeconds:    30,
		BackoffMultiplier: 2.0,
	}

	tests := []struct {
		attempt  int
		expected time.Duration
	}{
		{1, 30 * time.Second},
		{2, 60 * time.Second},
		{3, 120 * time.Second},
		{4, 240 * time.Second},
	}

	for _, tc := range tests {
		result := CalculateBackoff(policy, tc.attempt)
		assert.Equal(t, tc.expected, result, "attempt %d", tc.attempt)
	}
}

func TestCalculateBackoff_CapsAtOneHour(t *testing.T) {
	policy := types.RetryPolicy{
		BackoffSeconds:    1800,
		BackoffMultiplier: 4.0,
	}

	result := CalculateBackoff(policy, 3)
	assert.Equal(t, 3600*time.Second, result)
}

func TestCalculateBackoff_DefaultMultiplier(t *testing.T) {
	policy := types.RetryPolicy{
		BackoffSeconds:    10,
		BackoffMultiplier: 0,
	}

	result := CalculateBackoff(policy, 2)
	assert.Equal(t, 20*time.Second, result)
}

func TestIsRetryable(t *testing.T) {
	policy := DefaultRetryPolicy()

	tests := []struct {
		category types.FailureCategory
		expected bool
	}{
		{types.FailureTransient, true},
		{types.FailureTimeout, true},
		{types.FailurePermanent, false},
		{types.FailureEvaluatorCrash, false},
	}

	for _, tc := range tests {
		result := IsRetryable(policy, tc.category)
		assert.Equal(t, tc.expected, result, "category %s", tc.category)
	}
}

func TestIsRetryable_EmptyCategory(t *testing.T) {
	policy := DefaultRetryPolicy()
	assert.True(t, IsRetryable(policy, ""))
}

func TestIsRetryable_EmptyCategory_CustomPolicy(t *testing.T) {
	policy := types.RetryPolicy{
		RetryableFailures: []types.FailureCategory{types.FailureTransient},
	}
	assert.True(t, IsRetryable(policy, ""))
}

func TestIsRetryable_EmptyPolicyDefaults(t *testing.T) {
	policy := types.RetryPolicy{}

	assert.True(t, IsRetryable(policy, types.FailureTransient))
	assert.True(t, IsRetryable(policy, types.FailureTimeout))
	assert.False(t, IsRetryable(policy, types.FailurePermanent))
}

func TestIsRetryable_CustomCategories(t *testing.T) {
	policy := types.RetryPolicy{
		RetryableFailures: []types.FailureCategory{types.FailureTransient},
	}

	assert.True(t, IsRetryable(policy, types.FailureTransient))
	assert.False(t, IsRetryable(policy, types.FailureTimeout))
	assert.False(t, IsRetryable(policy, types.FailurePermanent))
}

func TestDefaultRetryPolicy(t *testing.T) {
	p := DefaultRetryPolicy()
	assert.Equal(t, 3, p.MaxAttempts)
	assert.Equal(t, 30, p.BackoffSeconds)
	assert.Equal(t, 2.0, p.BackoffMultiplier)
	assert.Contains(t, p.RetryableFailures, types.FailureTransient)
	assert.Contains(t, p.RetryableFailures, types.FailureTimeout)
}
