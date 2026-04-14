package lambda_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	awslambda "github.com/dwsmith1983/interlock/internal/aws/lambda"
)

func TestWithTimeout_DerivesFromParentDeadline(t *testing.T) {
	// Arrange: parent context with a deadline 3s from now.
	deadline := time.Now().Add(3 * time.Second)
	parent, parentCancel := context.WithDeadline(context.Background(), deadline)
	defer parentCancel()

	// Act: derive a timeout context with default 500ms safety buffer.
	ctx, cancel := awslambda.WithTimeout(parent)
	defer cancel()

	// Assert: derived deadline should be ~500ms before parent deadline.
	derivedDeadline, ok := ctx.Deadline()
	require.True(t, ok, "derived context must have a deadline")

	// The derived deadline should be approximately (parent deadline - 500ms).
	expected := deadline.Add(-500 * time.Millisecond)
	diff := derivedDeadline.Sub(expected).Abs()
	assert.Less(t, diff, 50*time.Millisecond,
		"derived deadline should be within 50ms of (parent deadline - 500ms buffer)")
}

func TestWithTimeout_ConfigurableBuffer(t *testing.T) {
	// Arrange: parent context with a deadline 5s from now.
	deadline := time.Now().Add(5 * time.Second)
	parent, parentCancel := context.WithDeadline(context.Background(), deadline)
	defer parentCancel()

	customBuffer := 2 * time.Second

	// Act: derive with a custom safety buffer.
	ctx, cancel := awslambda.WithTimeout(parent, awslambda.WithSafetyBuffer(customBuffer))
	defer cancel()

	// Assert: derived deadline should be ~2s before parent deadline.
	derivedDeadline, ok := ctx.Deadline()
	require.True(t, ok, "derived context must have a deadline")

	expected := deadline.Add(-customBuffer)
	diff := derivedDeadline.Sub(expected).Abs()
	assert.Less(t, diff, 50*time.Millisecond,
		"derived deadline should be within 50ms of (parent deadline - 2s buffer)")
}

func TestWithTimeout_NoParentDeadline_AppliesMaxTimeout(t *testing.T) {
	// Arrange: parent with no deadline.
	parent := context.Background()

	// Act: derive timeout.
	before := time.Now()
	ctx, cancel := awslambda.WithTimeout(parent)
	defer cancel()

	// Assert: derived context should have a deadline ~15 min from now.
	derivedDeadline, ok := ctx.Deadline()
	require.True(t, ok, "derived context must have a deadline even without parent deadline")

	expected := before.Add(15 * time.Minute)
	diff := derivedDeadline.Sub(expected).Abs()
	assert.Less(t, diff, 100*time.Millisecond,
		"derived deadline should be within 100ms of 15 min max timeout")
}

func TestWithTimeout_NoParentDeadline_CustomMaxTimeout(t *testing.T) {
	// Arrange: parent with no deadline, custom max timeout.
	parent := context.Background()
	customMax := 5 * time.Minute

	// Act
	before := time.Now()
	ctx, cancel := awslambda.WithTimeout(parent, awslambda.WithMaxTimeout(customMax))
	defer cancel()

	// Assert
	derivedDeadline, ok := ctx.Deadline()
	require.True(t, ok)

	expected := before.Add(customMax)
	diff := derivedDeadline.Sub(expected).Abs()
	assert.Less(t, diff, 100*time.Millisecond,
		"derived deadline should be within 100ms of custom 5 min max timeout")
}

func TestWithTimeout_CancelPropagates(t *testing.T) {
	// Arrange: parent with a generous deadline.
	parent, parentCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer parentCancel()

	// Act: derive and then cancel the derived context.
	ctx, cancel := awslambda.WithTimeout(parent)
	cancel()

	// Assert: derived context should be cancelled.
	select {
	case <-ctx.Done():
		assert.ErrorIs(t, ctx.Err(), context.Canceled)
	default:
		t.Fatal("expected derived context to be cancelled after cancel() call")
	}
}

func TestWithTimeout_ParentCancellationPropagates(t *testing.T) {
	// Arrange: parent context we will cancel.
	parent, parentCancel := context.WithTimeout(context.Background(), 10*time.Second)

	ctx, cancel := awslambda.WithTimeout(parent)
	defer cancel()

	// Act: cancel the parent.
	parentCancel()

	// Assert: derived context should also be done.
	select {
	case <-ctx.Done():
		// Parent cancellation should propagate.
	case <-time.After(1 * time.Second):
		t.Fatal("expected derived context to be done after parent cancellation")
	}
}

func TestWithTimeout_BufferLargerThanRemaining_FloorAtMinimum(t *testing.T) {
	// Arrange: parent deadline only 200ms away, buffer is 500ms.
	// The derived context should still have a positive (small) timeout, not negative.
	deadline := time.Now().Add(200 * time.Millisecond)
	parent, parentCancel := context.WithDeadline(context.Background(), deadline)
	defer parentCancel()

	// Act
	ctx, cancel := awslambda.WithTimeout(parent)
	defer cancel()

	// Assert: context should still have a deadline and not be immediately expired.
	derivedDeadline, ok := ctx.Deadline()
	require.True(t, ok, "derived context must have a deadline")

	// The derived deadline should be at or very close to now (minimum floor).
	// It should NOT be in the past relative to when we set it up.
	remaining := time.Until(derivedDeadline)
	assert.GreaterOrEqual(t, remaining, time.Duration(0),
		"remaining time should be non-negative even when buffer exceeds parent remaining")
}
