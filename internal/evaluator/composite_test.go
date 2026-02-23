package evaluator

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dwsmith1983/interlock/pkg/types"
)

func TestCompositeRunner_BuiltinDispatch(t *testing.T) {
	http := NewHTTPRunner("")
	composite := NewCompositeRunner(http)

	called := false
	composite.Register("test-handler", func(_ context.Context, input types.EvaluatorInput) (*types.EvaluatorOutput, error) {
		called = true
		return &types.EvaluatorOutput{
			Status: types.TraitPass,
			Reason: "test passed",
		}, nil
	})

	out, err := composite.Run(context.Background(), "builtin:test-handler", types.EvaluatorInput{
		PipelineID: "test",
		TraitType:  "check",
	}, 5*time.Second)

	require.NoError(t, err)
	assert.True(t, called)
	assert.Equal(t, types.TraitPass, out.Status)
	assert.Equal(t, "test passed", out.Reason)
}

func TestCompositeRunner_UnknownBuiltin(t *testing.T) {
	http := NewHTTPRunner("")
	composite := NewCompositeRunner(http)

	out, err := composite.Run(context.Background(), "builtin:nonexistent", types.EvaluatorInput{}, 5*time.Second)

	require.NoError(t, err)
	assert.Equal(t, types.TraitFail, out.Status)
	assert.Contains(t, out.Reason, "unknown builtin evaluator")
	assert.Equal(t, types.FailurePermanent, out.FailureCategory)
}

func TestCompositeRunner_HTTPFallthrough(t *testing.T) {
	http := NewHTTPRunner("http://localhost:9999")
	composite := NewCompositeRunner(http)

	// Non-builtin paths go to HTTP — this will fail because the server doesn't exist,
	// but it proves the routing works (error from HTTP, not "unknown builtin").
	_, err := composite.Run(context.Background(), "check-freshness", types.EvaluatorInput{
		PipelineID: "test",
		TraitType:  "freshness",
	}, 1*time.Second)

	// Should get an HTTP error, not a builtin error
	require.Error(t, err)
	assert.Contains(t, err.Error(), "evaluator request failed")
}
