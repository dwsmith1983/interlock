package evaluator

import (
	"context"
	"testing"
	"time"

	"github.com/interlock-systems/interlock/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunnerPass(t *testing.T) {
	runner := NewRunner(nil)
	input := types.EvaluatorInput{
		PipelineID: "test",
		TraitType:  "freshness",
		Config:     map[string]interface{}{"maxLagSeconds": 60},
	}

	output, err := runner.Run(context.Background(), "../../testdata/evaluators/pass", input, 10*time.Second)
	require.NoError(t, err)
	assert.Equal(t, types.TraitPass, output.Status)
	assert.NotNil(t, output.Value)
}

func TestRunnerFail(t *testing.T) {
	runner := NewRunner(nil)
	input := types.EvaluatorInput{
		PipelineID: "test",
		TraitType:  "freshness",
		Config:     map[string]interface{}{},
	}

	output, err := runner.Run(context.Background(), "../../testdata/evaluators/fail", input, 10*time.Second)
	require.NoError(t, err)
	assert.Equal(t, types.TraitFail, output.Status)
	assert.Equal(t, "deliberate test failure", output.Reason)
}

func TestRunnerTimeout(t *testing.T) {
	runner := NewRunner(nil)
	input := types.EvaluatorInput{
		PipelineID: "test",
		TraitType:  "freshness",
		Config:     map[string]interface{}{},
	}

	output, err := runner.Run(context.Background(), "../../testdata/evaluators/slow", input, 500*time.Millisecond)
	require.NoError(t, err)
	assert.Equal(t, types.TraitFail, output.Status)
	assert.Equal(t, "EVALUATOR_TIMEOUT", output.Reason)
	assert.Equal(t, types.FailureTimeout, output.FailureCategory)
}

func TestRunnerExitError(t *testing.T) {
	runner := NewRunner(nil)
	input := types.EvaluatorInput{
		PipelineID: "test",
		TraitType:  "freshness",
		Config:     map[string]interface{}{},
	}

	// exit-error uses exit code 1 → transient
	output, err := runner.Run(context.Background(), "../../testdata/evaluators/exit-error", input, 10*time.Second)
	require.NoError(t, err)
	assert.Equal(t, types.TraitFail, output.Status)
	assert.Contains(t, output.Reason, "EVALUATOR_ERROR")
	assert.Equal(t, types.FailureTransient, output.FailureCategory)
}

func TestRunnerExitCode2_Permanent(t *testing.T) {
	runner := NewRunner(nil)
	input := types.EvaluatorInput{
		PipelineID: "test",
		TraitType:  "freshness",
		Config:     map[string]interface{}{},
	}

	output, err := runner.Run(context.Background(), "../../testdata/evaluators/exit-permanent", input, 10*time.Second)
	require.NoError(t, err)
	assert.Equal(t, types.TraitFail, output.Status)
	assert.Equal(t, types.FailurePermanent, output.FailureCategory)
}

func TestRunnerSignalKill_Crash(t *testing.T) {
	runner := NewRunner(nil)
	input := types.EvaluatorInput{
		PipelineID: "test",
		TraitType:  "freshness",
		Config:     map[string]interface{}{},
	}

	output, err := runner.Run(context.Background(), "../../testdata/evaluators/exit-signal", input, 10*time.Second)
	require.NoError(t, err)
	assert.Equal(t, types.TraitFail, output.Status)
	assert.Equal(t, types.FailureEvaluatorCrash, output.FailureCategory)
}

func TestRunnerBadJSON(t *testing.T) {
	runner := NewRunner(nil)
	input := types.EvaluatorInput{
		PipelineID: "test",
		TraitType:  "freshness",
		Config:     map[string]interface{}{},
	}

	output, err := runner.Run(context.Background(), "../../testdata/evaluators/bad-json", input, 10*time.Second)
	require.NoError(t, err)
	assert.Equal(t, types.TraitFail, output.Status)
	assert.Contains(t, output.Reason, "EVALUATOR_OUTPUT_INVALID")
	assert.Equal(t, types.FailurePermanent, output.FailureCategory)
}
