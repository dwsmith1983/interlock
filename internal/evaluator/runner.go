// Package evaluator handles subprocess-based trait evaluation.
package evaluator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"time"

	"github.com/interlock-systems/interlock/pkg/types"
)

// Runner executes evaluator subprocesses.
type Runner struct {
	baseDirs []string
}

// NewRunner creates a new evaluator runner that resolves evaluators from the given directories.
func NewRunner(baseDirs []string) *Runner {
	return &Runner{baseDirs: baseDirs}
}

// Run executes an evaluator subprocess with the given input and timeout.
// It pipes config JSON to stdin and reads result JSON from stdout.
func (r *Runner) Run(ctx context.Context, evaluatorPath string, input types.EvaluatorInput, timeout time.Duration) (*types.EvaluatorOutput, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	inputJSON, err := json.Marshal(input.Config)
	if err != nil {
		return nil, fmt.Errorf("marshaling evaluator input: %w", err)
	}

	cmd := exec.CommandContext(ctx, evaluatorPath)
	cmd.Stdin = bytes.NewReader(inputJSON)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		fc := classifyExitError(ctx, err)
		if ctx.Err() == context.DeadlineExceeded {
			return &types.EvaluatorOutput{
				Status:          types.TraitFail,
				Reason:          "EVALUATOR_TIMEOUT",
				FailureCategory: fc,
			}, nil
		}
		return &types.EvaluatorOutput{
			Status:          types.TraitFail,
			Reason:          fmt.Sprintf("EVALUATOR_ERROR: %v (stderr: %s)", err, stderr.String()),
			FailureCategory: fc,
		}, nil
	}

	var output types.EvaluatorOutput
	if err := json.Unmarshal(stdout.Bytes(), &output); err != nil {
		return &types.EvaluatorOutput{
			Status:          types.TraitFail,
			Reason:          fmt.Sprintf("EVALUATOR_OUTPUT_INVALID: %v (stdout: %s)", err, stdout.String()),
			FailureCategory: types.FailurePermanent,
		}, nil
	}

	return &output, nil
}

// classifyExitError categorizes a subprocess execution error.
func classifyExitError(ctx context.Context, err error) types.FailureCategory {
	if ctx.Err() == context.DeadlineExceeded {
		return types.FailureTimeout
	}

	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		code := exitErr.ExitCode()
		switch {
		case code == 2:
			return types.FailurePermanent
		case code >= 128:
			return types.FailureEvaluatorCrash
		default:
			return types.FailureTransient
		}
	}

	return types.FailureTransient
}
