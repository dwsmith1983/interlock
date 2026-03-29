// Package orchestrator implements the multi-mode orchestrator Lambda handler.
// It dispatches to evaluate, trigger, check-job, and lifecycle management modes
// delegated by the Step Function state machine.
package orchestrator

import (
	"context"
	"fmt"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
)

// HandleOrchestrator is the entry point for the orchestrator Lambda.
// It dispatches to one of five modes: evaluate, trigger, check-job, post-run, validation-exhausted.
func HandleOrchestrator(ctx context.Context, d *lambda.Deps, input lambda.OrchestratorInput) (lambda.OrchestratorOutput, error) {
	switch input.Mode {
	case "evaluate":
		return handleEvaluate(ctx, d, input)
	case "trigger":
		return handleTrigger(ctx, d, input)
	case "check-job":
		return handleCheckJob(ctx, d, input)
	case "validation-exhausted":
		return handleValidationExhausted(ctx, d, input)
	case "trigger-exhausted":
		return handleTriggerExhausted(ctx, d, input)
	case "complete-trigger":
		return handleCompleteTrigger(ctx, d, input)
	case "job-poll-exhausted":
		return handleJobPollExhausted(ctx, d, input)
	default:
		return lambda.OrchestratorOutput{}, fmt.Errorf("unknown orchestrator mode: %q", input.Mode)
	}
}
