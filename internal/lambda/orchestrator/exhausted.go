package orchestrator

import (
	"context"
	"fmt"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// handleValidationExhausted publishes a VALIDATION_EXHAUSTED event when
// the evaluation window closes without all rules passing.
func handleValidationExhausted(ctx context.Context, d *lambda.Deps, input lambda.OrchestratorInput) (lambda.OrchestratorOutput, error) {
	if err := d.Store.WriteJobEvent(ctx, input.PipelineID, input.ScheduleID, input.Date, types.JobEventValidationExhausted, "", 0, "evaluation window exhausted without passing"); err != nil {
		return lambda.OrchestratorOutput{}, fmt.Errorf("write validation-exhausted joblog: %w", err)
	}

	if err := lambda.PublishEvent(ctx, d, string(types.EventValidationExhausted), input.PipelineID, input.ScheduleID, input.Date, "evaluation window exhausted without passing"); err != nil {
		d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventValidationExhausted, "error", err)
	}

	return lambda.OrchestratorOutput{
		Mode:   "validation-exhausted",
		Status: "exhausted",
	}, nil
}

// handleJobPollExhausted publishes a JOB_POLL_EXHAUSTED event when the
// job poll window closes without the job reaching a terminal state.
func handleJobPollExhausted(ctx context.Context, d *lambda.Deps, input lambda.OrchestratorInput) (lambda.OrchestratorOutput, error) {
	if err := d.Store.WriteJobEvent(ctx, input.PipelineID, input.ScheduleID, input.Date,
		types.JobEventJobPollExhausted, input.RunID, 0, "job poll window exhausted"); err != nil {
		return lambda.OrchestratorOutput{}, fmt.Errorf("write job-poll-exhausted joblog: %w", err)
	}

	if err := lambda.PublishEvent(ctx, d, string(types.EventJobPollExhausted), input.PipelineID, input.ScheduleID, input.Date,
		"job poll window exhausted"); err != nil {
		d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventJobPollExhausted, "error", err)
	}

	return lambda.OrchestratorOutput{
		Mode:   "job-poll-exhausted",
		Status: "exhausted",
	}, nil
}

// handleTriggerExhausted publishes RETRY_EXHAUSTED when trigger retries are
// exhausted, writes a joblog entry for audit, and releases the trigger lock
// so the pipeline can be re-triggered.
func handleTriggerExhausted(ctx context.Context, d *lambda.Deps, input lambda.OrchestratorInput) (lambda.OrchestratorOutput, error) {
	errMsg := ""
	if cause, ok := input.ErrorInfo["Cause"].(string); ok {
		errMsg = cause
	}

	// Dual-write: joblog entry (audit) + EventBridge event (alerting).
	if err := d.Store.WriteJobEvent(ctx, input.PipelineID, input.ScheduleID, input.Date,
		types.JobEventInfraTriggerExhausted, "", 0, errMsg); err != nil {
		return lambda.OrchestratorOutput{}, fmt.Errorf("write trigger-exhausted joblog: %w", err)
	}

	if err := lambda.PublishEvent(ctx, d, string(types.EventRetryExhausted), input.PipelineID, input.ScheduleID, input.Date,
		fmt.Sprintf("trigger retries exhausted for %s: %s", input.PipelineID, errMsg)); err != nil {
		d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventRetryExhausted, "error", err)
	}

	// Release lock so pipeline can be re-triggered.
	if err := d.Store.ReleaseTriggerLock(ctx, input.PipelineID, input.ScheduleID, input.Date); err != nil {
		d.Logger.WarnContext(ctx, "failed to release trigger lock after exhaustion",
			"pipeline", input.PipelineID, "error", err)
	}

	return lambda.OrchestratorOutput{
		Mode:   "trigger-exhausted",
		Status: "exhausted",
	}, nil
}
