package orchestrator

import (
	"context"
	"errors"
	"fmt"

	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// StatusRetrying is complete-trigger's status when the next attempt runs;
// the ASL's IsRetrying Choice then skips CancelSLA.
const StatusRetrying = "retrying"

// handleCompleteTrigger decides what happens after a run ends, and is the
// only place that retries a failed job. Success sets COMPLETED, records the
// completion time (which starts the post-run watch window) and captures the
// post-run baseline. A fail or timeout starts the next attempt, or sets
// FAILED_FINAL when the date is excluded, the job's outcome is unknown or the
// retry budget is spent.
func handleCompleteTrigger(ctx context.Context, d *lambda.Deps, input lambda.OrchestratorInput) (lambda.OrchestratorOutput, error) {
	if input.Event != types.JobEventSuccess {
		return handleRunFailure(ctx, d, input)
	}

	if err := d.Store.MarkTriggerCompleted(ctx, input.PipelineID, input.ScheduleID, input.Date, d.Now()); err != nil {
		return lambda.OrchestratorOutput{}, fmt.Errorf("set trigger status: %w", err)
	}

	// Capture post-run baseline for drift detection.
	if err := lambda.CapturePostRunBaseline(ctx, d, input.PipelineID, input.ScheduleID, input.Date); err != nil {
		d.Logger.WarnContext(ctx, "failed to capture post-run baseline",
			"pipeline", input.PipelineID, "error", err)
		if pubErr := lambda.PublishEvent(ctx, d, string(types.EventBaselineCaptureFailed), input.PipelineID, input.ScheduleID, input.Date,
			fmt.Sprintf("baseline capture failed for %s: %v", input.PipelineID, err)); pubErr != nil {
			d.Logger.WarnContext(ctx, "failed to publish baseline capture failure event", "error", pubErr)
		}
	}

	return lambda.OrchestratorOutput{Mode: "complete-trigger", Status: types.TriggerStatusCompleted}, nil
}

// handleRunFailure applies the retry budget to a failed or timed-out run.
func handleRunFailure(ctx context.Context, d *lambda.Deps, input lambda.OrchestratorInput) (lambda.OrchestratorOutput, error) {
	pipelineID, date := input.PipelineID, input.Date
	cfg, err := lambda.GetValidatedConfig(ctx, d, pipelineID)
	if err != nil {
		return lambda.OrchestratorOutput{}, fmt.Errorf("load config for %q: %w", pipelineID, err)
	}
	if cfg == nil || cfg.DryRun {
		d.Logger.WarnContext(ctx, "no valid config, or dry-run, not retrying", "pipeline", pipelineID)
		return failFinal(ctx, d, input, "", "", nil)
	}
	if lambda.IsExcludedDate(cfg, date) {
		return failFinal(ctx, d, input, types.EventPipelineExcluded,
			fmt.Sprintf("job failure retry skipped for %s: execution date %s excluded by calendar", pipelineID, date), nil)
	}

	category, err := failureCategory(ctx, d, input)
	if err != nil {
		return lambda.OrchestratorOutput{}, err
	}
	maxRetries := cfg.Job.MaxRetries
	switch category {
	case types.FailurePermanent:
		maxRetries = types.IntOrDefault(cfg.Job.MaxCodeRetries, 1)
	case types.FailureOutcomeUnknown:
		d.Logger.WarnContext(ctx, "job outcome unknown, not retrying", "pipeline", pipelineID, "runId", input.RunID)
		return failFinal(ctx, d, input, "", "", nil)
	}
	retries := lambda.RetriesSoFar(input.ExecutionName)
	if retries >= maxRetries {
		return failFinal(ctx, d, input, types.EventRetryExhausted,
			fmt.Sprintf("retry limit reached (%d/%d) for %s", retries, maxRetries, pipelineID),
			map[string]interface{}{"retries": retries, "maxRetries": maxRetries, "runId": input.RunID})
	}
	return startRetry(ctx, d, cfg, input)
}

// startRetry records the rerun, starts the next execution and then keeps the
// trigger row's lock. Each step is idempotent, so the Task's Retry may run it
// again: a repeated start gets ExecutionAlreadyExists.
func startRetry(ctx context.Context, d *lambda.Deps, cfg *types.PipelineConfig, input lambda.OrchestratorInput) (lambda.OrchestratorOutput, error) {
	pipelineID, schedule, date := input.PipelineID, input.ScheduleID, input.Date
	if input.ExecutionName == "" {
		return lambda.OrchestratorOutput{}, fmt.Errorf("retry %q/%s/%s: executionName is required", pipelineID, schedule, date)
	}
	next := lambda.NextExecName(input.ExecutionName)
	if _, err := d.Store.WriteRerunOnce(ctx, pipelineID, schedule, date, next, "job-fail-retry", input.Event); err != nil {
		return lambda.OrchestratorOutput{}, fmt.Errorf("write rerun for %q: %w", pipelineID, err)
	}

	started := true
	if err := lambda.StartSFNWithName(ctx, d, cfg, pipelineID, schedule, date, next); err != nil {
		var exists *sfntypes.ExecutionAlreadyExists
		if !errors.As(err, &exists) {
			return startFailed(ctx, d, input, next, err)
		}
		started = false
	}
	if err := holdLock(ctx, d, input, started); err != nil {
		return lambda.OrchestratorOutput{}, err
	}
	d.Logger.InfoContext(ctx, "retry running", "pipeline", pipelineID, "schedule", schedule, "date", date,
		"execution", next, "startedNow", started)
	return lambda.OrchestratorOutput{Mode: "complete-trigger", Status: StatusRetrying, NextExecution: next}, nil
}

// startFailed ends the run after the next execution could not start (the SDK
// has already retried throttling): FAILED_FINAL, then INFRA_FAILURE. A
// COMPLETED row means an earlier, lost invocation started the next attempt
// and it already succeeded, so the row is kept and the chain is left to it.
func startFailed(ctx context.Context, d *lambda.Deps, input lambda.OrchestratorInput, next string, startErr error) (lambda.OrchestratorOutput, error) {
	written, err := d.Store.FailTriggerUnlessCompleted(ctx, input.PipelineID, input.ScheduleID, input.Date)
	if err != nil {
		return lambda.OrchestratorOutput{}, fmt.Errorf("set trigger status: %w", err)
	}
	if !written {
		d.Logger.InfoContext(ctx, "retry did not start but the trigger row is COMPLETED, left alone",
			"pipeline", input.PipelineID, "execution", next, "error", startErr)
		return lambda.OrchestratorOutput{Mode: "complete-trigger", Status: StatusRetrying, NextExecution: next}, nil
	}
	if err := lambda.PublishEvent(ctx, d, string(types.EventInfraFailure), input.PipelineID, input.ScheduleID, input.Date,
		fmt.Sprintf("retry %s for %s did not start: %v", next, input.PipelineID, startErr),
		map[string]interface{}{"execution": next, "error": startErr.Error()}); err != nil {
		d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventInfraFailure, "error", err)
	}
	return lambda.OrchestratorOutput{Mode: "complete-trigger", Status: types.TriggerStatusFailedFinal}, nil
}

// holdLock gives a RUNNING trigger row a fresh stale deadline (staleAt) for
// the next attempt. A
// finished row is left alone (a Task retry can run after the next attempt
// ended); a missing row is recreated only by the invocation that started it.
func holdLock(ctx context.Context, d *lambda.Deps, input lambda.OrchestratorInput, started bool) error {
	pipelineID, schedule, date := input.PipelineID, input.ScheduleID, input.Date
	staleAfter := lambda.ResolveTriggerLockTTL()
	held, err := d.Store.RefreshRunningTriggerLock(ctx, pipelineID, schedule, date, staleAfter)
	if err != nil {
		return fmt.Errorf("refresh trigger lock for %q: %w", pipelineID, err)
	}
	if !held && started {
		if held, err = d.Store.AcquireTriggerLock(ctx, pipelineID, schedule, date, staleAfter); err != nil {
			return fmt.Errorf("acquire trigger lock for %q: %w", pipelineID, err)
		}
	}
	if !held {
		d.Logger.InfoContext(ctx, "trigger row is not RUNNING, left alone", "pipeline", pipelineID, "schedule", schedule, "date", date)
	}
	return nil
}

// failureCategory reads the run's own terminal row; no row counts as
// transient. A failed lookup is an error, which the Task's retry reads again.
func failureCategory(ctx context.Context, d *lambda.Deps, input lambda.OrchestratorInput) (types.FailureCategory, error) {
	rec, err := d.Store.GetJobResult(ctx, input.PipelineID, input.ScheduleID, input.Date, input.RunID)
	if err != nil {
		return "", fmt.Errorf("read failure category: %w", err)
	}
	if rec == nil {
		return types.FailureTransient, nil
	}
	return types.FailureCategory(rec.Category), nil
}

// failFinal sets FAILED_FINAL and then publishes eventType, if any. Writing
// first means a Task retry after a failed write does not alert twice.
func failFinal(ctx context.Context, d *lambda.Deps, input lambda.OrchestratorInput,
	eventType types.EventDetailType, msg string, detail map[string]interface{}) (lambda.OrchestratorOutput, error) {
	if err := d.Store.SetTriggerStatus(ctx, input.PipelineID, input.ScheduleID, input.Date, types.TriggerStatusFailedFinal); err != nil {
		return lambda.OrchestratorOutput{}, fmt.Errorf("set trigger status: %w", err)
	}
	if eventType != "" {
		if err := lambda.PublishEvent(ctx, d, string(eventType), input.PipelineID, input.ScheduleID, input.Date, msg, detail); err != nil {
			d.Logger.WarnContext(ctx, "failed to publish event", "type", eventType, "error", err)
		}
	}
	return lambda.OrchestratorOutput{Mode: "complete-trigger", Status: types.TriggerStatusFailedFinal}, nil
}
