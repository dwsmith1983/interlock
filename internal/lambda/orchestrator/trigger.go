package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// handleTrigger builds a TriggerConfig from the JobConfig, executes it,
// publishes JOB_TRIGGERED, and returns the run ID.
func handleTrigger(ctx context.Context, d *lambda.Deps, input lambda.OrchestratorInput) (lambda.OrchestratorOutput, error) {
	// Configuration failures return a Lambda error rather than a partial
	// result: the HasTriggerResult Choice tests IsPresent on $.triggerResult,
	// so a partial result would send the execution to CheckJob, which
	// dereferences $.triggerResult.runId and raises an uncatchable
	// States.Runtime while the trigger lock stays RUNNING. A Lambda error is
	// retried by the Trigger state and then caught by TriggerRetryExhausted,
	// which releases the lock.
	cfg, err := d.Store.GetConfig(ctx, input.PipelineID)
	if err != nil {
		return lambda.OrchestratorOutput{}, fmt.Errorf("trigger get config: %w", err)
	}
	if cfg == nil {
		return lambda.OrchestratorOutput{}, fmt.Errorf("trigger: config not found for pipeline %q", input.PipelineID)
	}

	triggerCfg, err := BuildTriggerConfig(cfg.Job)
	if err != nil {
		return lambda.OrchestratorOutput{}, fmt.Errorf("trigger build config: %w", err)
	}

	// The execution name identifies this run; a job that reports its own
	// result writes it as the runId of its JOB# row.
	runID := input.ExecutionName
	if runID == "" {
		runID = fmt.Sprintf("%s-%s-%s-%d", input.PipelineID, input.ScheduleID, input.Date, d.Now().UnixMilli())
	}
	if triggerCfg.HTTP != nil {
		triggerCfg.HTTP.Run = types.RunIdentity{
			PipelineID: input.PipelineID, ScheduleID: input.ScheduleID, Date: input.Date, RunID: runID,
		}
	}
	InjectDateArgs(&triggerCfg, input.Date)

	metadata, err := d.TriggerRunner.Execute(ctx, &triggerCfg)
	if err != nil {
		errMsg := fmt.Sprintf("trigger execute: %v", err)
		// Log infra failure to joblog for audit trail, then return Lambda error
		// so Step Functions Retry handles exponential backoff.
		if writeErr := d.Store.WriteJobEvent(ctx, input.PipelineID, input.ScheduleID, input.Date, types.JobEventInfraTriggerFailure, runID, 0, errMsg); writeErr != nil {
			d.Logger.WarnContext(ctx, "failed to write infra trigger failure to joblog", "error", writeErr, "pipeline", input.PipelineID)
		}
		return lambda.OrchestratorOutput{}, fmt.Errorf("%s", errMsg)
	}

	if err := lambda.PublishEvent(ctx, d, string(types.EventJobTriggered), input.PipelineID, input.ScheduleID, input.Date, fmt.Sprintf("triggered %s job", cfg.Job.Type)); err != nil {
		d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventJobTriggered, "error", err)
	}

	switch cfg.Job.Type {
	case types.TriggerHTTP:
		// The job reports its own result: the run stays RUNNING until a
		// JOB# row for runID appears or the poll window closes.
	case types.TriggerCommand, types.TriggerLambda:
		if err := d.Store.WriteJobEvent(ctx, input.PipelineID, input.ScheduleID, input.Date,
			types.JobEventSuccess, runID, 0, fmt.Sprintf("%s trigger completed synchronously", cfg.Job.Type)); err != nil {
			d.Logger.Warn("failed to write job success joblog", "error", err, "pipeline", input.PipelineID, "schedule", input.ScheduleID, "date", input.Date)
		}
	default:
		if id := ExtractRunID(metadata); id != "" {
			runID = id
		}
	}

	// CheckJob dereferences $.triggerResult.metadata, so it must be an object.
	if metadata == nil {
		metadata = map[string]interface{}{}
	}

	return lambda.OrchestratorOutput{
		Mode:     "trigger",
		RunID:    runID,
		JobType:  string(cfg.Job.Type),
		Metadata: metadata,
	}, nil
}

// BuildTriggerConfig converts a JobConfig into a TriggerConfig by
// JSON-marshalling the config map and unmarshalling it into the typed sub-struct.
// It delegates to the canonical TriggerUnmarshalers registry in the parent
// lambda package to avoid duplication.
func BuildTriggerConfig(job types.JobConfig) (types.TriggerConfig, error) {
	tc := types.TriggerConfig{Type: job.Type}

	if len(job.Config) == 0 {
		return tc, nil
	}

	data, err := json.Marshal(job.Config)
	if err != nil {
		return tc, fmt.Errorf("marshal job config: %w", err)
	}

	unmarshal, ok := lambda.TriggerUnmarshalers[job.Type]
	if !ok {
		return tc, fmt.Errorf("unsupported trigger type: %s", job.Type)
	}
	if err := unmarshal(data, &tc); err != nil {
		return tc, fmt.Errorf("unmarshal %s config: %w", job.Type, err)
	}

	return tc, nil
}

// ExtractRunID searches trigger metadata for a recognisable run identifier.
// The key list lives in the parent lambda package and is shared with the
// deprecated root orchestrator, but the rest of that root copy is not kept in
// step with this package.
func ExtractRunID(metadata map[string]interface{}) string {
	return lambda.ExtractRunID(metadata)
}

// InjectDateArgs parses the execution date and injects --par_day (and --par_hour
// for hourly dates) into Glue trigger arguments. For HTTP triggers with no
// explicit body, injects a JSON body with par_day, par_hour (hourly dates
// only) and the run identity.
func InjectDateArgs(tc *types.TriggerConfig, date string) {
	datePart, hourPart := lambda.ParseExecutionDate(date)
	parDay := strings.ReplaceAll(datePart, "-", "")

	if tc.Glue != nil {
		if tc.Glue.Arguments == nil {
			tc.Glue.Arguments = make(map[string]string)
		}
		tc.Glue.Arguments["--par_day"] = parDay
		if hourPart != "" {
			tc.Glue.Arguments["--par_hour"] = hourPart
		}
	}

	if tc.HTTP != nil && tc.HTTP.Body == "" {
		run := tc.HTTP.Run
		payload := map[string]string{
			"par_day": parDay, "pipelineId": run.PipelineID, "scheduleId": run.ScheduleID, "date": run.Date, "runId": run.RunID,
		}
		if hourPart != "" {
			payload["par_hour"] = hourPart
		}
		b, _ := json.Marshal(payload) // json.Marshal is infallible for map[string]string (no channels, funcs, or complex types)
		tc.HTTP.Body = string(b)
		tc.HTTP.BodyGenerated = true
	}
}
