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
	InjectDateArgs(&triggerCfg, input.Date)

	metadata, err := d.TriggerRunner.Execute(ctx, &triggerCfg)
	if err != nil {
		errMsg := fmt.Sprintf("trigger execute: %v", err)
		// Log infra failure to joblog for audit trail, then return Lambda error
		// so Step Functions Retry handles exponential backoff.
		if writeErr := d.Store.WriteJobEvent(ctx, input.PipelineID, input.ScheduleID, input.Date, types.JobEventInfraTriggerFailure, "", 0, errMsg); writeErr != nil {
			d.Logger.WarnContext(ctx, "failed to write infra trigger failure to joblog", "error", writeErr, "pipeline", input.PipelineID)
		}
		return lambda.OrchestratorOutput{}, fmt.Errorf("%s", errMsg)
	}

	runID := ExtractRunID(metadata)

	if err := lambda.PublishEvent(ctx, d, string(types.EventJobTriggered), input.PipelineID, input.ScheduleID, input.Date, fmt.Sprintf("triggered %s job", cfg.Job.Type)); err != nil {
		d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventJobTriggered, "error", err)
	}

	// Non-polling triggers (http, command, lambda) complete synchronously
	// during Execute. Write success to joblog immediately and set a sentinel
	// runId so the Step Functions CheckJob JSONPath resolves. An empty
	// (non-nil) map carries no run identity either, so it takes the same path.
	if len(metadata) == 0 {
		if err := d.Store.WriteJobEvent(ctx, input.PipelineID, input.ScheduleID, input.Date,
			types.JobEventSuccess, "sync", 0, fmt.Sprintf("%s trigger completed synchronously", cfg.Job.Type)); err != nil {
			d.Logger.Warn("failed to write sync job success joblog", "error", err, "pipeline", input.PipelineID, "schedule", input.ScheduleID, "date", input.Date)
		}
		runID = "sync"
		metadata = map[string]interface{}{"completedSync": true}
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
// The key list is owned by the parent lambda package so the two orchestrator
// implementations cannot drift apart.
func ExtractRunID(metadata map[string]interface{}) string {
	return lambda.ExtractRunID(metadata)
}

// InjectDateArgs parses the execution date and injects --par_day (and --par_hour
// for hourly dates) into Glue trigger arguments. For HTTP triggers with no
// explicit body, injects a JSON body with par_day and par_hour.
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
		payload := map[string]string{"par_day": parDay}
		if hourPart != "" {
			payload["par_hour"] = hourPart
		}
		b, _ := json.Marshal(payload) // json.Marshal is infallible for map[string]string (no channels, funcs, or complex types)
		tc.HTTP.Body = string(b)
	}
}
