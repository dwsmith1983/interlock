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
	cfg, err := d.Store.GetConfig(ctx, input.PipelineID)
	if err != nil {
		return lambda.OrchestratorOutput{Mode: "trigger", Error: err.Error()}, nil
	}
	if cfg == nil {
		return lambda.OrchestratorOutput{Mode: "trigger", Error: fmt.Sprintf("config not found for pipeline %q", input.PipelineID)}, nil
	}

	triggerCfg, err := BuildTriggerConfig(cfg.Job)
	if err != nil {
		return lambda.OrchestratorOutput{Mode: "trigger", Error: fmt.Sprintf("build trigger config: %v", err)}, nil
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
	// runId so the Step Functions CheckJob JSONPath resolves.
	if metadata == nil {
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
func BuildTriggerConfig(job types.JobConfig) (types.TriggerConfig, error) {
	tc := types.TriggerConfig{Type: job.Type}

	if len(job.Config) == 0 {
		return tc, nil
	}

	data, err := json.Marshal(job.Config)
	if err != nil {
		return tc, fmt.Errorf("marshal job config: %w", err)
	}

	switch job.Type {
	case types.TriggerHTTP:
		var c types.HTTPTriggerConfig
		if err := json.Unmarshal(data, &c); err != nil {
			return tc, fmt.Errorf("unmarshal http config: %w", err)
		}
		tc.HTTP = &c
	case types.TriggerCommand:
		var c types.CommandTriggerConfig
		if err := json.Unmarshal(data, &c); err != nil {
			return tc, fmt.Errorf("unmarshal command config: %w", err)
		}
		tc.Command = &c
	case types.TriggerAirflow:
		var c types.AirflowTriggerConfig
		if err := json.Unmarshal(data, &c); err != nil {
			return tc, fmt.Errorf("unmarshal airflow config: %w", err)
		}
		tc.Airflow = &c
	case types.TriggerGlue:
		var c types.GlueTriggerConfig
		if err := json.Unmarshal(data, &c); err != nil {
			return tc, fmt.Errorf("unmarshal glue config: %w", err)
		}
		tc.Glue = &c
	case types.TriggerEMR:
		var c types.EMRTriggerConfig
		if err := json.Unmarshal(data, &c); err != nil {
			return tc, fmt.Errorf("unmarshal emr config: %w", err)
		}
		tc.EMR = &c
	case types.TriggerEMRServerless:
		var c types.EMRServerlessTriggerConfig
		if err := json.Unmarshal(data, &c); err != nil {
			return tc, fmt.Errorf("unmarshal emr-serverless config: %w", err)
		}
		tc.EMRServerless = &c
	case types.TriggerStepFunction:
		var c types.StepFunctionTriggerConfig
		if err := json.Unmarshal(data, &c); err != nil {
			return tc, fmt.Errorf("unmarshal step-function config: %w", err)
		}
		tc.StepFunction = &c
	case types.TriggerDatabricks:
		var c types.DatabricksTriggerConfig
		if err := json.Unmarshal(data, &c); err != nil {
			return tc, fmt.Errorf("unmarshal databricks config: %w", err)
		}
		tc.Databricks = &c
	case types.TriggerLambda:
		var c types.LambdaTriggerConfig
		if err := json.Unmarshal(data, &c); err != nil {
			return tc, fmt.Errorf("unmarshal lambda config: %w", err)
		}
		tc.Lambda = &c
	default:
		return tc, fmt.Errorf("unsupported trigger type: %s", job.Type)
	}

	return tc, nil
}

// ExtractRunID searches trigger metadata for a recognisable run identifier.
func ExtractRunID(metadata map[string]interface{}) string {
	if metadata == nil {
		return ""
	}
	// Priority order of common identifier keys across trigger types.
	for _, key := range []string{"runId", "jobRunId", "glue_job_run_id", "executionArn", "stepId", "dagRunId"} {
		if v, ok := metadata[key]; ok {
			if s, ok := v.(string); ok && s != "" {
				return s
			}
		}
	}
	return ""
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
