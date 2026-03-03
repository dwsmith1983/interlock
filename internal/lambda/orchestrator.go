package lambda

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/dwsmith1983/interlock/internal/validation"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// HandleOrchestrator is the entry point for the orchestrator Lambda.
// It dispatches to one of five modes: evaluate, trigger, check-job, post-run, validation-exhausted.
func HandleOrchestrator(ctx context.Context, d *Deps, input OrchestratorInput) (OrchestratorOutput, error) {
	switch input.Mode {
	case "evaluate":
		return handleEvaluate(ctx, d, input)
	case "trigger":
		return handleTrigger(ctx, d, input)
	case "check-job":
		return handleCheckJob(ctx, d, input)
	case "post-run":
		return handlePostRun(ctx, d, input)
	case "validation-exhausted":
		return handleValidationExhausted(ctx, d, input)
	default:
		return OrchestratorOutput{}, fmt.Errorf("unknown orchestrator mode: %q", input.Mode)
	}
}

// handleEvaluate fetches config and sensors, evaluates validation rules, and
// optionally publishes a VALIDATION_PASSED event.
func handleEvaluate(ctx context.Context, d *Deps, input OrchestratorInput) (OrchestratorOutput, error) {
	cfg, err := d.Store.GetConfig(ctx, input.PipelineID)
	if err != nil {
		return OrchestratorOutput{Mode: "evaluate", Error: err.Error()}, nil
	}
	if cfg == nil {
		return OrchestratorOutput{Mode: "evaluate", Error: fmt.Sprintf("config not found for pipeline %q", input.PipelineID)}, nil
	}

	sensors, err := d.Store.GetAllSensors(ctx, input.PipelineID)
	if err != nil {
		return OrchestratorOutput{Mode: "evaluate", Error: err.Error()}, nil
	}

	result := validation.EvaluateRules(cfg.Validation.Trigger, cfg.Validation.Rules, sensors, time.Now())

	if result.Passed {
		_ = publishEvent(ctx, d, string(types.EventValidationPassed), input.PipelineID, input.ScheduleID, input.Date, "all validation rules passed")
	}

	status := "not_ready"
	if result.Passed {
		status = "passed"
	}

	return OrchestratorOutput{
		Mode:    "evaluate",
		Status:  status,
		Results: result.Results,
	}, nil
}

// handleTrigger builds a TriggerConfig from the JobConfig, executes it,
// publishes JOB_TRIGGERED, and returns the run ID.
func handleTrigger(ctx context.Context, d *Deps, input OrchestratorInput) (OrchestratorOutput, error) {
	cfg, err := d.Store.GetConfig(ctx, input.PipelineID)
	if err != nil {
		return OrchestratorOutput{Mode: "trigger", Error: err.Error()}, nil
	}
	if cfg == nil {
		return OrchestratorOutput{Mode: "trigger", Error: fmt.Sprintf("config not found for pipeline %q", input.PipelineID)}, nil
	}

	triggerCfg, err := buildTriggerConfig(cfg.Job)
	if err != nil {
		return OrchestratorOutput{Mode: "trigger", Error: fmt.Sprintf("build trigger config: %v", err)}, nil
	}
	injectDateArgs(&triggerCfg, input.Date)

	metadata, err := d.TriggerRunner.Execute(ctx, &triggerCfg)
	if err != nil {
		errMsg := fmt.Sprintf("trigger execute: %v", err)
		// Log infra failure to joblog for audit trail, then return Lambda error
		// so Step Functions Retry handles exponential backoff.
		if writeErr := d.Store.WriteJobEvent(ctx, input.PipelineID, input.ScheduleID, input.Date, types.JobEventInfraTriggerFailure, "", 0, errMsg); writeErr != nil {
			d.Logger.WarnContext(ctx, "failed to write infra trigger failure to joblog", "error", writeErr, "pipeline", input.PipelineID)
		}
		return OrchestratorOutput{}, fmt.Errorf("%s", errMsg)
	}

	runID := extractRunID(metadata)

	_ = publishEvent(ctx, d, string(types.EventJobTriggered), input.PipelineID, input.ScheduleID, input.Date, fmt.Sprintf("triggered %s job", cfg.Job.Type))

	return OrchestratorOutput{
		Mode:     "trigger",
		RunID:    runID,
		JobType:  string(cfg.Job.Type),
		Metadata: metadata,
	}, nil
}

// handleCheckJob queries the job log for the latest event. If no event exists
// and a StatusChecker is configured, it polls the trigger API directly and
// writes terminal results (succeeded/failed) to the job log.
func handleCheckJob(ctx context.Context, d *Deps, input OrchestratorInput) (OrchestratorOutput, error) {
	record, err := d.Store.GetLatestJobEvent(ctx, input.PipelineID, input.ScheduleID, input.Date)
	if err != nil {
		return OrchestratorOutput{Mode: "check-job", Error: err.Error()}, nil
	}

	if record != nil {
		// Only return terminal events; skip intermediate events like
		// infra-trigger-failure so the StatusChecker can poll actual job status.
		switch record.Event {
		case types.JobEventSuccess, types.JobEventFail, types.JobEventTimeout:
			return OrchestratorOutput{
				Mode:  "check-job",
				Event: record.Event,
			}, nil
		}
	}

	// No terminal joblog entry — try polling the trigger API directly.
	if d.StatusChecker == nil || len(input.Metadata) == 0 {
		return OrchestratorOutput{Mode: "check-job"}, nil
	}

	cfg, err := d.Store.GetConfig(ctx, input.PipelineID)
	if err != nil {
		return OrchestratorOutput{Mode: "check-job", Error: err.Error()}, nil
	}
	if cfg == nil {
		return OrchestratorOutput{Mode: "check-job"}, nil
	}

	result, err := d.StatusChecker.CheckStatus(ctx, cfg.Job.Type, input.Metadata, nil)
	if err != nil {
		d.Logger.WarnContext(ctx, "status check failed", "error", err, "pipeline", input.PipelineID)
		return OrchestratorOutput{Mode: "check-job"}, nil
	}

	switch result.State {
	case "succeeded":
		_ = d.Store.WriteJobEvent(ctx, input.PipelineID, input.ScheduleID, input.Date, types.JobEventSuccess, input.RunID, 0, "")
		_ = publishEvent(ctx, d, string(types.EventJobCompleted), input.PipelineID, input.ScheduleID, input.Date, "job succeeded")
		return OrchestratorOutput{Mode: "check-job", Event: "success"}, nil
	case "failed":
		_ = d.Store.WriteJobEvent(ctx, input.PipelineID, input.ScheduleID, input.Date, types.JobEventFail, input.RunID, 0, result.Message)
		_ = publishEvent(ctx, d, string(types.EventJobFailed), input.PipelineID, input.ScheduleID, input.Date, "job failed: "+result.Message)
		return OrchestratorOutput{Mode: "check-job", Event: "fail"}, nil
	default:
		// Still running — return no event so SFN loops back to WaitForJob.
		return OrchestratorOutput{Mode: "check-job"}, nil
	}
}

// handlePostRun evaluates post-run validation rules if configured.
func handlePostRun(ctx context.Context, d *Deps, input OrchestratorInput) (OrchestratorOutput, error) {
	cfg, err := d.Store.GetConfig(ctx, input.PipelineID)
	if err != nil {
		return OrchestratorOutput{Mode: "post-run", Error: err.Error()}, nil
	}
	if cfg == nil {
		return OrchestratorOutput{Mode: "post-run", Error: fmt.Sprintf("config not found for pipeline %q", input.PipelineID)}, nil
	}

	if cfg.PostRun == nil || len(cfg.PostRun.Rules) == 0 {
		return OrchestratorOutput{Mode: "post-run", Status: "passed"}, nil
	}

	sensors, err := d.Store.GetAllSensors(ctx, input.PipelineID)
	if err != nil {
		return OrchestratorOutput{Mode: "post-run", Error: err.Error()}, nil
	}

	result := validation.EvaluateRules("ALL", cfg.PostRun.Rules, sensors, time.Now())

	status := "not_ready"
	if result.Passed {
		status = "passed"
	}

	return OrchestratorOutput{
		Mode:    "post-run",
		Status:  status,
		Results: result.Results,
	}, nil
}

// handleValidationExhausted publishes a VALIDATION_EXHAUSTED event when
// the evaluation window closes without all rules passing.
func handleValidationExhausted(ctx context.Context, d *Deps, input OrchestratorInput) (OrchestratorOutput, error) {
	_ = publishEvent(ctx, d, string(types.EventValidationExhausted), input.PipelineID, input.ScheduleID, input.Date, "evaluation window exhausted without passing")

	return OrchestratorOutput{
		Mode:   "validation-exhausted",
		Status: "exhausted",
	}, nil
}

// buildTriggerConfig converts a JobConfig into a TriggerConfig by
// JSON-marshalling the config map and unmarshalling it into the typed sub-struct.
func buildTriggerConfig(job types.JobConfig) (types.TriggerConfig, error) {
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
	default:
		return tc, fmt.Errorf("unsupported trigger type: %s", job.Type)
	}

	return tc, nil
}

// extractRunID searches trigger metadata for a recognisable run identifier.
func extractRunID(metadata map[string]interface{}) string {
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

// injectDateArgs parses the execution date and injects --par_day (and --par_hour
// for hourly dates) into Glue trigger arguments. Only modifies Glue triggers.
func injectDateArgs(tc *types.TriggerConfig, date string) {
	if tc.Glue == nil {
		return
	}
	if tc.Glue.Arguments == nil {
		tc.Glue.Arguments = make(map[string]string)
	}

	datePart, hourPart := ParseExecutionDate(date)
	// Convert YYYY-MM-DD to YYYYMMDD for Glue.
	parDay := strings.ReplaceAll(datePart, "-", "")
	tc.Glue.Arguments["--par_day"] = parDay
	if hourPart != "" {
		tc.Glue.Arguments["--par_hour"] = hourPart
	}
}

// ParseExecutionDate splits a composite date into date and hour parts.
// "2026-03-03T10" -> ("2026-03-03", "10")
// "2026-03-03"    -> ("2026-03-03", "")
func ParseExecutionDate(date string) (datePart, hourPart string) {
	if idx := strings.Index(date, "T"); idx >= 0 {
		return date[:idx], date[idx+1:]
	}
	return date, ""
}
