package lambda

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/dwsmith1983/interlock/internal/store"
	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/dwsmith1983/interlock/pkg/validation"
)

// HandleOrchestrator is the entry point for the orchestrator Lambda.
// Deprecated: Production callers should use orchestrator.HandleOrchestrator.
// Retained for backward compatibility with existing tests.
func HandleOrchestrator(ctx context.Context, d *Deps, input OrchestratorInput) (OrchestratorOutput, error) {
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

	RemapPerPeriodSensors(sensors, input.Date)

	result := validation.EvaluateRules(cfg.Validation.Trigger, cfg.Validation.Rules, sensors, d.Now())

	if result.Passed {
		if err := PublishEvent(ctx, d, string(types.EventValidationPassed), input.PipelineID, input.ScheduleID, input.Date, "all validation rules passed"); err != nil {
			d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventValidationPassed, "error", err)
		}
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
	InjectDateArgs(&triggerCfg, input.Date)

	metadata, err := d.TriggerRunner.Execute(ctx, &triggerCfg)
	if err != nil {
		errMsg := fmt.Sprintf("trigger execute: %v", err)
		if writeErr := d.Store.WriteJobEvent(ctx, input.PipelineID, input.ScheduleID, input.Date, types.JobEventInfraTriggerFailure, "", 0, errMsg); writeErr != nil {
			d.Logger.WarnContext(ctx, "failed to write infra trigger failure to joblog", "error", writeErr, "pipeline", input.PipelineID)
		}
		return OrchestratorOutput{}, fmt.Errorf("%s", errMsg)
	}

	runID := extractRunID(metadata)

	if err := PublishEvent(ctx, d, string(types.EventJobTriggered), input.PipelineID, input.ScheduleID, input.Date, fmt.Sprintf("triggered %s job", cfg.Job.Type)); err != nil {
		d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventJobTriggered, "error", err)
	}

	if metadata == nil {
		if err := d.Store.WriteJobEvent(ctx, input.PipelineID, input.ScheduleID, input.Date,
			types.JobEventSuccess, "sync", 0, fmt.Sprintf("%s trigger completed synchronously", cfg.Job.Type)); err != nil {
			d.Logger.Warn("failed to write sync job success joblog", "error", err, "pipeline", input.PipelineID, "schedule", input.ScheduleID, "date", input.Date)
		}
		runID = "sync"
		metadata = map[string]interface{}{"completedSync": true}
	}

	return OrchestratorOutput{
		Mode:     "trigger",
		RunID:    runID,
		JobType:  string(cfg.Job.Type),
		Metadata: metadata,
	}, nil
}

// handleCheckJob queries the job log for the latest event.
func handleCheckJob(ctx context.Context, d *Deps, input OrchestratorInput) (OrchestratorOutput, error) {
	record, err := d.Store.GetLatestJobEvent(ctx, input.PipelineID, input.ScheduleID, input.Date)
	if err != nil {
		return OrchestratorOutput{Mode: "check-job", Error: err.Error()}, nil
	}

	if record != nil {
		switch record.Event {
		case types.JobEventSuccess, types.JobEventFail, types.JobEventTimeout:
			return OrchestratorOutput{
				Mode:  "check-job",
				Event: record.Event,
			}, nil
		}
	}

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
		if err := d.Store.WriteJobEvent(ctx, input.PipelineID, input.ScheduleID, input.Date, types.JobEventSuccess, input.RunID, 0, ""); err != nil {
			d.Logger.Warn("failed to write polled job success joblog", "error", err, "pipeline", input.PipelineID, "schedule", input.ScheduleID, "date", input.Date)
		}
		return OrchestratorOutput{Mode: "check-job", Event: "success"}, nil
	case "failed":
		var writeOpts []store.JobEventOption
		if result.FailureCategory != "" {
			writeOpts = append(writeOpts, store.WithFailureCategory(result.FailureCategory))
		}
		if err := d.Store.WriteJobEvent(ctx, input.PipelineID, input.ScheduleID, input.Date, types.JobEventFail, input.RunID, 0, result.Message, writeOpts...); err != nil {
			d.Logger.Warn("failed to write polled job failure joblog", "error", err, "pipeline", input.PipelineID, "schedule", input.ScheduleID, "date", input.Date)
		}
		if err := PublishEvent(ctx, d, string(types.EventJobFailed), input.PipelineID, input.ScheduleID, input.Date, "job failed: "+result.Message); err != nil {
			d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventJobFailed, "error", err)
		}
		return OrchestratorOutput{Mode: "check-job", Event: "fail"}, nil
	default:
		return OrchestratorOutput{Mode: "check-job"}, nil
	}
}

// ExtractFloat retrieves a numeric value from a sensor data map, handling both
// float64 (native JSON) and string representations.
func ExtractFloat(data map[string]interface{}, key string) float64 {
	v, ok := data[key]
	if !ok {
		return 0
	}
	switch n := v.(type) {
	case float64:
		return n
	case string:
		f, _ := strconv.ParseFloat(n, 64)
		return f
	default:
		return 0
	}
}

// handleValidationExhausted publishes a VALIDATION_EXHAUSTED event when
// the evaluation window closes without all rules passing.
func handleValidationExhausted(ctx context.Context, d *Deps, input OrchestratorInput) (OrchestratorOutput, error) {
	if err := d.Store.WriteJobEvent(ctx, input.PipelineID, input.ScheduleID, input.Date, types.JobEventValidationExhausted, "", 0, "evaluation window exhausted without passing"); err != nil {
		return OrchestratorOutput{}, fmt.Errorf("write validation-exhausted joblog: %w", err)
	}

	if err := PublishEvent(ctx, d, string(types.EventValidationExhausted), input.PipelineID, input.ScheduleID, input.Date, "evaluation window exhausted without passing"); err != nil {
		d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventValidationExhausted, "error", err)
	}

	return OrchestratorOutput{
		Mode:   "validation-exhausted",
		Status: "exhausted",
	}, nil
}

// handleJobPollExhausted publishes a JOB_POLL_EXHAUSTED event when the
// job poll window closes without the job reaching a terminal state.
func handleJobPollExhausted(ctx context.Context, d *Deps, input OrchestratorInput) (OrchestratorOutput, error) {
	if err := d.Store.WriteJobEvent(ctx, input.PipelineID, input.ScheduleID, input.Date,
		types.JobEventJobPollExhausted, input.RunID, 0, "job poll window exhausted"); err != nil {
		return OrchestratorOutput{}, fmt.Errorf("write job-poll-exhausted joblog: %w", err)
	}

	if err := PublishEvent(ctx, d, string(types.EventJobPollExhausted), input.PipelineID, input.ScheduleID, input.Date,
		"job poll window exhausted"); err != nil {
		d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventJobPollExhausted, "error", err)
	}

	return OrchestratorOutput{
		Mode:   "job-poll-exhausted",
		Status: "exhausted",
	}, nil
}

// handleTriggerExhausted publishes RETRY_EXHAUSTED when trigger retries are
// exhausted, writes a joblog entry for audit, and releases the trigger lock
// so the pipeline can be re-triggered.
func handleTriggerExhausted(ctx context.Context, d *Deps, input OrchestratorInput) (OrchestratorOutput, error) {
	errMsg := ""
	if cause, ok := input.ErrorInfo["Cause"].(string); ok {
		errMsg = cause
	}

	if err := d.Store.WriteJobEvent(ctx, input.PipelineID, input.ScheduleID, input.Date,
		types.JobEventInfraTriggerExhausted, "", 0, errMsg); err != nil {
		return OrchestratorOutput{}, fmt.Errorf("write trigger-exhausted joblog: %w", err)
	}

	if err := PublishEvent(ctx, d, string(types.EventRetryExhausted), input.PipelineID, input.ScheduleID, input.Date,
		fmt.Sprintf("trigger retries exhausted for %s: %s", input.PipelineID, errMsg)); err != nil {
		d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventRetryExhausted, "error", err)
	}

	if err := d.Store.ReleaseTriggerLock(ctx, input.PipelineID, input.ScheduleID, input.Date); err != nil {
		d.Logger.WarnContext(ctx, "failed to release trigger lock after exhaustion",
			"pipeline", input.PipelineID, "error", err)
	}

	return OrchestratorOutput{
		Mode:   "trigger-exhausted",
		Status: "exhausted",
	}, nil
}

// handleCompleteTrigger sets the trigger row to its terminal status.
func handleCompleteTrigger(ctx context.Context, d *Deps, input OrchestratorInput) (OrchestratorOutput, error) {
	status := types.TriggerStatusCompleted
	if input.Event != types.JobEventSuccess {
		status = types.TriggerStatusFailedFinal
	}

	if err := d.Store.SetTriggerStatus(ctx, input.PipelineID, input.ScheduleID, input.Date, status); err != nil {
		return OrchestratorOutput{}, fmt.Errorf("set trigger status: %w", err)
	}

	if input.Event == types.JobEventSuccess {
		if err := capturePostRunBaseline(ctx, d, input.PipelineID, input.ScheduleID, input.Date); err != nil {
			d.Logger.WarnContext(ctx, "failed to capture post-run baseline",
				"pipeline", input.PipelineID, "error", err)
			if pubErr := PublishEvent(ctx, d, string(types.EventBaselineCaptureFailed), input.PipelineID, input.ScheduleID, input.Date,
				fmt.Sprintf("baseline capture failed for %s: %v", input.PipelineID, err)); pubErr != nil {
				d.Logger.WarnContext(ctx, "failed to publish baseline capture failure event", "error", pubErr)
			}
		}
	}

	return OrchestratorOutput{
		Mode:   "complete-trigger",
		Status: status,
	}, nil
}

// capturePostRunBaseline reads all sensors and writes a date-scoped baseline
// snapshot if the pipeline has PostRun config.
func capturePostRunBaseline(ctx context.Context, d *Deps, pipelineID, scheduleID, date string) error {
	cfg, err := d.Store.GetConfig(ctx, pipelineID)
	if err != nil {
		return fmt.Errorf("get config: %w", err)
	}
	if cfg == nil || cfg.PostRun == nil || len(cfg.PostRun.Rules) == 0 {
		return nil
	}

	sensors, err := d.Store.GetAllSensors(ctx, pipelineID)
	if err != nil {
		return fmt.Errorf("get sensors: %w", err)
	}

	RemapPerPeriodSensors(sensors, date)

	baseline := make(map[string]interface{})
	for _, rule := range cfg.PostRun.Rules {
		if data, ok := sensors[rule.Key]; ok {
			baseline[rule.Key] = data
		}
	}

	if len(baseline) == 0 {
		return nil
	}

	baselineKey := "postrun-baseline#" + date
	if err := d.Store.WriteSensor(ctx, pipelineID, baselineKey, baseline); err != nil {
		return fmt.Errorf("write baseline: %w", err)
	}

	if err := PublishEvent(ctx, d, string(types.EventPostRunBaselineCaptured), pipelineID, scheduleID, date,
		fmt.Sprintf("post-run baseline captured for %s", pipelineID)); err != nil {
		d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventPostRunBaselineCaptured, "error", err)
	}

	return nil
}

// buildTriggerConfig converts a JobConfig into a TriggerConfig.
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

// extractRunID searches trigger metadata for a recognisable run identifier.
func extractRunID(metadata map[string]interface{}) string {
	if metadata == nil {
		return ""
	}
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
// for hourly dates) into Glue trigger arguments.
// Deprecated: Production callers should use orchestrator.InjectDateArgs.
// Retained for backward compatibility with existing tests.
func InjectDateArgs(tc *types.TriggerConfig, date string) {
	datePart, hourPart := ParseExecutionDate(date)
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
		b, _ := json.Marshal(payload)
		tc.HTTP.Body = string(b)
	}
}
