package lambda

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
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
	case "trigger-exhausted":
		return handleTriggerExhausted(ctx, d, input)
	case "complete-trigger":
		return handleCompleteTrigger(ctx, d, input)
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

	remapPerPeriodSensors(sensors, input.Date)

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
	InjectDateArgs(&triggerCfg, input.Date)

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

// handlePostRun evaluates post-run validation rules if configured and detects
// data drift by comparing current audit sensor values against a saved baseline.
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

	remapPerPeriodSensors(sensors, input.Date)

	// Check for data drift: compare audit counts against baseline.
	baselineKey := "postrun-baseline"
	baseline, hasBaseline := sensors[baselineKey]
	auditResult, hasAudit := sensors["audit-result"]

	if hasAudit && hasBaseline {
		prevCount := extractFloat(baseline, "sensor_count")
		currCount := extractFloat(auditResult, "sensor_count")
		if prevCount > 0 && currCount > 0 && currCount != prevCount {
			delta := currCount - prevCount
			alertDetail := map[string]interface{}{
				"previousCount": prevCount,
				"currentCount":  currCount,
				"delta":         delta,
				"source":        "post-run monitor",
				"actionHint":    fmt.Sprintf("%.0f new records detected — re-run triggered", delta),
			}
			_ = publishEvent(ctx, d, string(types.EventDataDrift), input.PipelineID, input.ScheduleID, input.Date,
				fmt.Sprintf("data drift detected for %s: %.0f → %.0f records", input.PipelineID, prevCount, currCount), alertDetail)

			// Trigger a re-run via the existing circuit breaker path.
			if writeErr := d.Store.WriteRerunRequest(ctx, input.PipelineID, input.ScheduleID, input.Date, "data-drift"); writeErr != nil {
				d.Logger.WarnContext(ctx, "failed to write rerun request on drift", "pipelineId", input.PipelineID, "error", writeErr)
			}

			return OrchestratorOutput{
				Mode:   "post-run",
				Status: "drift",
			}, nil
		}
	}

	// First iteration: save current audit result as baseline for future comparisons.
	if hasAudit && !hasBaseline {
		if writeErr := d.Store.WriteSensor(ctx, input.PipelineID, baselineKey, auditResult); writeErr != nil {
			d.Logger.WarnContext(ctx, "failed to write post-run baseline", "pipelineId", input.PipelineID, "error", writeErr)
		}
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

// extractFloat retrieves a numeric value from a sensor data map, handling both
// float64 (native JSON) and string representations.
func extractFloat(data map[string]interface{}, key string) float64 {
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

	_ = publishEvent(ctx, d, string(types.EventValidationExhausted), input.PipelineID, input.ScheduleID, input.Date, "evaluation window exhausted without passing")

	return OrchestratorOutput{
		Mode:   "validation-exhausted",
		Status: "exhausted",
	}, nil
}

// remapPerPeriodSensors adds base-key aliases for per-period sensor keys.
// For example, sensor "hourly-status#20260303T07" becomes accessible under
// key "hourly-status" when the execution date is "2026-03-03T07". This allows
// validation rules with key "hourly-status" to match per-period sensor records.
// Handles both normalized (2026-03-03) and compact (20260303) date formats.
func remapPerPeriodSensors(sensors map[string]map[string]interface{}, date string) {
	if date == "" {
		return
	}
	// Build candidate suffixes: the normalized date and compact form.
	suffixes := []string{"#" + date}
	compact := strings.ReplaceAll(date, "-", "")
	if compact != date {
		suffixes = append(suffixes, "#"+compact)
	}
	for key, data := range sensors {
		for _, suffix := range suffixes {
			if strings.HasSuffix(key, suffix) {
				base := strings.TrimSuffix(key, suffix)
				if _, exists := sensors[base]; !exists {
					sensors[base] = data
				}
				break
			}
		}
	}
}

// handleTriggerExhausted publishes RETRY_EXHAUSTED when trigger retries are
// exhausted, writes a joblog entry for audit, and releases the trigger lock
// so the pipeline can be re-triggered.
func handleTriggerExhausted(ctx context.Context, d *Deps, input OrchestratorInput) (OrchestratorOutput, error) {
	errMsg := ""
	if cause, ok := input.ErrorInfo["Cause"].(string); ok {
		errMsg = cause
	}

	// Dual-write: joblog entry (audit) + EventBridge event (alerting).
	if err := d.Store.WriteJobEvent(ctx, input.PipelineID, input.ScheduleID, input.Date,
		types.JobEventInfraTriggerExhausted, "", 0, errMsg); err != nil {
		return OrchestratorOutput{}, fmt.Errorf("write trigger-exhausted joblog: %w", err)
	}

	_ = publishEvent(ctx, d, string(types.EventRetryExhausted), input.PipelineID, input.ScheduleID, input.Date,
		fmt.Sprintf("trigger retries exhausted for %s: %s", input.PipelineID, errMsg))

	// Release lock so pipeline can be re-triggered.
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
// Success → COMPLETED; fail/timeout → FAILED_FINAL.
func handleCompleteTrigger(ctx context.Context, d *Deps, input OrchestratorInput) (OrchestratorOutput, error) {
	status := types.TriggerStatusCompleted
	if input.Event != types.JobEventSuccess {
		status = types.TriggerStatusFailedFinal
	}

	if err := d.Store.SetTriggerStatus(ctx, input.PipelineID, input.ScheduleID, input.Date, status); err != nil {
		return OrchestratorOutput{}, fmt.Errorf("set trigger status: %w", err)
	}

	return OrchestratorOutput{
		Mode:   "complete-trigger",
		Status: status,
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

// InjectDateArgs parses the execution date and injects --par_day (and --par_hour
// for hourly dates) into Glue trigger arguments. For HTTP triggers with no
// explicit body, injects a JSON body with par_day and par_hour.
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

// ParseExecutionDate splits a composite date into date and hour parts.
// "2026-03-03T10" -> ("2026-03-03", "10")
// "2026-03-03"    -> ("2026-03-03", "")
func ParseExecutionDate(date string) (datePart, hourPart string) {
	if idx := strings.Index(date, "T"); idx >= 0 {
		return date[:idx], date[idx+1:]
	}
	return date, ""
}
