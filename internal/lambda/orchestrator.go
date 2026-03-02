package lambda

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dwsmith1983/interlock/internal/validation"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// HandleOrchestrator is the entry point for the orchestrator Lambda.
// It dispatches to one of four modes: evaluate, trigger, check-job, post-run.
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

	metadata, err := d.TriggerRunner.Execute(ctx, &triggerCfg)
	if err != nil {
		return OrchestratorOutput{Mode: "trigger", Error: fmt.Sprintf("trigger execute: %v", err)}, nil
	}

	runID := extractRunID(metadata)

	_ = publishEvent(ctx, d, string(types.EventJobTriggered), input.PipelineID, input.ScheduleID, input.Date, fmt.Sprintf("triggered %s job", cfg.Job.Type))

	return OrchestratorOutput{
		Mode:    "trigger",
		RunID:   runID,
		JobType: string(cfg.Job.Type),
	}, nil
}

// handleCheckJob queries the job log for the latest event.
func handleCheckJob(ctx context.Context, d *Deps, input OrchestratorInput) (OrchestratorOutput, error) {
	record, err := d.Store.GetLatestJobEvent(ctx, input.PipelineID, input.ScheduleID, input.Date)
	if err != nil {
		return OrchestratorOutput{Mode: "check-job", Error: err.Error()}, nil
	}

	if record == nil {
		return OrchestratorOutput{Mode: "check-job"}, nil
	}

	return OrchestratorOutput{
		Mode:  "check-job",
		Event: record.Event,
	}, nil
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
	for _, key := range []string{"runId", "jobRunId", "executionArn", "stepId", "dagRunId"} {
		if v, ok := metadata[key]; ok {
			if s, ok := v.(string); ok && s != "" {
				return s
			}
		}
	}
	return ""
}
