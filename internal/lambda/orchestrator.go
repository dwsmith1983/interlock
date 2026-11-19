package lambda

import (
	"context"
	"fmt"

	"github.com/dwsmith1983/interlock/pkg/types"
)

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

// runIDMetadataKeys lists the trigger-metadata keys that carry a remote run
// identifier, in priority order. The first six are the keys emitted by the
// executors in internal/trigger; the remaining generic keys are retained for
// backward compatibility with records written by older versions.
var runIDMetadataKeys = []string{
	"glue_job_run_id",
	"emr_step_id",
	"emr_sl_job_run_id",
	"sfn_execution_arn",
	"airflow_dag_run_id",
	"databricks_run_id",
	"runId",
	"jobRunId",
	"executionArn",
	"stepId",
	"dagRunId",
}

// ExtractRunID searches trigger metadata for a recognisable run identifier.
// A nil map is safe: indexing a nil map returns the zero value.
func ExtractRunID(metadata map[string]interface{}) string {
	for _, key := range runIDMetadataKeys {
		v, ok := metadata[key]
		if !ok {
			continue
		}
		if s, ok := v.(string); ok && s != "" {
			return s
		}
	}
	return ""
}
