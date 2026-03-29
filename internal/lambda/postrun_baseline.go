package lambda

import (
	"context"
	"fmt"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// CapturePostRunBaseline reads all sensors and writes a date-scoped baseline
// snapshot if the pipeline has PostRun config. The baseline is stored as
// "postrun-baseline#<date>" so drift detection can compare against it.
func CapturePostRunBaseline(ctx context.Context, d *Deps, pipelineID, scheduleID, date string) error {
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

	// Build baseline from post-run rule keys, namespaced by rule key
	// to prevent field name collisions between different sensors.
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
