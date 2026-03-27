package lambda

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/dwsmith1983/interlock/pkg/validation"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// handleDryRunTrigger processes a sensor event for a dry-run pipeline.
// It evaluates trigger conditions and validation rules, records observations
// as EventBridge events, but never starts a Step Function execution.
// Calendar exclusions are already checked before this function is called.
func handleDryRunTrigger(ctx context.Context, d *Deps, cfg *types.PipelineConfig, pipelineID, scheduleID, date string, now time.Time) error {
	// Check for existing dry-run marker — if present, this is late data.
	marker, err := d.Store.GetDryRunMarker(ctx, pipelineID, scheduleID, date)
	if err != nil {
		return fmt.Errorf("get dry-run marker for %q: %w", pipelineID, err)
	}

	if marker != nil {
		// Late data: sensor arrived after we already recorded a would-trigger.
		triggeredAtStr, ok := marker.Data["triggeredAt"].(string)
		if !ok || triggeredAtStr == "" {
			d.Logger.WarnContext(ctx, "dry-run marker missing triggeredAt", "pipelineId", pipelineID)
			return nil
		}
		triggeredAt, parseErr := time.Parse(time.RFC3339, triggeredAtStr)
		if parseErr != nil {
			d.Logger.WarnContext(ctx, "dry-run marker has invalid triggeredAt",
				"pipelineId", pipelineID, "value", triggeredAtStr, "error", parseErr)
			return nil
		}
		lateBy := now.Sub(triggeredAt)

		if pubErr := publishEvent(ctx, d, string(types.EventDryRunLateData), pipelineID, scheduleID, date,
			fmt.Sprintf("dry-run: late data arrived %.0fm after trigger point for %s", lateBy.Minutes(), pipelineID),
			map[string]interface{}{
				"triggeredAt": triggeredAtStr,
				"lateBy":      lateBy.String(),
			}); pubErr != nil {
			d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventDryRunLateData, "error", pubErr)
		}
		return nil
	}

	// No marker yet — evaluate full validation rules.
	sensors, err := d.Store.GetAllSensors(ctx, pipelineID)
	if err != nil {
		return fmt.Errorf("get sensors for dry-run %q: %w", pipelineID, err)
	}
	RemapPerPeriodSensors(sensors, date)

	result := validation.EvaluateRules(cfg.Validation.Trigger, cfg.Validation.Rules, sensors, now)
	if !result.Passed {
		d.Logger.Info("dry-run: trigger condition met but validation rules not satisfied",
			"pipelineId", pipelineID,
			"date", date,
		)
		return nil
	}

	// All rules pass — write dry-run marker.
	written, err := d.Store.WriteDryRunMarker(ctx, pipelineID, scheduleID, date, now)
	if err != nil {
		return fmt.Errorf("write dry-run marker for %q: %w", pipelineID, err)
	}
	if !written {
		// Race: another invocation wrote the marker first. Treat as late data.
		return nil
	}

	// Capture post-run baseline if PostRun is configured.
	if cfg.PostRun != nil && len(cfg.PostRun.Rules) > 0 {
		if baselineErr := capturePostRunBaseline(ctx, d, pipelineID, scheduleID, date); baselineErr != nil {
			d.Logger.WarnContext(ctx, "dry-run: failed to capture post-run baseline",
				"pipelineId", pipelineID, "error", baselineErr)
		}
	}

	// Publish WOULD_TRIGGER event.
	if pubErr := publishEvent(ctx, d, string(types.EventDryRunWouldTrigger), pipelineID, scheduleID, date,
		fmt.Sprintf("dry-run: would trigger %s at %s", pipelineID, now.Format(time.RFC3339)),
		map[string]interface{}{
			"triggeredAt":    now.UTC().Format(time.RFC3339),
			"rulesEvaluated": len(cfg.Validation.Rules),
		}); pubErr != nil {
		d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventDryRunWouldTrigger, "error", pubErr)
	}

	// SLA projection if configured.
	var slaVerdict *dryRunSLAVerdict
	if cfg.SLA != nil && cfg.SLA.ExpectedDuration != "" {
		slaVerdict = publishDryRunSLAProjection(ctx, d, cfg, pipelineID, scheduleID, date, now)
	}

	// Publish DRY_RUN_COMPLETED to close the observation loop.
	completedDetail := map[string]interface{}{
		"triggeredAt": now.UTC().Format(time.RFC3339),
	}
	if slaVerdict != nil {
		completedDetail["slaStatus"] = slaVerdict.Status
		completedDetail["estimatedCompletion"] = slaVerdict.EstimatedCompletion
		if slaVerdict.Deadline != "" {
			completedDetail["deadline"] = slaVerdict.Deadline
		}
	} else {
		completedDetail["slaStatus"] = "n/a"
	}

	if pubErr := publishEvent(ctx, d, string(types.EventDryRunCompleted), pipelineID, scheduleID, date,
		fmt.Sprintf("dry-run: observation complete for %s/%s", pipelineID, date),
		completedDetail); pubErr != nil {
		d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventDryRunCompleted, "error", pubErr)
	}

	d.Logger.Info("dry-run: would trigger",
		"pipelineId", pipelineID,
		"schedule", scheduleID,
		"date", date,
	)
	return nil
}

// dryRunSLAVerdict holds the SLA projection result for inclusion in the
// DRY_RUN_COMPLETED event detail.
type dryRunSLAVerdict struct {
	Status              string // "met" or "breach"
	EstimatedCompletion string // RFC3339
	Deadline            string // RFC3339, empty if no deadline configured
}

// publishDryRunSLAProjection computes and publishes an SLA projection event
// for a dry-run pipeline. Reuses handleSLACalculate to resolve the breach
// deadline consistently with production SLA monitoring (including hourly
// pipeline T+1 adjustment). Returns the verdict for inclusion in the
// DRY_RUN_COMPLETED event.
func publishDryRunSLAProjection(ctx context.Context, d *Deps, cfg *types.PipelineConfig, pipelineID, scheduleID, date string, triggeredAt time.Time) *dryRunSLAVerdict {
	expectedDur, err := time.ParseDuration(cfg.SLA.ExpectedDuration)
	if err != nil {
		d.Logger.WarnContext(ctx, "dry-run: invalid expectedDuration", "error", err)
		return nil
	}

	estimatedCompletion := triggeredAt.Add(expectedDur)
	detail := map[string]interface{}{
		"triggeredAt":         triggeredAt.UTC().Format(time.RFC3339),
		"estimatedCompletion": estimatedCompletion.UTC().Format(time.RFC3339),
		"expectedDuration":    cfg.SLA.ExpectedDuration,
	}

	verdict := &dryRunSLAVerdict{
		Status:              "met",
		EstimatedCompletion: estimatedCompletion.UTC().Format(time.RFC3339),
	}

	message := fmt.Sprintf("dry-run: SLA projection for %s — estimated completion %s",
		pipelineID, estimatedCompletion.Format(time.RFC3339))

	if cfg.SLA.Deadline != "" {
		// Reuse the production SLA calculation to resolve the breach deadline.
		slaInput := SLAMonitorInput{
			Mode:             "calculate",
			PipelineID:       pipelineID,
			ScheduleID:       scheduleID,
			Date:             date,
			Deadline:         cfg.SLA.Deadline,
			ExpectedDuration: cfg.SLA.ExpectedDuration,
			Timezone:         cfg.SLA.Timezone,
		}
		slaOutput, calcErr := handleSLACalculate(slaInput, triggeredAt)
		if calcErr != nil {
			d.Logger.WarnContext(ctx, "dry-run: SLA deadline resolution failed", "error", calcErr)
		} else if slaOutput.BreachAt != "" {
			breachAt, parseErr := time.Parse(time.RFC3339, slaOutput.BreachAt)
			if parseErr == nil {
				detail["deadline"] = slaOutput.BreachAt
				verdict.Deadline = slaOutput.BreachAt
				margin := breachAt.Sub(estimatedCompletion)
				detail["marginSeconds"] = margin.Seconds()
				if estimatedCompletion.After(breachAt) {
					verdict.Status = "breach"
					message = fmt.Sprintf("dry-run: SLA projection for %s — would breach by %.0fm",
						pipelineID, math.Abs(margin.Minutes()))
				} else {
					message = fmt.Sprintf("dry-run: SLA projection for %s — SLA met with %.0fm margin",
						pipelineID, margin.Minutes())
				}
			}
		}
	}

	detail["status"] = verdict.Status

	if pubErr := publishEvent(ctx, d, string(types.EventDryRunSLAProjection), pipelineID, scheduleID, date, message, detail); pubErr != nil {
		d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventDryRunSLAProjection, "error", pubErr)
	}

	return verdict
}

// handleDryRunPostRunSensor handles post-run sensor events for dry-run pipelines.
// Compares sensor data against the baseline captured at WOULD_TRIGGER time.
func handleDryRunPostRunSensor(ctx context.Context, d *Deps, cfg *types.PipelineConfig, pipelineID, sensorKey string, sensorData map[string]interface{}) error {
	scheduleID := resolveScheduleID(cfg)
	date := ResolveExecutionDate(sensorData, d.now())

	// Check DRY_RUN# marker — if nil, no trigger happened yet.
	marker, err := d.Store.GetDryRunMarker(ctx, pipelineID, scheduleID, date)
	if err != nil {
		return fmt.Errorf("get dry-run marker for post-run %q: %w", pipelineID, err)
	}
	if marker == nil {
		return nil
	}

	// Read baseline captured at WOULD_TRIGGER time.
	baselineKey := "postrun-baseline#" + date
	baseline, err := d.Store.GetSensorData(ctx, pipelineID, baselineKey)
	if err != nil {
		return fmt.Errorf("get baseline for dry-run post-run: %w", err)
	}
	if baseline == nil {
		return nil
	}

	// Find matching post-run rule for this sensor key.
	var ruleBaseline map[string]interface{}
	for _, rule := range cfg.PostRun.Rules {
		if strings.HasPrefix(sensorKey, rule.Key) {
			if nested, ok := baseline[rule.Key].(map[string]interface{}); ok {
				ruleBaseline = nested
			}
			break
		}
	}
	if ruleBaseline == nil {
		return nil // No baseline for this rule (stale or first run).
	}

	// Compare drift.
	driftField := resolveDriftField(cfg.PostRun)
	threshold := 0.0
	if cfg.PostRun.DriftThreshold != nil {
		threshold = *cfg.PostRun.DriftThreshold
	}
	dr := DetectDrift(ruleBaseline, sensorData, driftField, threshold)
	if dr.Drifted {
		if pubErr := publishEvent(ctx, d, string(types.EventDryRunDrift), pipelineID, scheduleID, date,
			fmt.Sprintf("dry-run: drift detected for %s: %.0f → %.0f — would re-run", pipelineID, dr.Previous, dr.Current),
			map[string]interface{}{
				"previousCount":  dr.Previous,
				"currentCount":   dr.Current,
				"delta":          dr.Delta,
				"driftThreshold": threshold,
				"driftField":     driftField,
				"sensorKey":      sensorKey,
			}); pubErr != nil {
			d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventDryRunDrift, "error", pubErr)
		}
	}

	return nil
}
