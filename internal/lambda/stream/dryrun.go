package stream

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/lambda/orchestrator"
	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/dwsmith1983/interlock/pkg/validation"
)

// handleDryRunTrigger processes a sensor event for a dry-run pipeline.
func handleDryRunTrigger(ctx context.Context, d *lambda.Deps, cfg *types.PipelineConfig, pipelineID, scheduleID, date string, now time.Time) error {
	marker, err := d.Store.GetDryRunMarker(ctx, pipelineID, scheduleID, date)
	if err != nil {
		return fmt.Errorf("get dry-run marker for %q: %w", pipelineID, err)
	}

	if marker != nil {
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

		if pubErr := lambda.PublishEvent(ctx, d, string(types.EventDryRunLateData), pipelineID, scheduleID, date,
			fmt.Sprintf("dry-run: late data arrived %.0fm after trigger point for %s", lateBy.Minutes(), pipelineID),
			map[string]interface{}{
				"triggeredAt": triggeredAtStr,
				"lateBy":      lateBy.String(),
			}); pubErr != nil {
			d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventDryRunLateData, "error", pubErr)
		}
		return nil
	}

	sensors, err := d.Store.GetAllSensors(ctx, pipelineID)
	if err != nil {
		return fmt.Errorf("get sensors for dry-run %q: %w", pipelineID, err)
	}
	lambda.RemapPerPeriodSensors(sensors, date)

	result := validation.EvaluateRules(cfg.Validation.Trigger, cfg.Validation.Rules, sensors, now)
	if !result.Passed {
		d.Logger.Info("dry-run: trigger condition met but validation rules not satisfied",
			"pipelineId", pipelineID, "date", date)
		return nil
	}

	written, err := d.Store.WriteDryRunMarker(ctx, pipelineID, scheduleID, date, now)
	if err != nil {
		return fmt.Errorf("write dry-run marker for %q: %w", pipelineID, err)
	}
	if !written {
		return nil
	}

	if cfg.PostRun != nil && len(cfg.PostRun.Rules) > 0 {
		if baselineErr := orchestrator.CapturePostRunBaseline(ctx, d, pipelineID, scheduleID, date); baselineErr != nil {
			d.Logger.WarnContext(ctx, "dry-run: failed to capture post-run baseline",
				"pipelineId", pipelineID, "error", baselineErr)
		}
	}

	if pubErr := lambda.PublishEvent(ctx, d, string(types.EventDryRunWouldTrigger), pipelineID, scheduleID, date,
		fmt.Sprintf("dry-run: would trigger %s at %s", pipelineID, now.Format(time.RFC3339)),
		map[string]interface{}{
			"triggeredAt":    now.UTC().Format(time.RFC3339),
			"rulesEvaluated": len(cfg.Validation.Rules),
		}); pubErr != nil {
		d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventDryRunWouldTrigger, "error", pubErr)
	}

	var slaVerdict *dryRunSLAVerdict
	if cfg.SLA != nil && cfg.SLA.ExpectedDuration != "" {
		slaVerdict = publishDryRunSLAProjection(ctx, d, cfg, pipelineID, scheduleID, date, now)
	}

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

	if pubErr := lambda.PublishEvent(ctx, d, string(types.EventDryRunCompleted), pipelineID, scheduleID, date,
		fmt.Sprintf("dry-run: observation complete for %s/%s", pipelineID, date),
		completedDetail); pubErr != nil {
		d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventDryRunCompleted, "error", pubErr)
	}

	d.Logger.Info("dry-run: would trigger",
		"pipelineId", pipelineID, "schedule", scheduleID, "date", date)
	return nil
}

// dryRunSLAVerdict holds the SLA projection result.
type dryRunSLAVerdict struct {
	Status              string
	EstimatedCompletion string
	Deadline            string
}

// publishDryRunSLAProjection computes and publishes an SLA projection event.
func publishDryRunSLAProjection(ctx context.Context, d *lambda.Deps, cfg *types.PipelineConfig, pipelineID, scheduleID, date string, triggeredAt time.Time) *dryRunSLAVerdict {
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
		slaInput := lambda.SLAMonitorInput{
			Mode:             "calculate",
			PipelineID:       pipelineID,
			ScheduleID:       scheduleID,
			Date:             date,
			Deadline:         cfg.SLA.Deadline,
			ExpectedDuration: cfg.SLA.ExpectedDuration,
			Timezone:         cfg.SLA.Timezone,
		}
		slaOutput, calcErr := lambda.HandleSLACalculate(slaInput, triggeredAt)
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

	if pubErr := lambda.PublishEvent(ctx, d, string(types.EventDryRunSLAProjection), pipelineID, scheduleID, date, message, detail); pubErr != nil {
		d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventDryRunSLAProjection, "error", pubErr)
	}

	return verdict
}

// handleDryRunPostRunSensor handles post-run sensor events for dry-run pipelines.
func handleDryRunPostRunSensor(ctx context.Context, d *lambda.Deps, cfg *types.PipelineConfig, pipelineID, sensorKey string, sensorData map[string]interface{}) error {
	scheduleID := lambda.ResolveScheduleID(cfg)
	date := lambda.ResolveExecutionDate(sensorData, d.Now())

	marker, err := d.Store.GetDryRunMarker(ctx, pipelineID, scheduleID, date)
	if err != nil {
		return fmt.Errorf("get dry-run marker for post-run %q: %w", pipelineID, err)
	}
	if marker == nil {
		return nil
	}

	baselineKey := "postrun-baseline#" + date
	baseline, err := d.Store.GetSensorData(ctx, pipelineID, baselineKey)
	if err != nil {
		return fmt.Errorf("get baseline for dry-run post-run: %w", err)
	}
	if baseline == nil {
		return nil
	}

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
		return nil
	}

	driftField := lambda.ResolveDriftField(cfg.PostRun)
	threshold := 0.0
	if cfg.PostRun.DriftThreshold != nil {
		threshold = *cfg.PostRun.DriftThreshold
	}
	dr := lambda.DetectDrift(ruleBaseline, sensorData, driftField, threshold)
	if dr.Drifted {
		if pubErr := lambda.PublishEvent(ctx, d, string(types.EventDryRunDrift), pipelineID, scheduleID, date,
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

// handleDryRunRerunRequest evaluates all rerun checks for dry-run pipelines.
func handleDryRunRerunRequest(ctx context.Context, d *lambda.Deps, cfg *types.PipelineConfig, pipelineID, schedule, date string, record events.DynamoDBEventRecord) error {
	if lambda.IsExcludedDate(cfg, date) {
		if pubErr := lambda.PublishEvent(ctx, d, string(types.EventDryRunRerunRejected), pipelineID, schedule, date,
			fmt.Sprintf("dry-run: rerun rejected for %s: execution date %s excluded by calendar", pipelineID, date),
			map[string]interface{}{
				"reason": "excluded by calendar",
			}); pubErr != nil {
			d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventDryRunRerunRejected, "error", pubErr)
		}
		return nil
	}

	reason := "manual"
	if img := record.Change.NewImage; img != nil {
		if r, ok := img["reason"]; ok && r.DataType() == events.DataTypeString {
			if v := r.String(); v != "" {
				reason = v
			}
		}
	}

	var budget int
	var sources []string
	switch reason {
	case "data-drift", "late-data":
		budget = types.IntOrDefault(cfg.Job.MaxDriftReruns, 1)
		sources = []string{"data-drift", "late-data"}
	default:
		budget = types.IntOrDefault(cfg.Job.MaxManualReruns, 1)
		sources = []string{reason}
	}

	count, err := d.Store.CountRerunsBySource(ctx, pipelineID, schedule, date, sources)
	if err != nil {
		return fmt.Errorf("dry-run: count reruns by source for %q: %w", pipelineID, err)
	}

	if count >= budget {
		if pubErr := lambda.PublishEvent(ctx, d, string(types.EventDryRunRerunRejected), pipelineID, schedule, date,
			fmt.Sprintf("dry-run: rerun rejected for %s: limit exceeded (%d/%d)", pipelineID, count, budget),
			map[string]interface{}{
				"reason":     "limit exceeded",
				"rerunCount": count,
				"budget":     budget,
			}); pubErr != nil {
			d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventDryRunRerunRejected, "error", pubErr)
		}
		return nil
	}

	cbStatus := "passed"
	job, err := d.Store.GetLatestJobEvent(ctx, pipelineID, schedule, date)
	if err != nil {
		return fmt.Errorf("dry-run: get latest job event for %q/%s/%s: %w", pipelineID, schedule, date, err)
	}

	if job == nil {
		cbStatus = "skipped (no job history)"
	} else if job.Event == types.JobEventSuccess {
		fresh, freshErr := checkSensorFreshness(ctx, d, pipelineID, job.SK)
		if freshErr != nil {
			return fmt.Errorf("dry-run: check sensor freshness for %q: %w", pipelineID, freshErr)
		}
		if !fresh {
			if pubErr := lambda.PublishEvent(ctx, d, string(types.EventDryRunRerunRejected), pipelineID, schedule, date,
				fmt.Sprintf("dry-run: rerun rejected for %s: previous run succeeded and no sensor data has changed", pipelineID),
				map[string]interface{}{
					"reason":         "circuit breaker",
					"circuitBreaker": "rejected",
				}); pubErr != nil {
				d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventDryRunRerunRejected, "error", pubErr)
			}
			return nil
		}
	}

	if pubErr := lambda.PublishEvent(ctx, d, string(types.EventDryRunWouldRerun), pipelineID, schedule, date,
		fmt.Sprintf("dry-run: would rerun %s (reason: %s)", pipelineID, reason),
		map[string]interface{}{
			"reason":         reason,
			"circuitBreaker": cbStatus,
			"rerunCount":     count,
			"budget":         budget,
		}); pubErr != nil {
		d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventDryRunWouldRerun, "error", pubErr)
	}

	d.Logger.Info("dry-run: would rerun",
		"pipelineId", pipelineID, "schedule", schedule, "date", date, "reason", reason)
	return nil
}

// handleDryRunJobFailure evaluates retry logic for a dry-run pipeline.
func handleDryRunJobFailure(ctx context.Context, d *lambda.Deps, cfg *types.PipelineConfig, pipelineID, schedule, date string) error {
	maxRetries := cfg.Job.MaxRetries

	latestJob, jobErr := d.Store.GetLatestJobEvent(ctx, pipelineID, schedule, date)
	if jobErr != nil {
		d.Logger.WarnContext(ctx, "dry-run: could not read latest job event for failure category",
			"pipelineId", pipelineID, "error", jobErr)
	}
	if latestJob != nil {
		if types.FailureCategory(latestJob.Category) == types.FailurePermanent {
			maxRetries = types.IntOrDefault(cfg.Job.MaxCodeRetries, 1)
		}
	}

	rerunCount, err := d.Store.CountRerunsBySource(ctx, pipelineID, schedule, date, []string{"job-fail-retry"})
	if err != nil {
		return fmt.Errorf("dry-run: count reruns for %q/%s/%s: %w", pipelineID, schedule, date, err)
	}

	if rerunCount >= maxRetries {
		if pubErr := lambda.PublishEvent(ctx, d, string(types.EventDryRunRetryExhausted), pipelineID, schedule, date,
			fmt.Sprintf("dry-run: retry limit reached (%d/%d) for %s", rerunCount, maxRetries, pipelineID),
			map[string]interface{}{
				"retries":    rerunCount,
				"maxRetries": maxRetries,
			}); pubErr != nil {
			d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventDryRunRetryExhausted, "error", pubErr)
		}
		return nil
	}

	if lambda.IsExcludedDate(cfg, date) {
		if pubErr := lambda.PublishEvent(ctx, d, string(types.EventDryRunRetryExhausted), pipelineID, schedule, date,
			fmt.Sprintf("dry-run: retry skipped for %s: execution date %s excluded by calendar", pipelineID, date),
			map[string]interface{}{
				"reason":     "excluded by calendar",
				"retries":    rerunCount,
				"maxRetries": maxRetries,
			}); pubErr != nil {
			d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventDryRunRetryExhausted, "error", pubErr)
		}
		return nil
	}

	if pubErr := lambda.PublishEvent(ctx, d, string(types.EventDryRunWouldRetry), pipelineID, schedule, date,
		fmt.Sprintf("dry-run: would retry %s (%d/%d)", pipelineID, rerunCount, maxRetries),
		map[string]interface{}{
			"retries":    rerunCount,
			"maxRetries": maxRetries,
		}); pubErr != nil {
		d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventDryRunWouldRetry, "error", pubErr)
	}

	d.Logger.Info("dry-run: would retry",
		"pipelineId", pipelineID, "schedule", schedule, "date", date,
		"retries", rerunCount, "maxRetries", maxRetries)
	return nil
}
