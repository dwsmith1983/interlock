// Package watchdog implements the periodic health-check Lambda handler.
// It detects stale triggers, missed schedules, missing post-run sensors,
// and SLA breaches.
package watchdog

import "context"

import lambda "github.com/dwsmith1983/interlock/internal/lambda"

// HandleWatchdog runs periodic health checks.
func HandleWatchdog(ctx context.Context, d *lambda.Deps) error {
	if err := detectStaleTriggers(ctx, d); err != nil {
		d.Logger.Error("stale trigger detection failed", "error", err)
	}
	if err := detectMissedSchedules(ctx, d); err != nil {
		d.Logger.Error("missed schedule detection failed", "error", err)
	}
	if err := detectMissedInclusionSchedules(ctx, d); err != nil {
		d.Logger.Error("missed inclusion schedule detection failed", "error", err)
	}
	if err := reconcileSensorTriggers(ctx, d); err != nil {
		d.Logger.Error("sensor trigger reconciliation failed", "error", err)
	}
	if err := scheduleSLAAlerts(ctx, d); err != nil {
		d.Logger.Error("proactive SLA scheduling failed", "error", err)
	}
	if err := checkTriggerDeadlines(ctx, d); err != nil {
		d.Logger.Error("trigger deadline check failed", "error", err)
	}
	if err := detectMissingPostRunSensors(ctx, d); err != nil {
		d.Logger.Error("post-run sensor absence detection failed", "error", err)
	}
	if err := detectRelativeSLABreaches(ctx, d); err != nil {
		d.Logger.Error("relative SLA breach detection failed", "error", err)
	}
	return nil
}
