package lambda

import "context"

// HandleWatchdog runs periodic health checks. It detects stale trigger
// executions (Step Function timeouts) and missed cron schedules. Errors from
// each check are logged but do not prevent the other check from running.
func HandleWatchdog(ctx context.Context, d *Deps) error {
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
