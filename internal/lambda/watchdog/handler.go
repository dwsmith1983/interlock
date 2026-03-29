// Package watchdog implements the periodic health-check Lambda handler.
// It detects stale triggers, missed schedules, missing post-run sensors,
// and SLA breaches.
package watchdog

import (
	"context"
	"errors"
	"fmt"
	"strings"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// HandleWatchdog runs periodic health checks.
func HandleWatchdog(ctx context.Context, d *lambda.Deps) error {
	checks := []struct {
		name string
		fn   func(context.Context, *lambda.Deps) error
	}{
		{"stale-triggers", detectStaleTriggers},
		{"missed-schedules", detectMissedSchedules},
		{"missed-inclusion-schedules", detectMissedInclusionSchedules},
		{"sensor-trigger-reconciliation", reconcileSensorTriggers},
		{"sla-scheduling", scheduleSLAAlerts},
		{"trigger-deadlines", checkTriggerDeadlines},
		{"post-run-sensors", detectMissingPostRunSensors},
		{"relative-sla-breaches", detectRelativeSLABreaches},
	}

	var errs []error
	var failed []string
	for _, c := range checks {
		if err := c.fn(ctx, d); err != nil {
			d.Logger.Error(c.name+" failed", "error", err)
			errs = append(errs, fmt.Errorf("%s: %w", c.name, err))
			failed = append(failed, c.name)
		}
	}

	if len(failed) > 0 {
		_ = lambda.PublishEvent(ctx, d, string(types.EventWatchdogDegraded), "", "", "",
			fmt.Sprintf("watchdog checks failed: %s", strings.Join(failed, ", ")))
		return errors.Join(errs...)
	}
	return nil
}
