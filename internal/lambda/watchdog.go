package lambda

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// Deprecated: Use watchdog.HandleWatchdog instead. Retained for test compatibility.
func HandleWatchdog(ctx context.Context, d *Deps) error {
	checks := []struct {
		name string
		fn   func(context.Context, *Deps) error
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
		_ = PublishEvent(ctx, d, string(types.EventWatchdogDegraded), "", "", "",
			fmt.Sprintf("watchdog checks failed: %s", strings.Join(failed, ", ")))
		return errors.Join(errs...)
	}
	return nil
}
