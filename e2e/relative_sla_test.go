//go:build e2e

package e2e

import (
	"testing"
	"time"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// A relative SLA (maxDuration 2h, expectedDuration 30m) counts from the first
// sensor arrival near 10:00. A second rule never passes, so the job never
// starts: each evaluation runs out of its 10 s (real) window and is released
// before the 14:00 cutoff, leaving no trigger row, and each watchdog pass
// starts evaluation again. A run left RUNNING would keep the trigger lock, so
// the JOB_TRIGGERED and EVALUATION_RELEASED counts would stop growing. The
// watchdog publishes RELATIVE_SLA_WARNING once from 11:30 and
// RELATIVE_SLA_BREACH once from 12:00.
func TestScenarioRelativeSLAWarning(t *testing.T) {
	const id = "relative-sla"
	start, date := startDay(t)
	at := func(hour, minute int) time.Time {
		return time.Date(start.Year(), start.Month(), start.Day(), hour, minute, 0, 0, time.UTC)
	}
	cfg := scenarioConfig(id)
	cfg.Schedule.Trigger.Cutoff = "14:00"
	cfg.Schedule.Evaluation = types.EvaluationWindow{Window: "10s", Interval: "5s"} // real seconds
	cfg.Validation.Rules = append(cfg.Validation.Rules, types.ValidationRule{Key: "upstream", Check: types.CheckExists})
	cfg.SLA = &types.SLAConfig{MaxDuration: "2h", ExpectedDuration: "30m"}
	putConfig(t, cfg)
	jt, released := string(types.EventJobTriggered), string(types.EventEvaluationReleased)
	warning, breach := string(types.EventRelativeSLAWarning), string(types.EventRelativeSLABreach)

	advanceTo(t, at(10, 0))
	writeReady(t, id, date)
	waitEvents(t, id, date, map[string]int{jt: 1, released: 1})

	// Each pass also restarts the released evaluation; wait for it to be
	// released again before the clock moves.
	advanceTo(t, at(11, 31))
	invokeWatchdog(t)
	waitEvents(t, id, date, map[string]int{jt: 2, released: 2, warning: 1})

	advanceTo(t, at(11, 40))
	invokeWatchdog(t)
	waitEvents(t, id, date, map[string]int{jt: 3, released: 3, warning: 1})

	advanceTo(t, at(12, 1))
	invokeWatchdog(t)
	waitEvents(t, id, date, map[string]int{jt: 4, released: 4, warning: 1, breach: 1})

	waitAlerts(t, id, alert{id, date, warning}, alert{id, date, breach})
	assertLaunches(t, id, 0)
}
