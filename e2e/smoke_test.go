//go:build e2e

package e2e

import (
	"slices"
	"testing"
	"time"

	"github.com/dwsmith1983/interlock/pkg/types"
)

const (
	smokePipeline   = "smoke"
	smokeSensor     = "smoke-ready"
	smokeDeadline   = "10:00"         // trigger window closes at 10:00 UTC each day
	settleTimeout   = 3 * time.Minute // first run: includes Lambda cold starts
	deliverTimeout  = time.Minute     // EventBridge delivery after a known publish
	streamSchedule  = "stream"        // lambda.ResolveScheduleID for a pipeline without cron
	smokeDateLayout = "2006-01-02"
)

// smokeConfig is a sensor-triggered pipeline whose http job is the mock. The
// trigger deadline gives the watchdog something to do on the next simulated
// day, which is how the test sees the advanced clock reach the Lambdas.
func smokeConfig() types.PipelineConfig {
	return types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: smokePipeline},
		Schedule: types.ScheduleConfig{
			Trigger: &types.TriggerCondition{
				Key: smokeSensor, Check: types.CheckEquals, Field: "status", Value: "ready",
				Deadline: smokeDeadline,
			},
			Evaluation: types.EvaluationWindow{Window: "60s", Interval: "5s"},
		},
		Validation: types.ValidationConfig{
			Trigger: "ALL",
			Rules:   []types.ValidationRule{{Key: smokeSensor, Check: types.CheckExists}},
		},
		Job: types.JobConfig{
			Type:   types.TriggerHTTP,
			Config: map[string]interface{}{"method": "POST", "url": mockURLForLambdas + "/jobs/" + smokePipeline + "/run"},
		},
	}
}

func TestHarnessSmoke(t *testing.T) {
	day := mockNow(t).UTC()
	date := day.Format(smokeDateLayout)

	// The config goes in first: the stream-router's config cache is pinned
	// to the simulated clock, so it must see the config on its first load.
	putConfig(t, smokeConfig())
	writeSensor(t, smokePipeline, smokeSensor, map[string]interface{}{"status": "ready", "date": date})

	// The http job reports its own result under the run ID it was sent.
	reportJob(t, smokePipeline, date, waitLaunch(t, smokePipeline, 1), types.JobEventSuccess)
	waitFor(t, settleTimeout, "the trigger row to reach COMPLETED", func() bool {
		return triggerStatus(t, smokePipeline, date) == types.TriggerStatusCompleted
	})
	if n := len(launches(t, smokePipeline)); n != 1 {
		t.Fatalf("mock job launches = %d, want exactly 1", n)
	}

	var evs []event
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("last events read: %v", eventTypes(evs))
		}
	})
	waitFor(t, deliverTimeout, "exactly the first run's events", func() bool {
		evs = events(t, smokePipeline)
		return exactRunEvents(evs, string(types.EventValidationPassed), string(types.EventJobCompleted))
	})

	// Next simulated day, past the trigger deadline. No sensor arrives, so
	// the watchdog closes that day's window: a FAILED_FINAL row keyed on the
	// simulated date and SENSOR_DEADLINE_EXPIRED stamped with simulated time.
	next := time.Date(day.Year(), day.Month(), day.Day()+1, 12, 0, 0, 0, time.UTC)
	nextDate := next.Format(smokeDateLayout)
	advanceTo(t, next)
	invokeWatchdog(t)
	if got := triggerStatus(t, smokePipeline, nextDate); got != types.TriggerStatusFailedFinal {
		t.Fatalf("trigger status for %s after the watchdog = %q, want %q", nextDate, got, types.TriggerStatusFailedFinal)
	}
	waitFor(t, deliverTimeout, "exactly the first run's events then SENSOR_DEADLINE_EXPIRED", func() bool {
		evs = events(t, smokePipeline)
		return exactRunEvents(evs, string(types.EventValidationPassed), string(types.EventJobCompleted),
			string(types.EventSensorDeadlineExpired))
	})
	// Between the advanced instant and the mock's now: a Lambda on the wall
	// clock (2026-11, after both) would fail the upper bound.
	expired, simNow := evs[len(evs)-1], mockNow(t)
	if expired.Type != string(types.EventSensorDeadlineExpired) || expired.Date != nextDate ||
		expired.At.Before(next) || expired.At.After(simNow) {
		t.Fatalf("last event = %+v, want %s for %s stamped in [%s, %s]", expired, types.EventSensorDeadlineExpired,
			nextDate, next.Format(time.RFC3339), simNow.Format(time.RFC3339))
	}

	// No SLA is configured, so nothing may have been scheduled.
	if scanned, fired := fireDueSchedules(t, smokePipeline); scanned != 0 || fired != 0 {
		t.Fatalf("schedules scanned=%d fired=%d, want 0 and 0 for a pipeline without an SLA", scanned, fired)
	}

	wantAlerts := []alert{
		{Pipeline: smokePipeline, Date: date, Type: string(types.EventJobCompleted)},
		{Pipeline: smokePipeline, Date: nextDate, Type: string(types.EventSensorDeadlineExpired)},
	}
	var got []alert
	waitFor(t, deliverTimeout, "JOB_COMPLETED and SENSOR_DEADLINE_EXPIRED on the alerts queue", func() bool {
		got = alerts(t, smokePipeline)
		return slices.Contains(got, wantAlerts[0]) && slices.Contains(got, wantAlerts[1])
	})
	if len(got) != len(wantAlerts) {
		t.Fatalf("alerts = %+v, want only %+v", got, wantAlerts)
	}
	if n := len(launches(t, smokePipeline)); n != 1 {
		t.Fatalf("mock job launches at the end = %d, want still 1", n)
	}
	t.Logf("events: %v; alerts: %+v", eventTypes(evs), got)
}

// exactRunEvents reports whether evs is exactly want in that order plus two
// JOB_TRIGGERED anywhere: the stream-router and the orchestrator both publish
// one, and the first races the first evaluate.
func exactRunEvents(evs []event, want ...string) bool {
	got := eventTypes(evs)
	return len(got) == len(want)+2 && countOf(got, string(types.EventJobTriggered)) == 2 && isSubsequence(want, got)
}

// isSubsequence reports whether want appears in got in order, gaps allowed.
func isSubsequence(want, got []string) bool {
	i := 0
	for _, g := range got {
		if i < len(want) && g == want[i] {
			i++
		}
	}
	return i == len(want)
}

func countOf(xs []string, x string) int {
	n := 0
	for _, v := range xs {
		if v == x {
			n++
		}
	}
	return n
}
