//go:build e2e

package e2e

// Each scenario starts a fresh simulated day with startDay and uses its own
// pipeline, and reads only that pipeline's events, alerts and schedules, so
// the scenarios do not depend on the order go test runs them in.

import (
	"fmt"
	"maps"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/dwsmith1983/interlock/pkg/types"
)

const (
	scenarioSensor = "ready"
	dayStartHour   = 6       // each scenario starts at 06:00 UTC on its day
	slaDeadline    = "08:00" // T+1: 08:00 UTC the day after the run's date
	slaExpected    = "1h"    // the warning fires an hour before the breach
	slaRunHour     = 9       // after a same-day 08:00, before the T+1 one
)

// scenarioConfig is a sensor-triggered pipeline whose http job is the mock,
// like smokeConfig without the trigger deadline.
func scenarioConfig(id string) types.PipelineConfig {
	return types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: id},
		Schedule: types.ScheduleConfig{
			Trigger:    &types.TriggerCondition{Key: scenarioSensor, Check: types.CheckEquals, Field: "status", Value: "ready"},
			Evaluation: types.EvaluationWindow{Window: "60s", Interval: "5s"},
		},
		Validation: types.ValidationConfig{
			Trigger: "ALL",
			Rules:   []types.ValidationRule{{Key: scenarioSensor, Check: types.CheckExists}},
		},
		Job: types.JobConfig{
			Type:   types.TriggerHTTP,
			Config: map[string]interface{}{"method": "POST", "url": mockURLForLambdas + "/jobs/" + id + "/run"},
		},
	}
}

// startDay moves the clock to dayStartHour on the day after the mock's now
// and returns that instant and its date.
func startDay(t *testing.T) (start time.Time, date string) {
	t.Helper()
	now := mockNow(t).UTC()
	start = time.Date(now.Year(), now.Month(), now.Day()+1, dayStartHour, 0, 0, 0, time.UTC)
	advanceTo(t, start)
	return start, start.Format(smokeDateLayout)
}

func writeReady(t *testing.T, pipeline, date string) {
	t.Helper()
	writeSensor(t, pipeline, scenarioSensor, map[string]interface{}{"status": "ready", "date": date})
}

// waitEvents waits until the pipeline's events for date are exactly want
// (type -> count) and each chain appears in order.
func waitEvents(t *testing.T, pipeline, date string, want map[string]int, chains ...[]string) {
	t.Helper()
	var evs []event
	match := func() bool {
		evs = events(t, pipeline)
		got := map[string]int{}
		for _, e := range evs {
			if e.Date != date {
				return false
			}
			got[e.Type]++
		}
		if !maps.Equal(got, want) {
			return false
		}
		for _, c := range chains {
			if !isSubsequence(c, eventTypes(evs)) {
				return false
			}
		}
		return true
	}
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("%s: last events read: %+v", pipeline, evs)
		}
	})
	waitFor(t, settleTimeout, fmt.Sprintf("%s events %v", pipeline, want), match)
}

// waitAlerts waits until every want alert is on the queue, drains the queue
// once more to catch a late extra, then fails unless want are the pipeline's
// only alerts.
func waitAlerts(t *testing.T, pipeline string, want ...alert) {
	t.Helper()
	waitFor(t, deliverTimeout, pipeline+" alerts", func() bool {
		got := alerts(t, pipeline)
		for _, w := range want {
			if !slices.Contains(got, w) {
				return false
			}
		}
		return true
	})
	if got := alerts(t, pipeline); len(got) != len(want) {
		t.Fatalf("%s alerts = %+v, want only %+v", pipeline, got, want)
	}
}

func assertLaunches(t *testing.T, pipeline string, want int) {
	t.Helper()
	if n := launches(t, pipeline); n != want {
		t.Fatalf("%s mock job launches = %d, want %d", pipeline, n, want)
	}
}

func assertTriggerStatus(t *testing.T, pipeline, date, want string) {
	t.Helper()
	if got := triggerStatus(t, pipeline, date); got != want {
		t.Fatalf("%s trigger status for %s = %q, want %q", pipeline, date, got, want)
	}
}

func waitCompleted(t *testing.T, pipeline, date string) {
	t.Helper()
	waitFor(t, settleTimeout, pipeline+" trigger row COMPLETED", func() bool {
		return triggerStatus(t, pipeline, date) == types.TriggerStatusCompleted
	})
}

func assertNothingFires(t *testing.T, pipeline string, at time.Time) {
	t.Helper()
	advanceTo(t, at)
	if scanned, fired := fireDueSchedules(t, pipeline); scanned != 0 || fired != 0 {
		t.Fatalf("at %s: %s schedules scanned=%d fired=%d, want 0 and 0", at.Format(time.RFC3339), pipeline, scanned, fired)
	}
}

// The watchdog schedules the SLA alerts, the run finishes before the warning,
// and the SFN's CancelSLASchedules deletes both schedules and publishes
// SLA_MET, so nothing is left to fire at the warning or breach time. The run
// starts at 09:00, after 08:00 on its own date, so the verdict is SLA_MET only
// while cancel applies the T+1 SLA date; a same-day deadline gives SLA_BREACH.
func TestScenarioSLAMet(t *testing.T) {
	const id = "sla-met"
	start, date := startDay(t)
	cfg := scenarioConfig(id)
	cfg.SLA = &types.SLAConfig{Deadline: slaDeadline, ExpectedDuration: slaExpected}
	putConfig(t, cfg)

	invokeWatchdog(t)
	breachAt := time.Date(start.Year(), start.Month(), start.Day()+1, 8, 0, 0, 0, time.UTC)
	warnAt := breachAt.Add(-time.Hour)
	all, scanned := dueSchedules(t, id, breachAt)
	got := map[string]time.Time{}
	for _, s := range all {
		got[s.Name] = s.at
	}
	want := map[string]time.Time{
		id + "-stream-" + date + "-sla-warning": warnAt,
		id + "-stream-" + date + "-sla-breach":  breachAt,
	}
	if scanned != 2 || !maps.EqualFunc(got, want, time.Time.Equal) {
		t.Fatalf("schedules after the watchdog = %v (scanned %d), want %v", got, scanned, want)
	}

	advanceTo(t, time.Date(start.Year(), start.Month(), start.Day(), slaRunHour, 0, 0, 0, time.UTC))
	writeReady(t, id, date)
	waitCompleted(t, id, date)
	vp, jc, met := string(types.EventValidationPassed), string(types.EventJobCompleted), string(types.EventSLAMet)
	waitEvents(t, id, date, map[string]int{string(types.EventJobTriggered): 2, vp: 1, jc: 1, met: 1},
		[]string{vp, jc}, []string{vp, met})

	assertNothingFires(t, id, warnAt)
	assertNothingFires(t, id, breachAt)
	waitAlerts(t, id, alert{id, date, jc}, alert{id, date, met})
	assertLaunches(t, id, 1)
	assertTriggerStatus(t, id, date, types.TriggerStatusCompleted)
}

// An http job is synchronous, so a failed launch is a Trigger task error: the
// 400 is permanent for that attempt (one launch), the orchestrator logs
// infra-trigger-failure and fails the task, and the Trigger state's Retry
// launches again after its 30 s interval. No JOB# fail row is written, so the
// stream-router's job-fail-retry path (job.maxRetries) does not run.
func TestScenarioRetryAfterFailedLaunch(t *testing.T) {
	const id = "retry-ok"
	_, date := startDay(t)
	mockDo(t, http.MethodPost, "/jobs/"+id+"/outcome", map[string][]int{"statuses": {http.StatusBadRequest}}, http.StatusNoContent)
	putConfig(t, scenarioConfig(id))

	writeReady(t, id, date)
	waitCompleted(t, id, date)
	vp, jc := string(types.EventValidationPassed), string(types.EventJobCompleted)
	waitEvents(t, id, date, map[string]int{string(types.EventJobTriggered): 2, vp: 1, jc: 1}, []string{vp, jc})

	wantLog := []string{types.JobEventInfraTriggerFailure, types.JobEventSuccess}
	if got := jobEvents(t, id); !slices.Equal(got, wantLog) {
		t.Fatalf("job log = %v, want %v", got, wantLog)
	}
	waitAlerts(t, id, alert{id, date, jc})
	assertLaunches(t, id, 2)
	assertTriggerStatus(t, id, date, types.TriggerStatusCompleted)
}

// A dry-run pipeline never starts the state machine: the stream-router writes
// a dry-run marker and publishes what it would have done. Both events come
// from one invocation, so a same-millisecond tie could sort them either way;
// only their counts are asserted.
func TestScenarioDryRun(t *testing.T) {
	const id = "dry-run"
	_, date := startDay(t)
	cfg := scenarioConfig(id)
	cfg.DryRun = true
	putConfig(t, cfg)

	writeReady(t, id, date)
	would, done := string(types.EventDryRunWouldTrigger), string(types.EventDryRunCompleted)
	waitEvents(t, id, date, map[string]int{would: 1, done: 1})

	waitAlerts(t, id, alert{id, date, would}, alert{id, date, done})
	assertLaunches(t, id, 0)
	assertTriggerStatus(t, id, date, "")
}

// The simulated day is in the pipeline's inline exclusion list, so the
// stream-router suppresses the trigger before taking the lock.
func TestScenarioCalendarExclusion(t *testing.T) {
	const id = "excluded"
	_, date := startDay(t)
	cfg := scenarioConfig(id)
	cfg.Schedule.Exclude = &types.ExclusionConfig{Dates: []string{date}}
	putConfig(t, cfg)

	writeReady(t, id, date)
	excluded := string(types.EventPipelineExcluded)
	waitEvents(t, id, date, map[string]int{excluded: 1})

	waitAlerts(t, id, alert{id, date, excluded})
	assertLaunches(t, id, 0)
	assertTriggerStatus(t, id, date, "")
}
