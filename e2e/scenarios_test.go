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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"

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

// waitAlerts waits until every want alert is on the queue (as many times as
// it is in want), drains the queue once more to catch a late extra, then
// fails unless want are the pipeline's only alerts.
func waitAlerts(t *testing.T, pipeline string, want ...alert) {
	t.Helper()
	count := func(as []alert, a alert) int {
		return len(slices.DeleteFunc(slices.Clone(as), func(x alert) bool { return x != a }))
	}
	waitFor(t, deliverTimeout, pipeline+" alerts", func() bool {
		got := alerts(t, pipeline)
		for _, w := range want {
			if count(got, w) < count(want, w) {
				return false
			}
		}
		return true
	})
	if got := alerts(t, pipeline); len(got) != len(want) {
		t.Fatalf("%s alerts = %+v, want only %+v", pipeline, got, want)
	}
}

// waitLaunch waits for the pipeline's nth launch (from 1) and returns the
// run ID it carried.
func waitLaunch(t *testing.T, pipeline string, n int) string {
	t.Helper()
	var ids []string
	waitFor(t, settleTimeout, fmt.Sprintf("%s launch %d", pipeline, n), func() bool {
		ids = launches(t, pipeline)
		return len(ids) >= n
	})
	if ids[n-1] == "" {
		t.Fatalf("%s launch %d carried no run ID", pipeline, n)
	}
	return ids[n-1]
}

func assertLaunches(t *testing.T, pipeline string, want int) {
	t.Helper()
	if n := len(launches(t, pipeline)); n != want {
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
	reportJob(t, id, date, waitLaunch(t, id, 1), types.JobEventSuccess)
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

// A rejected launch is a Trigger task error: the 400 is permanent for that
// attempt (one launch), the orchestrator logs infra-trigger-failure and fails
// the task, and the Trigger state's Retry launches again after its 30 s
// interval. The retry is the same execution, so both launches carry the same
// run ID, and the job reports success for it. No JOB# fail row is written, so
// CompleteTrigger's failure retry (job.maxRetries) does not run.
func TestScenarioRetryAfterFailedLaunch(t *testing.T) {
	const id = "retry-ok"
	_, date := startDay(t)
	mockDo(t, http.MethodPost, "/jobs/"+id+"/outcome", map[string][]int{"statuses": {http.StatusBadRequest}}, http.StatusNoContent)
	putConfig(t, scenarioConfig(id))

	writeReady(t, id, date)
	runID := waitLaunch(t, id, 2)
	if ids := launches(t, id); ids[0] != runID {
		t.Fatalf("launch run IDs = %v, want both from one execution", ids)
	}
	reportJob(t, id, date, runID, types.JobEventSuccess)
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

// The job reports fail for run A. Execution A's CompleteTrigger is the only
// actor that reacts (job.maxRetries 1): it keeps the trigger row RUNNING,
// starts B as <A>-r1 and ends through IsRetrying and Done, leaving the SLA to
// B. The stream-router only publishes JOB_FAILED for the row. B must not read
// A's fail: it keeps polling until the job reports success for B, and its
// CompleteTrigger leaves the trigger row COMPLETED.
func TestScenarioRerunIgnoresPreviousFailure(t *testing.T) {
	const id = "rerun-after-fail"
	_, date := startDay(t)
	cfg := scenarioConfig(id)
	cfg.Job.MaxRetries = 1
	putConfig(t, cfg)

	writeReady(t, id, date)
	runA := waitLaunch(t, id, 1)
	reportJob(t, id, date, runA, types.JobEventFail)

	runB := waitLaunch(t, id, 2)
	if want := runA + "-r1"; runB != want {
		t.Fatalf("rerun run ID = %q, want %q", runB, want)
	}
	waitFor(t, settleTimeout, "execution A to finish", func() bool {
		return executionStatus(t, runA) != sfntypes.ExecutionStatusRunning
	})
	if got := executionStatus(t, runA); got != sfntypes.ExecutionStatusSucceeded {
		t.Fatalf("execution A = %s, want SUCCEEDED (a retry ends through IsRetrying and Done)", got)
	}
	if h := executionHistory(t, runA); !stateExited(h, "IsRetrying") || stateExited(h, "CheckCancelSLA") {
		t.Fatalf("execution A must leave through IsRetrying without CheckCancelSLA")
	}
	// Let B check the job log at least once before its result exists: it
	// either parks in WaitForJob again or, reading A's fail, finishes.
	waitFor(t, settleTimeout, "execution B to run CheckJob", func() bool {
		h := executionHistory(t, runB)
		return stateExited(h, "CheckJob") &&
			(h[len(h)-1].Type == sfntypes.HistoryEventTypeWaitStateEntered || executionStatus(t, runB) != sfntypes.ExecutionStatusRunning)
	})
	if got := executionStatus(t, runB); got != sfntypes.ExecutionStatusRunning {
		t.Fatalf("execution B = %s after CheckJob with no result for B, want RUNNING: it must not read run A's fail", got)
	}

	reportJob(t, id, date, runB, types.JobEventSuccess)
	waitFor(t, settleTimeout, "execution B to finish", func() bool {
		return executionStatus(t, runB) != sfntypes.ExecutionStatusRunning
	})
	if got := executionStatus(t, runB); got != sfntypes.ExecutionStatusSucceeded {
		t.Fatalf("execution B = %s, want SUCCEEDED", got)
	}

	vp, jc, jf := string(types.EventValidationPassed), string(types.EventJobCompleted), string(types.EventJobFailed)
	waitEvents(t, id, date, map[string]int{string(types.EventJobTriggered): 3, vp: 2, jf: 1, jc: 1}, []string{vp, vp, jc})
	wantLog := []string{types.JobEventFail, types.JobEventSuccess}
	if got := jobEvents(t, id); !slices.Equal(got, wantLog) {
		t.Fatalf("job log = %v, want %v", got, wantLog)
	}
	waitAlerts(t, id, alert{id, date, jf}, alert{id, date, jc})
	assertLaunches(t, id, 2)
	assertTriggerStatus(t, id, date, types.TriggerStatusCompleted)
	t.Logf("run A %s, run B %s, job log %v", runA, runB, wantLog)
}

// job.maxRetries 1 and both attempts fail: A's CompleteTrigger starts
// <A>-r1, and B's, with the budget spent, sets FAILED_FINAL and publishes
// RETRY_EXHAUSTED instead of starting a third attempt. Each fail row is one
// JOB_FAILED from the stream-router.
func TestScenarioRetriesExhausted(t *testing.T) {
	const id = "retries-exhausted"
	_, date := startDay(t)
	cfg := scenarioConfig(id)
	cfg.Job.MaxRetries = 1
	putConfig(t, cfg)

	writeReady(t, id, date)
	runA := waitLaunch(t, id, 1)
	reportJob(t, id, date, runA, types.JobEventFail)
	runB := waitLaunch(t, id, 2)
	if want := runA + "-r1"; runB != want {
		t.Fatalf("retry run ID = %q, want %q", runB, want)
	}
	reportJob(t, id, date, runB, types.JobEventFail)
	waitFor(t, settleTimeout, "execution B to finish", func() bool {
		return executionStatus(t, runB) != sfntypes.ExecutionStatusRunning
	})
	if got := executionStatus(t, runB); got != sfntypes.ExecutionStatusSucceeded {
		t.Fatalf("execution B = %s, want SUCCEEDED", got)
	}

	vp, jf, ex := string(types.EventValidationPassed), string(types.EventJobFailed), string(types.EventRetryExhausted)
	waitEvents(t, id, date, map[string]int{string(types.EventJobTriggered): 3, vp: 2, jf: 2, ex: 1})
	if got, want := jobEvents(t, id), []string{types.JobEventFail, types.JobEventFail}; !slices.Equal(got, want) {
		t.Fatalf("job log = %v, want %v", got, want)
	}
	waitAlerts(t, id, alert{id, date, jf}, alert{id, date, jf}, alert{id, date, ex})
	assertLaunches(t, id, 2)
	assertTriggerStatus(t, id, date, types.TriggerStatusFailedFinal)
}

// The SLA stays live through a CompleteTrigger retry. Run A fails, A's
// CompleteTrigger starts <A>-r1 and keeps the trigger row RUNNING, so the
// newest job row is A's fail while B polls. The warning the watchdog
// scheduled fires then and must publish SLA_WARNING. B succeeds before the
// breach, and its CancelSLASchedules deletes the breach schedule and
// publishes SLA_MET.
func TestScenarioSLAWarningDuringRetry(t *testing.T) {
	const id = "sla-warn-retry"
	start, date := startDay(t)
	cfg := scenarioConfig(id)
	cfg.Job.MaxRetries = 1
	cfg.SLA = &types.SLAConfig{Deadline: slaDeadline, ExpectedDuration: slaExpected}
	putConfig(t, cfg)

	invokeWatchdog(t)
	breachAt := time.Date(start.Year(), start.Month(), start.Day()+1, 8, 0, 0, 0, time.UTC)
	warnAt := breachAt.Add(-time.Hour)

	advanceTo(t, time.Date(start.Year(), start.Month(), start.Day(), slaRunHour, 0, 0, 0, time.UTC))
	writeReady(t, id, date)
	runA := waitLaunch(t, id, 1)
	reportJob(t, id, date, runA, types.JobEventFail)
	runB := waitLaunch(t, id, 2)
	if want := runA + "-r1"; runB != want {
		t.Fatalf("retry run ID = %q, want %q", runB, want)
	}
	waitFor(t, settleTimeout, "execution A to finish", func() bool {
		return executionStatus(t, runA) != sfntypes.ExecutionStatusRunning
	})

	advanceTo(t, warnAt)
	if scanned, fired := fireDueSchedules(t, id); scanned != 2 || fired != 1 {
		t.Fatalf("at the warning: schedules scanned=%d fired=%d, want 2 and 1", scanned, fired)
	}
	assertTriggerStatus(t, id, date, types.TriggerStatusRunning)
	warn := string(types.EventSLAWarning)
	waitFor(t, settleTimeout, id+" SLA_WARNING event", func() bool {
		return slices.Contains(eventTypes(events(t, id)), warn)
	})

	reportJob(t, id, date, runB, types.JobEventSuccess)
	waitCompleted(t, id, date)
	vp, jc, jf, met := string(types.EventValidationPassed), string(types.EventJobCompleted),
		string(types.EventJobFailed), string(types.EventSLAMet)
	waitEvents(t, id, date,
		map[string]int{string(types.EventJobTriggered): 3, vp: 2, jf: 1, warn: 1, jc: 1, met: 1},
		[]string{vp, vp, jc}, []string{warn, jc}, []string{warn, met})

	assertNothingFires(t, id, breachAt)
	waitAlerts(t, id, alert{id, date, jf}, alert{id, date, warn}, alert{id, date, jc}, alert{id, date, met})
	assertLaunches(t, id, 2)
	assertTriggerStatus(t, id, date, types.TriggerStatusCompleted)
}

// One team's malformed CONFIG row (a string where job.maxRetries wants a
// number) must not stop the other pipelines: the good one triggers and
// completes, and a watchdog pass over both succeeds (the invoke fails if any
// check does).
func TestScenarioBadConfigIsolated(t *testing.T) {
	const id, bad = "bad-config-neighbour", "bad-config"
	_, date := startDay(t)
	badKey := map[string]ddbtypes.AttributeValue{
		"PK": &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(bad)},
		"SK": &ddbtypes.AttributeValueMemberS{Value: types.ConfigSK},
	}
	putItem(t, map[string]ddbtypes.AttributeValue{"PK": badKey["PK"], "SK": badKey["SK"],
		"config": &ddbtypes.AttributeValueMemberS{Value: `{"pipeline":{"id":"bad-config"},"job":{"maxRetries":"3"}}`}})
	t.Cleanup(func() { // keep the bad row out of the other scenarios
		ctx, cancel := callCtx()
		defer cancel()
		if _, err := ddb.DeleteItem(ctx, &dynamodb.DeleteItemInput{TableName: aws.String(controlTable), Key: badKey}); err != nil {
			t.Errorf("delete %s config: %v", bad, err)
		}
	})
	putConfig(t, scenarioConfig(id))

	writeReady(t, id, date)
	reportJob(t, id, date, waitLaunch(t, id, 1), types.JobEventSuccess)
	waitCompleted(t, id, date)
	vp, jc := string(types.EventValidationPassed), string(types.EventJobCompleted)
	waitEvents(t, id, date, map[string]int{string(types.EventJobTriggered): 2, vp: 1, jc: 1}, []string{vp, jc})

	invokeWatchdog(t)
	waitAlerts(t, id, alert{id, date, jc})
	assertLaunches(t, id, 1)
}

func stateExited(h []sfntypes.HistoryEvent, state string) bool {
	for _, e := range h {
		if d := e.StateExitedEventDetails; d != nil && aws.ToString(d.Name) == state {
			return true
		}
	}
	return false
}
