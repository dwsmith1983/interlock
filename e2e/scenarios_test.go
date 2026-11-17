//go:build e2e

package e2e

// Each scenario starts a fresh simulated day with startDay and uses its own
// pipeline, and reads only that pipeline's events, alerts and schedules, so
// the scenarios do not depend on the order go test runs them in.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strings"
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
	slaDeadline    = "08:00" // 08:00 UTC, deadlineDayOffset days after the run's date
	slaExpected    = "1h"    // the warning fires an hour before the breach
	slaRunHour     = 9       // after a same-day 08:00, before the next day's
)

// scenarioConfig is a sensor-triggered pipeline whose http job is the mock,
// like smokeConfig without the cutoff.
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

// putRunDate writes the RUN# row that tells the watchdog the pipeline
// expects a run on date.
func putRunDate(t *testing.T, pipeline, date string) {
	t.Helper()
	putItem(t, map[string]ddbtypes.AttributeValue{
		"PK": &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipeline)},
		"SK": &ddbtypes.AttributeValueMemberS{Value: types.RunSK(date)},
	})
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

// Two pipelines share the 08:00 deadline and run at 09:00 on their date; the
// watchdog schedules their SLA alerts at 06:00. With deadlineDayOffset 1
// ("sla-met") the deadline is 08:00 the next day: the run finishes before
// the warning, and CancelSLASchedules deletes both schedules and publishes
// SLA_MET, so nothing is left to fire. With offset 0 ("sla-same-day") the
// deadline is 08:00 the same day: the warning fires at 07:00 and the breach at
// 08:00 while the pipeline has not started, and the 09:00 run's cancel
// publishes its own SLA_BREACH verdict. Both have a RUN# row for the date;
// a third ("sla-no-row") has the same-day SLA and no row, so the watchdog
// schedules nothing for it and it gets no alerts.
func TestScenarioSLAMet(t *testing.T) {
	const metID, sameDayID, noRowID = "sla-met", "sla-same-day", "sla-no-row"
	start, date := startDay(t)
	at := func(day, hour int) time.Time {
		return time.Date(start.Year(), start.Month(), start.Day()+day, hour, 0, 0, 0, time.UTC)
	}
	for id, offset := range map[string]int{metID: 1, sameDayID: 0, noRowID: 0} {
		cfg := scenarioConfig(id)
		cfg.SLA = &types.SLAConfig{Deadline: slaDeadline, DeadlineDayOffset: offset, ExpectedDuration: slaExpected}
		putConfig(t, cfg)
	}
	putRunDate(t, metID, date)
	putRunDate(t, sameDayID, date)

	invokeWatchdog(t)
	if _, scanned := dueSchedules(t, noRowID, at(1, 8)); scanned != 0 {
		t.Fatalf("%s schedules after the watchdog = %d, want 0 without a RUN# row", noRowID, scanned)
	}
	for id, breachAt := range map[string]time.Time{metID: at(1, 8), sameDayID: at(0, 8)} {
		all, scanned := dueSchedules(t, id, at(1, 8))
		got := map[string]time.Time{}
		for _, s := range all {
			got[s.Name] = s.at
		}
		want := map[string]time.Time{
			id + "-stream-" + date + "-sla-warning": breachAt.Add(-time.Hour),
			id + "-stream-" + date + "-sla-breach":  breachAt,
		}
		if scanned != 2 || !maps.EqualFunc(got, want, time.Time.Equal) {
			t.Fatalf("%s schedules after the watchdog = %v (scanned %d), want %v", id, got, scanned, want)
		}
	}

	warn, breach := string(types.EventSLAWarning), string(types.EventSLABreach)
	for i, hour := range []int{7, 8} {
		advanceTo(t, at(0, hour))
		if scanned, fired := fireDueSchedules(t, sameDayID); scanned != 2-i || fired != 1 {
			t.Fatalf("%s at %02d:00: schedules scanned=%d fired=%d, want %d and 1", sameDayID, hour, scanned, fired, 2-i)
		}
	}
	waitEvents(t, sameDayID, date, map[string]int{warn: 1, breach: 1}, []string{warn, breach})

	advanceTo(t, at(0, slaRunHour))
	vp, jc, met := string(types.EventValidationPassed), string(types.EventJobCompleted), string(types.EventSLAMet)
	for _, id := range []string{metID, sameDayID} {
		writeReady(t, id, date)
		reportJob(t, id, date, waitLaunch(t, id, 1), types.JobEventSuccess)
		waitCompleted(t, id, date)
	}
	jt := string(types.EventJobTriggered)
	waitEvents(t, metID, date, map[string]int{jt: 2, vp: 1, jc: 1, met: 1}, []string{vp, jc}, []string{vp, met})
	waitEvents(t, sameDayID, date, map[string]int{jt: 2, vp: 1, jc: 1, warn: 1, breach: 2},
		[]string{warn, breach, vp, jc}, []string{vp, breach})

	assertNothingFires(t, metID, at(1, 7))
	assertNothingFires(t, metID, at(1, 8))
	if scanned, fired := fireDueSchedules(t, sameDayID); scanned != 0 || fired != 0 {
		t.Fatalf("%s schedules scanned=%d fired=%d, want 0 and 0", sameDayID, scanned, fired)
	}
	waitAlerts(t, metID, alert{metID, date, jc}, alert{metID, date, met})
	waitAlerts(t, sameDayID, alert{sameDayID, date, warn}, alert{sameDayID, date, breach},
		alert{sameDayID, date, jc}, alert{sameDayID, date, breach})
	for _, id := range []string{metID, sameDayID} {
		assertLaunches(t, id, 1)
		assertTriggerStatus(t, id, date, types.TriggerStatusCompleted)
	}
	waitAlerts(t, noRowID)
}

// Cron is the start-by time for each RUN# date (08:00 here), never a start.
// "start-by-on-time" is ready at 07:00, so the watchdog stays quiet. For
// "start-by-late", ready at 10:00, the 08:05 pass publishes one SCHEDULE_MISSED,
// the 09:00 pass none, and the sensor still launches the job once.
func TestScenarioStartBy(t *testing.T) {
	const onTimeID, lateID, cronSchedule = "start-by-on-time", "start-by-late", "cron"
	start, date := startDay(t)
	at := func(hour, minute int) time.Time {
		return time.Date(start.Year(), start.Month(), start.Day(), hour, minute, 0, 0, time.UTC)
	}
	for _, id := range []string{onTimeID, lateID} {
		cfg := scenarioConfig(id)
		cfg.Schedule.Cron = "0 8 * * *"
		putConfig(t, cfg)
		putRunDate(t, id, date)
	}
	jt, vp, jc := string(types.EventJobTriggered), string(types.EventValidationPassed), string(types.EventJobCompleted)
	missed := string(types.EventScheduleMissed)
	// run waits for the execution to succeed (row COMPLETED) before the clock moves.
	run := func(id string) {
		t.Helper()
		writeReady(t, id, date)
		runID := waitLaunch(t, id, 1)
		reportJobOn(t, cronSchedule, id, date, runID, types.JobEventSuccess)
		waitFor(t, settleTimeout, id+" execution to succeed", func() bool {
			return executionStatus(t, runID) == sfntypes.ExecutionStatusSucceeded
		})
	}

	advanceTo(t, at(7, 0))
	run(onTimeID)
	waitEvents(t, onTimeID, date, map[string]int{jt: 2, vp: 1, jc: 1}, []string{vp, jc})

	advanceTo(t, at(8, 5))
	invokeWatchdog(t)
	waitEvents(t, lateID, date, map[string]int{missed: 1})
	advanceTo(t, at(9, 0))
	invokeWatchdog(t)

	advanceTo(t, at(10, 0))
	run(lateID)
	waitEvents(t, lateID, date, map[string]int{missed: 1, jt: 2, vp: 1, jc: 1}, []string{missed, vp, jc})

	waitAlerts(t, onTimeID, alert{onTimeID, date, jc})
	waitAlerts(t, lateID, alert{lateID, date, missed}, alert{lateID, date, jc})
	assertLaunches(t, onTimeID, 1)
	assertLaunches(t, lateID, 1)
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

// A sensor that turns ready after the 14:00 cutoff starts nothing. The date
// has no RUN# row, so the watchdog's 13:00 pass does nothing; the
// stream-router closes the date with FAILED_FINAL and one
// SENSOR_DEADLINE_EXPIRED, and a later watchdog pass, whose reconciler sees
// the ready sensor, adds nothing.
func TestScenarioSensorAfterCutoff(t *testing.T) {
	const id = "sensor-after-cutoff"
	start, date := startDay(t)
	at := func(hour, minute int) time.Time {
		return time.Date(start.Year(), start.Month(), start.Day(), hour, minute, 0, 0, time.UTC)
	}
	cfg := scenarioConfig(id)
	cfg.Schedule.Trigger.Cutoff = "14:00"
	putConfig(t, cfg)

	advanceTo(t, at(13, 0))
	invokeWatchdog(t)
	advanceTo(t, at(14, 30))
	writeReady(t, id, date)
	expired := string(types.EventSensorDeadlineExpired)
	waitEvents(t, id, date, map[string]int{expired: 1})
	assertTriggerStatus(t, id, date, types.TriggerStatusFailedFinal)

	advanceTo(t, at(14, 31)) // a duplicate would get its own millisecond in the events table
	invokeWatchdog(t)
	waitAlerts(t, id, alert{id, date, expired})
	waitEvents(t, id, date, map[string]int{expired: 1})
	assertTriggerStatus(t, id, date, types.TriggerStatusFailedFinal)
	assertLaunches(t, id, 0)
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

// A lambda job whose function returns an error has run and failed: the
// orchestrator writes a PERMANENT fail row for the run instead of failing the
// Trigger task, so the Trigger state's Retry never runs, and CompleteTrigger
// applies job.maxCodeRetries (0) rather than job.maxRetries (1). The function
// is interlock's own orchestrator, which rejects an unknown mode.
func TestScenarioLambdaFunctionError(t *testing.T) {
	const id = "lambda-function-error"
	_, date := startDay(t)
	cfg := scenarioConfig(id)
	noCodeRetries := 0
	cfg.Job = types.JobConfig{
		Type:           types.TriggerLambda,
		Config:         map[string]interface{}{"functionName": orchestratorFn, "payload": `{"mode":"no-such-mode"}`},
		MaxRetries:     1,
		MaxCodeRetries: &noCodeRetries,
	}
	putConfig(t, cfg)

	writeReady(t, id, date)
	vp, jf, ex := string(types.EventValidationPassed), string(types.EventJobFailed), string(types.EventRetryExhausted)
	waitEvents(t, id, date, map[string]int{string(types.EventJobTriggered): 2, vp: 1, jf: 1, ex: 1})

	rows := jobRows(t, id)
	if len(rows) != 1 || rows[0].Event != types.JobEventFail || rows[0].Category != string(types.FailurePermanent) ||
		!strings.Contains(rows[0].Error, "unknown orchestrator mode") {
		t.Fatalf("job log = %+v, want one PERMANENT fail row with the function's error", rows)
	}
	run := rows[0].RunID
	waitFor(t, settleTimeout, "the execution to finish", func() bool {
		return executionStatus(t, run) != sfntypes.ExecutionStatusRunning
	})
	for _, e := range executionHistory(t, run) {
		if e.Type == sfntypes.HistoryEventTypeTaskFailed || e.Type == sfntypes.HistoryEventTypeLambdaFunctionFailed {
			t.Fatalf("execution %s has a failed task (%s): the Trigger state must not retry a job failure", run, e.Type)
		}
	}
	waitAlerts(t, id, alert{id, date, jf}, alert{id, date, ex})
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
	cfg.SLA = &types.SLAConfig{Deadline: slaDeadline, DeadlineDayOffset: 1, ExpectedDuration: slaExpected}
	putConfig(t, cfg)
	putRunDate(t, id, date)

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
// completes, and the watchdog reports the bad row once, not again on its next
// pass. The absence is checked against a sentinel on the same path: a third
// pass after the marker's 24 h TTL reports again, so exactly two alerts and
// two events arrive (a report from the second pass would make three). A
// pipeline whose ID breaks the ID rule, and one whose job.type is not a
// trigger type (command), are reported the same way and never launch, though
// their sensors are ready.
func TestScenarioBadConfigIsolated(t *testing.T) {
	const id, bad, badID, badType = "bad-config-neighbour", "bad-config", "bad.config-id", "bad-config-type"
	typeCfg := scenarioConfig(badType)
	typeCfg.Job = types.JobConfig{Type: "command", Config: map[string]interface{}{"command": "true"}}
	badConfigs := map[string]string{bad: `{"pipeline":{"id":"bad-config"},"job":{"maxRetries":"3"}}`}
	for pipeline, cfg := range map[string]types.PipelineConfig{badID: scenarioConfig(badID), badType: typeCfg} {
		data, err := json.Marshal(cfg)
		if err != nil {
			t.Fatalf("marshal config: %v", err)
		}
		badConfigs[pipeline] = string(data)
	}
	start, date := startDay(t)
	keys := []map[string]ddbtypes.AttributeValue{}
	for pipeline, config := range badConfigs {
		sum := sha256.Sum256([]byte(config))
		pk := &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipeline)}
		putItem(t, map[string]ddbtypes.AttributeValue{"PK": pk, "SK": &ddbtypes.AttributeValueMemberS{Value: types.ConfigSK},
			"config": &ddbtypes.AttributeValueMemberS{Value: config}})
		for _, sk := range []string{types.ConfigSK, "CONFIG_INVALID#" + hex.EncodeToString(sum[:])[:16]} {
			keys = append(keys, map[string]ddbtypes.AttributeValue{"PK": pk, "SK": &ddbtypes.AttributeValueMemberS{Value: sk}})
		}
	}
	t.Cleanup(func() { // the configs and their markers; later watchdog passes would report them again
		ctx, cancel := callCtx()
		defer cancel()
		for _, key := range keys {
			if _, err := ddb.DeleteItem(ctx, &dynamodb.DeleteItemInput{TableName: aws.String(controlTable), Key: key}); err != nil {
				t.Errorf("delete %v %v: %v", key["PK"], key["SK"], err)
			}
		}
	})
	putConfig(t, scenarioConfig(id))

	writeReady(t, badID, date)
	writeReady(t, badType, date)
	writeReady(t, id, date)
	reportJob(t, id, date, waitLaunch(t, id, 1), types.JobEventSuccess)
	waitCompleted(t, id, date)
	vp, jc := string(types.EventValidationPassed), string(types.EventJobCompleted)
	waitEvents(t, id, date, map[string]int{string(types.EventJobTriggered): 2, vp: 1, jc: 1}, []string{vp, jc})

	invalid := string(types.EventConfigInvalid)
	invokeWatchdog(t)
	waitEvents(t, bad, "", map[string]int{invalid: 1})
	waitEvents(t, badID, "", map[string]int{invalid: 1})
	waitEvents(t, badType, "", map[string]int{invalid: 1})
	advanceTo(t, start.Add(time.Hour))
	invokeWatchdog(t) // the marker holds: no report
	advanceTo(t, start.Add(25*time.Hour))
	invokeWatchdog(t) // the marker has expired: the sentinel report
	for _, b := range []string{bad, badID, badType} {
		waitEvents(t, b, "", map[string]int{invalid: 2})
		waitAlerts(t, b, alert{b, "", invalid}, alert{b, "", invalid})
	}
	waitAlerts(t, id, alert{id, date, jc})
	assertLaunches(t, id, 1)
	assertLaunches(t, badID, 0)
	assertLaunches(t, badType, 0)
}

func stateExited(h []sfntypes.HistoryEvent, state string) bool {
	for _, e := range h {
		if d := e.StateExitedEventDetails; d != nil && aws.ToString(d.Name) == state {
			return true
		}
	}
	return false
}
