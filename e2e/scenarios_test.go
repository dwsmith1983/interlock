//go:build e2e

package e2e

// Each scenario starts a fresh simulated day with startDay and uses its own
// pipeline, and reads only that pipeline's events, alerts and schedules, so
// the scenarios do not depend on the order go test runs them in.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
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
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"

	"github.com/dwsmith1983/interlock/pkg/types"
)

const (
	scenarioSensor = "ready"
	dayStartHour   = 6       // each scenario starts at 06:00 UTC on its day
	slaDeadline    = "08:00" // 08:00 UTC, deadlineDayOffset days after the run's date
	slaExpected    = "1h"    // the warning fires an hour before the breach
	slaRunHour     = 9       // after a same-day 08:00
)

// scenarioConfig is a sensor-triggered pipeline whose http job is the mock,
// like smokeConfig. Its 23:00 cutoff falls after every scenario's first start.
func scenarioConfig(id string) types.PipelineConfig {
	return types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: id},
		Schedule: types.ScheduleConfig{
			Trigger:    &types.TriggerCondition{Key: scenarioSensor, Check: types.CheckEquals, Field: "status", Value: "ready", Cutoff: "23:00"},
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

// assertDetails fails unless the pipeline's events of eventType are as many as
// want, in order, and each one's details hold that want's keys and values.
func assertDetails(t *testing.T, pipeline, eventType string, want ...map[string]any) {
	t.Helper()
	var got []map[string]any
	for _, e := range events(t, pipeline) {
		if e.Type == eventType {
			got = append(got, e.Detail)
		}
	}
	ok := len(got) == len(want)
	for i := 0; ok && i < len(want); i++ {
		for k, v := range want[i] {
			ok = ok && got[i][k] == v
		}
	}
	if !ok {
		t.Fatalf("%s %s details = %v, want %v", pipeline, eventType, got, want)
	}
}

// slaDetail is what an SLA event's details must hold: its source and status,
// and the scenarios' deadline, which falls at breachAt.
func slaDetail(source, status string, breachAt time.Time) map[string]any {
	return map[string]any{"source": source, "status": status,
		"deadline": slaDeadline, "timezone": "UTC", "breachAt": breachAt.Format(time.RFC3339)}
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

// Two pipelines share the 08:00 deadline and have a RUN# row for the date.
// With deadlineDayOffset 0 ("sla-same-day") the 06:00 watchdog pass schedules
// the SLA alerts: the warning fires at 07:00 and the breach at 08:00 while the
// pipeline has not started, and the 09:00 run's cancel publishes its own
// SLA_BREACH verdict. With offset 1 ("sla-met") the deadline is 08:00 the next
// day, so the 06:00 pass schedules nothing (the warning is 25h away) and the
// next day's 02:00 pass does. The 03:00 run finishes before the warning:
// CancelSLASchedules deletes both schedules and publishes SLA_MET, so nothing
// is left to fire. A third ("sla-no-row") has the same-day SLA and no row, so
// the watchdog schedules nothing for it and it gets no alerts.
func TestScenarioSLAMet(t *testing.T) {
	const metID, sameDayID, noRowID = "sla-met", "sla-same-day", "sla-no-row"
	start, date := startDay(t)
	at := func(day, hour int) time.Time {
		return time.Date(start.Year(), start.Month(), start.Day()+day, hour, 0, 0, 0, time.UTC)
	}
	for id, offset := range map[string]int{metID: 1, sameDayID: 0, noRowID: 0} {
		cfg := scenarioConfig(id)
		cfg.SLA = &types.SLAConfig{Deadline: slaDeadline, DeadlineDayOffset: offset, ExpectedDuration: slaExpected}
		cfg.Schedule.Trigger.CutoffDayOffset = offset
		putConfig(t, cfg)
	}
	putRunDate(t, metID, date)
	putRunDate(t, sameDayID, date)

	invokeWatchdog(t)
	if _, scanned := dueSchedules(t, noRowID, at(1, 8)); scanned != 0 {
		t.Fatalf("%s schedules after the watchdog = %d, want 0 without a RUN# row", noRowID, scanned)
	}
	assertScheduled := func(id string, breachAt time.Time) {
		t.Helper()
		all, scanned := dueSchedules(t, id, breachAt)
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
	assertScheduled(sameDayID, at(0, 8))
	if _, scanned := dueSchedules(t, metID, at(1, 8)); scanned != 0 {
		t.Fatalf("%s schedules 25h before the warning = %d, want 0", metID, scanned)
	}

	warn, breach := string(types.EventSLAWarning), string(types.EventSLABreach)
	for i, hour := range []int{7, 8} {
		advanceTo(t, at(0, hour))
		if scanned, fired := fireDueSchedules(t, sameDayID); scanned != 2-i || fired != 1 {
			t.Fatalf("%s at %02d:00: schedules scanned=%d fired=%d, want %d and 1", sameDayID, hour, scanned, fired, 2-i)
		}
	}
	waitEvents(t, sameDayID, date, map[string]int{warn: 1, breach: 1}, []string{warn, breach})

	run := func(id string) {
		t.Helper()
		writeReady(t, id, date)
		reportJob(t, id, date, waitLaunch(t, id, 1), types.JobEventSuccess)
		waitCompleted(t, id, date)
	}
	advanceTo(t, at(0, slaRunHour))
	run(sameDayID)
	vp, jc, met := string(types.EventValidationPassed), string(types.EventJobCompleted), string(types.EventSLAMet)
	jt := string(types.EventJobTriggered)
	waitEvents(t, sameDayID, date, map[string]int{jt: 2, vp: 1, jc: 1, warn: 1, breach: 2},
		[]string{warn, breach, vp, jc}, []string{vp, breach})
	assertDetails(t, sameDayID, warn, slaDetail("schedule", "not started", at(0, 8)))
	assertDetails(t, sameDayID, breach, slaDetail("schedule", "not started", at(0, 8)),
		slaDetail("cancel", types.TriggerStatusCompleted, at(0, 8)))

	advanceTo(t, at(1, 2))
	invokeWatchdog(t)
	assertScheduled(metID, at(1, 8))
	advanceTo(t, at(1, 3))
	run(metID)
	waitEvents(t, metID, date, map[string]int{jt: 2, vp: 1, jc: 1, met: 1}, []string{vp, jc}, []string{vp, met})
	assertDetails(t, metID, met, slaDetail("cancel", types.TriggerStatusCompleted, at(1, 8)))

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

// A run whose evaluation began before its cutoff (14:00 UTC, set as New York
// wall time so a Lambda that cannot load the zone puts it hours earlier, before
// the sensor), and whose rules pass only after it, ends at the next
// evaluation: Evaluate reports cutoff,
// IsReady sends it to ValidationExhausted, which publishes one
// SENSOR_DEADLINE_EXPIRED (source orchestrator) and sets FAILED_FINAL, and the
// execution ends without Trigger or the SLA cancel. A later watchdog pass adds
// nothing and the job never launches.
func TestScenarioCutoffDuringEvaluation(t *testing.T) {
	const id = "cutoff-during-eval"
	start, date := startDay(t)
	at := func(hour, minute int) time.Time {
		return time.Date(start.Year(), start.Month(), start.Day(), hour, minute, 0, 0, time.UTC)
	}
	newYork, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("load zone: %v", err)
	}
	cfg := scenarioConfig(id)
	cfg.Schedule.Timezone = "America/New_York"
	cfg.Schedule.Trigger.Cutoff = at(14, 0).In(newYork).Format("15:04")
	cfg.Validation.Rules = append(cfg.Validation.Rules, types.ValidationRule{Key: "upstream", Check: types.CheckExists})
	putConfig(t, cfg)

	// The window is 60 s of real time, so nothing here waits on anything slow.
	advanceTo(t, at(13, 50))
	writeReady(t, id, date)
	var exec string
	waitFor(t, settleTimeout, id+" first evaluation", func() bool {
		for _, e := range runningExecutions(t) {
			if name := aws.ToString(e.Name); strings.HasPrefix(name, id+"-") {
				exec = name
			}
		}
		return exec != "" && stateExited(executionHistory(t, exec), "IsReady")
	})
	assertTriggerStatus(t, id, date, types.TriggerStatusRunning)
	assertLaunches(t, id, 0)

	advanceTo(t, at(14, 0))
	writeSensor(t, id, "upstream", map[string]interface{}{"date": date})
	jt, expired := string(types.EventJobTriggered), string(types.EventSensorDeadlineExpired)
	waitEvents(t, id, date, map[string]int{jt: 1, expired: 1})
	waitFor(t, settleTimeout, id+" execution to end", func() bool {
		return executionStatus(t, exec) != sfntypes.ExecutionStatusRunning
	})
	if got := executionStatus(t, exec); got != sfntypes.ExecutionStatusSucceeded {
		t.Fatalf("execution %s = %s, want SUCCEEDED", exec, got)
	}
	if h := executionHistory(t, exec); !stateExited(h, "ValidationExhausted") || stateExited(h, "Trigger") || stateExited(h, "CheckCancelSLA") {
		t.Fatalf("execution %s did not end through ValidationExhausted alone", exec)
	}
	assertTriggerStatus(t, id, date, types.TriggerStatusFailedFinal)

	advanceTo(t, at(14, 1))
	invokeWatchdog(t)
	waitAlerts(t, id, alert{id, date, expired})
	waitEvents(t, id, date, map[string]int{jt: 1, expired: 1})
	assertLaunches(t, id, 0)
}

// Three pipelines with a 14:00 cutoff and a 10 s evaluation window become ready
// at 13:00 without their "upstream" sensor. Each evaluation runs out before
// the cutoff, so ValidationExhausted releases the date: no trigger row, an
// evaluation-released job row, EVALUATION_RELEASED and no launch.
// "released-then-ready" gets its upstream sensor at 13:30 and a new trigger
// sensor write, which starts it again; it launches and completes.
// "released-never-ready" is restarted by the watchdog's reconciler at 13:40
// (JOB_TRIGGERED, not TRIGGER_RECOVERED), runs out again, and the 14:01 pass
// closes it with one SENSOR_DEADLINE_EXPIRED; a later pass adds nothing, and
// EVALUATION_RELEASED is never an alert. "released-cron" has schedule.cron (only
// its start-by time) and gets its upstream sensor at 13:30 with no new trigger
// write: the 13:40 reconciler pass restarts it (JOB_TRIGGERED, not
// TRIGGER_RECOVERED) and it launches once and completes.
func TestScenarioEvaluationReleased(t *testing.T) {
	const readyID, neverID, cronID, cronSchedule = "released-then-ready", "released-never-ready", "released-cron", "cron"
	start, date := startDay(t)
	at := func(hour, minute int) time.Time {
		return time.Date(start.Year(), start.Month(), start.Day(), hour, minute, 0, 0, time.UTC)
	}
	for _, id := range []string{readyID, neverID, cronID} {
		cfg := scenarioConfig(id)
		if id == cronID {
			cfg.Schedule.Cron = "0 8 * * *"
		}
		cfg.Schedule.Trigger.Cutoff = "14:00"
		cfg.Schedule.Evaluation = types.EvaluationWindow{Window: "10s", Interval: "5s"} // real seconds
		cfg.Validation.Rules = append(cfg.Validation.Rules, types.ValidationRule{Key: "upstream", Check: types.CheckExists})
		putConfig(t, cfg)
	}

	advanceTo(t, at(13, 0))
	jt, released := string(types.EventJobTriggered), string(types.EventEvaluationReleased)
	writeReady(t, readyID, date)
	writeReady(t, neverID, date)
	writeReady(t, cronID, date)
	for _, id := range []string{readyID, neverID} {
		waitEvents(t, id, date, map[string]int{jt: 1, released: 1})
		assertReleased(t, id, date, 1)
	}
	waitEvents(t, cronID, date, map[string]int{jt: 1, released: 1})

	// An identical item makes no stream record, so the trigger sensor changes.
	advanceTo(t, at(13, 30))
	writeSensor(t, cronID, "upstream", map[string]interface{}{"date": date})
	writeSensor(t, readyID, "upstream", map[string]interface{}{"date": date})
	writeSensor(t, readyID, scenarioSensor, map[string]interface{}{"status": "ready", "date": date, "attempt": 2})
	reportJob(t, readyID, date, waitLaunch(t, readyID, 1), types.JobEventSuccess)
	waitCompleted(t, readyID, date)
	vp, jc := string(types.EventValidationPassed), string(types.EventJobCompleted)
	waitEvents(t, readyID, date, map[string]int{jt: 3, released: 1, vp: 1, jc: 1}, []string{released, vp, jc})
	assertLaunches(t, readyID, 1)

	advanceTo(t, at(13, 40))
	invokeWatchdog(t) // restarts neverID and cronID: readyID has a trigger row
	waitEvents(t, neverID, date, map[string]int{jt: 2, released: 2})
	assertReleased(t, neverID, date, 2)
	cronRun := waitLaunch(t, cronID, 1)
	reportJobOn(t, cronSchedule, cronID, date, cronRun, types.JobEventSuccess)
	waitFor(t, settleTimeout, cronID+" execution to succeed", func() bool {
		return executionStatus(t, cronRun) == sfntypes.ExecutionStatusSucceeded
	})
	waitEvents(t, cronID, date, map[string]int{jt: 3, released: 1, vp: 1, jc: 1}, []string{released, vp, jc})

	advanceTo(t, at(14, 1))
	invokeWatchdog(t)
	expired := string(types.EventSensorDeadlineExpired)
	waitEvents(t, neverID, date, map[string]int{jt: 2, released: 2, expired: 1})
	assertTriggerStatus(t, neverID, date, types.TriggerStatusFailedFinal)

	advanceTo(t, at(14, 2))
	invokeWatchdog(t)
	waitAlerts(t, neverID, alert{neverID, date, expired})
	waitEvents(t, neverID, date, map[string]int{jt: 2, released: 2, expired: 1})
	assertLaunches(t, neverID, 0)
	waitAlerts(t, readyID, alert{readyID, date, jc})
	assertTriggerStatus(t, readyID, date, types.TriggerStatusCompleted)
	waitAlerts(t, cronID, alert{cronID, date, jc})
	assertLaunches(t, cronID, 1)
}

// assertReleased checks a date whose n evaluations all ran out before its
// cutoff: no trigger row, n evaluation-released job rows, n executions that
// succeeded through ValidationExhausted without Trigger, and no launch.
func assertReleased(t *testing.T, id, date string, n int) {
	t.Helper()
	assertTriggerStatus(t, id, date, "")
	if got, want := jobEvents(t, id), slices.Repeat([]string{types.JobEventEvaluationReleased}, n); !slices.Equal(got, want) {
		t.Fatalf("%s job log = %v, want %v", id, got, want)
	}
	names := executionNames(t, id+"-"+streamSchedule+"-")
	if len(names) != n {
		t.Fatalf("%s executions = %v, want %d", id, names, n)
	}
	for _, name := range names {
		waitFor(t, settleTimeout, name+" to end", func() bool { return executionStatus(t, name) != sfntypes.ExecutionStatusRunning })
		h := executionHistory(t, name)
		if got := executionStatus(t, name); got != sfntypes.ExecutionStatusSucceeded || !stateExited(h, "ValidationExhausted") || stateExited(h, "Trigger") {
			t.Fatalf("execution %s = %s, want SUCCEEDED through ValidationExhausted without Trigger", name, got)
		}
	}
	assertLaunches(t, id, 0)
}

// The job reports fail for run A. Execution A's CompleteTrigger is the only
// actor that reacts (job.maxRetries 1): it keeps the trigger row RUNNING,
// starts B as <A>-r1 and ends through IsRetrying and Done, leaving the SLA to
// B. Only B waits in RetryBackoff before it evaluates. The stream-router only
// publishes JOB_FAILED for the row. B must not read A's fail: it keeps polling
// until the job reports success for B, and its CompleteTrigger leaves the
// trigger row COMPLETED.
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
	if a, b := stateExited(executionHistory(t, runA), "RetryBackoff"), stateExited(executionHistory(t, runB), "RetryBackoff"); a || !b {
		t.Fatalf("exited RetryBackoff: execution A %v, execution B %v, want only B (the retry)", a, b)
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
	rejectedLambdaRun(t, "lambda-function-error", `{"mode":"no-such-mode"}`)
}

// A lambda job learns the run it serves: the identity variables in its
// payload expand to the run's date and ID. The orchestrator puts the mode it
// rejects in the fail row's error, which keeps only the first 200 bytes of
// the function's error, so the date goes first.
func TestScenarioLambdaRunIdentity(t *testing.T) {
	date, row := rejectedLambdaRun(t, "lambda-run-identity", `{"mode":"${INTERLOCK_DATE}/${INTERLOCK_RUN_ID}"}`)
	const runIDPrefix = 40 // past the pipeline ID, within the 200 bytes kept
	want := date + "/" + row.RunID[:min(len(row.RunID), runIDPrefix)]
	if !strings.Contains(row.Error, want) {
		t.Fatalf("fail row error = %q, want it to contain the run date and ID %q", row.Error, want)
	}
}

// rejectedLambdaRun runs pipeline id's lambda job, the orchestrator invoked
// with payload, on a fresh day, checks that the rejected mode ends the run
// as a PERMANENT job failure, and returns the date and the run's fail row.
func rejectedLambdaRun(t *testing.T, id, payload string) (string, jobRow) {
	t.Helper()
	_, date := startDay(t)
	cfg := scenarioConfig(id)
	noCodeRetries := 0
	cfg.Job = types.JobConfig{
		Type:           types.TriggerLambda,
		Config:         map[string]interface{}{"functionName": orchestratorFn, "payload": payload},
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
	return date, rows[0]
}

// A step-function job gets the run date as top-level keys of its execution
// input, next to its configured arguments. The job is a one-state Pass
// machine, so the run completes as soon as CheckJob polls it.
func TestScenarioStepFunctionRunDate(t *testing.T) {
	const id = "sfn-run-date"
	_, date := startDay(t)
	ctx, cancel := callCtx()
	sm, err := sfnAPI.CreateStateMachine(ctx, &sfn.CreateStateMachineInput{
		Name:       aws.String(prefix + "-" + id),
		Definition: aws.String(`{"StartAt":"Done","States":{"Done":{"Type":"Pass","End":true}}}`),
		RoleArn:    aws.String("arn:aws:iam::000000000000:role/" + id),
	})
	cancel()
	if err != nil {
		t.Fatalf("create the job state machine: %v", err)
	}
	// A rerun against a kept stack must not see this run's execution.
	t.Cleanup(func() {
		ctx, cancel := callCtx()
		defer cancel()
		_, err := sfnAPI.DeleteStateMachine(ctx, &sfn.DeleteStateMachineInput{StateMachineArn: sm.StateMachineArn})
		var notFound *sfntypes.StateMachineDoesNotExist
		if err != nil && !errors.As(err, &notFound) {
			t.Errorf("delete the job state machine: %v", err)
		}
	})
	cfg := scenarioConfig(id)
	cfg.Job = types.JobConfig{Type: types.TriggerStepFunction, Config: map[string]interface{}{
		"stateMachineArn": aws.ToString(sm.StateMachineArn), "arguments": map[string]interface{}{"env": "e2e"},
	}}
	putConfig(t, cfg)

	writeReady(t, id, date)
	waitCompleted(t, id, date)
	ctx, cancel = callCtx()
	defer cancel()
	execs, err := sfnAPI.ListExecutions(ctx, &sfn.ListExecutionsInput{StateMachineArn: sm.StateMachineArn})
	if err != nil || len(execs.Executions) != 1 {
		t.Fatalf("job executions = %+v (%v), want one", execs, err)
	}
	out, err := sfnAPI.DescribeExecution(ctx, &sfn.DescribeExecutionInput{ExecutionArn: execs.Executions[0].ExecutionArn})
	if err != nil {
		t.Fatalf("describe the job execution: %v", err)
	}
	var input map[string]string
	want := map[string]string{"env": "e2e", "par_day": strings.ReplaceAll(date, "-", "")}
	if err := json.Unmarshal([]byte(aws.ToString(out.Input)), &input); err != nil || !maps.Equal(input, want) {
		t.Fatalf("job input = %s (%v), want %v", aws.ToString(out.Input), err, want)
	}
}

// The SLA stays live through a CompleteTrigger retry. At 06:00, two hours
// before the breach, the watchdog schedules the alerts and run A starts. A
// fails, and its CompleteTrigger starts <A>-r1 and keeps the trigger row
// RUNNING, so the newest job row is A's fail while B polls. The warning fires
// at 07:00 and must publish SLA_WARNING. B succeeds before the breach, and its
// CancelSLASchedules deletes the breach schedule and publishes SLA_MET.
func TestScenarioSLAWarningDuringRetry(t *testing.T) {
	const id = "sla-warn-retry"
	start, date := startDay(t)
	cfg := scenarioConfig(id)
	cfg.Job.MaxRetries = 1
	cfg.SLA = &types.SLAConfig{Deadline: slaDeadline, ExpectedDuration: slaExpected}
	putConfig(t, cfg)
	putRunDate(t, id, date)

	invokeWatchdog(t)
	breachAt := time.Date(start.Year(), start.Month(), start.Day(), 8, 0, 0, 0, time.UTC)
	warnAt := breachAt.Add(-time.Hour)

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
	assertDetails(t, id, warn, slaDetail("schedule", types.TriggerStatusRunning, breachAt))
	assertDetails(t, id, met, slaDetail("cancel", types.TriggerStatusCompleted, breachAt))

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
// pipeline whose ID breaks the ID rule, one whose job.type is not a trigger
// type (command), and one whose schedule.timezone is not a zone name
// (America/NewYork) are reported the same way and never launch, though their
// sensors are ready.
func TestScenarioBadConfigIsolated(t *testing.T) {
	const id, bad, badID, badType, badZone = "bad-config-neighbour", "bad-config", "bad.config-id", "bad-config-type", "bad-config-zone"
	typeCfg := scenarioConfig(badType)
	typeCfg.Job = types.JobConfig{Type: "command", Config: map[string]interface{}{"command": "true"}}
	zoneCfg := scenarioConfig(badZone)
	zoneCfg.Schedule.Timezone = "America/NewYork"
	badConfigs := map[string]string{bad: `{"pipeline":{"id":"bad-config"},"job":{"maxRetries":"3"}}`}
	for pipeline, cfg := range map[string]types.PipelineConfig{badID: scenarioConfig(badID), badType: typeCfg, badZone: zoneCfg} {
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
	writeReady(t, badZone, date)
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
	waitEvents(t, badZone, "", map[string]int{invalid: 1})
	advanceTo(t, start.Add(time.Hour))
	invokeWatchdog(t) // the marker holds: no report
	advanceTo(t, start.Add(25*time.Hour))
	invokeWatchdog(t) // the marker has expired: the sentinel report
	for _, b := range []string{bad, badID, badType, badZone} {
		waitEvents(t, b, "", map[string]int{invalid: 2})
		waitAlerts(t, b, alert{b, "", invalid}, alert{b, "", invalid})
	}
	waitAlerts(t, id, alert{id, date, jc})
	assertLaunches(t, id, 1)
	assertLaunches(t, badID, 0)
	assertLaunches(t, badType, 0)
	assertLaunches(t, badZone, 0)
}

// An execution that fails on an infrastructure error ends its date at once. The
// config row turns unreadable while the pipeline evaluates, so evaluations
// report an error until the window closes, and ValidationExhausted then fails
// through its retries. RecordInfraFailure reads the execution's start time,
// publishes one INFRA_FAILURE and sets FAILED_FINAL, and the execution fails.
func TestScenarioInfraFailure(t *testing.T) {
	const id = "infra-failure"
	_, date := startDay(t)
	cfg := scenarioConfig(id)
	cfg.Schedule.Evaluation.Window = "15s"
	cfg.Validation.Rules = append(cfg.Validation.Rules, types.ValidationRule{Key: "upstream", Check: types.CheckExists})
	putConfig(t, cfg)
	t.Cleanup(func() { putConfig(t, cfg) }) // later watchdog passes would report the unreadable row

	writeReady(t, id, date)
	var exec string
	waitFor(t, settleTimeout, id+" execution", func() bool {
		if names := executionNames(t, id+"-"); len(names) == 1 {
			exec = names[0]
		}
		return exec != ""
	})
	putItem(t, map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(id)},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: types.ConfigSK},
		"config": &ddbtypes.AttributeValueMemberS{Value: "{"},
	})

	infra := string(types.EventInfraFailure)
	waitEvents(t, id, date, map[string]int{string(types.EventJobTriggered): 1, infra: 1})
	for _, e := range events(t, id) {
		if e.Type == infra && (!strings.HasPrefix(e.Message, "execution for "+id+" failed on an infrastructure error: ") ||
			!strings.Contains(e.Message, "validation-exhausted get config: unmarshal config")) {
			t.Fatalf("INFRA_FAILURE message = %q, want it to name %s and the config error", e.Message, id)
		}
	}
	waitFor(t, settleTimeout, id+" execution to fail", func() bool {
		return executionStatus(t, exec) == sfntypes.ExecutionStatusFailed
	})
	if !stateExited(executionHistory(t, exec), "RecordInfraFailure") {
		t.Fatalf("execution %s did not leave RecordInfraFailure", exec)
	}
	assertTriggerStatus(t, id, date, types.TriggerStatusFailedFinal)
	waitAlerts(t, id, alert{id, date, infra})
	assertLaunches(t, id, 0)
}

func stateExited(h []sfntypes.HistoryEvent, state string) bool {
	for _, e := range h {
		if d := e.StateExitedEventDetails; d != nil && aws.ToString(d.Name) == state {
			return true
		}
	}
	return false
}
