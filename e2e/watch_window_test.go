//go:build e2e

package e2e

import (
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"

	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/dwsmith1983/interlock/pkg/validation"
)

// assertOwnRowTTL checks that the pipeline's control row sk carries the ttl
// Interlock gives its own rows: the simulated time of the write plus
// MaxPostRunWatchWindow and a day. The clock only moves forward and the store
// reads it at the write, so the write happened between notBefore (a clock
// reading taken before the step that writes the row) and the clock now.
func assertOwnRowTTL(t *testing.T, pipeline, sk string, notBefore time.Time) {
	t.Helper()
	ctx, cancel := callCtx()
	defer cancel()
	out, err := ddb.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(controlTable),
		ConsistentRead: aws.Bool(true),
		Key: map[string]ddbtypes.AttributeValue{
			"PK": &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipeline)},
			"SK": &ddbtypes.AttributeValueMemberS{Value: sk},
		},
	})
	if err != nil {
		t.Fatalf("get %s for %s: %v", sk, pipeline, err)
	}
	life := validation.MaxPostRunWatchWindow + 24*time.Hour
	lo, hi := notBefore.Add(life).Unix(), mockNow(t).Add(life).Unix()
	n, ok := out.Item["ttl"].(*ddbtypes.AttributeValueMemberN)
	if !ok {
		t.Fatalf("%s for %s: ttl = %#v, want a number in [%d, %d]", sk, pipeline, out.Item["ttl"], lo, hi)
	}
	ttl, err := strconv.ParseInt(n.Value, 10, 64)
	if err != nil || ttl < lo || ttl > hi {
		t.Fatalf("%s for %s: ttl = %s, want a number in [%d, %d]", sk, pipeline, n.Value, lo, hi)
	}
}

// waitExecutionDone waits until the named execution has ended, so its
// complete-trigger (completion time and baseline) is done before the clock
// moves.
func waitExecutionDone(t *testing.T, name string) {
	t.Helper()
	waitFor(t, settleTimeout, "execution "+name+" to finish", func() bool {
		return executionStatus(t, name) != sfntypes.ExecutionStatusRunning
	})
}

// A completed date is watched for late data for postRun.watchWindow (2h)
// after its latest completion. A trigger sensor rewritten at 07:30, inside the
// first run's window (ends near 08:00), is late data: one LATE_DATA_ARRIVAL and
// a late-data rerun. Its completion near 07:30 starts a new window ending near
// 09:30. A rewrite at 09:45 starts no rerun: it publishes only
// MANUAL_RERUN_REQUIRED (blocked by the window), and a second rewrite only
// MANUAL_RERUN_STILL_REQUIRED, which is not alerted. A manual RERUN_REQUEST
// written after them still runs. The clock keeps ticking between advances, so
// every check keeps at least 15 simulated minutes from a window's end.
func TestScenarioLateDataWatchWindow(t *testing.T) {
	const id = "late-data-window"
	start, date := startDay(t)
	at := func(hour, minute int) time.Time {
		return time.Date(start.Year(), start.Month(), start.Day(), hour, minute, 0, 0, time.UTC)
	}
	cfg := scenarioConfig(id)
	cfg.PostRun = &types.PostRunConfig{WatchWindow: "2h"}
	putConfig(t, cfg)
	jt, vp, jc := string(types.EventJobTriggered), string(types.EventValidationPassed), string(types.EventJobCompleted)
	late, accepted := string(types.EventLateDataArrival), string(types.EventRerunAccepted)
	manual, still := string(types.EventManualRerunRequired), string(types.EventManualRerunStillRequired)

	writeReady(t, id, date)
	run1 := waitLaunch(t, id, 1)
	assertOwnRowTTL(t, id, types.SensorSK("first-sensor-arrival#"+date), start)
	reportJob(t, id, date, run1, types.JobEventSuccess)
	waitEvents(t, id, date, map[string]int{jt: 2, vp: 1, jc: 1})
	waitCompleted(t, id, date)
	waitExecutionDone(t, run1)

	advanceTo(t, at(7, 30))
	writeSensor(t, id, scenarioSensor, map[string]interface{}{"status": "ready", "date": date, "attempt": 2})
	run2 := waitLaunch(t, id, 2)
	reportJob(t, id, date, run2, types.JobEventSuccess)
	waitEvents(t, id, date, map[string]int{jt: 3, vp: 2, jc: 2, late: 1, accepted: 1})
	waitCompleted(t, id, date)
	waitExecutionDone(t, run2)

	advanceTo(t, at(9, 45))
	writeSensor(t, id, scenarioSensor, map[string]interface{}{"status": "ready", "date": date, "attempt": 3})
	waitEvents(t, id, date, map[string]int{jt: 3, vp: 2, jc: 2, late: 1, accepted: 1, manual: 1})
	writeSensor(t, id, scenarioSensor, map[string]interface{}{"status": "ready", "date": date, "attempt": 4})
	waitEvents(t, id, date, map[string]int{jt: 3, vp: 2, jc: 2, late: 1, accepted: 1, manual: 1, still: 1})
	putItem(t, map[string]ddbtypes.AttributeValue{
		"PK":          &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(id)},
		"SK":          &ddbtypes.AttributeValueMemberS{Value: types.RerunRequestSK(streamSchedule, date)},
		"reason":      &ddbtypes.AttributeValueMemberS{Value: "manual"},
		"requestedAt": &ddbtypes.AttributeValueMemberS{Value: mockNow(t).UTC().Format(time.RFC3339)},
	})
	run3 := waitLaunch(t, id, 3)
	reportJob(t, id, date, run3, types.JobEventSuccess)
	waitEvents(t, id, date, map[string]int{jt: 4, vp: 3, jc: 3, late: 1, accepted: 2, manual: 1, still: 1})
	waitCompleted(t, id, date)
	want := map[string]any{"reason": "late-data", "blockedBy": "postRun.watchWindow", "source": "sensor", "sensorKey": scenarioSensor}
	assertDetails(t, id, manual, want)
	assertDetails(t, id, still, want)

	wantLog := []string{types.JobEventSuccess, types.JobEventRerunAccepted,
		types.JobEventSuccess, types.JobEventRerunAccepted, types.JobEventSuccess}
	if got := jobEvents(t, id); !slices.Equal(got, wantLog) {
		t.Fatalf("job log = %v, want %v", got, wantLog)
	}
	assertLaunches(t, id, 3)
	assertTriggerStatus(t, id, date, types.TriggerStatusCompleted)
	waitAlerts(t, id, alert{id, date, jc}, alert{id, date, jc}, alert{id, date, jc},
		alert{id, date, late}, alert{id, date, accepted}, alert{id, date, accepted}, alert{id, date, manual})
}

// rerunRequestRow is the pipeline's RERUN_REQUEST row for date, nil when there
// is none.
func rerunRequestRow(t *testing.T, pipeline, date string) map[string]ddbtypes.AttributeValue {
	t.Helper()
	ctx, cancel := callCtx()
	defer cancel()
	out, err := ddb.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(controlTable),
		ConsistentRead: aws.Bool(true),
		Key: map[string]ddbtypes.AttributeValue{
			"PK": &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipeline)},
			"SK": &ddbtypes.AttributeValueMemberS{Value: types.RerunRequestSK(streamSchedule, date)},
		},
	})
	if err != nil {
		t.Fatalf("get rerun request for %s: %v", pipeline, err)
	}
	return out.Item
}

// arrivalMarker is the data of the pipeline's postrun-check marker for date,
// nil when there is none.
func arrivalMarker(t *testing.T, pipeline, date string) map[string]ddbtypes.AttributeValue {
	t.Helper()
	ctx, cancel := callCtx()
	defer cancel()
	out, err := ddb.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(controlTable),
		ConsistentRead: aws.Bool(true),
		Key: map[string]ddbtypes.AttributeValue{
			"PK": &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipeline)},
			"SK": &ddbtypes.AttributeValueMemberS{Value: types.SensorSK("postrun-check#" + date)},
		},
	})
	if err != nil {
		t.Fatalf("get postrun-check marker for %s: %v", pipeline, err)
	}
	if out.Item == nil {
		return nil
	}
	data, _ := out.Item["data"].(*ddbtypes.AttributeValueMemberM)
	if data == nil {
		t.Fatalf("postrun-check marker for %s: data = %#v, want a map", pipeline, out.Item["data"])
	}
	return data.Value
}

// Drift on a completed date is acted on only inside its post-run watch window
// (2h). The post-run sensor's count is 100 when the run completes near 06:00,
// and complete-trigger captures it as the baseline (POST_RUN_BASELINE_CAPTURED,
// not an alert). At 07:00 it is 120, which publishes POST_RUN_DRIFT and a
// data-drift rerun request, rejected because maxDriftReruns is 0, so the
// baseline stays 100. That 07:00 write, the first after completion, leaves
// the postrun-check marker with arrived and the own-row ttl, which the write
// before the run did not. At 08:15, after the window, a count of 130 publishes
// only MANUAL_RERUN_REQUIRED (data-drift, blocked by the window) and writes no
// request; a count of 140 then publishes only MANUAL_RERUN_STILL_REQUIRED,
// which is not alerted. A manual RERUN_REQUEST written after them is rejected
// (maxManualReruns 0). Neither rejection writes a job row; each RERUN_REJECTED names the request's reason and why.
func TestScenarioDriftWatchWindow(t *testing.T) {
	const id, postRunSensor = "drift-window", "row-count"
	start, date := startDay(t)
	at := func(hour, minute int) time.Time {
		return time.Date(start.Year(), start.Month(), start.Day(), hour, minute, 0, 0, time.UTC)
	}
	zero := 0
	cfg := scenarioConfig(id)
	cfg.Job.MaxDriftReruns, cfg.Job.MaxManualReruns = &zero, &zero
	cfg.PostRun = &types.PostRunConfig{
		Rules:      []types.ValidationRule{{Key: postRunSensor, Check: types.CheckExists}},
		DriftField: "count", WatchWindow: "2h",
	}
	putConfig(t, cfg)
	jt, vp, jc := string(types.EventJobTriggered), string(types.EventValidationPassed), string(types.EventJobCompleted)
	drift, rejected, base := string(types.EventPostRunDrift), string(types.EventRerunRejected), string(types.EventPostRunBaselineCaptured)
	manual, still := string(types.EventManualRerunRequired), string(types.EventManualRerunStillRequired)

	writeSensor(t, id, postRunSensor, map[string]interface{}{"count": 100, "date": date})
	writeReady(t, id, date)
	run := waitLaunch(t, id, 1)
	reported := mockNow(t)
	reportJob(t, id, date, run, types.JobEventSuccess)
	waitEvents(t, id, date, map[string]int{jt: 2, vp: 1, jc: 1, base: 1})
	waitCompleted(t, id, date)
	waitExecutionDone(t, run)
	assertOwnRowTTL(t, id, types.SensorSK("postrun-baseline#"+date), reported)
	if m := arrivalMarker(t, id, date); m != nil {
		t.Fatalf("postrun-check marker before any post-run write = %v, want none", m)
	}

	advanceTo(t, at(7, 0))
	writeSensor(t, id, postRunSensor, map[string]interface{}{"count": 120, "date": date})
	waitEvents(t, id, date, map[string]int{jt: 2, vp: 1, jc: 1, base: 1, drift: 1, rejected: 1})
	// Read before the manual request below overwrites the row.
	assertOwnRowTTL(t, id, types.RerunRequestSK(streamSchedule, date), at(7, 0))
	m := arrivalMarker(t, id, date)
	if v, ok := m["arrived"].(*ddbtypes.AttributeValueMemberS); !ok || v.Value != "true" {
		t.Fatalf("postrun-check marker = %#v, want arrived true", m)
	}
	assertOwnRowTTL(t, id, types.SensorSK("postrun-check#"+date), at(7, 0))

	advanceTo(t, at(8, 15))
	writeSensor(t, id, postRunSensor, map[string]interface{}{"count": 130, "date": date})
	waitEvents(t, id, date, map[string]int{jt: 2, vp: 1, jc: 1, base: 1, drift: 1, rejected: 1, manual: 1})
	writeSensor(t, id, postRunSensor, map[string]interface{}{"count": 140, "date": date})
	waitEvents(t, id, date, map[string]int{jt: 2, vp: 1, jc: 1, base: 1, drift: 1, rejected: 1, manual: 1, still: 1})
	putItem(t, map[string]ddbtypes.AttributeValue{
		"PK":          &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(id)},
		"SK":          &ddbtypes.AttributeValueMemberS{Value: types.RerunRequestSK(streamSchedule, date)},
		"reason":      &ddbtypes.AttributeValueMemberS{Value: "manual"},
		"requestedAt": &ddbtypes.AttributeValueMemberS{Value: mockNow(t).UTC().Format(time.RFC3339)},
	})
	waitEvents(t, id, date, map[string]int{jt: 2, vp: 1, jc: 1, base: 1, drift: 1, rejected: 2, manual: 1, still: 1})
	want := map[string]any{"reason": "data-drift", "blockedBy": "postRun.watchWindow", "source": "sensor",
		"sensorKey": postRunSensor, "driftField": "count"}
	assertDetails(t, id, manual, want)
	assertDetails(t, id, still, want)

	wantLog := []string{types.JobEventSuccess}
	if got := jobEvents(t, id); !slices.Equal(got, wantLog) {
		t.Fatalf("job log = %v, want %v", got, wantLog)
	}
	assertDetails(t, id, rejected,
		map[string]any{"reason": "data-drift", "why": "drift rerun limit exceeded"},
		map[string]any{"reason": "manual", "why": "manual rerun limit exceeded"})
	assertLaunches(t, id, 1)
	assertTriggerStatus(t, id, date, types.TriggerStatusCompleted)
	waitAlerts(t, id, alert{id, date, jc}, alert{id, date, drift}, alert{id, date, rejected}, alert{id, date, rejected},
		alert{id, date, manual})
}
