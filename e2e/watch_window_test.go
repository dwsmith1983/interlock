//go:build e2e

package e2e

import (
	"slices"
	"testing"
	"time"

	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"

	"github.com/dwsmith1983/interlock/pkg/types"
)

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
// 09:30. A rewrite at 09:45 is ignored: no event and no rerun request. A manual
// RERUN_REQUEST written after it still runs, and since the router handles the
// control stream's records in order, its RERUN_ACCEPTED shows the 09:45 sensor
// was handled first. The clock keeps ticking between advances, so every check
// keeps at least 15 simulated minutes from a window's end.
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

	writeReady(t, id, date)
	run1 := waitLaunch(t, id, 1)
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
	putItem(t, map[string]ddbtypes.AttributeValue{
		"PK":          &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(id)},
		"SK":          &ddbtypes.AttributeValueMemberS{Value: types.RerunRequestSK(streamSchedule, date)},
		"reason":      &ddbtypes.AttributeValueMemberS{Value: "manual"},
		"requestedAt": &ddbtypes.AttributeValueMemberS{Value: mockNow(t).UTC().Format(time.RFC3339)},
	})
	run3 := waitLaunch(t, id, 3)
	reportJob(t, id, date, run3, types.JobEventSuccess)
	waitEvents(t, id, date, map[string]int{jt: 4, vp: 3, jc: 3, late: 1, accepted: 2})
	waitCompleted(t, id, date)

	wantLog := []string{types.JobEventSuccess, types.JobEventLateDataArrival, types.JobEventRerunAccepted,
		types.JobEventSuccess, types.JobEventRerunAccepted, types.JobEventSuccess}
	if got := jobEvents(t, id); !slices.Equal(got, wantLog) {
		t.Fatalf("job log = %v, want %v", got, wantLog)
	}
	assertLaunches(t, id, 3)
	assertTriggerStatus(t, id, date, types.TriggerStatusCompleted)
	waitAlerts(t, id, alert{id, date, jc}, alert{id, date, jc}, alert{id, date, jc},
		alert{id, date, late}, alert{id, date, accepted}, alert{id, date, accepted})
}
