//go:build e2e

package e2e

import (
	"slices"
	"testing"
	"time"

	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// With postRun.watchWindow 2h and job.maxAutoRerunAge 1h, the run completing
// near 06:00 is watched until near 08:00 but rerun automatically only until
// near 07:00. A trigger sensor rewritten at 07:30 publishes LATE_DATA_ARRIVAL
// and MANUAL_RERUN_REQUIRED and writes no request (checked for noRequestHold,
// as handled request rows are not deleted), so nothing launches. A second
// rewrite publishes MANUAL_RERUN_STILL_REQUIRED; a manual RERUN_REQUEST then
// launches run 2.
func TestScenarioManualRerunRequired(t *testing.T) {
	const id, noRequestHold = "manual-rerun", 10 * time.Second
	start, date := startDay(t)
	cfg := scenarioConfig(id)
	cfg.PostRun = &types.PostRunConfig{WatchWindow: "2h"}
	cfg.Job.MaxAutoRerunAge = "1h"
	putConfig(t, cfg)
	jt, vp, jc := string(types.EventJobTriggered), string(types.EventValidationPassed), string(types.EventJobCompleted)
	late, accepted := string(types.EventLateDataArrival), string(types.EventRerunAccepted)
	manual, still := string(types.EventManualRerunRequired), string(types.EventManualRerunStillRequired)

	writeReady(t, id, date)
	run1 := waitLaunch(t, id, 1)
	reportJob(t, id, date, run1, types.JobEventSuccess)
	waitEvents(t, id, date, map[string]int{jt: 2, vp: 1, jc: 1})
	waitCompleted(t, id, date)
	waitExecutionDone(t, run1)

	advanceTo(t, start.Add(90*time.Minute))
	writeSensor(t, id, scenarioSensor, map[string]interface{}{"status": "ready", "date": date, "attempt": 2})
	waitEvents(t, id, date, map[string]int{jt: 2, vp: 1, jc: 1, late: 1, manual: 1})
	for until := time.Now().Add(noRequestHold); time.Now().Before(until); time.Sleep(pollInterval) {
		if item := rerunRequestRow(t, id, date); item != nil {
			t.Fatalf("rerun request for %s = %v, want none", id, item)
		}
	}
	writeSensor(t, id, scenarioSensor, map[string]interface{}{"status": "ready", "date": date, "attempt": 3})
	waitEvents(t, id, date, map[string]int{jt: 2, vp: 1, jc: 1, late: 2, manual: 1, still: 1})
	want := map[string]any{"reason": "late-data", "blockedBy": "job.maxAutoRerunAge", "source": "sensor", "sensorKey": scenarioSensor}
	assertDetails(t, id, manual, want)
	assertDetails(t, id, still, want)
	if got, want := jobEvents(t, id), []string{types.JobEventSuccess}; !slices.Equal(got, want) {
		t.Fatalf("job log = %v, want %v", got, want)
	}
	assertLaunches(t, id, 1)
	assertTriggerStatus(t, id, date, types.TriggerStatusCompleted)

	putItem(t, map[string]ddbtypes.AttributeValue{
		"PK":          &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(id)},
		"SK":          &ddbtypes.AttributeValueMemberS{Value: types.RerunRequestSK(streamSchedule, date)},
		"reason":      &ddbtypes.AttributeValueMemberS{Value: "manual"},
		"requestedAt": &ddbtypes.AttributeValueMemberS{Value: mockNow(t).UTC().Format(time.RFC3339)},
	})
	run2 := waitLaunch(t, id, 2)
	reportJob(t, id, date, run2, types.JobEventSuccess)
	waitEvents(t, id, date, map[string]int{jt: 3, vp: 2, jc: 2, late: 2, manual: 1, still: 1, accepted: 1})
	waitCompleted(t, id, date)
	waitAlerts(t, id, alert{id, date, jc}, alert{id, date, jc}, alert{id, date, late}, alert{id, date, late},
		alert{id, date, manual}, alert{id, date, accepted})
}
