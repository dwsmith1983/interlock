//go:build e2e

package e2e

import (
	"testing"
	"time"

	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// Three pipelines with a post-run rule (row-count exists, the default 2h
// sensorTimeout) complete soon after 06:00: "postrun-missing-t1" for
// yesterday's date (cutoffDayOffset 1), "postrun-missing-hourly" for hour 05
// of today and "postrun-missing-arrived", T+1 like the first. The third's
// per-period post-run sensor (row-count#<date>, with date) is written after
// completion: POST_RUN_PASSED (not alerted) and the postrun-check marker with
// arrived. A watchdog pass before the deadline alerts nothing and writes no
// marker. At 08:30, past it, a pass publishes one POST_RUN_SENSOR_MISSING
// for each of the first two, for its trigger key, and writes the alerted
// marker with the own-row ttl; a second pass adds nothing.
func TestScenarioPostRunSensorMissing(t *testing.T) {
	const t1, hourly, arrived, postRunSensor = "postrun-missing-t1", "postrun-missing-hourly", "postrun-missing-arrived", "row-count"
	start, today := startDay(t)
	yesterday := start.AddDate(0, 0, -1).Format(smokeDateLayout)
	dates := map[string]string{t1: yesterday, hourly: today + "T05", arrived: yesterday}
	jt, vp, jc := string(types.EventJobTriggered), string(types.EventValidationPassed), string(types.EventJobCompleted)
	passed, missing := string(types.EventPostRunPassed), string(types.EventPostRunSensorMissing)
	ids := []string{t1, hourly, arrived}
	for _, id := range ids {
		cfg := scenarioConfig(id)
		cfg.Schedule.Trigger.CutoffDayOffset = 1
		cfg.PostRun = &types.PostRunConfig{Rules: []types.ValidationRule{{Key: postRunSensor, Check: types.CheckExists}}}
		putConfig(t, cfg)
	}
	for _, id := range ids {
		date := dates[id]
		ready := map[string]interface{}{"status": "ready", "date": date[:10]}
		if id == hourly {
			ready["hour"] = "05"
		}
		writeSensor(t, id, scenarioSensor, ready)
		run := waitLaunch(t, id, 1)
		reportJob(t, id, date, run, types.JobEventSuccess)
		waitEvents(t, id, date, map[string]int{jt: 2, vp: 1, jc: 1})
		waitCompleted(t, id, date)
		waitExecutionDone(t, run)
	}

	invokeWatchdog(t)
	for _, id := range ids {
		if m := arrivalMarker(t, id, dates[id]); m != nil {
			t.Fatalf("%s postrun-check marker before the deadline = %v, want none", id, m)
		}
	}
	wrote := mockNow(t)
	writeSensor(t, arrived, postRunSensor+"#"+yesterday, map[string]interface{}{"date": yesterday, "count": 5})
	waitEvents(t, arrived, yesterday, map[string]int{jt: 2, vp: 1, jc: 1, passed: 1})
	assertMarker(t, arrived, yesterday, "arrived")
	assertOwnRowTTL(t, arrived, types.SensorSK("postrun-check#"+yesterday), wrote)

	pass := start.Add(150 * time.Minute)
	advanceTo(t, pass)
	invokeWatchdog(t)
	invokeWatchdog(t)
	for _, id := range []string{t1, hourly} {
		date := dates[id]
		waitEvents(t, id, date, map[string]int{jt: 2, vp: 1, jc: 1, missing: 1})
		assertDetails(t, id, missing, map[string]any{"source": "watchdog", "ruleKeys": postRunSensor})
		assertMarker(t, id, date, "alerted")
		assertOwnRowTTL(t, id, types.SensorSK("postrun-check#"+date), pass)
		waitAlerts(t, id, alert{id, date, jc}, alert{id, date, missing})
	}
	assertMarker(t, arrived, yesterday, "arrived")
	waitEvents(t, arrived, yesterday, map[string]int{jt: 2, vp: 1, jc: 1, passed: 1})
	waitAlerts(t, arrived, alert{arrived, yesterday, jc})
}

// assertMarker fails unless the pipeline's postrun-check marker for date
// holds only field, set to "true".
func assertMarker(t *testing.T, pipeline, date, field string) {
	t.Helper()
	m := arrivalMarker(t, pipeline, date)
	if v, ok := m[field].(*ddbtypes.AttributeValueMemberS); len(m) != 1 || !ok || v.Value != "true" {
		t.Fatalf("%s postrun-check marker for %s = %#v, want only %s true", pipeline, date, m, field)
	}
}
