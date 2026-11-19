package stream

import (
	"cmp"
	"context"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/interlock/internal/store/storetest"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// sensorRecord is the stream record of the "upstream" sensor naming date.
func sensorRecord(date string) events.DynamoDBEventRecord {
	return sensorRecordWith("upstream", map[string]events.DynamoDBAttributeValue{"date": events.NewStringAttribute(date)})
}

// sensorRecordWith is the stream record of pipeline p's sensor key holding data.
func sensorRecordWith(key string, data map[string]events.DynamoDBAttributeValue) events.DynamoDBEventRecord {
	return events.DynamoDBEventRecord{
		EventName: "INSERT",
		Change: events.DynamoDBStreamRecord{
			SequenceNumber: "1",
			Keys: map[string]events.DynamoDBAttributeValue{
				"PK": events.NewStringAttribute(types.PipelinePK("p")),
				"SK": events.NewStringAttribute(types.SensorSK(key)),
			},
			NewImage: map[string]events.DynamoDBAttributeValue{"data": events.NewMapAttribute(data)},
		},
	}
}

// TestSensorEvent_HourlyKeysStartSeparately: per-hour sensor keys match the
// trigger key by prefix, and each one's date and hour name its own run date,
// so two hours (across midnight here) start two runs.
func TestSensorEvent_HourlyKeysStartSeparately(t *testing.T) {
	d, mem, sfnFake, eb := jobWorld(t, types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "p"},
		Schedule: types.ScheduleConfig{Trigger: &types.TriggerCondition{
			Key: "hourly-status", Check: types.CheckEquals, Field: "status", Value: "ready"}},
		Job: types.JobConfig{Type: types.TriggerHTTP},
	})
	hour := func(date, hour string) map[string]events.DynamoDBAttributeValue {
		return map[string]events.DynamoDBAttributeValue{"status": events.NewStringAttribute("ready"),
			"date": events.NewStringAttribute(date), "hour": events.NewStringAttribute(hour)}
	}

	for _, record := range []events.DynamoDBEventRecord{
		sensorRecordWith("hourly-status#20260307T23", hour("20260307", "23")),
		sensorRecordWith("hourly-status#20260308T00", hour("20260308", "00")),
	} {
		if err := handleRecord(context.Background(), d, record); err != nil {
			t.Fatalf("handleRecord: %v", err)
		}
	}

	var gotEvents []string
	for _, e := range eb.events {
		gotEvents = append(gotEvents, e.Type)
	}
	triggered := string(types.EventJobTriggered)
	if want := []string{triggered, triggered}; sfnFake.calls != 2 || !slices.Equal(gotEvents, want) {
		t.Errorf("StartExecution calls %d, events %v; want 2 and %v", sfnFake.calls, gotEvents, want)
	}
	for _, date := range []string{"2026-03-07T23", "2026-03-08T00"} {
		var status string
		if row := mem.Get("control", types.PipelinePK("p"), types.TriggerSK("stream", date)); row != nil {
			status = row["status"].(*ddbtypes.AttributeValueMemberS).Value
		}
		if status != types.TriggerStatusRunning {
			t.Errorf("trigger %s status = %q, want %q", date, status, types.TriggerStatusRunning)
		}
	}
}

// TestSensorEvent_Cutoff: a sensor naming a date past its cutoff starts
// nothing. With no trigger row the date is closed with FAILED_FINAL and one
// SENSOR_DEADLINE_EXPIRED; otherwise the sensor is handled like one for a
// held lock. Cutoff 14:00 UTC and a sensor for 2026-03-10 unless a row says
// otherwise.
func TestSensorEvent_Cutoff(t *testing.T) {
	at := func(day, hour, minute int) time.Time { return time.Date(2026, 3, day, hour, minute, 0, 0, time.UTC) }
	const yesterday = "2026-03-09"
	running, final := types.TriggerStatusRunning, types.TriggerStatusFailedFinal
	triggered, expired := string(types.EventJobTriggered), string(types.EventSensorDeadlineExpired)
	setCutoff := func(c string) func(*types.PipelineConfig) {
		return func(cfg *types.PipelineConfig) { cfg.Schedule.Trigger.Cutoff = c }
	}
	tests := []struct {
		name       string
		date       string // the sensor's date; empty means 2026-03-10
		now        time.Time
		trigger    string // status of an existing trigger row
		jobRow     string // event of an existing job row
		failEvents int    // failed publishes; each error is followed by a redelivery
		deliveries int    // 0 means 1
		tweak      func(*types.PipelineConfig)
		wantSFN    int
		wantStatus string // trigger row at the end; empty means none
		wantEvents []string
		wantAt     string // cutoffAt of the SENSOR_DEADLINE_EXPIRED event
		wantRerun  bool
	}{
		{name: "one minute before", now: at(10, 13, 59), wantSFN: 1, wantStatus: running, wantEvents: []string{triggered}},
		{name: "at cutoff, no row", now: at(10, 14, 0), wantStatus: final, wantEvents: []string{expired}, wantAt: "2026-03-10T14:00:00Z"},
		{name: "FAILED_FINAL row", now: at(10, 14, 30), trigger: final, wantStatus: final},
		{name: "RUNNING row", now: at(10, 14, 30), trigger: running, wantStatus: running},
		{name: "COMPLETED row, late data", now: at(10, 14, 30), trigger: types.TriggerStatusCompleted, jobRow: types.JobEventSuccess,
			wantStatus: types.TriggerStatusCompleted, wantEvents: []string{string(types.EventLateDataArrival)}, wantRerun: true},
		{name: "no cutoff", now: at(10, 14, 30), tweak: setCutoff(""), wantSFN: 1, wantStatus: running, wantEvents: []string{triggered}},
		{name: "hourly cutoff, daily sensor", now: at(10, 14, 30), tweak: setCutoff(":15"), wantSFN: 1, wantStatus: running, wantEvents: []string{triggered}},
		{name: "yesterday's sensor", date: yesterday, now: at(10, 10, 0), wantStatus: final, wantEvents: []string{expired}, wantAt: "2026-03-09T14:00:00Z"},
		{name: "excluded date", date: yesterday, now: at(10, 14, 30), tweak: func(c *types.PipelineConfig) {
			c.Schedule.Exclude = &types.ExclusionConfig{Dates: []string{yesterday}}
		}},
		{name: "terminal job, no row", now: at(10, 14, 30), jobRow: types.JobEventSuccess},
		{name: "exhausted launch, no row", now: at(10, 14, 30), jobRow: types.JobEventInfraTriggerExhausted,
			wantStatus: final, wantEvents: []string{expired}, wantAt: "2026-03-10T14:00:00Z"},
		{name: "released, sensor before the cutoff", now: at(10, 13, 30), jobRow: types.JobEventEvaluationReleased,
			wantSFN: 1, wantStatus: running, wantEvents: []string{triggered}},
		{name: "released, late sensor", now: at(10, 14, 30), jobRow: types.JobEventEvaluationReleased, deliveries: 2,
			wantStatus: final, wantEvents: []string{expired}, wantAt: "2026-03-10T14:00:00Z"},
		{name: "dry run", now: at(10, 14, 30), tweak: func(c *types.PipelineConfig) { c.DryRun = true },
			wantEvents: []string{string(types.EventDryRunWouldTrigger), string(types.EventDryRunCompleted)}},
		{name: "publish fails", now: at(10, 14, 30), failEvents: 1},
		{name: "publish fails, redelivered", now: at(10, 14, 30), failEvents: 1, deliveries: 2,
			wantStatus: final, wantEvents: []string{expired}, wantAt: "2026-03-10T14:00:00Z"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			date := cmp.Or(tt.date, "2026-03-10")
			cfg := types.PipelineConfig{
				Pipeline: types.PipelineIdentity{ID: "p"},
				Schedule: types.ScheduleConfig{Trigger: &types.TriggerCondition{Key: "upstream", Check: types.CheckExists, Cutoff: "14:00"}},
				Job:      types.JobConfig{Type: types.TriggerHTTP},
			}
			if tt.tweak != nil {
				tt.tweak(&cfg)
			}
			d, mem, sfnFake, eb := jobWorld(t, cfg)
			pk := &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("p")}
			if tt.trigger != "" {
				row := map[string]ddbtypes.AttributeValue{"PK": pk,
					"SK":     &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK("stream", date)},
					"status": &ddbtypes.AttributeValueMemberS{Value: tt.trigger}}
				if tt.trigger == types.TriggerStatusCompleted {
					// Inside the default 4h post-run watch window at 14:30.
					row["completedAt"] = &ddbtypes.AttributeValueMemberS{Value: "2026-03-10T13:00:00Z"}
				}
				mem.Put("control", row)
			}
			if tt.jobRow != "" {
				mem.Put("joblog", map[string]ddbtypes.AttributeValue{"PK": pk,
					"SK":    &ddbtypes.AttributeValueMemberS{Value: types.JobSK("stream", date, "1773129600000")},
					"event": &ddbtypes.AttributeValueMemberS{Value: tt.jobRow}})
			}
			d.NowFunc = func() time.Time { return tt.now }
			eb.fail = tt.failEvents

			for i := 0; i < max(tt.deliveries, 1); i++ {
				err := handleRecord(context.Background(), d, sensorRecord(date))
				if wantErr := i < tt.failEvents; (err != nil) != wantErr {
					t.Fatalf("delivery %d: handleRecord error %v, want error %v", i, err, wantErr)
				}
			}

			var gotEvents []string
			for _, e := range eb.events {
				gotEvents = append(gotEvents, e.Type)
			}
			if sfnFake.calls != tt.wantSFN || !slices.Equal(gotEvents, tt.wantEvents) {
				t.Fatalf("StartExecution calls %d, events %v; want %d and %v", sfnFake.calls, gotEvents, tt.wantSFN, tt.wantEvents)
			}
			if tt.wantAt != "" {
				want := map[string]interface{}{"source": "stream-router", "cutoff": cfg.Schedule.Trigger.Cutoff, "cutoffAt": tt.wantAt,
					"actionHint": "cutoff passed and the pipeline did not start; nothing more will start for this date. " +
						"Investigate, then submit a RERUN_REQUEST to run it manually"}
				if got := eb.events[0].Detail; !maps.Equal(got, want) {
					t.Errorf("SENSOR_DEADLINE_EXPIRED detail = %v, want %v", got, want)
				}
			}
			var status string
			if row := mem.Get("control", pk.Value, types.TriggerSK("stream", date)); row != nil {
				status = row["status"].(*ddbtypes.AttributeValueMemberS).Value
			}
			if status != tt.wantStatus {
				t.Errorf("trigger status = %q, want %q", status, tt.wantStatus)
			}
			if rerun := mem.Get("control", pk.Value, types.RerunRequestSK("stream", date)) != nil; rerun != tt.wantRerun {
				t.Errorf("rerun request written = %v, want %v", rerun, tt.wantRerun)
			}
		})
	}
}

// completedRow is a COMPLETED trigger row for pipeline p on date, with
// completedAt when it is not empty.
func completedRow(date, completedAt string) map[string]ddbtypes.AttributeValue {
	row := map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("p")},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK("stream", date)},
		"status": &ddbtypes.AttributeValueMemberS{Value: types.TriggerStatusCompleted},
	}
	if completedAt != "" {
		row["completedAt"] = &ddbtypes.AttributeValueMemberS{Value: completedAt}
	}
	return row
}

// jobEvents lists the event of each job row, oldest first.
func jobEvents(mem *storetest.Mem) []string {
	var got []string
	for _, row := range mem.Items("joblog") {
		got = append(got, row["event"].(*ddbtypes.AttributeValueMemberS).Value)
	}
	return got
}

// TestLateData_WatchWindow: a trigger sensor for a COMPLETED date is late data
// only while the post-run watch window is open: from completedAt until
// completedAt plus postRun.watchWindow (6h here unless a row says otherwise,
// 4h by default). At the limit, or with no completedAt, it is ignored.
func TestLateData_WatchWindow(t *testing.T) {
	const date = "2026-03-10"
	late := []string{string(types.EventLateDataArrival)}
	lateJobs := []string{types.JobEventSuccess, types.JobEventLateDataArrival}
	window := func(w string) func(*types.PipelineConfig) {
		return func(c *types.PipelineConfig) { c.PostRun = &types.PostRunConfig{WatchWindow: w} }
	}
	cutoff := func(c *types.PipelineConfig) { c.Schedule.Trigger.Cutoff = "14:00" }
	tests := []struct {
		name        string
		tweaks      []func(*types.PipelineConfig)
		completedAt string
		now         string
		wantEvents  []string // nil means none; a late sensor also writes a request and a job row
	}{
		{name: "inside (guard)", completedAt: "2026-03-10T12:00:00Z", now: "2026-03-10T17:59:59Z", wantEvents: late},
		{name: "at the limit", completedAt: "2026-03-10T12:00:00Z", now: "2026-03-10T18:00:00Z"},
		{name: "two days later", completedAt: "2026-03-10T12:00:00Z", now: "2026-03-12T09:00:00Z"},
		{name: "default window, inside (guard)", tweaks: []func(*types.PipelineConfig){func(c *types.PipelineConfig) { c.PostRun = nil }},
			completedAt: "2026-03-10T12:00:00Z", now: "2026-03-10T15:59:59Z", wantEvents: late},
		{name: "default window, at the limit", tweaks: []func(*types.PipelineConfig){func(c *types.PipelineConfig) { c.PostRun = nil }},
			completedAt: "2026-03-10T12:00:00Z", now: "2026-03-10T16:00:00Z"},
		{name: "no completedAt", now: "2026-03-10T12:30:00Z"},
		{name: "inside, past the cutoff (guard)", tweaks: []func(*types.PipelineConfig){cutoff},
			completedAt: "2026-03-10T12:00:00Z", now: "2026-03-10T14:30:00Z", wantEvents: late},
		{name: "outside, before the cutoff", tweaks: []func(*types.PipelineConfig){window("1h"), cutoff},
			completedAt: "2026-03-10T12:00:00Z", now: "2026-03-10T13:30:00Z"},
		{name: "rerun completed later (guard)", completedAt: "2026-03-11T08:00:00Z", now: "2026-03-11T13:59:59Z", wantEvents: late},
		{name: "rerun's window ended", completedAt: "2026-03-11T08:00:00Z", now: "2026-03-11T14:00:00Z"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := types.PipelineConfig{
				Pipeline: types.PipelineIdentity{ID: "p"},
				Schedule: types.ScheduleConfig{Trigger: &types.TriggerCondition{Key: "upstream", Check: types.CheckExists}},
				Job:      types.JobConfig{Type: types.TriggerHTTP},
				PostRun:  &types.PostRunConfig{WatchWindow: "6h"},
			}
			for _, f := range tt.tweaks {
				f(&cfg)
			}
			d, mem, sfnFake, eb := jobWorld(t, cfg)
			pk := types.PipelinePK("p")
			mem.Put("control", completedRow(date, tt.completedAt))
			mem.Put("joblog", map[string]ddbtypes.AttributeValue{"PK": &ddbtypes.AttributeValueMemberS{Value: pk},
				"SK":    &ddbtypes.AttributeValueMemberS{Value: types.JobSK("stream", date, "1773129600000")},
				"event": &ddbtypes.AttributeValueMemberS{Value: types.JobEventSuccess}})
			now := mustTime(t, tt.now)
			d.NowFunc = func() time.Time { return now }

			if err := handleRecord(context.Background(), d, sensorRecord(date)); err != nil {
				t.Fatalf("handleRecord: %v", err)
			}

			var gotEvents []string
			for _, e := range eb.events {
				gotEvents = append(gotEvents, e.Type)
			}
			wantJobs, wantRerun := []string{types.JobEventSuccess}, tt.wantEvents != nil
			if wantRerun {
				wantJobs = lateJobs
			}
			if sfnFake.calls != 0 || !slices.Equal(gotEvents, tt.wantEvents) {
				t.Errorf("StartExecution calls %d, events %v; want 0 and %v", sfnFake.calls, gotEvents, tt.wantEvents)
			}
			if got := mem.Get("control", pk, types.TriggerSK("stream", date))["status"].(*ddbtypes.AttributeValueMemberS).Value; got != types.TriggerStatusCompleted {
				t.Errorf("trigger status = %q, want COMPLETED", got)
			}
			if rerun := mem.Get("control", pk, types.RerunRequestSK("stream", date)) != nil; rerun != wantRerun {
				t.Errorf("rerun request written = %v, want %v", rerun, wantRerun)
			}
			if got := jobEvents(mem); !slices.Equal(got, wantJobs) {
				t.Errorf("job rows = %v, want %v", got, wantJobs)
			}
		})
	}
}

// mustTime parses an RFC3339 instant.
func mustTime(t *testing.T, s string) time.Time {
	t.Helper()
	v, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatal(err)
	}
	return v
}
