package stream

import (
	"cmp"
	"context"
	"encoding/json"
	"maps"
	"reflect"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/interlock/internal/store/storetest"
	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/dwsmith1983/interlock/pkg/validation"
)

// httpJobConfig is the smallest job.config validation accepts for an http job.
var httpJobConfig = map[string]interface{}{"url": "https://example.com/job"}

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
			Key: "hourly-status", Check: types.CheckEquals, Field: "status", Value: "ready", Cutoff: "23:59", CutoffDayOffset: 7}},
		Job: types.JobConfig{Type: types.TriggerHTTP, Config: httpJobConfig},
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
	if got, want := startDates(t, sfnFake), []string{"2026-03-07T23", "2026-03-08T00"}; !slices.Equal(got, want) {
		t.Errorf("execution input dates = %v, want %v", got, want)
	}
}

// startDates lists the date in each started execution's input, in order.
func startDates(t *testing.T, sfnFake *countingSFN) []string {
	t.Helper()
	var dates []string
	for _, in := range sfnFake.inputs {
		var input struct {
			Date string `json:"date"`
		}
		if err := json.Unmarshal([]byte(in), &input); err != nil {
			t.Fatalf("decode execution input %q: %v", in, err)
		}
		dates = append(dates, input.Date)
	}
	return dates
}

// TestSensorEvent_Gates: what stops a trigger sensor before it starts a run.
// The sensor names 2026-03-06 (a Friday) and now is testNow (Saturday
// 2026-03-07) unless a row says otherwise. Calendar exclusion is checked on the
// wall-clock date, not the sensor's date, and publishes PIPELINE_EXCLUDED.
// A completed row inside its watch window would publish a post-run verdict
// for a sensor that matched a post-run rule.
func TestSensorEvent_Gates(t *testing.T) {
	const date = "2026-03-06"
	triggered, excluded := []string{string(types.EventJobTriggered)}, []string{string(types.EventPipelineExcluded)}
	exclude := func(e types.ExclusionConfig) func(*types.PipelineConfig) {
		return func(c *types.PipelineConfig) { c.Schedule.Exclude = &e }
	}
	weekends := exclude(types.ExclusionConfig{Weekends: true})
	tests := []struct {
		name       string
		key        string // sensor key; empty means upstream
		status     string // the sensor's status field, if set
		now        string // empty means testNow
		completed  bool   // the date has a COMPLETED trigger row
		tweak      func(*types.PipelineConfig)
		wantSFN    int
		wantEvents []string
	}{
		{name: "exact key (guard)", wantSFN: 1, wantEvents: triggered},
		{name: "no rule", key: "other-sensor"},
		{name: "post-run rules, no match", key: "other-sensor", completed: true, tweak: func(c *types.PipelineConfig) {
			c.PostRun = &types.PostRunConfig{Rules: []types.ValidationRule{{Key: "row-count", Check: types.CheckExists}}}
		}},
		{name: "no config", tweak: func(c *types.PipelineConfig) { c.Pipeline.ID = "other" }},
		{name: "check fails", status: "not-ready", tweak: func(c *types.PipelineConfig) {
			c.Schedule.Trigger = &types.TriggerCondition{Key: "upstream", Check: types.CheckEquals, Field: "status", Value: "ready", Cutoff: "23:59", CutoffDayOffset: 7}
		}},
		{name: "wall clock excluded", tweak: exclude(types.ExclusionConfig{Dates: []string{"2026-03-07"}}), wantEvents: excluded},
		{name: "Saturday", tweak: weekends, wantEvents: excluded},
		{name: "Monday (guard)", tweak: weekends, now: "2026-03-09T12:00:00Z", wantSFN: 1, wantEvents: triggered},
		{name: "sensor date excluded, wall clock not (guard)", tweak: exclude(types.ExclusionConfig{Dates: []string{date}}),
			wantSFN: 1, wantEvents: triggered},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := types.PipelineConfig{
				Pipeline: types.PipelineIdentity{ID: "p"},
				Schedule: types.ScheduleConfig{Trigger: &types.TriggerCondition{Key: "upstream", Check: types.CheckExists, Cutoff: "23:59", CutoffDayOffset: 7}},
				Job:      types.JobConfig{Type: types.TriggerHTTP, Config: httpJobConfig},
			}
			if tt.tweak != nil {
				tt.tweak(&cfg)
			}
			d, mem, sfnFake, eb := jobWorld(t, cfg)
			if tt.completed {
				mem.Put("control", completedRow(date, "2026-03-07T11:00:00Z"))
			}
			if tt.now != "" {
				now := mustTime(t, tt.now)
				d.NowFunc = func() time.Time { return now }
			}
			data := map[string]events.DynamoDBAttributeValue{"date": events.NewStringAttribute(date)}
			if tt.status != "" {
				data["status"] = events.NewStringAttribute(tt.status)
			}

			if err := handleRecord(context.Background(), d, sensorRecordWith(cmp.Or(tt.key, "upstream"), data)); err != nil {
				t.Fatalf("handleRecord: %v", err)
			}

			var gotEvents []string
			for _, e := range eb.events {
				gotEvents = append(gotEvents, e.Type)
			}
			if sfnFake.calls != tt.wantSFN || !slices.Equal(gotEvents, tt.wantEvents) {
				t.Errorf("StartExecution calls %d, events %v; want %d and %v", sfnFake.calls, gotEvents, tt.wantSFN, tt.wantEvents)
			}
			if present := mem.Get("control", types.PipelinePK("p"), types.TriggerSK("stream", date)) != nil; present != (tt.wantSFN == 1 || tt.completed) {
				t.Errorf("trigger row present = %v, want %v", present, tt.wantSFN == 1 || tt.completed)
			}
		})
	}
}

// TestSensorEvent_FirstArrival: a run started by a sensor records when the
// date's first sensor arrived (the start of a relative SLA) and keeps an
// earlier value. A failed start releases the trigger lock, publishes nothing
// and fails the record.
func TestSensorEvent_FirstArrival(t *testing.T) {
	tests := []struct {
		name, date, now, seeded string
		sfnFail                 int
		wantArrival             string // empty means not checked
	}{
		{name: "written", date: "2026-03-10", now: "2026-03-10T14:30:00Z", wantArrival: "2026-03-10T14:30:00Z"},
		{name: "kept", date: "2026-03-10", now: "2026-03-10T15:00:00Z", seeded: "2026-03-10T14:00:00Z", wantArrival: "2026-03-10T14:00:00Z"},
		{name: "start fails", date: "2026-03-08", now: "2026-03-10T14:30:00Z", sfnFail: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, mem, sfnFake, eb := jobWorld(t, types.PipelineConfig{
				Pipeline: types.PipelineIdentity{ID: "p"},
				Schedule: types.ScheduleConfig{Trigger: &types.TriggerCondition{Key: "upstream", Check: types.CheckExists, Cutoff: "23:59", CutoffDayOffset: 7}},
				Job:      types.JobConfig{Type: types.TriggerHTTP, Config: httpJobConfig},
			})
			ctx, arrivalKey := context.Background(), "first-sensor-arrival#"+tt.date
			if tt.seeded != "" {
				seededAt := mustTime(t, tt.seeded)
				d.Store.Now = func() time.Time { return seededAt }
				if err := d.Store.WriteSensor(ctx, "p", arrivalKey, map[string]interface{}{"arrivedAt": tt.seeded}); err != nil {
					t.Fatal(err)
				}
			}
			now := mustTime(t, tt.now)
			d.NowFunc = func() time.Time { return now }
			d.Store.Now = func() time.Time { return now }
			sfnFake.fail = tt.sfnFail

			err := handleRecord(ctx, d, sensorRecord(tt.date))
			if wantErr := tt.sfnFail > 0; (err != nil) != wantErr {
				t.Fatalf("handleRecord error = %v, want an error: %v", err, wantErr)
			}

			wantStarts, wantEvents := 1, []string{string(types.EventJobTriggered)}
			if tt.sfnFail > 0 {
				wantStarts, wantEvents = 0, nil
			}
			var gotEvents []string
			for _, e := range eb.events {
				gotEvents = append(gotEvents, e.Type)
			}
			if sfnFake.calls != wantStarts || !slices.Equal(gotEvents, wantEvents) {
				t.Errorf("StartExecution calls %d, events %v; want %d and %v", sfnFake.calls, gotEvents, wantStarts, wantEvents)
			}
			if present := mem.Get("control", types.PipelinePK("p"), types.TriggerSK("stream", tt.date)) != nil; present != (wantStarts == 1) {
				t.Errorf("trigger row present = %v, want %v", present, wantStarts == 1)
			}
			if tt.wantArrival != "" {
				got, err := d.Store.GetSensorData(ctx, "p", arrivalKey)
				if err != nil {
					t.Fatal(err)
				}
				if got["arrivedAt"] != tt.wantArrival {
					t.Errorf("%s arrivedAt = %v, want %s", arrivalKey, got["arrivedAt"], tt.wantArrival)
				}
				// The row expires counted from its first write, which a kept
				// arrival does not refresh.
				row := mem.Get("control", types.PipelinePK("p"), types.SensorSK(arrivalKey))
				wantTTL := strconv.FormatInt(mustTime(t, tt.wantArrival).Add(validation.MaxPostRunWatchWindow+24*time.Hour).Unix(), 10)
				if ttl, ok := row["ttl"].(*ddbtypes.AttributeValueMemberN); !ok || ttl.Value != wantTTL {
					t.Errorf("%s ttl = %v, want %s", arrivalKey, row["ttl"], wantTTL)
				}
			}
		})
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
		{name: "hourly cutoff, daily sensor", now: at(10, 14, 30), tweak: setCutoff(":15"), wantSFN: 1, wantStatus: running, wantEvents: []string{triggered}},
		{name: "hourly cutoff, daily sensor after its day", now: at(11, 0, 15), tweak: setCutoff(":15"),
			wantStatus: final, wantEvents: []string{expired}, wantAt: "2026-03-11T00:15:00Z"},
		{name: "yesterday's sensor", date: yesterday, now: at(10, 10, 0), wantStatus: final, wantEvents: []string{expired}, wantAt: "2026-03-09T14:00:00Z"},
		{name: "excluded date", date: yesterday, now: at(10, 14, 30), tweak: func(c *types.PipelineConfig) {
			c.Schedule.Exclude = &types.ExclusionConfig{Dates: []string{yesterday}}
		}},
		{name: "terminal job, no row", now: at(10, 14, 30), jobRow: types.JobEventSuccess},
		{name: "exhausted job row, no trigger row", now: at(10, 14, 30), jobRow: types.JobEventInfraTriggerExhausted,
			wantStatus: final, wantEvents: []string{expired}, wantAt: "2026-03-10T14:00:00Z"},
		// A launch that ran out of retries ends FAILED_FINAL: no later sensor
		// write starts it again, before or after the cutoff.
		{name: "exhausted launch, before the cutoff", now: at(10, 13, 30), trigger: final,
			jobRow: types.JobEventInfraTriggerExhausted, wantStatus: final},
		{name: "exhausted launch, past the cutoff", now: at(10, 14, 30), trigger: final,
			jobRow: types.JobEventInfraTriggerExhausted, wantStatus: final},
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
				Job:      types.JobConfig{Type: types.TriggerHTTP, Config: httpJobConfig},
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
// 4h by default). From the limit on it publishes only MANUAL_RERUN_REQUIRED,
// blocked by the window, and writes no request; with no completedAt or a
// failed run it publishes nothing.
func TestLateData_WatchWindow(t *testing.T) {
	const date = "2026-03-10"
	late, manual := []string{string(types.EventLateDataArrival)}, []string{evManual}
	window := func(w string) func(*types.PipelineConfig) {
		return func(c *types.PipelineConfig) { c.PostRun = &types.PostRunConfig{WatchWindow: w} }
	}
	cutoff := func(c *types.PipelineConfig) { c.Schedule.Trigger.Cutoff = "14:00" }
	tests := []struct {
		name        string
		tweaks      []func(*types.PipelineConfig)
		completedAt string
		now         string
		jobEvent    string   // the run's job row; empty means success
		above       []string // newer job rows the stream-router wrote about reruns and late data before it stopped
		wantEvents  []string // nil means none; LATE_DATA_ARRIVAL also writes a request, and no job row
	}{
		{name: "inside (guard)", completedAt: "2026-03-10T12:00:00Z", now: "2026-03-10T17:59:59Z", wantEvents: late},
		{name: "inside, job failed", completedAt: "2026-03-10T12:00:00Z", now: "2026-03-10T17:59:59Z", jobEvent: types.JobEventFail},
		{name: "inside, rejected rerun on top", completedAt: "2026-03-10T12:00:00Z", now: "2026-03-10T17:59:59Z",
			above: []string{types.JobEventLateDataArrival, types.JobEventRerunRejected}, wantEvents: late},
		{name: "inside, fail under a rejected rerun", completedAt: "2026-03-10T12:00:00Z", now: "2026-03-10T17:59:59Z",
			jobEvent: types.JobEventFail, above: []string{types.JobEventRerunRejected}},
		{name: "at the limit", completedAt: "2026-03-10T12:00:00Z", now: "2026-03-10T18:00:00Z", wantEvents: manual},
		{name: "at the limit, job failed", completedAt: "2026-03-10T12:00:00Z", now: "2026-03-10T18:00:00Z", jobEvent: types.JobEventFail},
		{name: "two days later", completedAt: "2026-03-10T12:00:00Z", now: "2026-03-12T09:00:00Z", wantEvents: manual},
		{name: "default window, inside (guard)", tweaks: []func(*types.PipelineConfig){func(c *types.PipelineConfig) { c.PostRun = nil }},
			completedAt: "2026-03-10T12:00:00Z", now: "2026-03-10T15:59:59Z", wantEvents: late},
		{name: "default window, at the limit", tweaks: []func(*types.PipelineConfig){func(c *types.PipelineConfig) { c.PostRun = nil }},
			completedAt: "2026-03-10T12:00:00Z", now: "2026-03-10T16:00:00Z", wantEvents: manual},
		{name: "no completedAt", now: "2026-03-10T12:30:00Z"},
		{name: "inside, past the cutoff (guard)", tweaks: []func(*types.PipelineConfig){cutoff},
			completedAt: "2026-03-10T12:00:00Z", now: "2026-03-10T14:30:00Z", wantEvents: late},
		{name: "outside, before the cutoff", tweaks: []func(*types.PipelineConfig){window("1h"), cutoff},
			completedAt: "2026-03-10T12:00:00Z", now: "2026-03-10T13:30:00Z", wantEvents: manual},
		{name: "rerun completed later (guard)", completedAt: "2026-03-11T08:00:00Z", now: "2026-03-11T13:59:59Z", wantEvents: late},
		{name: "rerun's window ended", completedAt: "2026-03-11T08:00:00Z", now: "2026-03-11T14:00:00Z", wantEvents: manual},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := types.PipelineConfig{
				Pipeline: types.PipelineIdentity{ID: "p"},
				Schedule: types.ScheduleConfig{Trigger: &types.TriggerCondition{Key: "upstream", Check: types.CheckExists, Cutoff: "23:59", CutoffDayOffset: 7}},
				Job:      types.JobConfig{Type: types.TriggerHTTP, Config: httpJobConfig},
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
				"event": &ddbtypes.AttributeValueMemberS{Value: cmp.Or(tt.jobEvent, types.JobEventSuccess)}})
			for i, event := range tt.above {
				mem.Put("joblog", map[string]ddbtypes.AttributeValue{"PK": &ddbtypes.AttributeValueMemberS{Value: pk},
					"SK":    &ddbtypes.AttributeValueMemberS{Value: types.JobSK("stream", date, strconv.Itoa(1773129600001+i))},
					"event": &ddbtypes.AttributeValueMemberS{Value: event}})
			}
			now := mustTime(t, tt.now)
			d.NowFunc = func() time.Time { return now }
			d.Store.Now = func() time.Time { return now }

			if err := handleRecord(context.Background(), d, sensorRecord(date)); err != nil {
				t.Fatalf("handleRecord: %v", err)
			}

			var gotEvents []string
			for _, e := range eb.events {
				gotEvents = append(gotEvents, e.Type)
			}
			wantJobs, wantRerun := append([]string{cmp.Or(tt.jobEvent, types.JobEventSuccess)}, tt.above...), slices.Equal(tt.wantEvents, late)
			if sfnFake.calls != 0 || !slices.Equal(gotEvents, tt.wantEvents) {
				t.Errorf("StartExecution calls %d, events %v; want 0 and %v", sfnFake.calls, gotEvents, tt.wantEvents)
			}
			for _, e := range eb.events {
				if e.Type == evManual && (e.Detail["reason"] != "late-data" || e.Detail["blockedBy"] != "postRun.watchWindow") {
					t.Errorf("MANUAL_RERUN_REQUIRED reason %v, blockedBy %v; want late-data, postRun.watchWindow", e.Detail["reason"], e.Detail["blockedBy"])
				}
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

// TestLateData_MaxAutoRerunAge: with a 200h watch window and
// job.maxAutoRerunAge 24h, a trigger sensor 25h after completedAt still
// publishes LATE_DATA_ARRIVAL and MANUAL_RERUN_REQUIRED but writes no
// late-data request, so a pending manual request is left as it was. Inside the
// age the request is written.
func TestLateData_MaxAutoRerunAge(t *testing.T) {
	const date = "2026-03-10"
	pk := types.PipelinePK("p")
	manual := map[string]ddbtypes.AttributeValue{
		"PK":          &ddbtypes.AttributeValueMemberS{Value: pk},
		"SK":          &ddbtypes.AttributeValueMemberS{Value: types.RerunRequestSK("stream", date)},
		"reason":      &ddbtypes.AttributeValueMemberS{Value: "manual"},
		"requestedAt": &ddbtypes.AttributeValueMemberS{Value: "2026-03-11T12:30:00Z"},
	}
	tests := []struct {
		name       string
		now        string
		manualRow  bool
		wantReason string // the request row's reason; empty means none
	}{
		{name: "past the age", now: "2026-03-11T13:00:00Z"},
		{name: "past the age, manual request pending", now: "2026-03-11T13:00:00Z", manualRow: true, wantReason: "manual"},
		{name: "inside the age (guard)", now: "2026-03-11T11:59:59Z", wantReason: "late-data"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, mem, sfnFake, eb := jobWorld(t, types.PipelineConfig{
				Pipeline: types.PipelineIdentity{ID: "p"},
				Schedule: types.ScheduleConfig{Trigger: &types.TriggerCondition{Key: "upstream", Check: types.CheckExists, Cutoff: "23:59", CutoffDayOffset: 7}},
				Job:      types.JobConfig{Type: types.TriggerHTTP, Config: httpJobConfig, MaxAutoRerunAge: "24h"},
				PostRun:  &types.PostRunConfig{WatchWindow: "200h"},
			})
			mem.Put("control", completedRow(date, "2026-03-10T12:00:00Z"))
			mem.Put("joblog", map[string]ddbtypes.AttributeValue{"PK": &ddbtypes.AttributeValueMemberS{Value: pk},
				"SK":    &ddbtypes.AttributeValueMemberS{Value: types.JobSK("stream", date, "1773129600000")},
				"event": &ddbtypes.AttributeValueMemberS{Value: types.JobEventSuccess}})
			if tt.manualRow {
				mem.Put("control", maps.Clone(manual))
			}
			now := mustTime(t, tt.now)
			d.NowFunc = func() time.Time { return now }
			d.Store.Now = func() time.Time { return now }

			if err := handleRecord(context.Background(), d, sensorRecord(date)); err != nil {
				t.Fatalf("handleRecord: %v", err)
			}

			var gotEvents []string
			for _, e := range eb.events {
				gotEvents = append(gotEvents, e.Type)
			}
			want := []string{string(types.EventLateDataArrival)}
			if tt.wantReason != "late-data" {
				want = append(want, string(types.EventManualRerunRequired))
			}
			if sfnFake.calls != 0 || !slices.Equal(gotEvents, want) {
				t.Errorf("StartExecution calls %d, events %v; want 0 and %v", sfnFake.calls, gotEvents, want)
			}
			req := mem.Get("control", pk, types.RerunRequestSK("stream", date))
			if got := strAttr(req["reason"]); got != tt.wantReason {
				t.Errorf("rerun request reason = %q, want %q", got, tt.wantReason)
			}
			if tt.manualRow && !reflect.DeepEqual(req, manual) {
				t.Errorf("manual request = %v, want it unchanged: %v", req, manual)
			}
			if got := mem.Get("control", pk, types.TriggerSK("stream", date))["status"].(*ddbtypes.AttributeValueMemberS).Value; got != types.TriggerStatusCompleted {
				t.Errorf("trigger status = %q, want COMPLETED", got)
			}
		})
	}
}

// TestLateData_RepeatedWritesAddNoJobRows: with the date's run a success and
// its drift rerun budget spent, each late trigger-sensor write publishes
// LATE_DATA_ARRIVAL and its late-data request is rejected with RERUN_REJECTED
// naming the request's reason and why. The joblog keeps only the run's row,
// so the lookup of the date's outcome does not grow with late writes.
func TestLateData_RepeatedWritesAddNoJobRows(t *testing.T) {
	const date = "2026-03-10"
	now := mustTime(t, "2026-03-10T13:00:00Z")
	d, mem, sfnFake, eb := rerunWorld(t, "1773133200000", &now)
	mem.Put("rerun", map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("p")},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: types.RerunSK("stream", date, 0)},
		"reason": &ddbtypes.AttributeValueMemberS{Value: "late-data"}})
	ctx := context.Background()

	for i := range 3 {
		if err := handleRecord(ctx, d, sensorRecord(date)); err != nil {
			t.Fatalf("late write %d: %v", i, err)
		}
		if err := handleRecord(ctx, d, rerunRequestRecord("late-data", now.Format(time.RFC3339))); err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		now = now.Add(20 * time.Minute)
	}

	late, rejected := string(types.EventLateDataArrival), string(types.EventRerunRejected)
	var gotTypes []string
	for _, e := range eb.events {
		gotTypes = append(gotTypes, e.Type)
	}
	if want := []string{late, rejected, late, rejected, late, rejected}; !slices.Equal(gotTypes, want) {
		t.Fatalf("events = %v, want %v", gotTypes, want)
	}
	wantDetail := map[string]interface{}{"reason": "late-data", "why": "drift rerun limit exceeded"}
	for i, e := range eb.events {
		if e.Type == rejected && !maps.Equal(e.Detail, wantDetail) {
			t.Errorf("event %d detail = %v, want %v", i, e.Detail, wantDetail)
		}
	}
	if got := jobEvents(mem); !slices.Equal(got, []string{types.JobEventSuccess}) {
		t.Errorf("job rows = %v, want [%s]", got, types.JobEventSuccess)
	}
	if sfnFake.calls != 0 || len(mem.Items("rerun")) != 1 {
		t.Errorf("StartExecution calls %d, rerun rows %d; want 0 and 1", sfnFake.calls, len(mem.Items("rerun")))
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

// TestSensorEvent_KeyBoundary: a sensor matches the trigger key only as that
// key or the key followed by # and a period. With trigger key orders and a
// post-run rule on orders-count, an orders-count write is the post-run
// sensor: it starts nothing for a date with no trigger row, and for a
// COMPLETED date inside its window a changed value is drift, not late data.
func TestSensorEvent_KeyBoundary(t *testing.T) {
	const date = "2026-03-10"
	threshold := 0.0
	tests := []struct {
		name       string
		key        string
		completed  bool // the date has a COMPLETED row and an orders-count baseline of 100
		wantSFN    int
		wantEvents []string
	}{
		{name: "post-run sensor, no trigger row", key: "orders-count"},
		{name: "post-run sensor, completed date", key: "orders-count", completed: true,
			wantEvents: []string{string(types.EventPostRunDrift)}},
		{name: "per-period trigger key (guard)", key: "orders#" + date, wantSFN: 1,
			wantEvents: []string{string(types.EventJobTriggered)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, mem, sfnFake, eb := jobWorld(t, types.PipelineConfig{
				Pipeline: types.PipelineIdentity{ID: "p"},
				Schedule: types.ScheduleConfig{Trigger: &types.TriggerCondition{Key: "orders", Check: types.CheckExists, Cutoff: "23:59", CutoffDayOffset: 7}},
				Job:      types.JobConfig{Type: types.TriggerHTTP, Config: httpJobConfig},
				PostRun: &types.PostRunConfig{DriftField: "count", DriftThreshold: &threshold, WatchWindow: "6h",
					Rules: []types.ValidationRule{{Key: "orders-count", Check: types.CheckExists}}},
			})
			now := mustTime(t, "2026-03-10T13:00:00Z")
			d.NowFunc = func() time.Time { return now }
			ctx := context.Background()
			if tt.completed {
				mem.Put("control", completedRow(date, "2026-03-10T12:00:00Z"))
				if err := d.Store.WriteSensor(ctx, "p", "postrun-baseline#"+date, map[string]interface{}{
					"orders-count": map[string]interface{}{"count": 100}}); err != nil {
					t.Fatal(err)
				}
			}
			data := map[string]events.DynamoDBAttributeValue{
				"date": events.NewStringAttribute(date), "count": events.NewNumberAttribute("200")}

			if err := handleRecord(ctx, d, sensorRecordWith(tt.key, data)); err != nil {
				t.Fatalf("handleRecord: %v", err)
			}

			if got := eventTypes(eb.events); sfnFake.calls != tt.wantSFN || !slices.Equal(got, tt.wantEvents) {
				t.Errorf("StartExecution calls %d, events %v; want %d and %v", sfnFake.calls, got, tt.wantSFN, tt.wantEvents)
			}
			wantRow := tt.wantSFN == 1 || tt.completed
			if present := mem.Get("control", types.PipelinePK("p"), types.TriggerSK("stream", date)) != nil; present != wantRow {
				t.Errorf("trigger row present = %v, want %v", present, wantRow)
			}
			if got := jobEvents(mem); len(got) != 0 {
				t.Errorf("job rows = %v, want none", got)
			}
		})
	}
}
