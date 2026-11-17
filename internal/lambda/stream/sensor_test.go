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

	"github.com/dwsmith1983/interlock/pkg/types"
)

// sensorRecord is the stream record of the "upstream" sensor naming date.
func sensorRecord(date string) events.DynamoDBEventRecord {
	return events.DynamoDBEventRecord{
		EventName: "INSERT",
		Change: events.DynamoDBStreamRecord{
			SequenceNumber: "1",
			Keys: map[string]events.DynamoDBAttributeValue{
				"PK": events.NewStringAttribute(types.PipelinePK("p")),
				"SK": events.NewStringAttribute(types.SensorSK("upstream")),
			},
			NewImage: map[string]events.DynamoDBAttributeValue{
				"data": events.NewMapAttribute(map[string]events.DynamoDBAttributeValue{"date": events.NewStringAttribute(date)}),
			},
		},
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
				mem.Put("control", map[string]ddbtypes.AttributeValue{"PK": pk,
					"SK":     &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK("stream", date)},
					"status": &ddbtypes.AttributeValueMemberS{Value: tt.trigger}})
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
