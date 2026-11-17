package watchdog

import (
	"cmp"
	"context"
	"maps"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// TestCheckTriggerDeadlines: each RUN# key whose cutoff passed with no trigger
// row is closed with FAILED_FINAL and one SENSOR_DEADLINE_EXPIRED. Cutoff 14:00
// UTC and one RUN# row for slaTestDate unless a case says otherwise.
func TestCheckTriggerDeadlines(t *testing.T) {
	at := func(day, hour, minute int) time.Time { return time.Date(2026, 3, day, hour, minute, 0, 0, time.UTC) }
	tests := []struct {
		name     string
		cutoff   string
		offset   int
		tz       string
		keys     []string // nil means slaTestDate
		now      time.Time
		trigger  string // status of an existing trigger row for each key
		jobRow   string // event of a job row for slaTestDate
		passes   int    // 0 means 1
		tweak    func(*types.PipelineConfig)
		wantDate string // the closed key; empty means none
		wantAt   string // cutoffAt of its event
	}{
		{name: "before the cutoff", now: at(10, 13, 55)},
		{name: "after the cutoff", now: at(10, 14, 5), wantDate: slaTestDate, wantAt: "2026-03-10T14:00:00Z"},
		{name: "second pass", now: at(10, 14, 5), passes: 2, wantDate: slaTestDate, wantAt: "2026-03-10T14:00:00Z"},
		{name: "offset 1", cutoff: "02:00", offset: 1, now: at(11, 2, 5), wantDate: slaTestDate, wantAt: "2026-03-11T02:00:00Z"},
		{name: "Tokyo", cutoff: "09:00", tz: "Asia/Tokyo", keys: []string{"2026-03-11"}, now: at(11, 0, 5), wantDate: "2026-03-11", wantAt: "2026-03-11T00:00:00Z"},
		{name: "running trigger row", now: at(10, 14, 5), trigger: types.TriggerStatusRunning},
		{name: "terminal job row", now: at(10, 14, 5), jobRow: types.JobEventSuccess},
		{name: "exhausted launch", now: at(10, 14, 5), jobRow: types.JobEventInfraTriggerExhausted, wantDate: slaTestDate, wantAt: "2026-03-10T14:00:00Z"},
		{name: "released evaluation", now: at(10, 14, 5), jobRow: types.JobEventEvaluationReleased, wantDate: slaTestDate, wantAt: "2026-03-10T14:00:00Z"},
		{name: "one key passed, one ahead", keys: []string{slaTestDate, "2026-03-11"}, now: at(10, 14, 5), wantDate: slaTestDate, wantAt: "2026-03-10T14:00:00Z"},
		{name: "no RUN# rows", keys: []string{}, now: at(10, 14, 5)},
		{name: "malformed key", keys: []string{"2026-3-10"}, now: at(10, 14, 5)},
		{name: "excluded date", keys: []string{"2026-03-14"}, now: at(14, 14, 5), tweak: func(c *types.PipelineConfig) { c.Schedule.Exclude = &types.ExclusionConfig{Weekends: true} }},
		{name: "dry run", now: at(10, 14, 5), tweak: func(c *types.PipelineConfig) { c.DryRun, c.Job.Type = true, types.TriggerHTTP }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cutoff := cmp.Or(tt.cutoff, "14:00")
			keys := tt.keys
			if keys == nil {
				keys = []string{slaTestDate}
			}
			d, mem, rec := slaDeps(t, "", tt.jobRow, func(c *types.PipelineConfig) {
				c.SLA, c.Schedule.Timezone = nil, tt.tz
				c.Schedule.Trigger.Cutoff, c.Schedule.Trigger.CutoffDayOffset = cutoff, tt.offset
				if tt.tweak != nil {
					tt.tweak(c)
				}
			})
			ctx, pk := context.Background(), types.PipelinePK("p")
			if _, err := d.Store.Client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
				TableName: aws.String("control"), Key: sItem("PK", pk, "SK", types.RunSK(slaTestDate)),
			}); err != nil {
				t.Fatalf("delete default RUN# row: %v", err)
			}
			for _, key := range keys {
				mem.Put("control", sItem("PK", pk, "SK", types.RunSK(key)))
				if tt.trigger != "" {
					mem.Put("control", sItem("PK", pk, "SK", types.TriggerSK("stream", key), "status", tt.trigger))
				}
			}
			d.NowFunc = func() time.Time { return tt.now }

			for pass := 0; pass < max(tt.passes, 1); pass++ {
				if err := checkTriggerDeadlines(ctx, d); err != nil {
					t.Fatalf("pass %d: checkTriggerDeadlines: %v", pass, err)
				}
			}

			if tt.wantDate == "" {
				if len(rec.events) != 0 {
					t.Fatalf("events = %v, want none", rec.events)
				}
			} else {
				want := map[string]interface{}{"source": "watchdog", "cutoff": cutoff, "cutoffAt": tt.wantAt,
					"actionHint": "cutoff passed and the pipeline did not start; nothing more will start for this date. " +
						"Investigate, then submit a RERUN_REQUEST to run it manually"}
				if len(rec.events) != 1 || rec.events[0] != string(types.EventSensorDeadlineExpired) ||
					rec.details[0].Date != tt.wantDate || !maps.Equal(rec.details[0].Detail, want) {
					t.Fatalf("events = %v with %+v, want one SENSOR_DEADLINE_EXPIRED for %s with %v", rec.events, rec.details, tt.wantDate, want)
				}
			}
			for _, key := range keys {
				wantStatus := tt.trigger
				if key == tt.wantDate {
					wantStatus = types.TriggerStatusFailedFinal
				}
				var got string
				if row := mem.Get("control", pk, types.TriggerSK("stream", key)); row != nil {
					got = row["status"].(*ddbtypes.AttributeValueMemberS).Value
				}
				if got != wantStatus {
					t.Errorf("trigger status for %s = %q, want %q", key, got, wantStatus)
				}
			}
		})
	}
}

// TestCheckTriggerDeadlines_PublishFailureRetries: a failed publish removes the
// FAILED_FINAL row it wrote, so the next pass closes the date and alerts.
func TestCheckTriggerDeadlines_PublishFailureRetries(t *testing.T) {
	d, mem, rec := slaDeps(t, "", "", func(c *types.PipelineConfig) { c.SLA, c.Schedule.Trigger.Cutoff = nil, "08:00" })
	sk := types.TriggerSK("stream", slaTestDate)
	rec.failEvents = 1
	for pass, want := range []int{0, 1} {
		if err := checkTriggerDeadlines(context.Background(), d); err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
		if closed := mem.Get("control", types.PipelinePK("p"), sk) != nil; len(rec.events) != want || closed != (want == 1) {
			t.Fatalf("pass %d: events %v, trigger row %v; want %d events and row %v", pass, rec.events, closed, want, want == 1)
		}
	}
}
