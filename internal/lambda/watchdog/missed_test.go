package watchdog

import (
	"cmp"
	"context"
	"maps"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// TestDetectMissedSchedules: one SCHEDULE_MISSED per RUN# key past its start-by
// time unless the job started, the run finished, or the key was alerted. Cron
// 0 8 * * * in UTC and one RUN# row for slaTestDate unless a case says otherwise.
func TestDetectMissedSchedules(t *testing.T) {
	at := func(day, hour, minute int) time.Time { return time.Date(2026, 3, day, hour, minute, 0, 0, time.UTC) }
	tests := []struct {
		name      string
		cron, tz  string
		keys      []string // nil means slaTestDate
		now       time.Time
		trigger   string // status of the cron trigger row for each key; "" means no row
		startedAt string
		marked    bool // the key's alert marker is already there
		tweak     func(*types.PipelineConfig)
		want      map[string]string // date -> startBy of each SCHEDULE_MISSED
	}{
		{name: "not started after start-by", now: at(10, 8, 5), want: map[string]string{slaTestDate: "2026-03-10T08:00:00Z"}},
		{name: "already alerted", now: at(10, 8, 5), marked: true},
		{name: "evaluating, job not started", now: at(10, 8, 5), trigger: types.TriggerStatusRunning,
			want: map[string]string{slaTestDate: "2026-03-10T08:00:00Z"}},
		{name: "job started", now: at(10, 8, 5), trigger: types.TriggerStatusRunning, startedAt: "2026-03-10T07:30:00Z"},
		{name: "completed", now: at(10, 8, 5), trigger: types.TriggerStatusCompleted},
		{name: "failed final", now: at(10, 8, 5), trigger: types.TriggerStatusFailedFinal},
		{name: "before start-by", now: at(10, 7, 55)},
		{name: "no RUN# row", keys: []string{}, now: at(10, 8, 5)},
		{name: "weekday cron, Saturday row", cron: "0 8 * * 1-5", keys: []string{"2026-03-14"}, now: at(14, 9, 0)},
		{name: "Tokyo", cron: "0 1 * * *", tz: "Asia/Tokyo", keys: []string{"2026-03-11"}, now: at(10, 16, 5),
			want: map[string]string{"2026-03-11": "2026-03-10T16:00:00Z"}},
		{name: "hourly key", cron: "5 * * * *", keys: []string{"2026-03-10T10"}, now: at(10, 10, 6),
			want: map[string]string{"2026-03-10T10": "2026-03-10T10:05:00Z"}},
		{name: "two keys", keys: []string{"2026-03-09", slaTestDate}, now: at(10, 8, 5),
			want: map[string]string{"2026-03-09": "2026-03-09T08:00:00Z", slaTestDate: "2026-03-10T08:00:00Z"}},
		{name: "excluded date", keys: []string{"2026-03-14"}, now: at(14, 9, 0),
			tweak: func(c *types.PipelineConfig) { c.Schedule.Exclude = &types.ExclusionConfig{Weekends: true} }},
		{name: "dry run", now: at(10, 8, 5), tweak: func(c *types.PipelineConfig) { c.DryRun, c.Job.Type = true, types.TriggerHTTP }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cron := cmp.Or(tt.cron, "0 8 * * *")
			keys := tt.keys
			if keys == nil {
				keys = []string{slaTestDate}
			}
			d, mem, rec := slaDeps(t, "", "", func(c *types.PipelineConfig) {
				c.Schedule.Cron, c.Schedule.Timezone, c.SLA = cron, tt.tz, nil
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
					mem.Put("control", sItem("PK", pk, "SK", types.TriggerSK("cron", key), "status", tt.trigger, "startedAt", tt.startedAt))
				}
				if tt.marked {
					if err := d.Store.WriteSensor(ctx, "p", "schedule-missed-check#"+key, map[string]interface{}{"alerted": "true"}); err != nil {
						t.Fatalf("write marker: %v", err)
					}
				}
			}
			d.NowFunc = func() time.Time { return tt.now }

			if err := detectMissedSchedules(ctx, d); err != nil {
				t.Fatalf("detectMissedSchedules: %v", err)
			}

			got := map[string]string{}
			for i, evt := range rec.details {
				startBy, _ := evt.Detail["startBy"].(string)
				got[evt.Date] = startBy
				want := map[string]interface{}{"source": "watchdog", "cron": cron, "startBy": startBy,
					"actionHint": "start-by time passed and the job has not started; the SLA is at risk"}
				if rec.events[i] != string(types.EventScheduleMissed) || evt.ScheduleID != "cron" || !maps.Equal(evt.Detail, want) {
					t.Errorf("event %d = %s on %q with %v, want SCHEDULE_MISSED on cron with %v", i, rec.events[i], evt.ScheduleID, evt.Detail, want)
				}
			}
			if len(rec.details) != len(tt.want) || !maps.Equal(got, tt.want) {
				t.Fatalf("SCHEDULE_MISSED date -> startBy = %v (%d events), want %v", got, len(rec.details), tt.want)
			}
			for _, key := range keys {
				marker := mem.Get("control", pk, types.SensorSK("schedule-missed-check#"+key))
				if _, alerted := tt.want[key]; (marker != nil) != (tt.marked || alerted) {
					t.Errorf("marker for %s present = %v, want %v", key, marker != nil, tt.marked || alerted)
				}
			}
		})
	}
}

// TestDetectMissedSchedules_PublishFailureRetries: no marker after a failed publish, so the next pass alerts.
func TestDetectMissedSchedules_PublishFailureRetries(t *testing.T) {
	d, mem, rec := slaDeps(t, "", "", func(c *types.PipelineConfig) { c.Schedule.Cron, c.SLA = "0 8 * * *", nil })
	sk := types.SensorSK("schedule-missed-check#" + slaTestDate)
	rec.failEvents = 1
	for pass, want := range []int{0, 1} {
		if err := detectMissedSchedules(context.Background(), d); err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
		if marked := mem.Get("control", types.PipelinePK("p"), sk) != nil; len(rec.events) != want || marked != (want == 1) {
			t.Fatalf("pass %d: events %v, marker %v; want %d events and marker %v", pass, rec.events, marked, want, want == 1)
		}
	}
}
