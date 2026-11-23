package watchdog

import (
	"cmp"
	"context"
	"maps"
	"slices"
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
		{name: "exhausted launch", now: at(10, 8, 5), trigger: types.TriggerStatusFailedFinal, startedAt: "2026-03-10T07:30:00Z"},
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
		{name: "dry run", now: at(10, 8, 5), tweak: dryRunHTTP},
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

// TestDetectMissedInclusionSchedules_PublishFailureRetries: no marker after a
// failed publish, so the next pass alerts, and the pass after adds nothing.
func TestDetectMissedInclusionSchedules_PublishFailureRetries(t *testing.T) {
	d, _, rec := slaDeps(t, "", "", func(c *types.PipelineConfig) {
		c.SLA, c.Schedule.Include = nil, &types.InclusionConfig{Dates: []string{"2026-03-05", "2026-03-15"}}
	})
	ctx := context.Background()
	rec.failEvents = 1
	for pass, want := range []int{0, 1, 1} {
		if err := detectMissedInclusionSchedules(ctx, d); err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
		marker, err := d.Store.GetSensorData(ctx, "p", "irregular-missed-check#2026-03-05")
		if err != nil {
			t.Fatal(err)
		}
		if marked := marker != nil; len(rec.events) != want || marked != (want == 1) ||
			(want == 1 && rec.events[0] != string(types.EventIrregularScheduleMissed)) {
			t.Fatalf("pass %d: events %v, marker %v; want %d IRREGULAR_SCHEDULE_MISSED and marker %v", pass, rec.events, marked, want, want == 1)
		}
	}
}

// TestDetectMissedInclusionSchedules_EveningBeforeThenMissed: a pass on the
// evening before the date in New York neither alerts nor marks the date, so
// the pass once the date's time has passed still alerts.
func TestDetectMissedInclusionSchedules_EveningBeforeThenMissed(t *testing.T) {
	d, _, rec := slaDeps(t, "", "", func(c *types.PipelineConfig) {
		c.SLA, c.Schedule.Time, c.Schedule.Timezone = nil, "09:00", "America/New_York"
		c.Schedule.Include = &types.InclusionConfig{Dates: []string{"2026-03-31"}}
	})
	ctx := context.Background()
	now := time.Date(2026, 3, 31, 0, 0, 0, 0, time.UTC)
	d.NowFunc = func() time.Time { return now }
	for pass, at := range []time.Time{now, time.Date(2026, 3, 31, 13, 30, 0, 0, time.UTC)} {
		now = at
		if err := detectMissedInclusionSchedules(ctx, d); err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
		marker, err := d.Store.GetSensorData(ctx, "p", "irregular-missed-check#2026-03-31")
		if err != nil {
			t.Fatal(err)
		}
		if marked := marker != nil; len(rec.events) != pass || marked != (pass == 1) {
			t.Fatalf("pass %d at %s: events %v, marker %v; want %d events and marker %v", pass, at.Format(time.RFC3339), rec.events, marked, pass, pass == 1)
		}
	}
}

// TestDetectMissedInclusionSchedules: one IRREGULAR_SCHEDULE_MISSED per past
// inclusion date (the newest three) with no trigger row and no marker, newest
// first, each followed by its marker. The clock is 09:00 on slaTestDate
// unless a case says otherwise. ScheduleTime holds schedule.time back on the
// date itself until that time in schedule.timezone.
func TestDetectMissedInclusionSchedules(t *testing.T) {
	type row struct {
		name    string
		dates   []string // schedule.include.dates; nil means no include
		trigger string   // date of a COMPLETED trigger row
		marked  string   // date whose marker is already there
		dryRun  bool
		tm, tz  string // schedule.time and schedule.timezone
		now     time.Time
		want    []string // dates alerted, in order
	}
	run := func(t *testing.T, tt row) {
		d, mem, rec := slaDeps(t, "", "", func(c *types.PipelineConfig) {
			c.SLA, c.Schedule.Time, c.Schedule.Timezone = nil, tt.tm, tt.tz
			if tt.dates != nil {
				c.Schedule.Include = &types.InclusionConfig{Dates: tt.dates}
			}
			if tt.dryRun {
				c.DryRun, c.Job.Type, c.Job.Config = true, types.TriggerHTTP, httpJobConfig
			}
		})
		ctx := context.Background()
		if !tt.now.IsZero() {
			d.NowFunc = func() time.Time { return tt.now }
		}
		if tt.trigger != "" {
			mem.Put("control", sItem("PK", types.PipelinePK("p"), "SK", types.TriggerSK("stream", tt.trigger), "status", types.TriggerStatusCompleted))
		}
		if tt.marked != "" {
			if err := d.Store.WriteSensor(ctx, "p", "irregular-missed-check#"+tt.marked, map[string]interface{}{"alerted": "true"}); err != nil {
				t.Fatalf("write marker: %v", err)
			}
		}

		if err := detectMissedInclusionSchedules(ctx, d); err != nil {
			t.Fatalf("detectMissedInclusionSchedules: %v", err)
		}

		var got []string
		for i, evt := range rec.details {
			got = append(got, evt.Date)
			want := map[string]interface{}{"source": "watchdog",
				"actionHint": "inclusion date " + evt.Date + " expected to have a trigger \u2014 none found"}
			if rec.events[i] != string(types.EventIrregularScheduleMissed) || evt.PipelineID != "p" ||
				evt.Message != "missed inclusion schedule for p on "+evt.Date || !maps.Equal(evt.Detail, want) {
				t.Errorf("event %d = %s for %q, %q with %v; want IRREGULAR_SCHEDULE_MISSED for p with %v", i, rec.events[i], evt.PipelineID, evt.Message, evt.Detail, want)
			}
		}
		if !slices.Equal(got, tt.want) {
			t.Fatalf("alerted dates = %v, want %v", got, tt.want)
		}
		for _, date := range tt.dates {
			marker, err := d.Store.GetSensorData(ctx, "p", "irregular-missed-check#"+date)
			if err != nil {
				t.Fatal(err)
			}
			wantMarker := date == tt.marked || slices.Contains(tt.want, date)
			if (marker != nil) != wantMarker || (marker != nil && marker["alerted"] != "true") {
				t.Errorf("marker for %s = %v, want present %v with alerted true", date, marker, wantMarker)
			}
		}
	}

	tests := []row{
		{name: "no include"},
		{name: "trigger exists", dates: []string{"2026-03-05", "2026-03-15"}, trigger: "2026-03-05"},
		{name: "trigger missing", dates: []string{"2026-03-05", "2026-03-15"}, want: []string{"2026-03-05"}},
		{name: "already alerted", dates: []string{"2026-03-05", "2026-03-15"}, marked: "2026-03-05"},
		{name: "all future", dates: []string{"2026-03-15", "2026-06-15", "2026-09-15"}},
		{name: "two missed", dates: []string{"2026-03-05", "2026-03-08", "2026-03-15"}, want: []string{"2026-03-08", "2026-03-05"}},
		{name: "newest three only", dates: []string{"2026-03-01", "2026-03-02", "2026-03-03", "2026-03-04", "2026-03-05", "2026-03-06"},
			want: []string{"2026-03-06", "2026-03-05", "2026-03-04"}},
		{name: "dry run", dates: []string{"2026-03-05", "2026-03-15"}, dryRun: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) { run(t, tt) })
	}

	t.Run("ScheduleTime", func(t *testing.T) {
		at := func(hour, minute int) time.Time { return time.Date(2026, 3, 31, hour, minute, 0, 0, time.UTC) }
		end := []string{"2026-03-31"}
		tests := []row{
			{name: "before the time", tm: "10:00", now: at(8, 0)},
			{name: "past the time", tm: "10:00", now: at(11, 0), want: end},
			{name: "Tokyo, past the time", tm: "10:00", tz: "Asia/Tokyo", now: at(1, 30), want: end},
			{name: "New York, before the time", tm: "10:00", tz: "America/New_York", now: at(13, 30)},
			// 22:00 on the date in New York is already the next day in UTC.
			{name: "New York evening, before the time", tm: "23:30", tz: "America/New_York", now: time.Date(2026, 4, 1, 2, 0, 0, 0, time.UTC)},
			{name: "no time", now: at(0, 5), want: end},
			// New York is UTC-4 here, so 00:00Z is 20:00 the evening before.
			{name: "New York, the evening before", tm: "09:00", tz: "America/New_York", now: at(0, 0)},
			{name: "New York, the evening before, no time", tz: "America/New_York", now: at(0, 0)},
			{name: "New York, on the date past the time", tm: "09:00", tz: "America/New_York", now: at(13, 30), want: end},
			{name: "Tokyo, date begun before UTC midnight", tz: "Asia/Tokyo", now: time.Date(2026, 3, 30, 15, 30, 0, 0, time.UTC), want: end},
		}
		for _, tt := range tests {
			tt.dates = end
			t.Run(tt.name, func(t *testing.T) { run(t, tt) })
		}
	})
}
