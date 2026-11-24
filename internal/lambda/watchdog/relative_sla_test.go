package watchdog

import (
	"cmp"
	"context"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// TestRelativeSLA: with the first sensor at 10:00, maxDuration 2h and
// expectedDuration 30m, the watchdog publishes RELATIVE_SLA_WARNING once from
// 11:30 while the run is unfinished and RELATIVE_SLA_BREACH once from 12:00.
// An arrival late on slaTestDate is still checked on the next day.
func TestRelativeSLA(t *testing.T) {
	const (
		warnKey   = "relative-sla-warning-check#" + slaTestDate
		breachKey = "relative-sla-breach-check#" + slaTestDate
	)
	at := func(hour, minute, second int) time.Time {
		return time.Date(2026, 3, 10, hour, minute, second, 0, time.UTC)
	}
	tests := []struct {
		name        string
		expected    string // sla.expectedDuration
		trigger     string
		jobRow      string // event of a job row for slaTestDate
		noArrival   bool
		arrivedAt   string // "" means 2026-03-10T10:00:00Z
		dryRun      bool
		failPublish bool
		markers     []string // present before the pass
		now         time.Time
		warningAt   string // of a RELATIVE_SLA_WARNING; "" means 11:30:00Z
		wantEvents  []string
		wantMarkers []string
	}{
		{name: "before the warning", expected: "30m", now: at(11, 29, 59)},
		{name: "at the warning", expected: "30m", now: at(11, 30, 0),
			wantEvents: []string{string(types.EventRelativeSLAWarning)}, wantMarkers: []string{warnKey}},
		{name: "warning deduplicated", expected: "30m", markers: []string{warnKey}, now: at(11, 35, 0),
			wantMarkers: []string{warnKey}},
		{name: "at the breach", expected: "30m", markers: []string{warnKey}, now: at(12, 0, 0),
			wantEvents: []string{string(types.EventRelativeSLABreach)}, wantMarkers: []string{warnKey, breachKey}},
		{name: "first seen after the breach", expected: "30m", now: at(12, 10, 0),
			wantEvents: []string{string(types.EventRelativeSLABreach)}, wantMarkers: []string{breachKey}},
		{name: "default warning offset", now: at(11, 30, 0),
			wantEvents: []string{string(types.EventRelativeSLAWarning)}, wantMarkers: []string{warnKey}},
		{name: "expectedDuration 1h", expected: "1h", now: at(11, 0, 0), warningAt: "2026-03-10T11:00:00Z",
			wantEvents: []string{string(types.EventRelativeSLAWarning)}, wantMarkers: []string{warnKey}},
		{name: "finished run", expected: "30m", trigger: types.TriggerStatusCompleted, now: at(11, 30, 0)},
		{name: "rerun of an exhausted date", expected: "30m", trigger: types.TriggerStatusRunning, jobRow: types.JobEventInfraTriggerExhausted,
			now: at(12, 0, 0), wantEvents: []string{string(types.EventRelativeSLABreach)}, wantMarkers: []string{breachKey}},
		{name: "warning publish fails", expected: "30m", failPublish: true, now: at(11, 30, 0)},
		{name: "breach publish fails", expected: "30m", markers: []string{warnKey}, failPublish: true, now: at(12, 0, 0),
			wantMarkers: []string{warnKey}},
		{name: "no first-sensor-arrival", expected: "30m", noArrival: true, now: at(11, 30, 0)},
		{name: "arrival the day before, breached", expected: "30m", arrivedAt: "2026-03-10T23:00:00Z",
			now: time.Date(2026, 3, 11, 3, 0, 0, 0, time.UTC), wantEvents: []string{string(types.EventRelativeSLABreach)}, wantMarkers: []string{breachKey}},
		{name: "arrival the day before, not yet warned", expected: "30m", arrivedAt: "2026-03-10T23:00:00Z",
			now: time.Date(2026, 3, 11, 0, 29, 59, 0, time.UTC)},
		{name: "dry run", expected: "30m", dryRun: true, now: at(12, 10, 0)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, mem, rec := slaDeps(t, tt.trigger, tt.jobRow, func(c *types.PipelineConfig) {
				c.SLA = &types.SLAConfig{MaxDuration: "2h", ExpectedDuration: tt.expected}
				if tt.dryRun {
					c.DryRun, c.Job.Type, c.Job.Config = true, types.TriggerHTTP, httpJobConfig
				}
			})
			now := tt.now
			d.NowFunc = func() time.Time { return now }
			ctx := context.Background()
			if !tt.noArrival {
				if err := d.Store.WriteSensor(ctx, "p", "first-sensor-arrival#"+slaTestDate,
					map[string]interface{}{"arrivedAt": cmp.Or(tt.arrivedAt, "2026-03-10T10:00:00Z")}); err != nil {
					t.Fatalf("write arrival: %v", err)
				}
			}
			for _, key := range tt.markers {
				if err := d.Store.WriteSensor(ctx, "p", key, map[string]interface{}{"alerted": "true"}); err != nil {
					t.Fatalf("write marker %s: %v", key, err)
				}
			}
			if tt.failPublish {
				rec.failEvents = 1
			}

			if err := detectRelativeSLABreaches(ctx, d); err != nil {
				t.Fatalf("detectRelativeSLABreaches: %v", err)
			}

			if !slices.Equal(rec.events, tt.wantEvents) {
				t.Fatalf("events = %v, want %v", rec.events, tt.wantEvents)
			}
			for i, evt := range rec.details {
				if evt.Date != slaTestDate {
					t.Errorf("%s on %q, want %q", rec.events[i], evt.Date, slaTestDate)
				}
				if rec.events[i] != string(types.EventRelativeSLAWarning) {
					continue
				}
				want := map[string]interface{}{
					"source":           "watchdog",
					"maxDuration":      "2h",
					"expectedDuration": tt.expected,
					"sensorArrivalAt":  "2026-03-10T10:00:00Z",
					"warningAt":        cmp.Or(tt.warningAt, "2026-03-10T11:30:00Z"),
					"breachAt":         "2026-03-10T12:00:00Z",
					"actionHint":       "relative SLA at risk; the run has not finished and the breach is at 2026-03-10T12:00:00Z",
				}
				if !maps.Equal(evt.Detail, want) {
					t.Errorf("warning detail = %v, want %v", evt.Detail, want)
				}
				if wantMsg := "relative SLA warning for p on " + slaTestDate; evt.Message != wantMsg || evt.Date != slaTestDate {
					t.Errorf("warning message %q on %q, want %q on %q", evt.Message, evt.Date, wantMsg, slaTestDate)
				}
			}
			for _, key := range []string{warnKey, breachKey} {
				present := mem.Get("control", types.PipelinePK("p"), types.SensorSK(key)) != nil
				if want := slices.Contains(tt.wantMarkers, key); present != want {
					t.Errorf("marker %s present = %v, want %v", key, present, want)
				}
			}
		})
	}
}
