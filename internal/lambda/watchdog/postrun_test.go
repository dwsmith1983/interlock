package watchdog

import (
	"bytes"
	"cmp"
	"context"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// TestDetectMissingPostRunSensors: POST_RUN_SENSOR_MISSING fires once
// postRun.sensorTimeout (2h by default) has passed since the trigger row's
// completedAt with no post-run sensor update after it. The job rows do not
// set the anchor. Completion 12:00 on slaTestDate unless a row says otherwise.
// An alert writes the postrun-check marker for the date.
func TestDetectMissingPostRunSensors(t *testing.T) {
	at := func(hour, minute int) time.Time { return time.Date(2026, 3, 10, hour, minute, 0, 0, time.UTC) }
	ms := func(tm time.Time) string { return strconv.FormatInt(tm.UnixMilli(), 10) }
	missing := []string{string(types.EventPostRunSensorMissing)}
	tests := []struct {
		name        string
		completedAt string
		jobAt       time.Time // a success job row at this time; zero means none
		updatedAt   time.Time // the post-run sensor's updatedAt; zero means no sensor
		now         time.Time
		noBaseline  bool
		marked      bool // the postrun-check marker is already there
		noPostRun   bool
		dryRun      bool
		status      string // of the trigger row; "" means COMPLETED
		timeout     string // postRun.sensorTimeout
		wantEvents  []string
		wantMarker  bool
		wantWarn    bool // a warning about completedAt
	}{
		{name: "past the timeout (guard)", completedAt: "2026-03-10T12:00:00Z", jobAt: at(12, 0), now: at(14, 30), wantEvents: missing, wantMarker: true},
		{name: "within the timeout (guard)", completedAt: "2026-03-10T12:00:00Z", jobAt: at(12, 0), now: at(13, 30)},
		{name: "sensor updated after completion (guard)", completedAt: "2026-03-10T12:00:00Z", jobAt: at(12, 0), updatedAt: at(13, 0), now: at(14, 30)},
		{name: "completedAt set, no success job row", completedAt: "2026-03-10T12:00:00Z", now: at(14, 0), wantEvents: missing, wantMarker: true},
		{name: "anchor is completedAt, not the job row", completedAt: "2026-03-10T12:00:00Z", jobAt: at(8, 0), now: at(13, 0)},
		{name: "no completedAt", jobAt: at(12, 0), now: at(14, 30)},
		{name: "completedAt does not parse", completedAt: "yesterday", jobAt: at(12, 0), now: at(14, 30), wantWarn: true},
		{name: "already alerted", completedAt: "2026-03-10T12:00:00Z", marked: true, now: at(14, 30)},
		{name: "no baseline", completedAt: "2026-03-10T12:00:00Z", noBaseline: true, now: at(14, 30)},
		{name: "trigger RUNNING", completedAt: "2026-03-10T12:00:00Z", status: types.TriggerStatusRunning, now: at(14, 30)},
		{name: "timeout 1h", completedAt: "2026-03-10T12:00:00Z", timeout: "1h", now: at(13, 30), wantEvents: missing, wantMarker: true},
		{name: "no postRun", completedAt: "2026-03-10T12:00:00Z", noPostRun: true, now: at(14, 30)},
		{name: "dry run", completedAt: "2026-03-10T12:00:00Z", dryRun: true, now: at(14, 30)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, mem, rec := slaDeps(t, "", "", func(c *types.PipelineConfig) {
				c.SLA = nil
				c.PostRun = &types.PostRunConfig{Rules: []types.ValidationRule{{Key: "row-count", Check: types.CheckExists}}, SensorTimeout: tt.timeout}
				if tt.noPostRun {
					c.PostRun = nil
				}
				if tt.dryRun {
					c.DryRun, c.Job.Type, c.Job.Config = true, types.TriggerHTTP, httpJobConfig
				}
			})
			pk := types.PipelinePK("p")
			trigger := []string{"PK", pk, "SK", types.TriggerSK("stream", slaTestDate), "status", cmp.Or(tt.status, types.TriggerStatusCompleted)}
			if tt.completedAt != "" {
				trigger = append(trigger, "completedAt", tt.completedAt)
			}
			mem.Put("control", sItem(trigger...))
			if !tt.jobAt.IsZero() {
				mem.Put("joblog", sItem("PK", pk, "SK", types.JobSK("stream", slaTestDate, ms(tt.jobAt)), "event", types.JobEventSuccess))
			}
			ctx := context.Background()
			if !tt.noBaseline {
				if err := d.Store.WriteSensor(ctx, "p", "postrun-baseline#"+slaTestDate, map[string]interface{}{"row-count": map[string]interface{}{"count": 1}}); err != nil {
					t.Fatal(err)
				}
			}
			if tt.marked {
				if err := d.Store.WriteSensor(ctx, "p", "postrun-check#"+slaTestDate, map[string]interface{}{"alerted": "true"}); err != nil {
					t.Fatal(err)
				}
			}
			if !tt.updatedAt.IsZero() {
				if err := d.Store.WriteSensor(ctx, "p", "row-count", map[string]interface{}{"updatedAt": tt.updatedAt.UnixMilli()}); err != nil {
					t.Fatal(err)
				}
			}
			d.NowFunc = func() time.Time { return tt.now }
			logs := &bytes.Buffer{}
			d.Logger = slog.New(slog.NewTextHandler(logs, nil))

			if err := detectMissingPostRunSensors(ctx, d); err != nil {
				t.Fatalf("detectMissingPostRunSensors: %v", err)
			}
			if !slices.Equal(rec.events, tt.wantEvents) {
				t.Errorf("events = %v, want %v", rec.events, tt.wantEvents)
			}
			marker, err := d.Store.GetSensorData(ctx, "p", "postrun-check#"+slaTestDate)
			if err != nil {
				t.Fatal(err)
			}
			if wantMarker := tt.wantMarker || tt.marked; (marker != nil) != wantMarker || (marker != nil && marker["alerted"] != "true") {
				t.Errorf("postrun-check marker = %v, want present %v with alerted true", marker, wantMarker)
			}
			if warned := strings.Contains(logs.String(), "level=WARN"); warned != tt.wantWarn {
				t.Errorf("warned = %v, want %v; logs: %s", warned, tt.wantWarn, logs)
			}
		})
	}
}

// TestDetectMissingPostRunSensors_PublishFailureRetries: no marker after a
// failed publish, so the next pass alerts, and the pass after adds nothing.
func TestDetectMissingPostRunSensors_PublishFailureRetries(t *testing.T) {
	d, mem, rec := slaDeps(t, "", "", func(c *types.PipelineConfig) {
		c.SLA = nil
		c.PostRun = &types.PostRunConfig{Rules: []types.ValidationRule{{Key: "row-count", Check: types.CheckExists}}}
	})
	mem.Put("control", sItem("PK", types.PipelinePK("p"), "SK", types.TriggerSK("stream", slaTestDate),
		"status", types.TriggerStatusCompleted, "completedAt", "2026-03-10T12:00:00Z"))
	ctx := context.Background()
	if err := d.Store.WriteSensor(ctx, "p", "postrun-baseline#"+slaTestDate, map[string]interface{}{"row-count": map[string]interface{}{"count": 1}}); err != nil {
		t.Fatal(err)
	}
	d.NowFunc = func() time.Time { return time.Date(2026, 3, 10, 14, 30, 0, 0, time.UTC) }
	rec.failEvents = 1
	for pass, want := range []int{0, 1, 1} {
		if err := detectMissingPostRunSensors(ctx, d); err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
		marker, err := d.Store.GetSensorData(ctx, "p", "postrun-check#"+slaTestDate)
		if err != nil {
			t.Fatal(err)
		}
		if marked := marker != nil; len(rec.events) != want || marked != (want == 1) ||
			(want == 1 && rec.events[0] != string(types.EventPostRunSensorMissing)) {
			t.Fatalf("pass %d: events %v, marker %v; want %d POST_RUN_SENSOR_MISSING and marker %v", pass, rec.events, marked, want, want == 1)
		}
	}
}
