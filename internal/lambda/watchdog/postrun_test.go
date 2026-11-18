package watchdog

import (
	"bytes"
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
		wantEvents  []string
		wantWarn    bool // a warning about completedAt
	}{
		{name: "past the timeout (guard)", completedAt: "2026-03-10T12:00:00Z", jobAt: at(12, 0), now: at(14, 30), wantEvents: missing},
		{name: "within the timeout (guard)", completedAt: "2026-03-10T12:00:00Z", jobAt: at(12, 0), now: at(13, 30)},
		{name: "sensor updated after completion (guard)", completedAt: "2026-03-10T12:00:00Z", jobAt: at(12, 0), updatedAt: at(13, 0), now: at(14, 30)},
		{name: "completedAt set, no success job row", completedAt: "2026-03-10T12:00:00Z", now: at(14, 0), wantEvents: missing},
		{name: "anchor is completedAt, not the job row", completedAt: "2026-03-10T12:00:00Z", jobAt: at(8, 0), now: at(13, 0)},
		{name: "no completedAt", jobAt: at(12, 0), now: at(14, 30)},
		{name: "completedAt does not parse", completedAt: "yesterday", jobAt: at(12, 0), now: at(14, 30), wantWarn: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, mem, rec := slaDeps(t, "", "", func(c *types.PipelineConfig) {
				c.SLA = nil
				c.PostRun = &types.PostRunConfig{Rules: []types.ValidationRule{{Key: "row-count", Check: types.CheckExists}}}
			})
			pk := types.PipelinePK("p")
			trigger := []string{"PK", pk, "SK", types.TriggerSK("stream", slaTestDate), "status", types.TriggerStatusCompleted}
			if tt.completedAt != "" {
				trigger = append(trigger, "completedAt", tt.completedAt)
			}
			mem.Put("control", sItem(trigger...))
			if !tt.jobAt.IsZero() {
				mem.Put("joblog", sItem("PK", pk, "SK", types.JobSK("stream", slaTestDate, ms(tt.jobAt)), "event", types.JobEventSuccess))
			}
			ctx := context.Background()
			if err := d.Store.WriteSensor(ctx, "p", "postrun-baseline#"+slaTestDate, map[string]interface{}{"row-count": map[string]interface{}{"count": 1}}); err != nil {
				t.Fatal(err)
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
			if warned := strings.Contains(logs.String(), "level=WARN"); warned != tt.wantWarn {
				t.Errorf("warned = %v, want %v; logs: %s", warned, tt.wantWarn, logs)
			}
		})
	}
}
