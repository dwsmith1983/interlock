package lambda

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dwsmith1983/interlock/pkg/types"
)

func TestStartByTime(t *testing.T) {
	utc := time.UTC
	newYork, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	tokyo, err := time.LoadLocation("Asia/Tokyo")
	require.NoError(t, err)
	berlin, err := time.LoadLocation("Europe/Berlin")
	require.NoError(t, err)
	// Chile moves its clocks forward at local midnight, so 2026-09-06 00:00
	// does not exist there.
	santiago, err := time.LoadLocation("America/Santiago")
	require.NoError(t, err)

	tests := []struct {
		name    string
		cron    string
		key     string
		loc     *time.Location
		want    string // RFC3339 in UTC; empty means no fire in the window
		wantErr bool
	}{
		{name: "daily", cron: "0 8 * * *", key: "2026-03-10", loc: utc, want: "2026-03-10T08:00:00Z"},
		{name: "weekday on saturday", cron: "0 8 * * 1-5", key: "2026-03-14", loc: utc},
		{name: "weekday on friday", cron: "0 8 * * 1-5", key: "2026-03-13", loc: newYork, want: "2026-03-13T12:00:00Z"},
		{name: "monthly first", cron: "30 6 1 * *", key: "2026-04-01", loc: utc, want: "2026-04-01T06:30:00Z"},
		{name: "monthly other day", cron: "30 6 1 * *", key: "2026-04-02", loc: utc},
		{name: "list", cron: "0 8 1,15 * *", key: "2026-03-15", loc: utc, want: "2026-03-15T08:00:00Z"},
		{name: "dom or dow", cron: "0 8 13 * 1", key: "2026-03-09", loc: utc, want: "2026-03-09T08:00:00Z"},
		{name: "dom or dow, neither", cron: "0 8 13 * 1", key: "2026-03-10", loc: utc},
		{name: "tokyo", cron: "0 1 * * *", key: "2026-03-11", loc: tokyo, want: "2026-03-10T16:00:00Z"},
		{name: "hourly key", cron: "5 * * * *", key: "2026-03-10T10", loc: utc, want: "2026-03-10T10:05:00Z"},
		{name: "hourly key, step", cron: "*/20 * * * *", key: "2026-03-10T10", loc: utc, want: "2026-03-10T10:00:00Z"},
		{name: "daily cron, hourly key", cron: "0 8 * * *", key: "2026-03-10T10", loc: utc},
		{name: "daily cron, its hour", cron: "0 8 * * *", key: "2026-03-10T08", loc: utc, want: "2026-03-10T08:00:00Z"},
		{name: "dst spring forward", cron: "30 2 * * *", key: "2026-03-08", loc: newYork},
		{name: "dst fall back", cron: "30 1 * * *", key: "2026-11-01", loc: newYork, want: "2026-11-01T05:30:00Z"},
		{name: "dst fall back, EST morning", cron: "0 8 * * *", key: "2026-11-01", loc: newYork, want: "2026-11-01T13:00:00Z"},
		{name: "dst fall back, 25-hour day", cron: "30 23 * * *", key: "2026-11-01", loc: newYork, want: "2026-11-02T04:30:00Z"},
		{name: "dst spring forward, late evening", cron: "30 23 * * *", key: "2026-03-08", loc: newYork, want: "2026-03-09T03:30:00Z"},
		{name: "dst spring forward, window ends at the real midnight", cron: "30 0 * * 1", key: "2026-03-08", loc: newYork},
		{name: "hourly key, repeated hour", cron: "*/20 * * * *", key: "2026-11-01T01", loc: newYork, want: "2026-11-01T05:00:00Z"},
		{name: "hourly key, hour before the gap", cron: "*/20 * * * *", key: "2026-03-08T01", loc: newYork, want: "2026-03-08T06:00:00Z"},
		{name: "hourly key, nonexistent hour", cron: "*/20 * * * *", key: "2026-03-08T02", loc: newYork},
		{name: "hourly key, hour after the gap", cron: "*/20 * * * *", key: "2026-03-08T03", loc: newYork, want: "2026-03-08T07:00:00Z"},
		{name: "hourly key, repeated hour east of UTC", cron: "*/20 * * * *", key: "2026-10-25T02", loc: berlin, want: "2026-10-25T01:00:00Z"},
		{name: "daily key, midnight gap", cron: "30 23 * * *", key: "2026-09-06", loc: santiago, want: "2026-09-07T02:30:00Z"},
		{name: "daily key, day before a midnight gap", cron: "30 23 * * *", key: "2026-09-05", loc: santiago, want: "2026-09-06T03:30:00Z"},
		{name: "leap day, next fire years away", cron: "0 0 29 2 *", key: "2026-03-10", loc: utc},
		{name: "never fires", cron: "0 0 31 2 *", key: "2026-03-10", loc: utc},
		{name: "bad cron", cron: "not a cron", key: "2026-03-10", loc: utc, wantErr: true},
		{name: "TZ prefix with no spaces", cron: "TZ=UTC\t0\t8\t*\t*", key: "2026-03-10", loc: utc, wantErr: true},
		{name: "bad key", cron: "0 8 * * *", key: "2026-3-10", loc: utc, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok, err := StartByTime(tt.cron, tt.key, tt.loc)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			if tt.want == "" {
				assert.False(t, ok, "got %s", got.UTC().Format(time.RFC3339))
				return
			}
			assert.True(t, ok)
			assert.Equal(t, tt.want, got.UTC().Format(time.RFC3339))
		})
	}
}

func cutoffConfig(cutoff string, offset int, tz string) *types.PipelineConfig {
	return &types.PipelineConfig{Schedule: types.ScheduleConfig{
		Timezone: tz,
		Trigger:  &types.TriggerCondition{Key: "upstream", Check: types.CheckExists, Cutoff: cutoff, CutoffDayOffset: offset},
	}}
}

func TestResolveCutoffTime(t *testing.T) {
	tests := []struct {
		name string
		cfg  *types.PipelineConfig
		key  string
		want string // RFC3339 in UTC; empty means the zero time
	}{
		{"same day", cutoffConfig("14:00", 0, "UTC"), "2026-03-10", "2026-03-10T14:00:00Z"},
		{"offset 1", cutoffConfig("02:00", 1, "UTC"), "2026-03-10", "2026-03-11T02:00:00Z"},
		{"offset across month", cutoffConfig("02:00", 1, "UTC"), "2026-03-31", "2026-04-01T02:00:00Z"},
		{"new york", cutoffConfig("14:00", 0, "America/New_York"), "2026-03-10", "2026-03-10T18:00:00Z"},
		// The cutoff is the first instant the local clock reads the cutoff or
		// later. 02:30 does not exist in New York on 2026-03-08, so it is the
		// gap's end, 03:00 EDT; a repeated wall time is its first occurrence.
		{"offset over DST start", cutoffConfig("02:30", 1, "America/New_York"), "2026-03-07", "2026-03-08T07:00:00Z"},
		{"DST end, first occurrence", cutoffConfig("01:30", 0, "America/New_York"), "2026-11-01", "2026-11-01T05:30:00Z"},
		{"DST end east of UTC, first occurrence", cutoffConfig("02:30", 0, "Europe/Berlin"), "2026-10-25", "2026-10-25T00:30:00Z"},
		{"DST start east of UTC", cutoffConfig("02:30", 0, "Europe/Berlin"), "2026-03-29", "2026-03-29T01:00:00Z"},
		{"hourly into the DST gap", cutoffConfig(":15", 0, "America/New_York"), "2026-03-08T01", "2026-03-08T07:00:00Z"},
		{"hourly", cutoffConfig(":15", 0, "UTC"), "2026-03-10T10", "2026-03-10T11:15:00Z"},
		{"hourly at day end", cutoffConfig(":15", 0, "UTC"), "2026-03-10T23", "2026-03-11T00:15:00Z"},
		{"daily cutoff, hourly key", cutoffConfig("14:00", 0, "UTC"), "2026-03-10T10", ""},
		{"hourly cutoff, daily key", cutoffConfig(":15", 0, "UTC"), "2026-03-10", ""},
		{"sla timezone fallback", func() *types.PipelineConfig {
			c := cutoffConfig("14:00", 0, "")
			c.SLA = &types.SLAConfig{Timezone: "America/New_York"}
			return c
		}(), "2026-03-10", "2026-03-10T18:00:00Z"},
		{"bad key", cutoffConfig("14:00", 0, "UTC"), "2026-3-10", ""},
		{"no cutoff", cutoffConfig("", 0, "UTC"), "2026-03-10", ""},
		{"none", &types.PipelineConfig{}, "2026-03-10", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ResolveCutoffTime(tt.cfg, tt.key)
			if tt.want == "" {
				assert.True(t, got.IsZero(), "got %s", got.UTC().Format(time.RFC3339))
				return
			}
			assert.Equal(t, tt.want, got.UTC().Format(time.RFC3339))
		})
	}
}

func TestCutoffPassed(t *testing.T) {
	cfg := cutoffConfig("14:00", 0, "UTC")
	at := func(h, m int) time.Time { return time.Date(2026, 3, 10, h, m, 0, 0, time.UTC) }
	assert.False(t, CutoffPassed(cfg, "2026-03-10", at(13, 59)))
	assert.True(t, CutoffPassed(cfg, "2026-03-10", at(14, 0)))
	assert.True(t, CutoffPassed(cfg, "2026-03-10", at(14, 1)))
	assert.False(t, CutoffPassed(cutoffConfig("", 0, "UTC"), "2026-03-10", at(14, 1)))
}

func TestResolveScheduleID(t *testing.T) {
	stream := &types.PipelineConfig{Schedule: types.ScheduleConfig{Trigger: &types.TriggerCondition{Key: "upstream"}}}
	assert.Equal(t, "stream", ResolveScheduleID(stream))
	assert.Equal(t, "cron", ResolveScheduleID(&types.PipelineConfig{Schedule: types.ScheduleConfig{Cron: "0 8 * * *"}}))
}
