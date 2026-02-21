package watcher

import (
	"testing"
	"time"

	"github.com/interlock-systems/interlock/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsScheduleActive_NoAfter(t *testing.T) {
	sched := types.ScheduleConfig{Name: "daily"}
	assert.True(t, isScheduleActive(sched, time.Now()), "schedule with no After should always be active")
}

func TestIsScheduleActive_BeforeAfterTime(t *testing.T) {
	// Use a fixed time: 07:30 UTC
	now := time.Date(2025, 6, 15, 7, 30, 0, 0, time.UTC)
	sched := types.ScheduleConfig{
		Name:  "morning",
		After: "08:00",
	}
	assert.False(t, isScheduleActive(sched, now), "schedule should not be active before After time")
}

func TestIsScheduleActive_AfterAfterTime(t *testing.T) {
	// Use a fixed time: 08:30 UTC
	now := time.Date(2025, 6, 15, 8, 30, 0, 0, time.UTC)
	sched := types.ScheduleConfig{
		Name:  "morning",
		After: "08:00",
	}
	assert.True(t, isScheduleActive(sched, now), "schedule should be active after After time")
}

func TestIsScheduleActive_ExactTime(t *testing.T) {
	now := time.Date(2025, 6, 15, 8, 0, 0, 0, time.UTC)
	sched := types.ScheduleConfig{
		Name:  "morning",
		After: "08:00",
	}
	assert.True(t, isScheduleActive(sched, now), "schedule should be active at exact After time")
}

func TestIsScheduleActive_WithTimezone(t *testing.T) {
	// 13:00 UTC = 09:00 America/New_York (EDT)
	now := time.Date(2025, 6, 15, 13, 0, 0, 0, time.UTC)
	sched := types.ScheduleConfig{
		Name:     "morning",
		After:    "08:00",
		Timezone: "America/New_York",
	}
	assert.True(t, isScheduleActive(sched, now), "should be active: 09:00 ET > 08:00 ET")

	// 11:00 UTC = 07:00 America/New_York (EDT)
	earlyNow := time.Date(2025, 6, 15, 11, 0, 0, 0, time.UTC)
	assert.False(t, isScheduleActive(sched, earlyNow), "should not be active: 07:00 ET < 08:00 ET")
}

func TestIsScheduleActive_InvalidAfter(t *testing.T) {
	sched := types.ScheduleConfig{
		Name:  "bad",
		After: "not-a-time",
	}
	// Fail-open: invalid After should allow activation
	assert.True(t, isScheduleActive(sched, time.Now()), "invalid After should fail-open")
}

func TestParseTimeOfDay(t *testing.T) {
	ref := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)

	tests := []struct {
		name    string
		hhmm    string
		wantH   int
		wantM   int
		wantErr bool
	}{
		{"basic", "08:30", 8, 30, false},
		{"midnight", "00:00", 0, 0, false},
		{"end of day", "23:59", 23, 59, false},
		{"single digit hour", "9:05", 9, 5, false},
		{"invalid hour", "25:00", 0, 0, true},
		{"invalid minute", "12:61", 0, 0, true},
		{"no colon", "0830", 0, 0, true},
		{"empty", "", 0, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseTimeOfDay(tt.hhmm, ref, time.UTC)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantH, result.Hour())
			assert.Equal(t, tt.wantM, result.Minute())
			assert.Equal(t, ref.Year(), result.Year())
			assert.Equal(t, ref.Month(), result.Month())
			assert.Equal(t, ref.Day(), result.Day())
		})
	}
}

func TestScheduleDeadline_FromSchedule(t *testing.T) {
	now := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	sched := types.ScheduleConfig{
		Name:     "morning",
		Deadline: "09:30",
	}
	pipeline := types.PipelineConfig{Name: "test"}

	deadline, ok := scheduleDeadline(sched, pipeline, now)
	assert.True(t, ok)
	assert.Equal(t, 9, deadline.Hour())
	assert.Equal(t, 30, deadline.Minute())
}

func TestScheduleDeadline_FallbackToSLA(t *testing.T) {
	now := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	sched := types.ScheduleConfig{
		Name: "morning",
	}
	pipeline := types.PipelineConfig{
		Name: "test",
		SLA: &types.SLAConfig{
			CompletionDeadline: "11:00",
		},
	}

	deadline, ok := scheduleDeadline(sched, pipeline, now)
	assert.True(t, ok)
	assert.Equal(t, 11, deadline.Hour())
}

func TestScheduleDeadline_NoDeadline(t *testing.T) {
	now := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	sched := types.ScheduleConfig{Name: "morning"}
	pipeline := types.PipelineConfig{Name: "test"}

	_, ok := scheduleDeadline(sched, pipeline, now)
	assert.False(t, ok)
}

func TestResolveSchedules_Default(t *testing.T) {
	p := types.PipelineConfig{Name: "test"}
	scheds := types.ResolveSchedules(p)
	require.Len(t, scheds, 1)
	assert.Equal(t, types.DefaultScheduleID, scheds[0].Name)
}

func TestResolveSchedules_Explicit(t *testing.T) {
	p := types.PipelineConfig{
		Name: "test",
		Schedules: []types.ScheduleConfig{
			{Name: "morning", After: "08:00"},
			{Name: "evening", After: "17:00"},
		},
	}
	scheds := types.ResolveSchedules(p)
	require.Len(t, scheds, 2)
	assert.Equal(t, "morning", scheds[0].Name)
	assert.Equal(t, "evening", scheds[1].Name)
}
