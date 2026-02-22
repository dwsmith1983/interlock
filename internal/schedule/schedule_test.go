package schedule

import (
	"testing"
	"time"

	"github.com/interlock-systems/interlock/internal/calendar"
	"github.com/interlock-systems/interlock/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsScheduleActive_NoAfter(t *testing.T) {
	sched := types.ScheduleConfig{Name: "daily"}
	assert.True(t, IsScheduleActive(sched, time.Now(), nil))
}

func TestIsScheduleActive_BeforeAfterTime(t *testing.T) {
	now := time.Date(2025, 6, 15, 7, 30, 0, 0, time.UTC)
	sched := types.ScheduleConfig{Name: "morning", After: "08:00"}
	assert.False(t, IsScheduleActive(sched, now, nil))
}

func TestIsScheduleActive_AfterAfterTime(t *testing.T) {
	now := time.Date(2025, 6, 15, 8, 30, 0, 0, time.UTC)
	sched := types.ScheduleConfig{Name: "morning", After: "08:00"}
	assert.True(t, IsScheduleActive(sched, now, nil))
}

func TestIsScheduleActive_ExactTime(t *testing.T) {
	now := time.Date(2025, 6, 15, 8, 0, 0, 0, time.UTC)
	sched := types.ScheduleConfig{Name: "morning", After: "08:00"}
	assert.True(t, IsScheduleActive(sched, now, nil))
}

func TestIsScheduleActive_WithTimezone(t *testing.T) {
	// 13:00 UTC = 09:00 America/New_York (EDT)
	now := time.Date(2025, 6, 15, 13, 0, 0, 0, time.UTC)
	sched := types.ScheduleConfig{Name: "morning", After: "08:00", Timezone: "America/New_York"}
	assert.True(t, IsScheduleActive(sched, now, nil))

	// 11:00 UTC = 07:00 America/New_York (EDT)
	earlyNow := time.Date(2025, 6, 15, 11, 0, 0, 0, time.UTC)
	assert.False(t, IsScheduleActive(sched, earlyNow, nil))
}

func TestIsScheduleActive_InvalidAfter(t *testing.T) {
	sched := types.ScheduleConfig{Name: "bad", After: "not-a-time"}
	assert.True(t, IsScheduleActive(sched, time.Now(), nil), "invalid After should fail-open")
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
			result, err := ParseTimeOfDay(tt.hhmm, ref, time.UTC)
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
	sched := types.ScheduleConfig{Name: "morning", Deadline: "09:30"}
	pipeline := types.PipelineConfig{Name: "test"}

	deadline, ok := ScheduleDeadline(sched, pipeline, now)
	assert.True(t, ok)
	assert.Equal(t, 9, deadline.Hour())
	assert.Equal(t, 30, deadline.Minute())
}

func TestScheduleDeadline_FallbackToSLA(t *testing.T) {
	now := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	sched := types.ScheduleConfig{Name: "morning"}
	pipeline := types.PipelineConfig{
		Name: "test",
		SLA:  &types.SLAConfig{CompletionDeadline: "11:00"},
	}

	deadline, ok := ScheduleDeadline(sched, pipeline, now)
	assert.True(t, ok)
	assert.Equal(t, 11, deadline.Hour())
}

func TestScheduleDeadline_NoDeadline(t *testing.T) {
	now := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	sched := types.ScheduleConfig{Name: "morning"}
	pipeline := types.PipelineConfig{Name: "test"}

	_, ok := ScheduleDeadline(sched, pipeline, now)
	assert.False(t, ok)
}

func TestIsExcluded_Weekend(t *testing.T) {
	now := time.Date(2025, 6, 14, 10, 0, 0, 0, time.UTC) // Saturday

	calReg := calendar.NewRegistry()
	require.NoError(t, calReg.Register(&types.Calendar{
		Name: "us-business",
		Days: []string{"saturday", "sunday"},
	}))

	pipeline := types.PipelineConfig{
		Name:       "test",
		Exclusions: &types.ExclusionConfig{Calendar: "us-business"},
	}

	assert.True(t, IsExcluded(pipeline, calReg, now))
}

func TestIsExcluded_Holiday(t *testing.T) {
	now := time.Date(2025, 12, 25, 10, 0, 0, 0, time.UTC)

	calReg := calendar.NewRegistry()
	require.NoError(t, calReg.Register(&types.Calendar{
		Name:  "us-business",
		Dates: []string{"2025-12-25"},
	}))

	pipeline := types.PipelineConfig{
		Name:       "test",
		Exclusions: &types.ExclusionConfig{Calendar: "us-business"},
	}

	assert.True(t, IsExcluded(pipeline, calReg, now))
}

func TestIsExcluded_NotExcluded(t *testing.T) {
	now := time.Date(2025, 6, 16, 10, 0, 0, 0, time.UTC) // Monday

	calReg := calendar.NewRegistry()
	require.NoError(t, calReg.Register(&types.Calendar{
		Name: "us-business",
		Days: []string{"saturday", "sunday"},
	}))

	pipeline := types.PipelineConfig{
		Name:       "test",
		Exclusions: &types.ExclusionConfig{Calendar: "us-business"},
	}

	assert.False(t, IsExcluded(pipeline, calReg, now))
}

func TestIsExcluded_NilConfig(t *testing.T) {
	now := time.Date(2025, 6, 14, 10, 0, 0, 0, time.UTC)
	pipeline := types.PipelineConfig{Name: "test"}
	assert.False(t, IsExcluded(pipeline, nil, now))
}

func TestIsExcluded_CaseInsensitive(t *testing.T) {
	now := time.Date(2025, 6, 14, 10, 0, 0, 0, time.UTC) // Saturday
	pipeline := types.PipelineConfig{
		Name:       "test",
		Exclusions: &types.ExclusionConfig{Days: []string{"Saturday", "SUNDAY"}},
	}
	assert.True(t, IsExcluded(pipeline, nil, now))
}

func TestIsExcluded_CalendarMergedWithInline(t *testing.T) {
	now := time.Date(2025, 11, 28, 10, 0, 0, 0, time.UTC) // Friday

	calReg := calendar.NewRegistry()
	require.NoError(t, calReg.Register(&types.Calendar{
		Name: "us-business",
		Days: []string{"saturday", "sunday"},
	}))

	pipeline := types.PipelineConfig{
		Name: "test",
		Exclusions: &types.ExclusionConfig{
			Calendar: "us-business",
			Dates:    []string{"2025-11-28"},
		},
	}

	assert.True(t, IsExcluded(pipeline, calReg, now))

	sat := time.Date(2025, 11, 29, 10, 0, 0, 0, time.UTC)
	assert.True(t, IsExcluded(pipeline, calReg, sat))
}
