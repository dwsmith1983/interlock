package lambda

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/dwsmith1983/interlock/pkg/types"
)

func TestIsExcluded(t *testing.T) {
	friday := time.Date(2026, 3, 6, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name     string
		schedule types.ScheduleConfig
		now      time.Time
		want     bool
	}{
		{"WeekendSaturday", types.ScheduleConfig{Exclude: &types.ExclusionConfig{Weekends: true}}, time.Date(2026, 3, 7, 12, 0, 0, 0, time.UTC), true},
		{"WeekendOnFriday", types.ScheduleConfig{Exclude: &types.ExclusionConfig{Weekends: true}}, friday, false},
		{"NoExclusions", types.ScheduleConfig{}, friday, false},
		{"ExcludedDate", types.ScheduleConfig{Exclude: &types.ExclusionConfig{Dates: []string{"2026-03-06"}}}, friday, true},
		// Friday 20:00 UTC is Saturday 05:00 in Tokyo.
		{"TimezoneTokyoSaturday", types.ScheduleConfig{Timezone: "Asia/Tokyo", Exclude: &types.ExclusionConfig{Weekends: true}},
			time.Date(2026, 3, 6, 20, 0, 0, 0, time.UTC), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsExcluded(&types.PipelineConfig{Schedule: tt.schedule}, tt.now); got != tt.want {
				t.Errorf("IsExcluded = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsExcludedDate_NoExclusions(t *testing.T) {
	cfg := &types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "p"},
		Schedule: types.ScheduleConfig{Exclude: nil},
	}
	assert.False(t, IsExcludedDate(cfg, "2026-03-07"), "nil exclusion must not exclude")
	assert.False(t, IsExcludedDate(cfg, "2026-03-08"), "nil exclusion must not exclude")
}

func TestIsExcludedDate_WeekendSaturday(t *testing.T) {
	cfg := &types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "p"},
		Schedule: types.ScheduleConfig{
			Exclude: &types.ExclusionConfig{Weekends: true},
		},
	}
	// 2026-03-07 is a Saturday.
	assert.True(t, IsExcludedDate(cfg, "2026-03-07"), "Saturday must be excluded when Weekends=true")
}

func TestIsExcludedDate_WeekendSunday(t *testing.T) {
	cfg := &types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "p"},
		Schedule: types.ScheduleConfig{
			Exclude: &types.ExclusionConfig{Weekends: true},
		},
	}
	// 2026-03-08 is a Sunday.
	assert.True(t, IsExcludedDate(cfg, "2026-03-08"), "Sunday must be excluded when Weekends=true")
}

func TestIsExcludedDate_WeekdayFriday(t *testing.T) {
	cfg := &types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "p"},
		Schedule: types.ScheduleConfig{
			Exclude: &types.ExclusionConfig{Weekends: true},
		},
	}
	// 2026-03-06 is a Friday.
	assert.False(t, IsExcludedDate(cfg, "2026-03-06"), "Friday must NOT be excluded when Weekends=true")
}

func TestIsExcludedDate_SpecificDate(t *testing.T) {
	cfg := &types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "p"},
		Schedule: types.ScheduleConfig{
			Exclude: &types.ExclusionConfig{Dates: []string{"2026-12-25", "2026-01-01"}},
		},
	}
	assert.True(t, IsExcludedDate(cfg, "2026-12-25"), "Christmas must be excluded")
	assert.True(t, IsExcludedDate(cfg, "2026-01-01"), "New Year's Day must be excluded")
	assert.False(t, IsExcludedDate(cfg, "2026-03-06"), "regular day must not be excluded")
}

func TestIsExcludedDate_CompositePerHourDate(t *testing.T) {
	cfg := &types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "p"},
		Schedule: types.ScheduleConfig{
			Exclude: &types.ExclusionConfig{Dates: []string{"2026-03-07"}},
		},
	}
	// Per-hour composite date "2026-03-07T10": the date portion is 2026-03-07.
	assert.True(t, IsExcludedDate(cfg, "2026-03-07T10"), "composite date must match date portion")
	assert.False(t, IsExcludedDate(cfg, "2026-03-06T10"), "non-excluded composite date must not be excluded")
}

func TestIsExcludedDate_InvalidDate(t *testing.T) {
	cfg := &types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "p"},
		Schedule: types.ScheduleConfig{
			Exclude: &types.ExclusionConfig{Weekends: true, Dates: []string{"2026-03-07"}},
		},
	}
	// Unparseable date: the safe default is false (do not exclude).
	assert.False(t, IsExcludedDate(cfg, "not-a-date"), "invalid date must return false (safe default)")
}

func TestIsExcludedDate_EmptyDate(t *testing.T) {
	cfg := &types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "p"},
		Schedule: types.ScheduleConfig{
			Exclude: &types.ExclusionConfig{Weekends: true},
		},
	}
	// Empty string is too short to parse, so the safe default is false.
	assert.False(t, IsExcludedDate(cfg, ""), "empty date must return false (safe default)")
}

func TestIsExcludedDate_ShortDate(t *testing.T) {
	cfg := &types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "p"},
		Schedule: types.ScheduleConfig{
			Exclude: &types.ExclusionConfig{Weekends: true},
		},
	}
	assert.False(t, IsExcludedDate(cfg, "2026-03"), "short date string must return false (safe default)")
}

func TestIsExcludedDate_Timezone(t *testing.T) {
	cfg := &types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "p"},
		Schedule: types.ScheduleConfig{
			Timezone: "America/Los_Angeles",
			Exclude:  &types.ExclusionConfig{Weekends: true},
		},
	}
	// 2026-03-07 is Saturday regardless of timezone.
	assert.True(t, IsExcludedDate(cfg, "2026-03-07"), "Saturday is excluded in any timezone")
	assert.False(t, IsExcludedDate(cfg, "2026-03-06"), "Friday is not excluded")
}
