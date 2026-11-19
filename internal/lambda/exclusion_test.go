package lambda

import (
	"testing"
	"time"

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
