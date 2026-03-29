package lambda

import (
	"time"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// IsExcludedTime is the core calendar exclusion check. It evaluates
// whether the given time falls on a weekend or a specifically excluded date.
func IsExcludedTime(excl *types.ExclusionConfig, t time.Time) bool {
	if excl == nil {
		return false
	}
	if excl.Weekends {
		day := t.Weekday()
		if day == time.Saturday || day == time.Sunday {
			return true
		}
	}
	dateStr := t.Format("2006-01-02")
	for _, d := range excl.Dates {
		if d == dateStr {
			return true
		}
	}
	return false
}

// IsExcludedDate checks calendar exclusions against a job's execution date
// (not wall-clock time). dateStr supports "YYYY-MM-DD" and "YYYY-MM-DDTHH".
func IsExcludedDate(cfg *types.PipelineConfig, dateStr string) bool {
	excl := cfg.Schedule.Exclude
	if excl == nil || len(dateStr) < 10 {
		return false
	}
	loc := ResolveTimezone(cfg.Schedule.Timezone)
	t, err := time.ParseInLocation("2006-01-02", dateStr[:10], loc)
	if err != nil {
		return false
	}
	return IsExcludedTime(excl, t)
}

// IsExcluded checks whether the pipeline should be excluded from running
// based on calendar exclusions (weekends and specific dates).
// When no timezone is configured, now is used as-is (preserving its
// original location, which is UTC in AWS Lambda).
func IsExcluded(cfg *types.PipelineConfig, now time.Time) bool {
	excl := cfg.Schedule.Exclude
	if excl == nil {
		return false
	}
	t := now
	if cfg.Schedule.Timezone != "" {
		t = now.In(ResolveTimezone(cfg.Schedule.Timezone))
	}
	return IsExcludedTime(excl, t)
}
