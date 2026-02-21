package watcher

import (
	"fmt"
	"strconv"
	"time"

	"github.com/interlock-systems/interlock/pkg/types"
)

// isScheduleActive returns true if the schedule window is currently eligible.
// A schedule with no After constraint is always active.
func isScheduleActive(sched types.ScheduleConfig, now time.Time) bool {
	if sched.After == "" {
		return true
	}
	loc := time.UTC
	if sched.Timezone != "" {
		if l, err := time.LoadLocation(sched.Timezone); err == nil {
			loc = l
		}
	}
	after, err := parseTimeOfDay(sched.After, now, loc)
	if err != nil {
		return true // fail-open: if we can't parse, allow evaluation
	}
	return !now.Before(after)
}

// parseTimeOfDay parses an "HH:MM" string into a time.Time on the same day as ref,
// in the given location.
func parseTimeOfDay(hhmm string, ref time.Time, loc *time.Location) (time.Time, error) {
	if len(hhmm) < 4 || len(hhmm) > 5 {
		return time.Time{}, fmt.Errorf("invalid time format %q: expected HH:MM", hhmm)
	}
	// Find the colon
	colonIdx := -1
	for i, c := range hhmm {
		if c == ':' {
			colonIdx = i
			break
		}
	}
	if colonIdx < 0 {
		return time.Time{}, fmt.Errorf("invalid time format %q: missing colon", hhmm)
	}

	hour, err := strconv.Atoi(hhmm[:colonIdx])
	if err != nil || hour < 0 || hour > 23 {
		return time.Time{}, fmt.Errorf("invalid hour in %q", hhmm)
	}
	minute, err := strconv.Atoi(hhmm[colonIdx+1:])
	if err != nil || minute < 0 || minute > 59 {
		return time.Time{}, fmt.Errorf("invalid minute in %q", hhmm)
	}

	refInLoc := ref.In(loc)
	return time.Date(refInLoc.Year(), refInLoc.Month(), refInLoc.Day(), hour, minute, 0, 0, loc), nil
}

// scheduleDeadline resolves the SLA deadline for a schedule window.
// It prefers the schedule-level Deadline, falling back to the pipeline SLA.
func scheduleDeadline(sched types.ScheduleConfig, pipeline types.PipelineConfig, now time.Time) (time.Time, bool) {
	if sched.Deadline != "" {
		loc := time.UTC
		if sched.Timezone != "" {
			if l, err := time.LoadLocation(sched.Timezone); err == nil {
				loc = l
			}
		}
		if t, err := parseTimeOfDay(sched.Deadline, now, loc); err == nil {
			return t, true
		}
	}
	// Fall back to pipeline SLA completion deadline
	if pipeline.SLA != nil && pipeline.SLA.CompletionDeadline != "" {
		if t, err := ParseSLADeadline(pipeline.SLA.CompletionDeadline, pipeline.SLA.Timezone, now); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}
