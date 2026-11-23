package lambda

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/dwsmith1983/interlock/pkg/validation"
)

// ResolveScheduleID returns "cron" if the pipeline uses a cron schedule,
// otherwise returns "stream".
func ResolveScheduleID(cfg *types.PipelineConfig) string {
	if cfg.Schedule.Cron != "" {
		return "cron"
	}
	return "stream"
}

// ResolveSLADate returns the calendar date an absolute "HH:MM" SLA deadline
// falls on: the execution date plus sla.deadlineDayOffset days. Hourly ":MM"
// deadlines, composite hourly dates ("2006-01-02T15") and unparseable dates
// are returned unchanged.
func ResolveSLADate(cfg *types.PipelineConfig, date string) string {
	if cfg == nil || cfg.SLA == nil || cfg.SLA.DeadlineDayOffset == 0 {
		return date
	}
	if strings.HasPrefix(cfg.SLA.Deadline, ":") {
		return date
	}
	t, err := time.Parse("2006-01-02", date)
	if err != nil {
		return date
	}
	return t.AddDate(0, 0, cfg.SLA.DeadlineDayOffset).Format("2006-01-02")
}

// ResolveCutoffTime returns the instant schedule.trigger.cutoff falls on for
// runKey, in schedule.timezone (else sla.timezone, else UTC). A daily "HH:MM"
// cutoff on a daily key is that wall time on the key's date plus
// cutoffDayOffset days; an hourly ":MM" cutoff on an hourly key is minute MM
// of the following hour. It is the zero time for a key of the other cadence,
// when the key or cutoff does not parse, or without a cutoff (validation
// requires one, so only a config read without validation lacks it).
func ResolveCutoffTime(cfg *types.PipelineConfig, runKey string) time.Time {
	if cfg == nil || cfg.Schedule.Trigger == nil || cfg.Schedule.Trigger.Cutoff == "" || !IsValidExecutionDate(runKey) {
		return time.Time{}
	}
	tz := cfg.Schedule.Timezone
	if tz == "" && cfg.SLA != nil {
		tz = cfg.SLA.Timezone
	}
	loc := ResolveTimezone(tz)
	datePart, hourPart := ParseExecutionDate(runKey)
	day, _ := time.Parse("2006-01-02", datePart)
	y, m, d := day.Date()

	hh, mm, ok := strings.Cut(cfg.Schedule.Trigger.Cutoff, ":")
	minute, err := strconv.Atoi(mm)
	if !ok || err != nil {
		return time.Time{}
	}
	if hh == "" {
		if hourPart == "" {
			return time.Time{}
		}
		h, _ := strconv.Atoi(hourPart)
		return firstLocalInstant(y, m, d, h+1, minute, loc)
	}
	hour, err := strconv.Atoi(hh)
	if err != nil || hourPart != "" {
		return time.Time{}
	}
	return firstLocalInstant(y, m, d+cfg.Schedule.Trigger.CutoffDayOffset, hour, minute, loc)
}

// firstLocalInstant returns the first instant the wall clock in loc reads
// y-m-d h:min or later (d and h may overflow, as with time.Date): the gap's
// end for a time inside a spring-forward gap, the first occurrence of a
// repeated fall-back time. It does not rely on time.Date's choice in either.
func firstLocalInstant(y int, m time.Month, d, h, minute int, loc *time.Location) time.Time {
	t := time.Date(y, m, d, h, minute, 0, 0, loc)
	want := time.Date(y, m, d, h, minute, 0, 0, time.UTC)
	got := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), 0, 0, time.UTC)
	start, end := t.ZoneBounds()
	switch {
	case got.Before(want): // landed before the gap
		return end
	case got.After(want): // landed after the gap
		return start
	case start.IsZero():
		return t
	}
	// The same wall time in the previous zone period, if it falls there, is earlier.
	_, off := t.Zone()
	_, prevOff := start.Add(-time.Second).Zone()
	if earlier := t.Add(time.Duration(off-prevOff) * time.Second); earlier.Before(start) {
		return earlier
	}
	return t
}

// CutoffPassed reports whether runKey's cutoff is set and now is at or after it.
func CutoffPassed(cfg *types.PipelineConfig, runKey string, now time.Time) bool {
	cutoff := ResolveCutoffTime(cfg, runKey)
	return !cutoff.IsZero() && !now.Before(cutoff)
}

// StartByTime returns the first fire of the cron expression expr inside the
// period of runKey, read in loc, and true; or false when expr does not fire
// in that period. A daily
// key "YYYY-MM-DD" covers local midnight to the next local midnight (23 or 25
// hours on a DST change); an hourly key "YYYY-MM-DDThh" covers that local
// hour. Consecutive keys' periods tile the timeline with no gap or overlap,
// and a nonexistent hour (02 on a spring-forward day in New York) has an empty
// period. On a fall-back day the repeated hour may belong to the key of its
// first or its second occurrence, depending on the zone (time.Date resolves
// an ambiguous time differently east and west of UTC). loc must be non-nil
// (use ResolveTimezone). An invalid cron or run key is an error.
func StartByTime(expr, runKey string, loc *time.Location) (time.Time, bool, error) {
	sched, err := validation.ParseCron(expr)
	if err != nil {
		return time.Time{}, false, err
	}
	if !IsValidExecutionDate(runKey) {
		return time.Time{}, false, fmt.Errorf("run key %q is not YYYY-MM-DD or YYYY-MM-DDThh", runKey)
	}
	datePart, hourPart := ParseExecutionDate(runKey)
	day, _ := time.Parse("2006-01-02", datePart)
	y, m, d := day.Date()
	start := localHourStart(y, m, d, 0, loc)
	end := localHourStart(y, m, d+1, 0, loc)
	if hourPart != "" {
		h, _ := strconv.Atoi(hourPart)
		start = localHourStart(y, m, d, h, loc)
		end = localHourStart(y, m, d, h+1, loc)
	}
	// Next is strictly after its argument, at whole seconds, and reads the
	// schedule in the argument's location; it returns the zero time when
	// nothing fires within five years.
	next := sched.Next(start.Add(-time.Second))
	if next.IsZero() || !next.Before(end) {
		return time.Time{}, false, nil
	}
	return next, true, nil
}

// localHourStart returns the instant StartByTime uses as the boundary at
// y-m-d h:00 in loc (d and h may overflow, as with time.Date). For a wall
// time that exists it is time.Date's answer, including its choice on an
// ambiguous fall-back time. For one inside a DST gap, time.Date returns an
// instant before the gap; the gap's end, where that zone period ends, is used
// instead, so the boundary never moves earlier than the wall time asked for.
func localHourStart(y int, m time.Month, d, h int, loc *time.Location) time.Time {
	t := time.Date(y, m, d, h, 0, 0, 0, loc)
	want := time.Date(y, m, d, h, 0, 0, 0, time.UTC)
	got := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), 0, 0, time.UTC)
	if got.Before(want) {
		_, t = t.ZoneBounds()
	}
	return t
}

// ResolveTriggerLockTTL returns how long after a run's latest attempt its
// trigger row's stale deadline (staleAt) falls: the SFN_TIMEOUT_SECONDS env
// var plus a 30-minute buffer. Defaults to 4h30m if the env var is not set or
// invalid.
func ResolveTriggerLockTTL() time.Duration {
	s := os.Getenv("SFN_TIMEOUT_SECONDS")
	if s == "" {
		return DefaultTriggerLockTTL
	}
	sec, err := strconv.Atoi(s)
	if err != nil || sec <= 0 {
		return DefaultTriggerLockTTL
	}
	return time.Duration(sec)*time.Second + TriggerLockBuffer
}
