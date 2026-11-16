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

// ResolveTriggerLockTTL returns the trigger lock TTL based on the
// SFN_TIMEOUT_SECONDS env var plus a 30-minute buffer. Defaults to
// 4h30m if the env var is not set or invalid.
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
