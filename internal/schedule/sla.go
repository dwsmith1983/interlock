package schedule

import (
	"fmt"
	"regexp"
	"strconv"
	"time"
)

var timeOfDayRegex = regexp.MustCompile(`^(\d{1,2}):(\d{2})$`)

// ParseSLADeadline parses a deadline string into a time.Time for today.
// Supports "HH:MM" (time of day) or Go duration strings like "2h", "30m".
//
// For duration format, referenceTime controls the base:
//   - If non-zero: duration is relative to referenceTime (event-driven SLA)
//   - If zero: duration is relative to midnight today (backward compatible)
//
// HH:MM format always resolves to wall-clock time, ignoring referenceTime.
func ParseSLADeadline(deadline, timezone string, now time.Time, referenceTime ...time.Time) (time.Time, error) {
	if deadline == "" {
		return time.Time{}, fmt.Errorf("empty deadline")
	}

	loc := now.Location()
	if timezone != "" {
		var err error
		loc, err = time.LoadLocation(timezone)
		if err != nil {
			return time.Time{}, fmt.Errorf("invalid timezone %q: %w", timezone, err)
		}
	}

	// Try HH:MM format
	if m := timeOfDayRegex.FindStringSubmatch(deadline); m != nil {
		hour, _ := strconv.Atoi(m[1])
		minute, _ := strconv.Atoi(m[2])
		if hour > 23 || minute > 59 {
			return time.Time{}, fmt.Errorf("invalid time %q", deadline)
		}
		nowInTZ := now.In(loc)
		t := time.Date(nowInTZ.Year(), nowInTZ.Month(), nowInTZ.Day(), hour, minute, 0, 0, loc)
		return t, nil
	}

	// Try duration format
	d, err := time.ParseDuration(deadline)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid deadline format %q: must be HH:MM or duration", deadline)
	}

	// Duration offset: relative to referenceTime (if provided and non-zero)
	// or midnight today (backward compatible).
	var ref time.Time
	if len(referenceTime) > 0 && !referenceTime[0].IsZero() {
		ref = referenceTime[0]
	} else {
		nowInTZ := now.In(loc)
		ref = time.Date(nowInTZ.Year(), nowInTZ.Month(), nowInTZ.Day(), 0, 0, 0, 0, loc)
	}
	return ref.Add(d), nil
}

// IsBreached checks if the current time has passed the deadline.
func IsBreached(deadline, now time.Time) bool {
	return now.After(deadline)
}
