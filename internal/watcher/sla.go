package watcher

import (
	"time"

	"github.com/dwsmith1983/interlock/internal/schedule"
)

// ParseSLADeadline parses a deadline string into a time.Time for today.
// Supports "HH:MM" (time of day) or Go duration strings like "2h", "30m".
func ParseSLADeadline(deadline, timezone string, now time.Time, referenceTime ...time.Time) (time.Time, error) {
	return schedule.ParseSLADeadline(deadline, timezone, now, referenceTime...)
}

// IsBreached checks if the current time has passed the deadline.
func IsBreached(deadline, now time.Time) bool {
	return schedule.IsBreached(deadline, now)
}
