package watcher

import (
	"time"

	"github.com/interlock-systems/interlock/internal/schedule"
)

// ParseSLADeadline parses a deadline string into a time.Time for today.
// Supports "HH:MM" (time of day) or Go duration strings like "2h", "30m".
func ParseSLADeadline(deadline string, timezone string, now time.Time) (time.Time, error) {
	return schedule.ParseSLADeadline(deadline, timezone, now)
}

// IsBreached checks if the current time has passed the deadline.
func IsBreached(deadline time.Time, now time.Time) bool {
	return schedule.IsBreached(deadline, now)
}
