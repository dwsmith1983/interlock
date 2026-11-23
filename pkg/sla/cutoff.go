package sla

import (
	"strconv"
	"strings"
	"time"
)

// hourlyKeyLayout is the time layout of an hourly run key.
const hourlyKeyLayout = "2006-01-02T15"

// CutoffAt returns the instant a trigger cutoff falls on for runKey, read in
// loc, which must be non-nil. runKey is "YYYY-MM-DD" or "YYYY-MM-DDThh" with hour
// 00-23. A daily "HH:MM" cutoff is that wall time on the key's date plus
// dayOffset days, for a daily and an hourly key alike. An hourly ":MM" cutoff
// falls MM minutes after the key's period ends: minute MM of the next hour
// for an hourly key, 00:MM of the next day for a daily key. It is the zero
// time when the cutoff or run key is empty or does not parse.
func CutoffAt(runKey, cutoff string, dayOffset int, loc *time.Location) time.Time {
	key, hourly, ok := parseRunKey(runKey)
	if !ok {
		return time.Time{}
	}
	y, m, d := key.Date()
	hh, mm, ok := strings.Cut(cutoff, ":")
	minute, err := strconv.Atoi(mm)
	if !ok || err != nil {
		return time.Time{}
	}
	if hh == "" {
		if hourly {
			return firstLocalInstant(y, m, d, key.Hour()+1, minute, loc)
		}
		return firstLocalInstant(y, m, d+1, 0, minute, loc)
	}
	h, err := strconv.Atoi(hh)
	if err != nil {
		return time.Time{}
	}
	return firstLocalInstant(y, m, d+dayOffset, h, minute, loc)
}

// parseRunKey reads a run key: "YYYY-MM-DD", or "YYYY-MM-DDThh" with hour
// 00-23, as midnight or that hour in UTC.
func parseRunKey(runKey string) (key time.Time, hourly, ok bool) {
	layout := "2006-01-02"
	if len(runKey) == len(hourlyKeyLayout) {
		layout = hourlyKeyLayout
	}
	key, err := time.Parse(layout, runKey)
	return key, layout == hourlyKeyLayout, err == nil
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
