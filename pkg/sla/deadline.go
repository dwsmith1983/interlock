// Package sla provides pure time calculation functions for SLA deadline computation.
package sla

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// CalculateAbsoluteDeadline computes breach and warning times from a deadline string.
//
// Deadline formats:
//   - "HH:MM" — absolute time of day (daily pipelines)
//   - ":MM"   — minutes past the processing hour (hourly pipelines)
//
// Date formats:
//   - "2006-01-02"    — daily
//   - "2006-01-02T15" — hourly (hour encoded in date)
//
// The warning time is breachAt minus expectedDuration.
func CalculateAbsoluteDeadline(date, deadline, expectedDuration, timezone string, now time.Time) (breach, warning time.Time, err error) {
	dur, err := time.ParseDuration(expectedDuration)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("parse expectedDuration %q: %w", expectedDuration, err)
	}

	loc := time.UTC
	if timezone != "" {
		loc, err = time.LoadLocation(timezone)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("load timezone %q: %w", timezone, err)
		}
	}

	now = now.In(loc)

	// Parse the execution date.
	baseDate := now
	baseHour := -1
	if date != "" {
		datePart, hourPart := parseExecutionDate(date)
		parsed, parseErr := time.Parse("2006-01-02", datePart)
		if parseErr == nil {
			if hourPart != "" {
				h := 0
				if hVal, atoiErr := strconv.Atoi(hourPart); atoiErr == nil {
					h = hVal
					baseHour = h
				}
				baseDate = time.Date(parsed.Year(), parsed.Month(), parsed.Day(),
					h, 0, 0, 0, loc)
			} else {
				baseDate = time.Date(parsed.Year(), parsed.Month(), parsed.Day(),
					now.Hour(), now.Minute(), 0, 0, loc)
			}
		}
	}

	// Parse deadline.
	if strings.HasPrefix(deadline, ":") {
		dl, parseErr := time.Parse("04", strings.TrimPrefix(deadline, ":"))
		if parseErr != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("parse deadline %q: %w", deadline, parseErr)
		}
		hour := baseDate.Hour()
		breach = time.Date(baseDate.Year(), baseDate.Month(), baseDate.Day(),
			hour, dl.Minute(), 0, 0, loc)
		if baseHour >= 0 {
			breach = breach.Add(time.Hour)
		} else if breach.Before(now) {
			breach = breach.Add(time.Hour)
		}
	} else {
		dl, parseErr := time.Parse("15:04", deadline)
		if parseErr != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("parse deadline %q: %w", deadline, parseErr)
		}
		breach = time.Date(baseDate.Year(), baseDate.Month(), baseDate.Day(),
			dl.Hour(), dl.Minute(), 0, 0, loc)
		if breach.Before(now) {
			breach = breach.Add(24 * time.Hour)
		}
	}

	warning = breach.Add(-dur)
	return breach, warning, nil
}

// CalculateRelativeDeadline computes breach and warning times from sensor arrival
// plus maxDuration. Warning offset uses expectedDuration if provided, otherwise
// defaults to 25% of maxDuration.
func CalculateRelativeDeadline(arrivalAt, maxDuration, expectedDuration string) (breach, warning time.Time, err error) {
	maxDur, err := time.ParseDuration(maxDuration)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("parse maxDuration %q: %w", maxDuration, err)
	}

	arrival, err := time.Parse(time.RFC3339, arrivalAt)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("parse arrivalAt %q: %w", arrivalAt, err)
	}

	breach = arrival.Add(maxDur)

	var warningOffset time.Duration
	if expectedDuration != "" {
		warningOffset, err = time.ParseDuration(expectedDuration)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("parse expectedDuration %q: %w", expectedDuration, err)
		}
	} else {
		warningOffset = maxDur / 4
	}
	warning = breach.Add(-warningOffset)

	return breach, warning, nil
}

// parseExecutionDate splits a composite date into date and hour parts.
// "2026-03-03T10" -> ("2026-03-03", "10")
// "2026-03-03"    -> ("2026-03-03", "")
func parseExecutionDate(date string) (datePart, hourPart string) {
	if idx := strings.Index(date, "T"); idx >= 0 {
		return date[:idx], date[idx+1:]
	}
	return date, ""
}
