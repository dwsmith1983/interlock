package lambda

import (
	"sort"
	"strconv"
	"strings"
	"time"
)

// ResolveExecutionDate builds the execution date from sensor data fields.
// If both "date" and "hour" are present, returns "YYYY-MM-DDThh".
// If only "date", returns "YYYY-MM-DD". Falls back to today's date.
func ResolveExecutionDate(sensorData map[string]interface{}, now time.Time) string {
	dateStr, _ := sensorData["date"].(string)
	hourStr, _ := sensorData["hour"].(string)

	if dateStr == "" {
		return now.Format("2006-01-02")
	}

	normalized := normalizeDate(dateStr)
	// Validate YYYY-MM-DD format.
	if _, err := time.Parse("2006-01-02", normalized); err != nil {
		return now.Format("2006-01-02")
	}

	if hourStr != "" {
		// Validate hour is 2-digit 00-23.
		if len(hourStr) == 2 {
			if h, err := strconv.Atoi(hourStr); err == nil && h >= 0 && h <= 23 {
				return normalized + "T" + hourStr
			}
		}
		return normalized
	}
	return normalized
}

// normalizeDate converts YYYYMMDD to YYYY-MM-DD. Already-dashed dates pass through.
func normalizeDate(s string) string {
	if len(s) == 8 && !strings.Contains(s, "-") {
		return s[:4] + "-" + s[4:6] + "-" + s[6:8]
	}
	return s
}

// ParseExecutionDate splits a composite date into date and hour parts.
// "2026-03-03T10" -> ("2026-03-03", "10")
// "2026-03-03"    -> ("2026-03-03", "")
func ParseExecutionDate(date string) (datePart, hourPart string) {
	if idx := strings.Index(date, "T"); idx >= 0 {
		return date[:idx], date[idx+1:]
	}
	return date, ""
}

// ResolveTimezone loads the time.Location for the given timezone name.
// Returns time.UTC if tz is empty or cannot be loaded.
func ResolveTimezone(tz string) *time.Location {
	if tz == "" {
		return time.UTC
	}
	if loc, err := time.LoadLocation(tz); err == nil {
		return loc
	}
	return time.UTC
}

// MostRecentInclusionDate returns the most recent date from dates that is on
// or before now (comparing date only, ignoring time of day). Dates must be
// YYYY-MM-DD strings; unparseable entries are silently skipped. Returns
// ("", false) if no dates qualify.
func MostRecentInclusionDate(dates []string, now time.Time) (string, bool) {
	nowDate := now.Format("2006-01-02")
	best := ""
	found := false
	for _, d := range dates {
		if _, err := time.Parse("2006-01-02", d); err != nil {
			continue
		}
		if d <= nowDate && d > best {
			best = d
			found = true
		}
	}
	return best, found
}

// maxInclusionLookback is the maximum number of past inclusion dates to check.
// Caps DynamoDB reads when the watchdog has been down for an extended period.
const maxInclusionLookback = 3

// PastInclusionDates returns dates from the list that are on or before now,
// sorted most recent first and capped at maxInclusionLookback (3) entries.
// The cap bounds DynamoDB reads when the watchdog has been down for an
// extended period. Dates must be YYYY-MM-DD strings; unparseable entries
// are silently skipped. Returns nil if no dates qualify.
func PastInclusionDates(dates []string, now time.Time) []string {
	nowDate := now.Format("2006-01-02")
	var past []string
	for _, d := range dates {
		if _, err := time.Parse("2006-01-02", d); err != nil {
			continue
		}
		if d <= nowDate {
			past = append(past, d)
		}
	}
	// Sort descending (most recent first) using string comparison on YYYY-MM-DD.
	sort.Sort(sort.Reverse(sort.StringSlice(past)))
	// Cap to maxInclusionLookback to bound downstream DynamoDB reads.
	if len(past) > maxInclusionLookback {
		past = past[:maxInclusionLookback]
	}
	return past
}
