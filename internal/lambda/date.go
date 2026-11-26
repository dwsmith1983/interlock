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
			if IsValidExecutionDate(normalized + "T" + hourStr) {
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

// IsValidExecutionDate reports whether date is an execution date as the
// stream writes it: "YYYY-MM-DD", or "YYYY-MM-DDThh" with hour 00-23.
func IsValidExecutionDate(date string) bool {
	datePart, hourPart := ParseExecutionDate(date)
	if _, err := time.Parse("2006-01-02", datePart); err != nil || len(datePart) != 10 {
		return false
	}
	if !strings.Contains(date, "T") {
		return true
	}
	if len(hourPart) != 2 || hourPart[0] < '0' || hourPart[0] > '9' || hourPart[1] < '0' || hourPart[1] > '9' {
		return false
	}
	h, _ := strconv.Atoi(hourPart)
	return h <= 23
}

// ResolveTimezone loads the time.Location for the given timezone name.
// Returns time.UTC if tz is empty or cannot be loaded. Configs from the
// config cache are validated, so a zone that does not load reaches the
// fallback only through an unvalidated read such as Store.GetConfig.
func ResolveTimezone(tz string) *time.Location {
	if tz == "" {
		return time.UTC
	}
	if loc, err := time.LoadLocation(tz); err == nil {
		return loc
	}
	return time.UTC
}

// The inclusion dates the watchdog checks: those from the last
// inclusionLookbackDays days of now's date (as long as the watchdog's RUN#
// lookback), and of those at most the maxInclusionLookback most recent. The
// day bound keeps an old date from alerting again once its marker is gone;
// the count caps DynamoDB reads.
const (
	inclusionLookbackDays = 8
	maxInclusionLookback  = 3
)

// PastInclusionDates returns dates from the list that fall between
// inclusionLookbackDays (8) days before now's date in loc and that date
// itself, both included, sorted most recent first and capped at
// maxInclusionLookback (3) entries. Older dates are dropped, so a missed date
// is reported only while it is in the window. Dates must be YYYY-MM-DD
// strings; unparseable entries are silently skipped. Returns nil if no dates
// qualify.
func PastInclusionDates(dates []string, now time.Time, loc *time.Location) []string {
	localNow := now.In(loc)
	nowDate := localNow.Format("2006-01-02")
	oldestDate := localNow.AddDate(0, 0, -inclusionLookbackDays).Format("2006-01-02")
	var past []string
	for _, d := range dates {
		if _, err := time.Parse("2006-01-02", d); err != nil {
			continue
		}
		if oldestDate <= d && d <= nowDate {
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
