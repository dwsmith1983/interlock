package lambda

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebTypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// ExtractKeys returns the PK and SK string values from a DynamoDB stream record.
func ExtractKeys(record events.DynamoDBEventRecord) (pk, sk string) {
	keys := record.Change.Keys
	if pkAttr, ok := keys["PK"]; ok && pkAttr.DataType() == events.DataTypeString {
		pk = pkAttr.String()
	}
	if skAttr, ok := keys["SK"]; ok && skAttr.DataType() == events.DataTypeString {
		sk = skAttr.String()
	}
	return pk, sk
}

// ExtractSensorData converts a DynamoDB stream NewImage to a plain map
// suitable for validation rule evaluation. If the item uses the canonical
// ControlRecord format (sensor fields nested inside a "data" map attribute),
// the "data" map is unwrapped so fields are accessible at the top level.
func ExtractSensorData(newImage map[string]events.DynamoDBAttributeValue) map[string]interface{} {
	if newImage == nil {
		return nil
	}

	skipKeys := map[string]bool{"PK": true, "SK": true, "ttl": true}
	result := make(map[string]interface{}, len(newImage))

	for k, av := range newImage {
		if skipKeys[k] {
			continue
		}
		result[k] = ConvertAttributeValue(av)
	}

	// Unwrap the "data" map if present (canonical ControlRecord sensor format).
	if dataMap, ok := result["data"].(map[string]interface{}); ok {
		return dataMap
	}
	return result
}

// ConvertAttributeValue converts a DynamoDB stream attribute value to a Go native type.
func ConvertAttributeValue(av events.DynamoDBAttributeValue) interface{} {
	switch av.DataType() {
	case events.DataTypeString:
		return av.String()
	case events.DataTypeNumber:
		// Try int first, fall back to float.
		if i, err := strconv.ParseInt(av.Number(), 10, 64); err == nil {
			return float64(i)
		}
		if f, err := strconv.ParseFloat(av.Number(), 64); err == nil {
			return f
		}
		return av.Number()
	case events.DataTypeBoolean:
		return av.Boolean()
	case events.DataTypeNull:
		return nil
	case events.DataTypeMap:
		m := av.Map()
		out := make(map[string]interface{}, len(m))
		for k, v := range m {
			out[k] = ConvertAttributeValue(v)
		}
		return out
	case events.DataTypeList:
		l := av.List()
		out := make([]interface{}, len(l))
		for i, v := range l {
			out[i] = ConvertAttributeValue(v)
		}
		return out
	default:
		return nil
	}
}

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

// ResolveScheduleID returns "cron" if the pipeline uses a cron schedule,
// otherwise returns "stream".
func ResolveScheduleID(cfg *types.PipelineConfig) string {
	if cfg.Schedule.Cron != "" {
		return "cron"
	}
	return "stream"
}

// PublishEvent sends an event to EventBridge. It is safe to call when
// EventBridge is nil or EventBusName is empty (returns nil with no action).
func PublishEvent(ctx context.Context, d *Deps, eventType, pipelineID, schedule, date, message string, detail ...map[string]interface{}) error {
	if d.EventBridge == nil || d.EventBusName == "" {
		return nil
	}

	evt := types.InterlockEvent{
		PipelineID: pipelineID,
		ScheduleID: schedule,
		Date:       date,
		Message:    message,
		Timestamp:  d.Now(),
	}
	if len(detail) > 0 && detail[0] != nil {
		evt.Detail = detail[0]
	}
	detailJSON, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal event detail: %w", err)
	}

	source := types.EventSource
	detailStr := string(detailJSON)

	out, err := d.EventBridge.PutEvents(ctx, &eventbridge.PutEventsInput{
		Entries: []ebTypes.PutEventsRequestEntry{
			{
				Source:       &source,
				DetailType:   &eventType,
				Detail:       &detailStr,
				EventBusName: &d.EventBusName,
			},
		},
	})
	if err != nil {
		return fmt.Errorf("publish %s event: %w", eventType, err)
	}
	if out.FailedEntryCount > 0 {
		code, msg := "", ""
		if len(out.Entries) > 0 && out.Entries[0].ErrorCode != nil {
			code = *out.Entries[0].ErrorCode
			if out.Entries[0].ErrorMessage != nil {
				msg = *out.Entries[0].ErrorMessage
			}
		}
		return fmt.Errorf("publish %s event: partial failure (code=%s, message=%s)", eventType, code, msg)
	}
	return nil
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

// IsExcludedTime is the core calendar exclusion check. It evaluates
// whether the given time falls on a weekend or a specifically excluded date.
func IsExcludedTime(excl *types.ExclusionConfig, t time.Time) bool {
	if excl == nil {
		return false
	}
	if excl.Weekends {
		day := t.Weekday()
		if day == time.Saturday || day == time.Sunday {
			return true
		}
	}
	dateStr := t.Format("2006-01-02")
	for _, d := range excl.Dates {
		if d == dateStr {
			return true
		}
	}
	return false
}

// IsExcludedDate checks calendar exclusions against a job's execution date
// (not wall-clock time). dateStr supports "YYYY-MM-DD" and "YYYY-MM-DDTHH".
func IsExcludedDate(cfg *types.PipelineConfig, dateStr string) bool {
	excl := cfg.Schedule.Exclude
	if excl == nil || len(dateStr) < 10 {
		return false
	}
	loc := ResolveTimezone(cfg.Schedule.Timezone)
	t, err := time.ParseInLocation("2006-01-02", dateStr[:10], loc)
	if err != nil {
		return false
	}
	return IsExcludedTime(excl, t)
}

// IsExcluded checks whether the pipeline should be excluded from running
// based on calendar exclusions (weekends and specific dates).
// When no timezone is configured, now is used as-is (preserving its
// original location, which is UTC in AWS Lambda).
func IsExcluded(cfg *types.PipelineConfig, now time.Time) bool {
	excl := cfg.Schedule.Exclude
	if excl == nil {
		return false
	}
	t := now
	if cfg.Schedule.Timezone != "" {
		t = now.In(ResolveTimezone(cfg.Schedule.Timezone))
	}
	return IsExcludedTime(excl, t)
}
