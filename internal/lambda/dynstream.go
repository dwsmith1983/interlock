package lambda

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebTypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// extractKeys returns the PK and SK string values from a DynamoDB stream record.
func extractKeys(record events.DynamoDBEventRecord) (pk, sk string) {
	keys := record.Change.Keys
	if pkAttr, ok := keys["PK"]; ok && pkAttr.DataType() == events.DataTypeString {
		pk = pkAttr.String()
	}
	if skAttr, ok := keys["SK"]; ok && skAttr.DataType() == events.DataTypeString {
		sk = skAttr.String()
	}
	return pk, sk
}

// extractSensorData converts a DynamoDB stream NewImage to a plain map
// suitable for validation rule evaluation. If the item uses the canonical
// ControlRecord format (sensor fields nested inside a "data" map attribute),
// the "data" map is unwrapped so fields are accessible at the top level.
func extractSensorData(newImage map[string]events.DynamoDBAttributeValue) map[string]interface{} {
	if newImage == nil {
		return nil
	}

	skipKeys := map[string]bool{"PK": true, "SK": true, "ttl": true}
	result := make(map[string]interface{}, len(newImage))

	for k, av := range newImage {
		if skipKeys[k] {
			continue
		}
		result[k] = convertAttributeValue(av)
	}

	// Unwrap the "data" map if present (canonical ControlRecord sensor format).
	if dataMap, ok := result["data"].(map[string]interface{}); ok {
		return dataMap
	}
	return result
}

// convertAttributeValue converts a DynamoDB stream attribute value to a Go native type.
func convertAttributeValue(av events.DynamoDBAttributeValue) interface{} {
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
			out[k] = convertAttributeValue(v)
		}
		return out
	case events.DataTypeList:
		l := av.List()
		out := make([]interface{}, len(l))
		for i, v := range l {
			out[i] = convertAttributeValue(v)
		}
		return out
	default:
		return nil
	}
}

// ResolveExecutionDate builds the execution date from sensor data fields.
// If both "date" and "hour" are present, returns "YYYY-MM-DDThh".
// If only "date", returns "YYYY-MM-DD". Falls back to today's date.
func ResolveExecutionDate(sensorData map[string]interface{}) string {
	dateStr, _ := sensorData["date"].(string)
	hourStr, _ := sensorData["hour"].(string)

	if dateStr == "" {
		return time.Now().Format("2006-01-02")
	}

	normalized := normalizeDate(dateStr)
	// Validate YYYY-MM-DD format.
	if _, err := time.Parse("2006-01-02", normalized); err != nil {
		return time.Now().Format("2006-01-02")
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

// resolveScheduleID returns "cron" if the pipeline uses a cron schedule,
// otherwise returns "stream".
func resolveScheduleID(cfg *types.PipelineConfig) string {
	if cfg.Schedule.Cron != "" {
		return "cron"
	}
	return "stream"
}

// publishEvent sends an event to EventBridge. It is safe to call when
// EventBridge is nil or EventBusName is empty (returns nil with no action).
func publishEvent(ctx context.Context, d *Deps, eventType, pipelineID, schedule, date, message string, detail ...map[string]interface{}) error {
	if d.EventBridge == nil || d.EventBusName == "" {
		return nil
	}

	evt := types.InterlockEvent{
		PipelineID: pipelineID,
		ScheduleID: schedule,
		Date:       date,
		Message:    message,
		Timestamp:  time.Now(),
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

	_, err = d.EventBridge.PutEvents(ctx, &eventbridge.PutEventsInput{
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
	return nil
}

// isExcludedDate checks calendar exclusions against a job's execution date
// (not wall-clock time). dateStr supports "YYYY-MM-DD" and "YYYY-MM-DDTHH".
func isExcludedDate(cfg *types.PipelineConfig, dateStr string) bool {
	excl := cfg.Schedule.Exclude
	if excl == nil {
		return false
	}
	if len(dateStr) < 10 {
		return false // unparseable, safe default
	}
	datePortion := dateStr[:10]

	// Resolve the location to interpret the execution date in.
	loc := time.UTC
	if cfg.Schedule.Timezone != "" {
		if l, err := time.LoadLocation(cfg.Schedule.Timezone); err == nil {
			loc = l
		}
	}

	// Parse the date as midnight in the configured timezone so that weekday
	// and date-string comparisons reflect the local calendar date.
	t, err := time.ParseInLocation("2006-01-02", datePortion, loc)
	if err != nil {
		return false // safe default
	}

	if excl.Weekends {
		day := t.Weekday()
		if day == time.Saturday || day == time.Sunday {
			return true
		}
	}
	dateStr2 := t.Format("2006-01-02")
	for _, d := range excl.Dates {
		if d == dateStr2 {
			return true
		}
	}
	return false
}

// isExcluded checks whether the pipeline should be excluded from running
// based on calendar exclusions (weekends and specific dates).
func isExcluded(cfg *types.PipelineConfig, now time.Time) bool {
	excl := cfg.Schedule.Exclude
	if excl == nil {
		return false
	}

	// Resolve timezone if configured.
	t := now
	if cfg.Schedule.Timezone != "" {
		if loc, err := time.LoadLocation(cfg.Schedule.Timezone); err == nil {
			t = now.In(loc)
		}
	}

	// Check weekends.
	if excl.Weekends {
		day := t.Weekday()
		if day == time.Saturday || day == time.Sunday {
			return true
		}
	}

	// Check specific dates.
	dateStr := t.Format("2006-01-02")
	for _, d := range excl.Dates {
		if d == dateStr {
			return true
		}
	}

	return false
}
