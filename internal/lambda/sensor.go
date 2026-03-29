package lambda

import (
	"strconv"
	"strings"

	"github.com/aws/aws-lambda-go/events"
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

// RemapPerPeriodSensors adds base-key aliases for per-period sensor keys.
// For example, sensor "hourly-status#20260303T07" becomes accessible under
// key "hourly-status" when the execution date is "2026-03-03T07". This allows
// validation rules with key "hourly-status" to match per-period sensor records.
// Handles both normalized (2026-03-03) and compact (20260303) date formats.
func RemapPerPeriodSensors(sensors map[string]map[string]interface{}, date string) {
	if date == "" {
		return
	}
	// Build candidate suffixes: the normalized date and compact form.
	suffixes := []string{"#" + date}
	compact := strings.ReplaceAll(date, "-", "")
	if compact != date {
		suffixes = append(suffixes, "#"+compact)
	}
	additions := make(map[string]map[string]interface{})
	for key, data := range sensors {
		for _, suffix := range suffixes {
			if strings.HasSuffix(key, suffix) {
				base := strings.TrimSuffix(key, suffix)
				additions[base] = data
				break
			}
		}
	}
	for k, v := range additions {
		sensors[k] = v
	}
}
