package evaluator

import (
	"encoding/json"
	"fmt"
)

// extractNumericValue finds a numeric value in a sensor's Value map.
// It tries the key matching sensorType first, then "value" as a fallback.
func extractNumericValue(values map[string]interface{}, sensorType string) (float64, bool) {
	for _, key := range []string{sensorType, "value"} {
		if v, ok := values[key]; ok {
			if f, ok := toFloat64(v); ok {
				return f, true
			}
		}
	}
	return 0, false
}

// toFloat64 coerces an interface{} to float64. Handles float64, int, json.Number.
func toFloat64(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	default:
		return 0, false
	}
}

// toStringSlice converts an interface{} to []string. Handles []interface{} from JSON.
func toStringSlice(v interface{}) ([]string, bool) {
	switch s := v.(type) {
	case []string:
		return s, true
	case []interface{}:
		result := make([]string, 0, len(s))
		for _, item := range s {
			str, ok := item.(string)
			if !ok {
				return nil, false
			}
			result = append(result, str)
		}
		return result, true
	default:
		return nil, false
	}
}

// configString extracts a string from a config map, returning an error output if missing.
func configString(config map[string]interface{}, key string) (string, error) {
	v, ok := config[key]
	if !ok {
		return "", fmt.Errorf("config missing: %s", key)
	}
	s, ok := v.(string)
	if !ok || s == "" {
		return "", fmt.Errorf("config invalid: %s must be a non-empty string", key)
	}
	return s, nil
}
