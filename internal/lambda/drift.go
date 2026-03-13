package lambda

import (
	"math"
	"strconv"
)

// ExtractFloatOk retrieves a numeric value from a sensor data map.
// Returns (value, true) if the key exists and is numeric, (0, false) otherwise.
// Unlike ExtractFloat, this distinguishes zero values from missing keys.
func ExtractFloatOk(data map[string]interface{}, key string) (float64, bool) {
	if data == nil {
		return 0, false
	}
	v, ok := data[key]
	if !ok {
		return 0, false
	}
	switch n := v.(type) {
	case float64:
		return n, true
	case string:
		f, err := strconv.ParseFloat(n, 64)
		if err != nil {
			return 0, false
		}
		return f, true
	default:
		return 0, false
	}
}

// DriftResult holds the outcome of a drift comparison.
type DriftResult struct {
	Drifted   bool
	Previous  float64
	Current   float64
	Delta     float64
	PrevFound bool
	CurrFound bool
}

// DetectDrift compares baseline and current sensor data for a drift field.
// Both values must be present for drift to be detected. Returns whether
// the absolute delta exceeds the threshold.
func DetectDrift(baseline, current map[string]interface{}, driftField string, threshold float64) DriftResult {
	prev, prevOk := ExtractFloatOk(baseline, driftField)
	curr, currOk := ExtractFloatOk(current, driftField)

	result := DriftResult{
		Previous:  prev,
		Current:   curr,
		PrevFound: prevOk,
		CurrFound: currOk,
	}

	if prevOk && currOk {
		result.Delta = curr - prev
		result.Drifted = math.Abs(result.Delta) > threshold
	}

	return result
}
