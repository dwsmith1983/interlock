// Package validation implements the declarative validation rule engine for Interlock types.
package validation

import (
	"fmt"
	"strconv"
	"time"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// RuleResult is the outcome of evaluating a single validation rule.
type RuleResult struct {
	Key    string `json:"key"`
	Passed bool   `json:"passed"`
	Reason string `json:"reason,omitempty"`
}

// RuleSetResult is the outcome of evaluating all validation rules.
type RuleSetResult struct {
	Passed  bool         `json:"passed"`
	Results []RuleResult `json:"results"`
}

// EvaluateRules evaluates all rules against the sensor data map.
// mode is "ALL" (every rule must pass) or "ANY" (at least one must pass).
// sensors maps SENSOR# SK to the sensor data for that key.
func EvaluateRules(mode string, rules []types.ValidationRule, sensors map[string]map[string]interface{}, now time.Time) RuleSetResult {
	results := make([]RuleResult, len(rules))
	passCount := 0

	for i, rule := range rules {
		sensorData := sensors[rule.Key] // nil if missing
		results[i] = EvaluateRule(rule, sensorData, now)
		if results[i].Passed {
			passCount++
		}
	}

	var passed bool
	switch mode {
	case "ANY":
		passed = passCount > 0
	default: // "ALL"
		passed = passCount == len(rules)
	}

	return RuleSetResult{Passed: passed, Results: results}
}

// EvaluateRule checks a single validation rule against sensor data.
// sensorData is nil if the sensor key doesn't exist in the control table.
func EvaluateRule(rule types.ValidationRule, sensorData map[string]interface{}, now time.Time) RuleResult {
	// exists: pass if sensorData != nil.
	if rule.Check == types.CheckExists {
		if sensorData != nil {
			return RuleResult{Key: rule.Key, Passed: true}
		}
		return RuleResult{Key: rule.Key, Passed: false, Reason: "sensor key does not exist"}
	}

	// All other operators require sensor data to be present.
	if sensorData == nil {
		return RuleResult{Key: rule.Key, Passed: false, Reason: "sensor key does not exist"}
	}

	fieldVal, ok := sensorData[rule.Field]
	if !ok {
		return RuleResult{Key: rule.Key, Passed: false, Reason: fmt.Sprintf("field %q not found in sensor data", rule.Field)}
	}

	switch rule.Check {
	case types.CheckEquals:
		actual := fmt.Sprintf("%v", fieldVal)
		expected := fmt.Sprintf("%v", rule.Value)
		if actual == expected {
			return RuleResult{Key: rule.Key, Passed: true}
		}
		return RuleResult{Key: rule.Key, Passed: false, Reason: fmt.Sprintf("expected %v, got %v", expected, actual)}

	case types.CheckGTE, types.CheckLTE, types.CheckGT, types.CheckLT:
		return evaluateNumeric(rule, fieldVal)

	case types.CheckAgeLT:
		return evaluateAgeLT(rule, fieldVal, now)

	default:
		return RuleResult{Key: rule.Key, Passed: false, Reason: fmt.Sprintf("unknown operator %q", rule.Check)}
	}
}

// evaluateNumeric handles gte, lte, gt, lt operators.
func evaluateNumeric(rule types.ValidationRule, fieldVal interface{}) RuleResult {
	actual, ok := toFloat64(fieldVal)
	if !ok {
		return RuleResult{Key: rule.Key, Passed: false, Reason: fmt.Sprintf("field %q value %v is not numeric", rule.Field, fieldVal)}
	}

	threshold, ok := toFloat64(rule.Value)
	if !ok {
		return RuleResult{Key: rule.Key, Passed: false, Reason: fmt.Sprintf("rule value %v is not numeric", rule.Value)}
	}

	var passed bool
	switch rule.Check {
	case types.CheckGTE:
		passed = actual >= threshold
	case types.CheckLTE:
		passed = actual <= threshold
	case types.CheckGT:
		passed = actual > threshold
	case types.CheckLT:
		passed = actual < threshold
	}

	if passed {
		return RuleResult{Key: rule.Key, Passed: true}
	}
	return RuleResult{Key: rule.Key, Passed: false, Reason: fmt.Sprintf("%v %s %v is false", actual, rule.Check, threshold)}
}

// evaluateAgeLT checks that now - timestamp < duration.
func evaluateAgeLT(rule types.ValidationRule, fieldVal interface{}, now time.Time) RuleResult {
	tsStr, ok := fieldVal.(string)
	if !ok {
		return RuleResult{Key: rule.Key, Passed: false, Reason: fmt.Sprintf("field %q value is not a string timestamp", rule.Field)}
	}

	ts, err := time.Parse(time.RFC3339, tsStr)
	if err != nil {
		return RuleResult{Key: rule.Key, Passed: false, Reason: fmt.Sprintf("field %q value %q is not a valid RFC3339 timestamp", rule.Field, tsStr)}
	}

	durStr, ok := rule.Value.(string)
	if !ok {
		return RuleResult{Key: rule.Key, Passed: false, Reason: fmt.Sprintf("rule value %v is not a duration string", rule.Value)}
	}

	dur, err := time.ParseDuration(durStr)
	if err != nil {
		return RuleResult{Key: rule.Key, Passed: false, Reason: fmt.Sprintf("rule value %q is not a valid Go duration", durStr)}
	}

	age := now.Sub(ts)
	if age < dur {
		return RuleResult{Key: rule.Key, Passed: true}
	}
	return RuleResult{Key: rule.Key, Passed: false, Reason: fmt.Sprintf("age %s exceeds threshold %s", age, dur)}
}

// toFloat64 attempts to convert a value to float64.
func toFloat64(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case int32:
		return float64(n), true
	case string:
		f, err := strconv.ParseFloat(n, 64)
		return f, err == nil
	default:
		return 0, false
	}
}
