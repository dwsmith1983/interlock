package validation

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	v2 "github.com/dwsmith1983/interlock/pkg/types/v2"
)

func TestEvaluateRule_Equals(t *testing.T) {
	rule := v2.ValidationRule{
		Key:   "SENSOR#orders-complete",
		Check: v2.CheckEquals,
		Field: "status",
		Value: "COMPLETE",
	}
	sensor := map[string]interface{}{"status": "COMPLETE"}
	result := EvaluateRule(rule, sensor, time.Now())

	assert.True(t, result.Passed)
	assert.Equal(t, "SENSOR#orders-complete", result.Key)
	assert.Empty(t, result.Reason)
}

func TestEvaluateRule_Equals_Fail(t *testing.T) {
	rule := v2.ValidationRule{
		Key:   "SENSOR#orders-complete",
		Check: v2.CheckEquals,
		Field: "status",
		Value: "COMPLETE",
	}
	sensor := map[string]interface{}{"status": "PENDING"}
	result := EvaluateRule(rule, sensor, time.Now())

	assert.False(t, result.Passed)
	assert.Contains(t, result.Reason, "PENDING")
}

func TestEvaluateRule_GTE(t *testing.T) {
	rule := v2.ValidationRule{
		Key:   "SENSOR#row-count",
		Check: v2.CheckGTE,
		Field: "count",
		Value: float64(100),
	}
	sensor := map[string]interface{}{"count": float64(150)}
	result := EvaluateRule(rule, sensor, time.Now())

	assert.True(t, result.Passed)
}

func TestEvaluateRule_GTE_Fail(t *testing.T) {
	rule := v2.ValidationRule{
		Key:   "SENSOR#row-count",
		Check: v2.CheckGTE,
		Field: "count",
		Value: float64(100),
	}
	sensor := map[string]interface{}{"count": float64(50)}
	result := EvaluateRule(rule, sensor, time.Now())

	assert.False(t, result.Passed)
	assert.Contains(t, result.Reason, "50")
}

func TestEvaluateRule_LTE(t *testing.T) {
	rule := v2.ValidationRule{
		Key:   "SENSOR#error-rate",
		Check: v2.CheckLTE,
		Field: "rate",
		Value: float64(0.05),
	}
	sensor := map[string]interface{}{"rate": float64(0.03)}
	result := EvaluateRule(rule, sensor, time.Now())

	assert.True(t, result.Passed)
}

func TestEvaluateRule_GT(t *testing.T) {
	rule := v2.ValidationRule{
		Key:   "SENSOR#row-count",
		Check: v2.CheckGT,
		Field: "count",
		Value: float64(100),
	}

	t.Run("pass", func(t *testing.T) {
		sensor := map[string]interface{}{"count": float64(101)}
		result := EvaluateRule(rule, sensor, time.Now())
		assert.True(t, result.Passed)
	})

	t.Run("fail_equal", func(t *testing.T) {
		sensor := map[string]interface{}{"count": float64(100)}
		result := EvaluateRule(rule, sensor, time.Now())
		assert.False(t, result.Passed)
	})
}

func TestEvaluateRule_LT(t *testing.T) {
	rule := v2.ValidationRule{
		Key:   "SENSOR#latency",
		Check: v2.CheckLT,
		Field: "ms",
		Value: float64(500),
	}

	t.Run("pass", func(t *testing.T) {
		sensor := map[string]interface{}{"ms": float64(200)}
		result := EvaluateRule(rule, sensor, time.Now())
		assert.True(t, result.Passed)
	})

	t.Run("fail_equal", func(t *testing.T) {
		sensor := map[string]interface{}{"ms": float64(500)}
		result := EvaluateRule(rule, sensor, time.Now())
		assert.False(t, result.Passed)
	})
}

func TestEvaluateRule_Exists(t *testing.T) {
	rule := v2.ValidationRule{
		Key:   "SENSOR#upstream-done",
		Check: v2.CheckExists,
	}
	sensor := map[string]interface{}{"ts": "2026-03-01T00:00:00Z"}
	result := EvaluateRule(rule, sensor, time.Now())

	assert.True(t, result.Passed)
}

func TestEvaluateRule_Exists_Nil(t *testing.T) {
	rule := v2.ValidationRule{
		Key:   "SENSOR#upstream-done",
		Check: v2.CheckExists,
	}
	result := EvaluateRule(rule, nil, time.Now())

	assert.False(t, result.Passed)
	assert.Equal(t, "sensor key does not exist", result.Reason)
}

func TestEvaluateRule_AgeLT(t *testing.T) {
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	rule := v2.ValidationRule{
		Key:   "SENSOR#freshness",
		Check: v2.CheckAgeLT,
		Field: "updatedAt",
		Value: "1h",
	}
	sensor := map[string]interface{}{
		"updatedAt": now.Add(-30 * time.Minute).Format(time.RFC3339),
	}
	result := EvaluateRule(rule, sensor, now)

	assert.True(t, result.Passed)
}

func TestEvaluateRule_AgeLT_Stale(t *testing.T) {
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	rule := v2.ValidationRule{
		Key:   "SENSOR#freshness",
		Check: v2.CheckAgeLT,
		Field: "updatedAt",
		Value: "1h",
	}
	sensor := map[string]interface{}{
		"updatedAt": now.Add(-2 * time.Hour).Format(time.RFC3339),
	}
	result := EvaluateRule(rule, sensor, now)

	assert.False(t, result.Passed)
	assert.Contains(t, result.Reason, "exceeds")
}

func TestEvaluateRule_MissingField(t *testing.T) {
	rule := v2.ValidationRule{
		Key:   "SENSOR#data",
		Check: v2.CheckEquals,
		Field: "status",
		Value: "DONE",
	}
	sensor := map[string]interface{}{"other": "value"}
	result := EvaluateRule(rule, sensor, time.Now())

	assert.False(t, result.Passed)
	assert.Contains(t, result.Reason, "not found in sensor data")
}

func TestEvaluateRule_MissingSensor(t *testing.T) {
	rule := v2.ValidationRule{
		Key:   "SENSOR#data",
		Check: v2.CheckEquals,
		Field: "status",
		Value: "DONE",
	}
	result := EvaluateRule(rule, nil, time.Now())

	assert.False(t, result.Passed)
	assert.Equal(t, "sensor key does not exist", result.Reason)
}

func TestEvaluateRule_UnknownOp(t *testing.T) {
	rule := v2.ValidationRule{
		Key:   "SENSOR#data",
		Check: v2.CheckOp("regex"),
		Field: "status",
		Value: ".*",
	}
	sensor := map[string]interface{}{"status": "DONE"}
	result := EvaluateRule(rule, sensor, time.Now())

	assert.False(t, result.Passed)
	assert.Contains(t, result.Reason, "unknown operator")
}

func TestEvaluateRules_ALL_Pass(t *testing.T) {
	rules := []v2.ValidationRule{
		{Key: "SENSOR#a", Check: v2.CheckEquals, Field: "status", Value: "OK"},
		{Key: "SENSOR#b", Check: v2.CheckGTE, Field: "count", Value: float64(10)},
	}
	sensors := map[string]map[string]interface{}{
		"SENSOR#a": {"status": "OK"},
		"SENSOR#b": {"count": float64(20)},
	}
	result := EvaluateRules("ALL", rules, sensors, time.Now())
	assert.True(t, result.Passed)
	assert.Len(t, result.Results, 2)
	assert.True(t, result.Results[0].Passed)
	assert.True(t, result.Results[1].Passed)
}

func TestEvaluateRules_ALL_OneFails(t *testing.T) {
	rules := []v2.ValidationRule{
		{Key: "SENSOR#a", Check: v2.CheckEquals, Field: "status", Value: "OK"},
		{Key: "SENSOR#b", Check: v2.CheckGTE, Field: "count", Value: float64(100)},
	}
	sensors := map[string]map[string]interface{}{
		"SENSOR#a": {"status": "OK"},
		"SENSOR#b": {"count": float64(20)},
	}
	result := EvaluateRules("ALL", rules, sensors, time.Now())
	assert.False(t, result.Passed)
	assert.True(t, result.Results[0].Passed)
	assert.False(t, result.Results[1].Passed)
}

func TestEvaluateRules_ANY_OnePasses(t *testing.T) {
	rules := []v2.ValidationRule{
		{Key: "SENSOR#a", Check: v2.CheckEquals, Field: "status", Value: "OK"},
		{Key: "SENSOR#b", Check: v2.CheckGTE, Field: "count", Value: float64(100)},
	}
	sensors := map[string]map[string]interface{}{
		"SENSOR#a": {"status": "OK"},
		"SENSOR#b": {"count": float64(20)},
	}
	result := EvaluateRules("ANY", rules, sensors, time.Now())
	assert.True(t, result.Passed)
}

func TestEvaluateRules_ANY_AllFail(t *testing.T) {
	rules := []v2.ValidationRule{
		{Key: "SENSOR#a", Check: v2.CheckEquals, Field: "status", Value: "OK"},
		{Key: "SENSOR#b", Check: v2.CheckGTE, Field: "count", Value: float64(100)},
	}
	sensors := map[string]map[string]interface{}{
		"SENSOR#a": {"status": "BAD"},
		"SENSOR#b": {"count": float64(20)},
	}
	result := EvaluateRules("ANY", rules, sensors, time.Now())
	assert.False(t, result.Passed)
}

func TestEvaluateRules_MissingSensor(t *testing.T) {
	rules := []v2.ValidationRule{
		{Key: "SENSOR#missing", Check: v2.CheckEquals, Field: "status", Value: "OK"},
	}
	sensors := map[string]map[string]interface{}{}
	result := EvaluateRules("ALL", rules, sensors, time.Now())
	assert.False(t, result.Passed)
	assert.Contains(t, result.Results[0].Reason, "sensor key")
}

func TestEvaluateRules_EmptyRules(t *testing.T) {
	sensors := map[string]map[string]interface{}{}
	result := EvaluateRules("ALL", nil, sensors, time.Now())
	assert.True(t, result.Passed) // no rules = vacuously true
	assert.Empty(t, result.Results)
}
