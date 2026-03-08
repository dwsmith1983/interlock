package lambda_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// ---------------------------------------------------------------------------
// ParseExecutionDate — table-driven
// ---------------------------------------------------------------------------

func TestParseExecutionDate(t *testing.T) {
	tests := []struct {
		name     string
		date     string
		wantDate string
		wantHour string
	}{
		{"hourly", "2026-03-03T10", "2026-03-03", "10"},
		{"daily", "2026-03-03", "2026-03-03", ""},
		{"empty", "", "", ""},
		{"empty_hour", "2026-03-03T", "2026-03-03", ""},
		{"no_date", "T10", "", "10"},
		{"compact_hourly", "20260303T07", "20260303", "07"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			date, hour := lambda.ParseExecutionDate(tt.date)
			assert.Equal(t, tt.wantDate, date)
			assert.Equal(t, tt.wantHour, hour)
		})
	}
}

// ---------------------------------------------------------------------------
// InjectDateArgs — table-driven
// ---------------------------------------------------------------------------

func TestInjectDateArgs(t *testing.T) {
	t.Run("glue_daily", func(t *testing.T) {
		tc := &types.TriggerConfig{Glue: &types.GlueTriggerConfig{}}
		lambda.InjectDateArgs(tc, "2026-03-03")
		assert.Equal(t, "20260303", tc.Glue.Arguments["--par_day"])
		assert.Empty(t, tc.Glue.Arguments["--par_hour"])
	})

	t.Run("glue_hourly", func(t *testing.T) {
		tc := &types.TriggerConfig{Glue: &types.GlueTriggerConfig{}}
		lambda.InjectDateArgs(tc, "2026-03-03T10")
		assert.Equal(t, "20260303", tc.Glue.Arguments["--par_day"])
		assert.Equal(t, "10", tc.Glue.Arguments["--par_hour"])
	})

	t.Run("glue_preserves_existing_args", func(t *testing.T) {
		tc := &types.TriggerConfig{Glue: &types.GlueTriggerConfig{
			Arguments: map[string]string{"--extra": "val"},
		}}
		lambda.InjectDateArgs(tc, "2026-03-03")
		assert.Equal(t, "20260303", tc.Glue.Arguments["--par_day"])
		assert.Equal(t, "val", tc.Glue.Arguments["--extra"])
	})

	t.Run("http_empty_body_daily", func(t *testing.T) {
		tc := &types.TriggerConfig{HTTP: &types.HTTPTriggerConfig{URL: "http://example.com"}}
		lambda.InjectDateArgs(tc, "2026-03-03")
		assert.Contains(t, tc.HTTP.Body, `"par_day":"20260303"`)
		assert.NotContains(t, tc.HTTP.Body, "par_hour")
	})

	t.Run("http_empty_body_hourly", func(t *testing.T) {
		tc := &types.TriggerConfig{HTTP: &types.HTTPTriggerConfig{URL: "http://example.com"}}
		lambda.InjectDateArgs(tc, "2026-03-03T10")
		assert.Contains(t, tc.HTTP.Body, `"par_day":"20260303"`)
		assert.Contains(t, tc.HTTP.Body, `"par_hour":"10"`)
	})

	t.Run("http_existing_body_not_overwritten", func(t *testing.T) {
		tc := &types.TriggerConfig{HTTP: &types.HTTPTriggerConfig{
			URL:  "http://example.com",
			Body: `{"custom":"data"}`,
		}}
		lambda.InjectDateArgs(tc, "2026-03-03")
		assert.Equal(t, `{"custom":"data"}`, tc.HTTP.Body, "existing body should not be overwritten")
	})

	t.Run("nil_glue_no_panic", func(t *testing.T) {
		tc := &types.TriggerConfig{}
		assert.NotPanics(t, func() { lambda.InjectDateArgs(tc, "2026-03-03") })
	})
}

// ---------------------------------------------------------------------------
// RemapPerPeriodSensors — table-driven
// ---------------------------------------------------------------------------

func TestRemapPerPeriodSensors(t *testing.T) {
	t.Run("normalized_date_suffix", func(t *testing.T) {
		sensors := map[string]map[string]interface{}{
			"hourly-status#2026-03-03T07": {"count": 100.0},
		}
		lambda.RemapPerPeriodSensors(sensors, "2026-03-03T07")
		assert.NotNil(t, sensors["hourly-status"], "base key should be added")
		assert.Equal(t, 100.0, sensors["hourly-status"]["count"])
	})

	t.Run("compact_date_suffix", func(t *testing.T) {
		sensors := map[string]map[string]interface{}{
			"hourly-status#20260303T07": {"count": 200.0},
		}
		lambda.RemapPerPeriodSensors(sensors, "2026-03-03T07")
		assert.NotNil(t, sensors["hourly-status"], "compact suffix should match")
		assert.Equal(t, 200.0, sensors["hourly-status"]["count"])
	})

	t.Run("no_matching_suffix", func(t *testing.T) {
		sensors := map[string]map[string]interface{}{
			"upstream-complete": {"complete": true},
		}
		lambda.RemapPerPeriodSensors(sensors, "2026-03-03")
		_, exists := sensors["upstream-complete"]
		assert.True(t, exists, "existing key should remain")
		// No base key alias should be created since there's no suffix match.
		assert.Len(t, sensors, 1)
	})

	t.Run("empty_date_noop", func(t *testing.T) {
		sensors := map[string]map[string]interface{}{
			"hourly-status#2026-03-03T07": {"count": 100.0},
		}
		lambda.RemapPerPeriodSensors(sensors, "")
		assert.Len(t, sensors, 1, "no remapping should occur with empty date")
	})

	t.Run("multiple_sensors", func(t *testing.T) {
		sensors := map[string]map[string]interface{}{
			"hourly-status#2026-03-03T07": {"count": 100.0},
			"daily-check#2026-03-03":      {"passed": true},
			"other-sensor":                {"val": 42.0},
		}
		lambda.RemapPerPeriodSensors(sensors, "2026-03-03T07")
		assert.NotNil(t, sensors["hourly-status"])
		// daily-check#2026-03-03 doesn't match #2026-03-03T07
		_, hasDailyBase := sensors["daily-check"]
		assert.False(t, hasDailyBase, "daily-check should not be remapped with hourly date")
	})

	t.Run("daily_date_remaps", func(t *testing.T) {
		sensors := map[string]map[string]interface{}{
			"daily-check#2026-03-03": {"passed": true},
		}
		lambda.RemapPerPeriodSensors(sensors, "2026-03-03")
		assert.NotNil(t, sensors["daily-check"])
		assert.Equal(t, true, sensors["daily-check"]["passed"])
	})
}
