package lambda_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dwsmith1983/interlock/internal/lambda"
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
// BUG-2 characterization: RemapPerPeriodSensors map mutation during range
// ---------------------------------------------------------------------------

func TestRemapPerPeriodSensors_MultipleSuffixes_MapMutation(t *testing.T) {
	// BUG-2 characterization: adding keys during range iteration.
	// With Go's map iteration, newly inserted keys may or may not be visited.
	// This test documents the nondeterministic behavior.
	sensors := map[string]map[string]interface{}{
		"hourly-status#2026-03-13": {"count": float64(10)},
		"daily-check#2026-03-13":   {"count": float64(20)},
		"weekly-scan#20260313":     {"count": float64(30)},
	}
	lambda.RemapPerPeriodSensors(sensors, "2026-03-13")
	// All base keys should be present
	assert.NotNil(t, sensors["hourly-status"], "hourly-status base key should exist")
	assert.NotNil(t, sensors["daily-check"], "daily-check base key should exist")
	assert.NotNil(t, sensors["weekly-scan"], "weekly-scan base key should exist")
}

func TestRemapPerPeriodSensors_StagedMerge_NoCrossContamination(t *testing.T) {
	// Verify staged merge doesn't allow newly-added base keys to match
	// as suffixed keys in the same iteration.
	sensors := map[string]map[string]interface{}{
		"hourly-status#2026-03-13": {"count": float64(10)},
	}
	lambda.RemapPerPeriodSensors(sensors, "2026-03-13")
	assert.NotNil(t, sensors["hourly-status"])
	assert.Equal(t, float64(10), sensors["hourly-status"]["count"])
	// Original suffixed key should still exist
	assert.NotNil(t, sensors["hourly-status#2026-03-13"])
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

	t.Run("no_match_other_hour", func(t *testing.T) {
		sensors := map[string]map[string]interface{}{
			"hourly-status#20260303T08": {"complete": true},
		}
		lambda.RemapPerPeriodSensors(sensors, "2026-03-03T07")
		_, exists := sensors["hourly-status"]
		assert.False(t, exists, "another hour's sensor must not be remapped")
		assert.Len(t, sensors, 1)
	})

	t.Run("per_period_overwrites_base", func(t *testing.T) {
		sensors := map[string]map[string]interface{}{
			"hourly-status":             {"source": "base"},
			"hourly-status#20260303T07": {"source": "per-period"},
		}
		lambda.RemapPerPeriodSensors(sensors, "2026-03-03T07")
		assert.Equal(t, "per-period", sensors["hourly-status"]["source"])
	})

	t.Run("compact_input_date", func(t *testing.T) {
		sensors := map[string]map[string]interface{}{
			"hourly-status#20260303T07": {"complete": true},
		}
		lambda.RemapPerPeriodSensors(sensors, "20260303T07")
		assert.Contains(t, sensors, "hourly-status")
	})
}
