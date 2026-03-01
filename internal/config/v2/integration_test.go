package config_test

import (
	"testing"
	"time"

	config "github.com/dwsmith1983/interlock/internal/config/v2"
	"github.com/dwsmith1983/interlock/internal/validation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_LoadAndEvaluate loads the sample v2 pipeline configs from
// testdata, builds mock sensor maps, and verifies end-to-end evaluation.
func TestIntegration_LoadAndEvaluate(t *testing.T) {
	pipelines, err := config.LoadPipelines("../../../testdata/v2/pipelines")
	require.NoError(t, err)
	require.Len(t, pipelines, 2)

	// os.ReadDir returns entries sorted by name: gold-revenue, silver-orders.
	gold := pipelines[0]
	silver := pipelines[1]

	assert.Equal(t, "gold-revenue", gold.Pipeline.ID)
	assert.Equal(t, "analytics-team", gold.Pipeline.Owner)
	assert.Equal(t, "silver-orders", silver.Pipeline.ID)
	assert.Equal(t, "data-platform", silver.Pipeline.Owner)

	now := time.Now()

	// --- gold-revenue: ALL rules pass ---
	t.Run("gold-revenue_all_pass", func(t *testing.T) {
		sensors := map[string]map[string]interface{}{
			"upstream-complete": {"status": "ready"},
			"row-count":        {"count": 5000},
			"freshness":        {"updatedAt": now.Add(-30 * time.Minute).Format(time.RFC3339)},
		}

		result := validation.EvaluateRules(
			gold.Validation.Trigger,
			gold.Validation.Rules,
			sensors,
			now,
		)
		assert.True(t, result.Passed, "expected all rules to pass")
		require.Len(t, result.Results, 3)
		for _, r := range result.Results {
			assert.True(t, r.Passed, "rule %s should pass: %s", r.Key, r.Reason)
		}
	})

	// --- gold-revenue: row-count too low (ALL mode fails) ---
	t.Run("gold-revenue_row_count_too_low", func(t *testing.T) {
		sensors := map[string]map[string]interface{}{
			"upstream-complete": {"status": "ready"},
			"row-count":        {"count": 500}, // below 1000 threshold
			"freshness":        {"updatedAt": now.Add(-30 * time.Minute).Format(time.RFC3339)},
		}

		result := validation.EvaluateRules(
			gold.Validation.Trigger,
			gold.Validation.Rules,
			sensors,
			now,
		)
		assert.False(t, result.Passed, "expected ALL-mode to fail when row-count is below threshold")

		// Verify which rule failed.
		for _, r := range result.Results {
			if r.Key == "row-count" {
				assert.False(t, r.Passed)
				assert.Contains(t, r.Reason, "gte")
			}
		}
	})

	// --- silver-orders: ANY mode, only one sensor present ---
	t.Run("silver-orders_any_one_present", func(t *testing.T) {
		sensors := map[string]map[string]interface{}{
			"orders-landed": {}, // exists but empty -- enough for "exists" check
		}

		result := validation.EvaluateRules(
			silver.Validation.Trigger,
			silver.Validation.Rules,
			sensors,
			now,
		)
		assert.True(t, result.Passed, "expected ANY-mode to pass when at least one sensor exists")

		// First rule (exists) should pass; second (orders-count missing) should fail.
		require.Len(t, result.Results, 2)
		assert.True(t, result.Results[0].Passed, "orders-landed exists check should pass")
		assert.False(t, result.Results[1].Passed, "orders-count should fail (sensor missing)")
	})
}
