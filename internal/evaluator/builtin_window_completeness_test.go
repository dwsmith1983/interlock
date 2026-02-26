package evaluator

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dwsmith1983/interlock/internal/testutil"
	"github.com/dwsmith1983/interlock/pkg/types"
)

func TestWindowCompletenessHandler_Pass_AllWindows(t *testing.T) {
	prov := testutil.NewMockProvider()
	ctx := context.Background()

	require.NoError(t, prov.PutSensorData(ctx, types.SensorData{
		PipelineID: "pipe-1",
		SensorType: "windows",
		Value: map[string]interface{}{
			"windows": []interface{}{"w1", "w2", "w3"},
		},
		UpdatedAt: time.Now(),
	}))

	handler := NewWindowCompletenessHandler(prov)
	out, err := handler(ctx, types.EvaluatorInput{
		PipelineID: "pipe-1",
		TraitType:  "window-completeness",
		Config: map[string]interface{}{
			"sensorType":      "windows",
			"expectedWindows": []interface{}{"w1", "w2", "w3"},
		},
	})

	require.NoError(t, err)
	assert.Equal(t, types.TraitPass, out.Status)
	assert.Contains(t, out.Reason, "all 3 expected windows present")
}

func TestWindowCompletenessHandler_Fail_MissingWindows(t *testing.T) {
	prov := testutil.NewMockProvider()
	ctx := context.Background()

	require.NoError(t, prov.PutSensorData(ctx, types.SensorData{
		PipelineID: "pipe-1",
		SensorType: "windows",
		Value: map[string]interface{}{
			"windows": []interface{}{"w1"},
		},
		UpdatedAt: time.Now(),
	}))

	handler := NewWindowCompletenessHandler(prov)
	out, err := handler(ctx, types.EvaluatorInput{
		PipelineID: "pipe-1",
		TraitType:  "window-completeness",
		Config: map[string]interface{}{
			"sensorType":      "windows",
			"expectedWindows": []interface{}{"w1", "w2", "w3"},
		},
	})

	require.NoError(t, err)
	assert.Equal(t, types.TraitFail, out.Status)
	assert.Contains(t, out.Reason, "missing 2 of 3")
	// Verify the missing windows are reported in the value map.
	missing, ok := out.Value["missing"].([]string)
	require.True(t, ok)
	assert.ElementsMatch(t, []string{"w2", "w3"}, missing)
}

func TestWindowCompletenessHandler_Pass_CountMet(t *testing.T) {
	prov := testutil.NewMockProvider()
	ctx := context.Background()

	require.NoError(t, prov.PutSensorData(ctx, types.SensorData{
		PipelineID: "pipe-1",
		SensorType: "windows",
		Value: map[string]interface{}{
			"windows": []interface{}{"w1", "w2", "w3"},
		},
		UpdatedAt: time.Now(),
	}))

	handler := NewWindowCompletenessHandler(prov)
	out, err := handler(ctx, types.EvaluatorInput{
		PipelineID: "pipe-1",
		TraitType:  "window-completeness",
		Config: map[string]interface{}{
			"sensorType":    "windows",
			"expectedCount": 3.0,
		},
	})

	require.NoError(t, err)
	assert.Equal(t, types.TraitPass, out.Status)
	assert.Contains(t, out.Reason, "meets expected")
}

func TestWindowCompletenessHandler_Fail_CountBelow(t *testing.T) {
	prov := testutil.NewMockProvider()
	ctx := context.Background()

	require.NoError(t, prov.PutSensorData(ctx, types.SensorData{
		PipelineID: "pipe-1",
		SensorType: "windows",
		Value: map[string]interface{}{
			"windows": []interface{}{"w1"},
		},
		UpdatedAt: time.Now(),
	}))

	handler := NewWindowCompletenessHandler(prov)
	out, err := handler(ctx, types.EvaluatorInput{
		PipelineID: "pipe-1",
		TraitType:  "window-completeness",
		Config: map[string]interface{}{
			"sensorType":    "windows",
			"expectedCount": 3.0,
		},
	})

	require.NoError(t, err)
	assert.Equal(t, types.TraitFail, out.Status)
	assert.Contains(t, out.Reason, "below expected")
}

func TestWindowCompletenessHandler_Fail_NoStrategy(t *testing.T) {
	prov := testutil.NewMockProvider()
	ctx := context.Background()

	require.NoError(t, prov.PutSensorData(ctx, types.SensorData{
		PipelineID: "pipe-1",
		SensorType: "windows",
		Value:      map[string]interface{}{},
		UpdatedAt:  time.Now(),
	}))

	handler := NewWindowCompletenessHandler(prov)
	out, err := handler(ctx, types.EvaluatorInput{
		PipelineID: "pipe-1",
		TraitType:  "window-completeness",
		Config: map[string]interface{}{
			"sensorType": "windows",
		},
	})

	require.NoError(t, err)
	assert.Equal(t, types.TraitFail, out.Status)
	assert.Equal(t, types.FailurePermanent, out.FailureCategory)
	assert.Contains(t, out.Reason, "expectedWindows or expectedCount required")
}
