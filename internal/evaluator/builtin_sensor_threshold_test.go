package evaluator

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dwsmith1983/interlock/internal/testutil"
	"github.com/dwsmith1983/interlock/pkg/types"
)

func TestSensorThresholdHandler_Pass_WithinThresholds(t *testing.T) {
	prov := testutil.NewMockProvider()
	ctx := context.Background()

	require.NoError(t, prov.PutSensorData(ctx, types.SensorData{
		PipelineID: "pipe-1",
		SensorType: "temperature",
		Value:      map[string]interface{}{"temperature": 25.0},
	}))

	handler := NewSensorThresholdHandler(prov)
	out, err := handler(ctx, types.EvaluatorInput{
		PipelineID: "pipe-1",
		TraitType:  "sensor-threshold",
		Config: map[string]interface{}{
			"sensorType": "temperature",
			"min":        10.0,
			"max":        30.0,
		},
	})

	require.NoError(t, err)
	assert.Equal(t, types.TraitPass, out.Status)
	assert.Contains(t, out.Reason, "within thresholds")
}

func TestSensorThresholdHandler_Fail_BelowMin(t *testing.T) {
	prov := testutil.NewMockProvider()
	ctx := context.Background()

	require.NoError(t, prov.PutSensorData(ctx, types.SensorData{
		PipelineID: "pipe-1",
		SensorType: "temperature",
		Value:      map[string]interface{}{"temperature": 5.0},
	}))

	handler := NewSensorThresholdHandler(prov)
	out, err := handler(ctx, types.EvaluatorInput{
		PipelineID: "pipe-1",
		TraitType:  "sensor-threshold",
		Config: map[string]interface{}{
			"sensorType": "temperature",
			"min":        10.0,
			"max":        30.0,
		},
	})

	require.NoError(t, err)
	assert.Equal(t, types.TraitFail, out.Status)
	assert.Contains(t, out.Reason, "below min")
}

func TestSensorThresholdHandler_Fail_AboveMax(t *testing.T) {
	prov := testutil.NewMockProvider()
	ctx := context.Background()

	require.NoError(t, prov.PutSensorData(ctx, types.SensorData{
		PipelineID: "pipe-1",
		SensorType: "temperature",
		Value:      map[string]interface{}{"temperature": 35.0},
	}))

	handler := NewSensorThresholdHandler(prov)
	out, err := handler(ctx, types.EvaluatorInput{
		PipelineID: "pipe-1",
		TraitType:  "sensor-threshold",
		Config: map[string]interface{}{
			"sensorType": "temperature",
			"min":        10.0,
			"max":        30.0,
		},
	})

	require.NoError(t, err)
	assert.Equal(t, types.TraitFail, out.Status)
	assert.Contains(t, out.Reason, "above max")
}

func TestSensorThresholdHandler_Fail_MissingSensor(t *testing.T) {
	prov := testutil.NewMockProvider()

	handler := NewSensorThresholdHandler(prov)
	out, err := handler(context.Background(), types.EvaluatorInput{
		PipelineID: "pipe-1",
		TraitType:  "sensor-threshold",
		Config: map[string]interface{}{
			"sensorType": "nonexistent",
			"min":        10.0,
			"max":        30.0,
		},
	})

	require.NoError(t, err)
	assert.Equal(t, types.TraitFail, out.Status)
	assert.Contains(t, out.Reason, "no sensor data")
}

func TestSensorThresholdHandler_Fail_MissingConfig(t *testing.T) {
	prov := testutil.NewMockProvider()

	handler := NewSensorThresholdHandler(prov)
	out, err := handler(context.Background(), types.EvaluatorInput{
		PipelineID: "pipe-1",
		TraitType:  "sensor-threshold",
		Config:     map[string]interface{}{},
	})

	require.NoError(t, err)
	assert.Equal(t, types.TraitFail, out.Status)
	assert.Equal(t, types.FailurePermanent, out.FailureCategory)
	assert.Contains(t, out.Reason, "sensorType")
}
