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

func TestSensorFreshnessHandler_Pass_Fresh(t *testing.T) {
	prov := testutil.NewMockProvider()
	ctx := context.Background()

	require.NoError(t, prov.PutSensorData(ctx, types.SensorData{
		PipelineID: "pipe-1",
		SensorType: "lag",
		Value:      map[string]interface{}{"lag": 0},
		UpdatedAt:  time.Now().Add(-1 * time.Minute),
	}))

	handler := NewSensorFreshnessHandler(prov)
	out, err := handler(ctx, types.EvaluatorInput{
		PipelineID: "pipe-1",
		TraitType:  "sensor-freshness",
		Config: map[string]interface{}{
			"sensorType": "lag",
			"maxAge":     "10m",
		},
	})

	require.NoError(t, err)
	assert.Equal(t, types.TraitPass, out.Status)
	assert.Contains(t, out.Reason, "fresh")
}

func TestSensorFreshnessHandler_Fail_Stale(t *testing.T) {
	prov := testutil.NewMockProvider()
	ctx := context.Background()

	require.NoError(t, prov.PutSensorData(ctx, types.SensorData{
		PipelineID: "pipe-1",
		SensorType: "lag",
		Value:      map[string]interface{}{"lag": 0},
		UpdatedAt:  time.Now().Add(-20 * time.Minute),
	}))

	handler := NewSensorFreshnessHandler(prov)
	out, err := handler(ctx, types.EvaluatorInput{
		PipelineID: "pipe-1",
		TraitType:  "sensor-freshness",
		Config: map[string]interface{}{
			"sensorType": "lag",
			"maxAge":     "10m",
		},
	})

	require.NoError(t, err)
	assert.Equal(t, types.TraitFail, out.Status)
	assert.Contains(t, out.Reason, "stale")
}

func TestSensorFreshnessHandler_Fail_MissingSensor(t *testing.T) {
	prov := testutil.NewMockProvider()

	handler := NewSensorFreshnessHandler(prov)
	out, err := handler(context.Background(), types.EvaluatorInput{
		PipelineID: "pipe-1",
		TraitType:  "sensor-freshness",
		Config: map[string]interface{}{
			"sensorType": "nonexistent",
			"maxAge":     "10m",
		},
	})

	require.NoError(t, err)
	assert.Equal(t, types.TraitFail, out.Status)
	assert.Contains(t, out.Reason, "no sensor data")
}

func TestSensorFreshnessHandler_Fail_BadMaxAge(t *testing.T) {
	prov := testutil.NewMockProvider()

	handler := NewSensorFreshnessHandler(prov)
	out, err := handler(context.Background(), types.EvaluatorInput{
		PipelineID: "pipe-1",
		TraitType:  "sensor-freshness",
		Config: map[string]interface{}{
			"sensorType": "lag",
			"maxAge":     "invalid",
		},
	})

	require.NoError(t, err)
	assert.Equal(t, types.TraitFail, out.Status)
	assert.Equal(t, types.FailurePermanent, out.FailureCategory)
	assert.Contains(t, out.Reason, "invalid maxAge")
}
