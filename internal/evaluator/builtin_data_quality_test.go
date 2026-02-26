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

func TestDataQualityHandler_Pass_GoodQuality(t *testing.T) {
	prov := testutil.NewMockProvider()
	ctx := context.Background()

	require.NoError(t, prov.PutSensorData(ctx, types.SensorData{
		PipelineID: "pipe-1",
		SensorType: "quality",
		Value: map[string]interface{}{
			"nullRate": 0.01,
		},
		UpdatedAt: time.Now(),
	}))

	handler := NewDataQualityHandler(prov)
	out, err := handler(ctx, types.EvaluatorInput{
		PipelineID: "pipe-1",
		TraitType:  "data-quality",
		Config: map[string]interface{}{
			"sensorType":  "quality",
			"maxNullRate": 0.05,
		},
	})

	require.NoError(t, err)
	assert.Equal(t, types.TraitPass, out.Status)
	assert.Contains(t, out.Reason, "passed")
}

func TestDataQualityHandler_Fail_HighNullRate(t *testing.T) {
	prov := testutil.NewMockProvider()
	ctx := context.Background()

	require.NoError(t, prov.PutSensorData(ctx, types.SensorData{
		PipelineID: "pipe-1",
		SensorType: "quality",
		Value: map[string]interface{}{
			"nullRate": 0.10,
		},
		UpdatedAt: time.Now(),
	}))

	handler := NewDataQualityHandler(prov)
	out, err := handler(ctx, types.EvaluatorInput{
		PipelineID: "pipe-1",
		TraitType:  "data-quality",
		Config: map[string]interface{}{
			"sensorType":  "quality",
			"maxNullRate": 0.05,
		},
	})

	require.NoError(t, err)
	assert.Equal(t, types.TraitFail, out.Status)
	assert.Contains(t, out.Reason, "null rate")
	assert.Contains(t, out.Reason, "exceeds")
}

func TestDataQualityHandler_Fail_SchemaDrift(t *testing.T) {
	prov := testutil.NewMockProvider()
	ctx := context.Background()

	require.NoError(t, prov.PutSensorData(ctx, types.SensorData{
		PipelineID: "pipe-1",
		SensorType: "quality",
		Value: map[string]interface{}{
			"schemaDrift":        true,
			"schemaDriftDetails": "column 'x' removed",
		},
		UpdatedAt: time.Now(),
	}))

	handler := NewDataQualityHandler(prov)
	out, err := handler(ctx, types.EvaluatorInput{
		PipelineID: "pipe-1",
		TraitType:  "data-quality",
		Config: map[string]interface{}{
			"sensorType":       "quality",
			"allowSchemaDrift": false,
		},
	})

	require.NoError(t, err)
	assert.Equal(t, types.TraitFail, out.Status)
	assert.Contains(t, out.Reason, "schema drift")
}

func TestDataQualityHandler_Pass_SchemaDriftAllowed(t *testing.T) {
	prov := testutil.NewMockProvider()
	ctx := context.Background()

	require.NoError(t, prov.PutSensorData(ctx, types.SensorData{
		PipelineID: "pipe-1",
		SensorType: "quality",
		Value: map[string]interface{}{
			"schemaDrift": true,
		},
		UpdatedAt: time.Now(),
	}))

	handler := NewDataQualityHandler(prov)
	out, err := handler(ctx, types.EvaluatorInput{
		PipelineID: "pipe-1",
		TraitType:  "data-quality",
		Config: map[string]interface{}{
			"sensorType":       "quality",
			"allowSchemaDrift": true,
		},
	})

	require.NoError(t, err)
	assert.Equal(t, types.TraitPass, out.Status)
}

func TestDataQualityHandler_Fail_MissingSensor(t *testing.T) {
	prov := testutil.NewMockProvider()

	handler := NewDataQualityHandler(prov)
	out, err := handler(context.Background(), types.EvaluatorInput{
		PipelineID: "pipe-1",
		TraitType:  "data-quality",
		Config: map[string]interface{}{
			"sensorType":  "nonexistent",
			"maxNullRate": 0.05,
		},
	})

	require.NoError(t, err)
	assert.Equal(t, types.TraitFail, out.Status)
	assert.Contains(t, out.Reason, "no sensor data")
}
