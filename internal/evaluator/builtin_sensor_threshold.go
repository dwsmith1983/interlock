package evaluator

import (
	"context"
	"fmt"

	"github.com/dwsmith1983/interlock/internal/provider"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// NewSensorThresholdHandler returns a BuiltinHandler that reads sensor data
// and compares a numeric value against min/max thresholds.
//
// Config: sensorType (string), min (float64, optional), max (float64, optional)
func NewSensorThresholdHandler(prov provider.Provider) BuiltinHandler {
	return func(ctx context.Context, input types.EvaluatorInput) (*types.EvaluatorOutput, error) {
		sensorType, err := configString(input.Config, "sensorType")
		if err != nil {
			return &types.EvaluatorOutput{
				Status:          types.TraitFail,
				Reason:          err.Error(),
				FailureCategory: types.FailurePermanent,
			}, nil
		}

		sensor, err := prov.GetSensorData(ctx, input.PipelineID, sensorType)
		if err != nil {
			return nil, fmt.Errorf("reading sensor data: %w", err)
		}
		if sensor == nil {
			return &types.EvaluatorOutput{
				Status: types.TraitFail,
				Reason: fmt.Sprintf("no sensor data for %s/%s", input.PipelineID, sensorType),
			}, nil
		}

		value, ok := extractNumericValue(sensor.Value, sensorType)
		if !ok {
			return &types.EvaluatorOutput{
				Status: types.TraitFail,
				Reason: fmt.Sprintf("sensor %s has no numeric value", sensorType),
			}, nil
		}

		// Check min threshold.
		if minVal, ok := toFloat64(input.Config["min"]); ok {
			if value < minVal {
				return &types.EvaluatorOutput{
					Status: types.TraitFail,
					Reason: fmt.Sprintf("sensor %s value %.2f below min %.2f", sensorType, value, minVal),
					Value:  map[string]interface{}{"value": value, "min": minVal},
				}, nil
			}
		}

		// Check max threshold.
		if maxVal, ok := toFloat64(input.Config["max"]); ok {
			if value > maxVal {
				return &types.EvaluatorOutput{
					Status: types.TraitFail,
					Reason: fmt.Sprintf("sensor %s value %.2f above max %.2f", sensorType, value, maxVal),
					Value:  map[string]interface{}{"value": value, "max": maxVal},
				}, nil
			}
		}

		return &types.EvaluatorOutput{
			Status: types.TraitPass,
			Reason: fmt.Sprintf("sensor %s value %.2f within thresholds", sensorType, value),
			Value:  map[string]interface{}{"value": value},
		}, nil
	}
}
