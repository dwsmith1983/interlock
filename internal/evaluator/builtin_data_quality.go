package evaluator

import (
	"context"
	"fmt"

	"github.com/dwsmith1983/interlock/internal/provider"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// NewDataQualityHandler returns a BuiltinHandler that validates data quality
// metrics from sensor data.
//
// Config: sensorType (string), maxNullRate (float64, 0-1), allowSchemaDrift (bool)
func NewDataQualityHandler(prov provider.Provider) BuiltinHandler {
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

		// Check null rate.
		if maxNullRateRaw, ok := input.Config["maxNullRate"]; ok {
			maxNullRate, ok := toFloat64(maxNullRateRaw)
			if !ok {
				return &types.EvaluatorOutput{
					Status:          types.TraitFail,
					Reason:          "config invalid: maxNullRate must be numeric",
					FailureCategory: types.FailurePermanent,
				}, nil
			}
			if nullRate, ok := toFloat64(sensor.Value["nullRate"]); ok {
				if nullRate > maxNullRate {
					return &types.EvaluatorOutput{
						Status: types.TraitFail,
						Reason: fmt.Sprintf("null rate %.4f exceeds max %.4f", nullRate, maxNullRate),
						Value: map[string]interface{}{
							"nullRate":    nullRate,
							"maxNullRate": maxNullRate,
						},
					}, nil
				}
			}
		}

		// Check schema drift.
		allowSchemaDrift, _ := input.Config["allowSchemaDrift"].(bool)
		if !allowSchemaDrift {
			if schemaDrifted, ok := sensor.Value["schemaDrift"].(bool); ok && schemaDrifted {
				driftDetails, _ := sensor.Value["schemaDriftDetails"].(string)
				return &types.EvaluatorOutput{
					Status: types.TraitFail,
					Reason: fmt.Sprintf("schema drift detected: %s", driftDetails),
					Value: map[string]interface{}{
						"schemaDrift":        true,
						"schemaDriftDetails": driftDetails,
					},
				}, nil
			}
		}

		return &types.EvaluatorOutput{
			Status: types.TraitPass,
			Reason: fmt.Sprintf("data quality checks passed for sensor %s", sensorType),
			Value:  sensor.Value,
		}, nil
	}
}
