package evaluator

import (
	"context"
	"fmt"
	"time"

	"github.com/dwsmith1983/interlock/internal/provider"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// NewSensorFreshnessHandler returns a BuiltinHandler that checks whether
// sensor data was updated within the configured maxAge duration.
//
// Config: sensorType (string), maxAge (duration string, e.g. "10m")
func NewSensorFreshnessHandler(prov provider.Provider) BuiltinHandler {
	return func(ctx context.Context, input types.EvaluatorInput) (*types.EvaluatorOutput, error) {
		sensorType, err := configString(input.Config, "sensorType")
		if err != nil {
			return &types.EvaluatorOutput{
				Status:          types.TraitFail,
				Reason:          err.Error(),
				FailureCategory: types.FailurePermanent,
			}, nil
		}

		maxAgeStr, err := configString(input.Config, "maxAge")
		if err != nil {
			return &types.EvaluatorOutput{
				Status:          types.TraitFail,
				Reason:          err.Error(),
				FailureCategory: types.FailurePermanent,
			}, nil
		}
		maxAge, err := time.ParseDuration(maxAgeStr)
		if err != nil {
			return &types.EvaluatorOutput{
				Status:          types.TraitFail,
				Reason:          fmt.Sprintf("invalid maxAge duration %q: %v", maxAgeStr, err),
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

		age := time.Since(sensor.UpdatedAt)
		if age > maxAge {
			return &types.EvaluatorOutput{
				Status: types.TraitFail,
				Reason: fmt.Sprintf("sensor %s is stale: age %s exceeds maxAge %s", sensorType, age.Truncate(time.Second), maxAge),
				Value: map[string]interface{}{
					"updatedAt": sensor.UpdatedAt.Format(time.RFC3339),
					"age":       age.String(),
					"maxAge":    maxAge.String(),
				},
			}, nil
		}

		return &types.EvaluatorOutput{
			Status: types.TraitPass,
			Reason: fmt.Sprintf("sensor %s is fresh: age %s within maxAge %s", sensorType, age.Truncate(time.Second), maxAge),
			Value: map[string]interface{}{
				"updatedAt": sensor.UpdatedAt.Format(time.RFC3339),
				"age":       age.String(),
			},
		}, nil
	}
}
