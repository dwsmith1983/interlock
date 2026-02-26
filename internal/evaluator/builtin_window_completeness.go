package evaluator

import (
	"context"
	"fmt"

	"github.com/dwsmith1983/interlock/internal/provider"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// NewWindowCompletenessHandler returns a BuiltinHandler that validates all
// expected processing windows are present in the sensor data.
//
// Config: sensorType (string), plus one of:
//   - expectedWindows ([]string): exact list of expected window IDs
//   - expectedCount (int): minimum number of windows required
func NewWindowCompletenessHandler(prov provider.Provider) BuiltinHandler {
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

		// Get the actual windows from sensor value.
		actualWindows, _ := toStringSlice(sensor.Value["windows"])

		// Strategy 1: Check against expectedWindows list.
		if expectedRaw, ok := input.Config["expectedWindows"]; ok {
			expected, ok := toStringSlice(expectedRaw)
			if !ok {
				return &types.EvaluatorOutput{
					Status:          types.TraitFail,
					Reason:          "config invalid: expectedWindows must be a string array",
					FailureCategory: types.FailurePermanent,
				}, nil
			}
			actualSet := make(map[string]bool, len(actualWindows))
			for _, w := range actualWindows {
				actualSet[w] = true
			}
			var missing []string
			for _, w := range expected {
				if !actualSet[w] {
					missing = append(missing, w)
				}
			}
			if len(missing) > 0 {
				return &types.EvaluatorOutput{
					Status: types.TraitFail,
					Reason: fmt.Sprintf("missing %d of %d expected windows", len(missing), len(expected)),
					Value: map[string]interface{}{
						"missing":  missing,
						"actual":   len(actualWindows),
						"expected": len(expected),
					},
				}, nil
			}
			return &types.EvaluatorOutput{
				Status: types.TraitPass,
				Reason: fmt.Sprintf("all %d expected windows present", len(expected)),
				Value:  map[string]interface{}{"windowCount": len(actualWindows)},
			}, nil
		}

		// Strategy 2: Check against expectedCount.
		if countRaw, ok := input.Config["expectedCount"]; ok {
			expectedCount, ok := toFloat64(countRaw)
			if !ok {
				return &types.EvaluatorOutput{
					Status:          types.TraitFail,
					Reason:          "config invalid: expectedCount must be numeric",
					FailureCategory: types.FailurePermanent,
				}, nil
			}
			// Also check sensor.Value["count"] as an alternative to the windows list.
			actualCount := float64(len(actualWindows))
			if countVal, ok := toFloat64(sensor.Value["count"]); ok && countVal > actualCount {
				actualCount = countVal
			}
			if actualCount < expectedCount {
				return &types.EvaluatorOutput{
					Status: types.TraitFail,
					Reason: fmt.Sprintf("window count %.0f below expected %.0f", actualCount, expectedCount),
					Value: map[string]interface{}{
						"actual":   actualCount,
						"expected": expectedCount,
					},
				}, nil
			}
			return &types.EvaluatorOutput{
				Status: types.TraitPass,
				Reason: fmt.Sprintf("window count %.0f meets expected %.0f", actualCount, expectedCount),
				Value:  map[string]interface{}{"windowCount": actualCount},
			}, nil
		}

		return &types.EvaluatorOutput{
			Status:          types.TraitFail,
			Reason:          "config missing: expectedWindows or expectedCount required",
			FailureCategory: types.FailurePermanent,
		}, nil
	}
}
