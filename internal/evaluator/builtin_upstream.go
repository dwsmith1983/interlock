package evaluator

import (
	"context"
	"fmt"

	"github.com/dwsmith1983/interlock/internal/provider"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// NewUpstreamJobLogHandler returns a BuiltinHandler that checks if an upstream
// pipeline's run log shows COMPLETED status for the given date and schedule.
// This enables zero-HTTP-roundtrip upstream dependency checking.
func NewUpstreamJobLogHandler(prov provider.Provider) BuiltinHandler {
	return func(ctx context.Context, input types.EvaluatorInput) (*types.EvaluatorOutput, error) {
		upstreamPipeline, _ := input.Config["upstreamPipeline"].(string)
		if upstreamPipeline == "" {
			return &types.EvaluatorOutput{
				Status:          types.TraitFail,
				Reason:          "config missing: upstreamPipeline",
				FailureCategory: types.FailurePermanent,
			}, nil
		}

		date, _ := input.Config["date"].(string)
		if date == "" {
			return &types.EvaluatorOutput{
				Status:          types.TraitFail,
				Reason:          "config missing: date",
				FailureCategory: types.FailurePermanent,
			}, nil
		}

		upstreamSchedule, _ := input.Config["upstreamSchedule"].(string)
		if upstreamSchedule == "" {
			upstreamSchedule, _ = input.Config["scheduleID"].(string)
		}
		if upstreamSchedule == "" {
			upstreamSchedule = types.DefaultScheduleID
		}

		entry, err := prov.GetRunLog(ctx, upstreamPipeline, date, upstreamSchedule)
		if err != nil {
			return nil, fmt.Errorf("querying upstream run log: %w", err)
		}

		if entry == nil {
			return &types.EvaluatorOutput{
				Status: types.TraitFail,
				Reason: fmt.Sprintf("upstream %s has no run log for %s/%s", upstreamPipeline, date, upstreamSchedule),
			}, nil
		}

		if entry.Status == types.RunCompleted {
			return &types.EvaluatorOutput{
				Status: types.TraitPass,
				Reason: fmt.Sprintf("upstream %s completed for %s/%s", upstreamPipeline, date, upstreamSchedule),
				Value: map[string]interface{}{
					"upstreamRunID": entry.RunID,
					"completedAt":   entry.CompletedAt,
				},
			}, nil
		}

		return &types.EvaluatorOutput{
			Status: types.TraitFail,
			Reason: fmt.Sprintf("upstream %s status is %s for %s/%s", upstreamPipeline, entry.Status, date, upstreamSchedule),
		}, nil
	}
}
