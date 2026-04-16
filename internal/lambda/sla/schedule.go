package sla

import (
	"context"
	"fmt"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
)

// handleSLASchedule creates one-time EventBridge Scheduler entries for the
// SLA warning and breach times.
func handleSLASchedule(ctx context.Context, d *lambda.Deps, input lambda.SLAMonitorInput) (lambda.SLAMonitorOutput, error) {
	calc, err := handleSLACalculate(input, d.Now())
	if err != nil {
		return lambda.SLAMonitorOutput{}, fmt.Errorf("schedule: %w", err)
	}

	if d.Scheduler == nil {
		d.Logger.WarnContext(ctx, "scheduler not configured, skipping SLA schedule creation",
			"pipeline", input.PipelineID)
		return calc, nil
	}

	if lambda.SkipScheduler() {
		d.Logger.InfoContext(ctx, "SKIP_SCHEDULER set, no-op CreateSchedule for SLA",
			"pipeline", input.PipelineID, "warningAt", calc.WarningAt, "breachAt", calc.BreachAt)
		return calc, nil
	}

	if err := lambda.CreateSLASchedules(ctx, d, input.PipelineID, input.ScheduleID, input.Date, calc, false); err != nil {
		return lambda.SLAMonitorOutput{}, err
	}

	d.Logger.InfoContext(ctx, "scheduled SLA alerts",
		"pipeline", input.PipelineID,
		"warningAt", calc.WarningAt,
		"breachAt", calc.BreachAt,
	)

	return calc, nil
}
