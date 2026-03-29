package sla

import (
	"context"
	"fmt"
	"time"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
)

// handleSLAReconcile calculates deadlines and fires any alerts for deadlines
// that have already passed.
func handleSLAReconcile(ctx context.Context, d *lambda.Deps, input lambda.SLAMonitorInput) (lambda.SLAMonitorOutput, error) {
	calc, err := handleSLACalculate(input, d.Now())
	if err != nil {
		return lambda.SLAMonitorOutput{}, fmt.Errorf("reconcile: %w", err)
	}

	now := d.Now().UTC()
	warningAt, _ := time.Parse(time.RFC3339, calc.WarningAt)
	breachAt, _ := time.Parse(time.RFC3339, calc.BreachAt)

	reconcileDetail := map[string]interface{}{
		"source":     "reconciliation",
		"warningAt":  calc.WarningAt,
		"breachAt":   calc.BreachAt,
		"actionHint": "fired by reconciliation fallback — check Scheduler health",
	}

	var alertType string
	switch {
	case now.After(breachAt) || now.Equal(breachAt):
		if err := lambda.PublishEvent(ctx, d, "SLA_BREACH", input.PipelineID, input.ScheduleID, input.Date,
			fmt.Sprintf("pipeline %s: SLA_BREACH", input.PipelineID), reconcileDetail); err != nil {
			d.Logger.WarnContext(ctx, "failed to publish event", "type", "SLA_BREACH", "error", err)
		}
		alertType = "SLA_BREACH"
	case now.After(warningAt) || now.Equal(warningAt):
		if err := lambda.PublishEvent(ctx, d, "SLA_WARNING", input.PipelineID, input.ScheduleID, input.Date,
			fmt.Sprintf("pipeline %s: SLA_WARNING", input.PipelineID), reconcileDetail); err != nil {
			d.Logger.WarnContext(ctx, "failed to publish event", "type", "SLA_WARNING", "error", err)
		}
		alertType = "SLA_WARNING"
	default:
		alertType = "SLA_MET"
	}

	return lambda.SLAMonitorOutput{
		AlertType: alertType,
		WarningAt: calc.WarningAt,
		BreachAt:  calc.BreachAt,
		FiredAt:   now.Format(time.RFC3339),
	}, nil
}
