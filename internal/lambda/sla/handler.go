// Package sla implements the SLA monitor Lambda handler.
// It calculates deadlines, schedules/cancels EventBridge Scheduler entries,
// fires warning/breach alerts, and reconciles missed alerts.
package sla

import (
	"context"
	"fmt"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
)

// HandleSLAMonitor processes SLA monitor requests from Step Functions.
func HandleSLAMonitor(ctx context.Context, d *lambda.Deps, input lambda.SLAMonitorInput) (lambda.SLAMonitorOutput, error) {
	switch input.Mode {
	case "calculate":
		return handleSLACalculate(input, d.Now())
	case "fire-alert":
		return handleSLAFireAlert(ctx, d, input)
	case "schedule":
		return handleSLASchedule(ctx, d, input)
	case "cancel":
		return handleSLACancel(ctx, d, input)
	case "reconcile":
		return handleSLAReconcile(ctx, d, input)
	default:
		return lambda.SLAMonitorOutput{}, fmt.Errorf("unknown SLA monitor mode: %q", input.Mode)
	}
}
