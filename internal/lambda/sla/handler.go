// Package sla implements the SLA monitor Lambda handler. It fires SLA
// warning and breach alerts and cancels SLA schedules when a run ends.
package sla

import (
	"context"
	"fmt"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
)

// HandleSLAMonitor processes SLA monitor requests from Step Functions.
func HandleSLAMonitor(ctx context.Context, d *lambda.Deps, input lambda.SLAMonitorInput) (lambda.SLAMonitorOutput, error) {
	switch input.Mode {
	case "fire-alert":
		return handleSLAFireAlert(ctx, d, input)
	case "cancel":
		return handleSLACancel(ctx, d, input)
	default:
		return lambda.SLAMonitorOutput{}, fmt.Errorf("unknown SLA monitor mode: %q", input.Mode)
	}
}
