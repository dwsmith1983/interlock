package lambda

import (
	"context"
	"fmt"
	"time"
)

// HandleSLAMonitor processes SLA monitor requests from Step Functions.
// It supports two modes:
//   - "calculate": computes warning and breach times from schedule config
//   - "fire-alert": publishes an SLA alert event to EventBridge
func HandleSLAMonitor(ctx context.Context, d *Deps, input SLAMonitorInput) (SLAMonitorOutput, error) {
	switch input.Mode {
	case "calculate":
		return handleSLACalculate(input)
	case "fire-alert":
		return handleSLAFireAlert(ctx, d, input)
	default:
		return SLAMonitorOutput{}, fmt.Errorf("unknown SLA monitor mode: %q", input.Mode)
	}
}

// handleSLACalculate computes warning and breach times from the deadline
// and expected duration. Warning time = deadline - expectedDuration.
// Breach time = deadline. Times wrap around midnight as needed.
func handleSLACalculate(input SLAMonitorInput) (SLAMonitorOutput, error) {
	deadline, err := time.Parse("15:04", input.Deadline)
	if err != nil {
		return SLAMonitorOutput{}, fmt.Errorf("parse deadline %q: %w", input.Deadline, err)
	}

	dur, err := time.ParseDuration(input.ExpectedDuration)
	if err != nil {
		return SLAMonitorOutput{}, fmt.Errorf("parse expectedDuration %q: %w", input.ExpectedDuration, err)
	}

	// Use a reference date so time arithmetic wraps correctly around midnight.
	ref := time.Date(0, 1, 1, deadline.Hour(), deadline.Minute(), 0, 0, time.UTC)

	if input.Timezone != "" {
		loc, err := time.LoadLocation(input.Timezone)
		if err != nil {
			return SLAMonitorOutput{}, fmt.Errorf("load timezone %q: %w", input.Timezone, err)
		}
		ref = time.Date(0, 1, 1, deadline.Hour(), deadline.Minute(), 0, 0, loc)
	}

	warningTime := ref.Add(-dur)

	return SLAMonitorOutput{
		WarningAt: warningTime.Format("15:04"),
		BreachAt:  ref.Format("15:04"),
	}, nil
}

// handleSLAFireAlert publishes an SLA alert event to EventBridge and
// returns the alert metadata.
func handleSLAFireAlert(ctx context.Context, d *Deps, input SLAMonitorInput) (SLAMonitorOutput, error) {
	msg := fmt.Sprintf("pipeline %s: %s", input.PipelineID, input.AlertType)

	if err := publishEvent(ctx, d, input.AlertType, input.PipelineID, input.ScheduleID, input.Date, msg); err != nil {
		return SLAMonitorOutput{}, fmt.Errorf("publish SLA event: %w", err)
	}

	return SLAMonitorOutput{
		AlertType: input.AlertType,
		FiredAt:   time.Now().Format(time.RFC3339),
	}, nil
}
