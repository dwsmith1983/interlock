package lambda

import (
	"context"
	"fmt"
	"strings"
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
// Breach time = deadline. Returns full ISO 8601 timestamps required by
// Step Functions TimestampPath.
func handleSLACalculate(input SLAMonitorInput) (SLAMonitorOutput, error) {
	dur, err := time.ParseDuration(input.ExpectedDuration)
	if err != nil {
		return SLAMonitorOutput{}, fmt.Errorf("parse expectedDuration %q: %w", input.ExpectedDuration, err)
	}

	loc := time.UTC
	if input.Timezone != "" {
		loc, err = time.LoadLocation(input.Timezone)
		if err != nil {
			return SLAMonitorOutput{}, fmt.Errorf("load timezone %q: %w", input.Timezone, err)
		}
	}

	now := time.Now().In(loc)

	// Parse the execution date to build a full timestamp.
	baseDate := now
	if input.Date != "" {
		parsed, err := time.Parse("2006-01-02", input.Date)
		if err == nil {
			baseDate = time.Date(parsed.Year(), parsed.Month(), parsed.Day(),
				now.Hour(), now.Minute(), 0, 0, loc)
		}
	}

	// Parse deadline. Supports two formats:
	//   "HH:MM" — absolute time of day (e.g., "02:00" for daily pipelines)
	//   ":MM"   — minutes past current hour (e.g., ":30" for hourly pipelines)
	var breachAt time.Time
	dl := input.Deadline
	if strings.HasPrefix(dl, ":") {
		// Relative to current hour: ":30" means current_hour:30
		deadline, err := time.Parse("04", strings.TrimPrefix(dl, ":"))
		if err != nil {
			return SLAMonitorOutput{}, fmt.Errorf("parse deadline %q: %w", dl, err)
		}
		breachAt = time.Date(baseDate.Year(), baseDate.Month(), baseDate.Day(),
			baseDate.Hour(), deadline.Minute(), 0, 0, loc)
		// If breach is already past, push to next hour
		if breachAt.Before(now) {
			breachAt = breachAt.Add(time.Hour)
		}
	} else {
		deadline, err := time.Parse("15:04", dl)
		if err != nil {
			return SLAMonitorOutput{}, fmt.Errorf("parse deadline %q: %w", dl, err)
		}
		breachAt = time.Date(baseDate.Year(), baseDate.Month(), baseDate.Day(),
			deadline.Hour(), deadline.Minute(), 0, 0, loc)
	}
	warningAt := breachAt.Add(-dur)

	return SLAMonitorOutput{
		WarningAt: warningAt.UTC().Format(time.RFC3339),
		BreachAt:  breachAt.UTC().Format(time.RFC3339),
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
