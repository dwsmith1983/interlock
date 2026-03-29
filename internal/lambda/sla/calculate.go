package sla

import (
	"time"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	pkgsla "github.com/dwsmith1983/interlock/pkg/sla"
)

// handleSLACalculate computes warning and breach times. Supports two modes:
//
//  1. Schedule-based (deadline): delegates to pkgsla.CalculateAbsoluteDeadline.
//  2. Relative (maxDuration + sensorArrivalAt): delegates to pkgsla.CalculateRelativeDeadline.
func handleSLACalculate(input lambda.SLAMonitorInput, now time.Time) (lambda.SLAMonitorOutput, error) {
	if input.MaxDuration != "" && input.SensorArrivalAt != "" {
		return handleRelativeSLACalculate(input)
	}
	breach, warning, err := pkgsla.CalculateAbsoluteDeadline(
		input.Date, input.Deadline, input.ExpectedDuration, input.Timezone, now)
	if err != nil {
		return lambda.SLAMonitorOutput{}, err
	}
	return lambda.SLAMonitorOutput{
		WarningAt: warning.UTC().Format(time.RFC3339),
		BreachAt:  breach.UTC().Format(time.RFC3339),
	}, nil
}

// handleRelativeSLACalculate computes warning and breach times from
// sensorArrivalAt + maxDuration, delegating to pkgsla.CalculateRelativeDeadline.
func handleRelativeSLACalculate(input lambda.SLAMonitorInput) (lambda.SLAMonitorOutput, error) {
	breach, warning, err := pkgsla.CalculateRelativeDeadline(
		input.SensorArrivalAt, input.MaxDuration, input.ExpectedDuration)
	if err != nil {
		return lambda.SLAMonitorOutput{}, err
	}
	return lambda.SLAMonitorOutput{
		WarningAt: warning.UTC().Format(time.RFC3339),
		BreachAt:  breach.UTC().Format(time.RFC3339),
	}, nil
}
