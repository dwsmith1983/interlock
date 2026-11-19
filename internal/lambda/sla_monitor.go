package lambda

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	schedulerTypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	pkgsla "github.com/dwsmith1983/interlock/pkg/sla"
)

// SkipScheduler reports whether the EventBridge Scheduler integration is
// disabled via the SKIP_SCHEDULER=true environment variable. Used for
// environments without EventBridge Scheduler support (e.g. LocalStack
// Community). When true, all scheduler.* calls are treated as successful
// no-ops so the rest of the SLA logic continues unaffected.
func SkipScheduler() bool {
	return os.Getenv("SKIP_SCHEDULER") == "true"
}

// HandleSLACalculate is the exported entry point for SLA calculation.
// Used by the stream dry-run SLA projection, the watchdog's SLA schedules and
// the sla package's cancel path.
func HandleSLACalculate(input SLAMonitorInput, now time.Time) (SLAMonitorOutput, error) {
	return handleSLACalculate(input, now)
}

// handleSLACalculate computes warning and breach times. Supports two modes:
//
//  1. Schedule-based (deadline): breachAt = deadline, warningAt = deadline - expectedDuration.
//  2. Relative (maxDuration + sensorArrivalAt): breachAt = sensorArrivalAt + maxDuration,
//     warningAt = breachAt - expectedDuration (or breachAt - 25% of maxDuration if no expectedDuration).
//
// Returns full ISO 8601 timestamps required by Step Functions TimestampPath.
func handleSLACalculate(input SLAMonitorInput, now time.Time) (SLAMonitorOutput, error) {
	if input.MaxDuration != "" && input.SensorArrivalAt != "" {
		return handleRelativeSLACalculate(input)
	}
	breach, warning, err := pkgsla.CalculateAbsoluteDeadline(
		input.Date, input.Deadline, input.ExpectedDuration, input.Timezone, now)
	if err != nil {
		return SLAMonitorOutput{}, err
	}
	return SLAMonitorOutput{
		WarningAt: warning.UTC().Format(time.RFC3339),
		BreachAt:  breach.UTC().Format(time.RFC3339),
	}, nil
}

// handleRelativeSLACalculate computes warning and breach times from
// sensorArrivalAt + maxDuration. Warning offset uses expectedDuration
// if provided, otherwise defaults to 25% of maxDuration (i.e. warning
// fires at 75% of the total allowed time).
func handleRelativeSLACalculate(input SLAMonitorInput) (SLAMonitorOutput, error) {
	breach, warning, err := pkgsla.CalculateRelativeDeadline(
		input.SensorArrivalAt, input.MaxDuration, input.ExpectedDuration)
	if err != nil {
		return SLAMonitorOutput{}, err
	}
	return SLAMonitorOutput{
		WarningAt: warning.UTC().Format(time.RFC3339),
		BreachAt:  breach.UTC().Format(time.RFC3339),
	}, nil
}

// SLAScheduleName returns the deterministic EventBridge Scheduler name for an
// SLA alert. validation.MaxPipelineIDLen and the pipeline ID rule make it fit
// the Scheduler rules (1-64 characters of [0-9a-zA-Z_.-]) as built.
func SLAScheduleName(pipelineID, scheduleID, date, suffix string) string {
	return fmt.Sprintf("%s-%s-%s-sla-%s", pipelineID, scheduleID, date, suffix)
}

// createOneTimeSchedule creates a one-time EventBridge Scheduler entry that
// invokes the SLA monitor Lambda at the given timestamp with the given payload.
func createOneTimeSchedule(ctx context.Context, d *Deps, name, timestamp string, payload SLAMonitorInput) error {
	if SkipScheduler() {
		d.Logger.InfoContext(ctx, "SKIP_SCHEDULER set, no-op CreateSchedule",
			"name", name, "timestamp", timestamp)
		return nil
	}

	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	// at() expression for one-time schedules: at(yyyy-mm-ddThh:mm:ss)
	scheduleExpr := "at(" + strings.TrimSuffix(timestamp, "Z") + ")"

	_, err = d.Scheduler.CreateSchedule(ctx, &scheduler.CreateScheduleInput{
		Name:                       aws.String(name),
		GroupName:                  aws.String(d.SchedulerGroupName),
		ScheduleExpression:         aws.String(scheduleExpr),
		ScheduleExpressionTimezone: aws.String("UTC"),
		FlexibleTimeWindow: &schedulerTypes.FlexibleTimeWindow{
			Mode: schedulerTypes.FlexibleTimeWindowModeOff,
		},
		ActionAfterCompletion: schedulerTypes.ActionAfterCompletionDelete,
		Target: &schedulerTypes.Target{
			Arn:     aws.String(d.SLAMonitorARN),
			RoleArn: aws.String(d.SchedulerRoleARN),
			Input:   aws.String(string(payloadJSON)),
		},
	})
	if err != nil {
		return fmt.Errorf("create one-time schedule %q: %w", name, err)
	}
	return nil
}

// CreateSLASchedules is the exported entry point for creating SLA schedules.
// Used by the watchdog sub-package for proactive SLA scheduling.
func CreateSLASchedules(ctx context.Context, d *Deps, pipelineID, scheduleID, date string, calc SLAMonitorOutput, onConflictSkip bool) error {
	return createSLASchedules(ctx, d, pipelineID, scheduleID, date, calc, onConflictSkip)
}

// createSLASchedules creates warning and breach one-time schedules, skipping
// one whose time in calc is empty (the watchdog clears a warning that is
// already due). Returns an error on the first schedule creation failure. If
// onConflictSkip is true, ConflictException errors are silently skipped
// (idempotent retries).
func createSLASchedules(ctx context.Context, d *Deps, pipelineID, scheduleID, date string, calc SLAMonitorOutput, onConflictSkip bool) error {
	for _, alert := range []struct {
		suffix    string
		alertType string
		timestamp string
	}{
		{"warning", "SLA_WARNING", calc.WarningAt},
		{"breach", "SLA_BREACH", calc.BreachAt},
	} {
		if alert.timestamp == "" {
			continue
		}
		name := SLAScheduleName(pipelineID, scheduleID, date, alert.suffix)
		payload := SLAMonitorInput{
			Mode:       "fire-alert",
			PipelineID: pipelineID,
			ScheduleID: scheduleID,
			Date:       date,
			AlertType:  alert.alertType,
		}
		if alert.alertType == "SLA_WARNING" {
			payload.BreachAt = calc.BreachAt
		}
		if err := createOneTimeSchedule(ctx, d, name, alert.timestamp, payload); err != nil {
			if onConflictSkip {
				var conflict *schedulerTypes.ConflictException
				if errors.As(err, &conflict) {
					continue
				}
			}
			return fmt.Errorf("create %s schedule: %w", alert.suffix, err)
		}
	}
	return nil
}
