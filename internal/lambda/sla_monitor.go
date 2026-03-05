package lambda

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	schedulerTypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// HandleSLAMonitor processes SLA monitor requests from Step Functions.
// It supports five modes:
//   - "calculate": computes warning and breach times from schedule config
//   - "fire-alert": publishes an SLA alert event to EventBridge
//   - "schedule":   creates one-time EventBridge Scheduler entries for warning/breach
//   - "cancel":     deletes unfired schedules and publishes SLA_MET if applicable
//   - "reconcile":  computes deadlines and fires any that have already passed (fallback)
func HandleSLAMonitor(ctx context.Context, d *Deps, input SLAMonitorInput) (SLAMonitorOutput, error) {
	switch input.Mode {
	case "calculate":
		return handleSLACalculate(input)
	case "fire-alert":
		return handleSLAFireAlert(ctx, d, input)
	case "schedule":
		return handleSLASchedule(ctx, d, input)
	case "cancel":
		return handleSLACancel(ctx, d, input)
	case "reconcile":
		return handleSLAReconcile(ctx, d, input)
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

	// Parse the execution date. Supports:
	//   "2006-01-02"    — daily
	//   "2006-01-02T15" — hourly (hour encoded in date)
	baseDate := now
	baseHour := -1 // -1 means "use current hour" for relative deadlines
	if input.Date != "" {
		datePart, hourPart := ParseExecutionDate(input.Date)
		parsed, err := time.Parse("2006-01-02", datePart)
		if err == nil {
			if hourPart != "" {
				h := 0
				if parsed, atoiErr := strconv.Atoi(hourPart); atoiErr == nil {
					h = parsed
					baseHour = h
				}
				baseDate = time.Date(parsed.Year(), parsed.Month(), parsed.Day(),
					h, 0, 0, 0, loc)
			} else {
				baseDate = time.Date(parsed.Year(), parsed.Month(), parsed.Day(),
					now.Hour(), now.Minute(), 0, 0, loc)
			}
		}
	}

	// Parse deadline. Supports two formats:
	//   "HH:MM" — absolute time of day (e.g., "02:00" for daily pipelines)
	//   ":MM"   — minutes past current hour (e.g., ":30" for hourly pipelines)
	var breachAt time.Time
	dl := input.Deadline
	if strings.HasPrefix(dl, ":") {
		deadline, err := time.Parse("04", strings.TrimPrefix(dl, ":"))
		if err != nil {
			return SLAMonitorOutput{}, fmt.Errorf("parse deadline %q: %w", dl, err)
		}
		hour := baseDate.Hour()
		breachAt = time.Date(baseDate.Year(), baseDate.Month(), baseDate.Day(),
			hour, deadline.Minute(), 0, 0, loc)
		// Only push to next hour if no explicit hour was set (daily pipeline)
		if baseHour < 0 && breachAt.Before(now) {
			breachAt = breachAt.Add(time.Hour)
		}
	} else {
		deadline, err := time.Parse("15:04", dl)
		if err != nil {
			return SLAMonitorOutput{}, fmt.Errorf("parse deadline %q: %w", dl, err)
		}
		breachAt = time.Date(baseDate.Year(), baseDate.Month(), baseDate.Day(),
			deadline.Hour(), deadline.Minute(), 0, 0, loc)
		if breachAt.Before(now) {
			breachAt = breachAt.Add(24 * time.Hour)
		}
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
	if input.AlertType == "SLA_WARNING" && input.BreachAt != "" {
		breachAt, err := time.Parse(time.RFC3339, input.BreachAt)
		if err == nil && !time.Now().UTC().Before(breachAt) {
			d.Logger.InfoContext(ctx, "suppressing SLA_WARNING (past breach time)",
				"pipeline", input.PipelineID, "breachAt", input.BreachAt)
			return SLAMonitorOutput{AlertType: input.AlertType, FiredAt: time.Now().UTC().Format(time.RFC3339)}, nil
		}
	}

	msg := fmt.Sprintf("pipeline %s: %s", input.PipelineID, input.AlertType)

	if err := publishEvent(ctx, d, input.AlertType, input.PipelineID, input.ScheduleID, input.Date, msg); err != nil {
		return SLAMonitorOutput{}, fmt.Errorf("publish SLA event: %w", err)
	}

	return SLAMonitorOutput{
		AlertType: input.AlertType,
		FiredAt:   time.Now().UTC().Format(time.RFC3339),
	}, nil
}

// handleSLASchedule creates one-time EventBridge Scheduler entries for the
// SLA warning and breach times. Each schedule invokes this Lambda with
// mode "fire-alert" at the exact timestamp, then auto-deletes.
func handleSLASchedule(ctx context.Context, d *Deps, input SLAMonitorInput) (SLAMonitorOutput, error) {
	calc, err := handleSLACalculate(input)
	if err != nil {
		return SLAMonitorOutput{}, fmt.Errorf("schedule: %w", err)
	}

	if d.Scheduler == nil {
		d.Logger.WarnContext(ctx, "scheduler not configured, skipping SLA schedule creation",
			"pipeline", input.PipelineID)
		return calc, nil
	}

	for _, alert := range []struct {
		suffix    string
		alertType string
		timestamp string
	}{
		{"warning", "SLA_WARNING", calc.WarningAt},
		{"breach", "SLA_BREACH", calc.BreachAt},
	} {
		name := slaScheduleName(input.PipelineID, input.ScheduleID, input.Date, alert.suffix)
		payload := SLAMonitorInput{
			Mode:       "fire-alert",
			PipelineID: input.PipelineID,
			ScheduleID: input.ScheduleID,
			Date:       input.Date,
			AlertType:  alert.alertType,
		}
		if alert.alertType == "SLA_WARNING" {
			payload.BreachAt = calc.BreachAt
		}
		if err := createOneTimeSchedule(ctx, d, name, alert.timestamp, payload); err != nil {
			return SLAMonitorOutput{}, fmt.Errorf("create %s schedule: %w", alert.suffix, err)
		}
	}

	d.Logger.InfoContext(ctx, "scheduled SLA alerts",
		"pipeline", input.PipelineID,
		"warningAt", calc.WarningAt,
		"breachAt", calc.BreachAt,
	)

	return calc, nil
}

// handleSLACancel deletes unfired SLA schedules and determines the final SLA
// outcome. If the job completed before the breach deadline, publishes SLA_MET.
// Warning/breach events were already published at the correct time by the
// Scheduler-invoked fire-alert calls.
func handleSLACancel(ctx context.Context, d *Deps, input SLAMonitorInput) (SLAMonitorOutput, error) {
	// If warningAt/breachAt not provided, recalculate from deadline/expectedDuration.
	if input.WarningAt == "" && input.BreachAt == "" && input.Deadline != "" {
		calc, err := handleSLACalculate(input)
		if err != nil {
			return SLAMonitorOutput{}, fmt.Errorf("cancel recalculate: %w", err)
		}
		input.WarningAt = calc.WarningAt
		input.BreachAt = calc.BreachAt
	}

	if d.Scheduler != nil {
		for _, suffix := range []string{"warning", "breach"} {
			name := slaScheduleName(input.PipelineID, input.ScheduleID, input.Date, suffix)
			_, err := d.Scheduler.DeleteSchedule(ctx, &scheduler.DeleteScheduleInput{
				Name:      aws.String(name),
				GroupName: aws.String(d.SchedulerGroupName),
			})
			if err != nil {
				// Ignore ResourceNotFoundException — schedule already fired and auto-deleted
				var rnf *schedulerTypes.ResourceNotFoundException
				if !errors.As(err, &rnf) {
					d.Logger.WarnContext(ctx, "delete schedule failed", "name", name, "error", err)
				}
			}
		}
	}

	// Determine final SLA status from the timestamps passed in
	now := time.Now().UTC()
	alertType := string(types.EventSLAMet)
	if input.BreachAt != "" {
		breachAt, _ := time.Parse(time.RFC3339, input.BreachAt)
		warningAt, _ := time.Parse(time.RFC3339, input.WarningAt)
		if !breachAt.IsZero() && (now.After(breachAt) || now.Equal(breachAt)) {
			alertType = "SLA_BREACH"
		} else if !warningAt.IsZero() && (now.After(warningAt) || now.Equal(warningAt)) {
			alertType = "SLA_WARNING"
		}
	}

	// Publish SLA_MET — the only outcome not already fired by the Scheduler
	if alertType == string(types.EventSLAMet) {
		_ = publishEvent(ctx, d, string(types.EventSLAMet), input.PipelineID, input.ScheduleID, input.Date,
			fmt.Sprintf("pipeline %s: %s", input.PipelineID, types.EventSLAMet))
	}

	d.Logger.InfoContext(ctx, "cancelled SLA schedules",
		"pipeline", input.PipelineID,
		"alertType", alertType,
	)

	return SLAMonitorOutput{
		AlertType: alertType,
		WarningAt: input.WarningAt,
		BreachAt:  input.BreachAt,
		FiredAt:   now.Format(time.RFC3339),
	}, nil
}

// slaScheduleName returns a deterministic EventBridge Scheduler name for an SLA alert.
func slaScheduleName(pipelineID, scheduleID, date, suffix string) string {
	return fmt.Sprintf("%s-%s-%s-sla-%s", pipelineID, scheduleID, date, suffix)
}

// createOneTimeSchedule creates a one-time EventBridge Scheduler entry that
// invokes the SLA monitor Lambda at the given timestamp with the given payload.
func createOneTimeSchedule(ctx context.Context, d *Deps, name, timestamp string, payload SLAMonitorInput) error {
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
	return err
}

// handleSLAReconcile calculates deadlines and fires any alerts for deadlines
// that have already passed. Fallback for environments without EventBridge
// Scheduler configured.
func handleSLAReconcile(ctx context.Context, d *Deps, input SLAMonitorInput) (SLAMonitorOutput, error) {
	calc, err := handleSLACalculate(input)
	if err != nil {
		return SLAMonitorOutput{}, fmt.Errorf("reconcile: %w", err)
	}

	now := time.Now().UTC()
	warningAt, _ := time.Parse(time.RFC3339, calc.WarningAt)
	breachAt, _ := time.Parse(time.RFC3339, calc.BreachAt)

	var alertType string
	switch {
	case now.After(breachAt) || now.Equal(breachAt):
		_ = publishEvent(ctx, d, "SLA_BREACH", input.PipelineID, input.ScheduleID, input.Date,
			fmt.Sprintf("pipeline %s: SLA_BREACH", input.PipelineID))
		alertType = "SLA_BREACH"
	case now.After(warningAt) || now.Equal(warningAt):
		// Past warning but before breach — fire warning only
		_ = publishEvent(ctx, d, "SLA_WARNING", input.PipelineID, input.ScheduleID, input.Date,
			fmt.Sprintf("pipeline %s: SLA_WARNING", input.PipelineID))
		alertType = "SLA_WARNING"
	default:
		alertType = "SLA_MET"
	}

	return SLAMonitorOutput{
		AlertType: alertType,
		WarningAt: calc.WarningAt,
		BreachAt:  calc.BreachAt,
		FiredAt:   now.Format(time.RFC3339),
	}, nil
}
