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
		return SLAMonitorOutput{}, fmt.Errorf("unknown SLA monitor mode: %q", input.Mode)
	}
}

// HandleSLACalculate is the exported entry point for SLA calculation.
// Used by the stream sub-package for dry-run SLA projections.
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
	// Relative SLA path: maxDuration + sensorArrivalAt.
	if input.MaxDuration != "" && input.SensorArrivalAt != "" {
		return handleRelativeSLACalculate(input)
	}

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

	now = now.In(loc)

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
		if baseHour >= 0 {
			// Hourly pipeline: data for hour H is processed in hour H+1,
			// so ":MM" means MM minutes into the processing window (H+1).
			breachAt = breachAt.Add(time.Hour)
		} else if breachAt.Before(now) {
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

// handleRelativeSLACalculate computes warning and breach times from
// sensorArrivalAt + maxDuration. Warning offset uses expectedDuration
// if provided, otherwise defaults to 25% of maxDuration (i.e. warning
// fires at 75% of the total allowed time).
func handleRelativeSLACalculate(input SLAMonitorInput) (SLAMonitorOutput, error) {
	maxDur, err := time.ParseDuration(input.MaxDuration)
	if err != nil {
		return SLAMonitorOutput{}, fmt.Errorf("parse maxDuration %q: %w", input.MaxDuration, err)
	}

	arrivalAt, err := time.Parse(time.RFC3339, input.SensorArrivalAt)
	if err != nil {
		return SLAMonitorOutput{}, fmt.Errorf("parse sensorArrivalAt %q: %w", input.SensorArrivalAt, err)
	}

	breachAt := arrivalAt.Add(maxDur)

	// Warning offset: use expectedDuration if provided, otherwise 25% of maxDuration.
	var warningOffset time.Duration
	if input.ExpectedDuration != "" {
		warningOffset, err = time.ParseDuration(input.ExpectedDuration)
		if err != nil {
			return SLAMonitorOutput{}, fmt.Errorf("parse expectedDuration %q: %w", input.ExpectedDuration, err)
		}
	} else {
		warningOffset = maxDur / 4
	}
	warningAt := breachAt.Add(-warningOffset)

	return SLAMonitorOutput{
		WarningAt: warningAt.UTC().Format(time.RFC3339),
		BreachAt:  breachAt.UTC().Format(time.RFC3339),
	}, nil
}

// handleSLAFireAlert publishes an SLA alert event to EventBridge and
// returns the alert metadata.
func handleSLAFireAlert(ctx context.Context, d *Deps, input SLAMonitorInput) (SLAMonitorOutput, error) {
	// Suppress alerts for pipelines that already completed or permanently failed.
	var tr *types.ControlRecord
	if d.Store != nil {
		suppressed := false
		var err error
		tr, err = d.Store.GetTrigger(ctx, input.PipelineID, input.ScheduleID, input.Date)
		switch {
		case err != nil:
			d.Logger.WarnContext(ctx, "trigger lookup failed in fire-alert, proceeding with alert",
				"pipeline", input.PipelineID, "error", err)
		case tr != nil && (tr.Status == types.TriggerStatusCompleted || tr.Status == types.TriggerStatusFailedFinal):
			d.Logger.InfoContext(ctx, "suppressing SLA alert (pipeline already finished)",
				"pipeline", input.PipelineID, "date", input.Date, "triggerStatus", tr.Status, "alertType", input.AlertType)
			suppressed = true
		case IsJobTerminal(ctx, d, input.PipelineID, input.ScheduleID, input.Date):
			// Joblog fallback: trigger row may be nil (cron pipeline), RUNNING
			// (not yet updated), or TTL-expired. Check joblog as secondary signal.
			d.Logger.InfoContext(ctx, "suppressing SLA alert (terminal joblog event found)",
				"pipeline", input.PipelineID, "date", input.Date, "alertType", input.AlertType)
			suppressed = true
		}
		if suppressed {
			return SLAMonitorOutput{AlertType: input.AlertType, FiredAt: d.Now().UTC().Format(time.RFC3339)}, nil
		}
	}

	if input.AlertType == "SLA_WARNING" && input.BreachAt != "" {
		breachAt, err := time.Parse(time.RFC3339, input.BreachAt)
		if err == nil && !d.Now().UTC().Before(breachAt) {
			d.Logger.InfoContext(ctx, "suppressing SLA_WARNING (past breach time)",
				"pipeline", input.PipelineID, "breachAt", input.BreachAt)
			return SLAMonitorOutput{AlertType: input.AlertType, FiredAt: d.Now().UTC().Format(time.RFC3339)}, nil
		}
	}

	// Build structured detail for alert consumers.
	status := "not started"
	if tr != nil {
		status = tr.Status
	}
	source := "schedule"
	actionHint := "pipeline not started — check sensor data"
	switch {
	case status == types.TriggerStatusRunning:
		actionHint = "pipeline running — may complete before breach"
	case status == "not started" && input.AlertType == "SLA_BREACH":
		actionHint = "pipeline not started — investigate trigger"
	}

	alertDetail := map[string]interface{}{
		"status":     status,
		"source":     source,
		"actionHint": actionHint,
	}
	if input.BreachAt != "" {
		alertDetail["breachAt"] = input.BreachAt
	}
	if input.Deadline != "" {
		alertDetail["deadline"] = input.Deadline
	}

	msg := fmt.Sprintf("pipeline %s: %s", input.PipelineID, input.AlertType)

	if err := PublishEvent(ctx, d, input.AlertType, input.PipelineID, input.ScheduleID, input.Date, msg, alertDetail); err != nil {
		return SLAMonitorOutput{}, fmt.Errorf("publish SLA event: %w", err)
	}

	return SLAMonitorOutput{
		AlertType: input.AlertType,
		FiredAt:   d.Now().UTC().Format(time.RFC3339),
	}, nil
}

// handleSLASchedule creates one-time EventBridge Scheduler entries for the
// SLA warning and breach times. Each schedule invokes this Lambda with
// mode "fire-alert" at the exact timestamp, then auto-deletes.
func handleSLASchedule(ctx context.Context, d *Deps, input SLAMonitorInput) (SLAMonitorOutput, error) {
	calc, err := handleSLACalculate(input, d.Now())
	if err != nil {
		return SLAMonitorOutput{}, fmt.Errorf("schedule: %w", err)
	}

	if d.Scheduler == nil {
		d.Logger.WarnContext(ctx, "scheduler not configured, skipping SLA schedule creation",
			"pipeline", input.PipelineID)
		return calc, nil
	}

	if err := createSLASchedules(ctx, d, input.PipelineID, input.ScheduleID, input.Date, calc, false); err != nil {
		return SLAMonitorOutput{}, err
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
	// If warningAt/breachAt not provided, recalculate from the available config.
	if input.WarningAt == "" && input.BreachAt == "" {
		if input.MaxDuration != "" && input.SensorArrivalAt != "" {
			calc, err := handleRelativeSLACalculate(input)
			if err != nil {
				return SLAMonitorOutput{}, fmt.Errorf("cancel recalculate (relative): %w", err)
			}
			input.WarningAt = calc.WarningAt
			input.BreachAt = calc.BreachAt
		} else if input.Deadline != "" {
			calc, err := handleSLACalculate(input, d.Now())
			if err != nil {
				return SLAMonitorOutput{}, fmt.Errorf("cancel recalculate: %w", err)
			}
			input.WarningAt = calc.WarningAt
			input.BreachAt = calc.BreachAt
		}
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

	// Determine final SLA status: binary MET or BREACH.
	// WARNING is not a valid completion outcome — if the job finished, it either
	// beat the breach deadline (MET) or missed it (BREACH).
	now := d.Now().UTC()
	alertType := string(types.EventSLAMet)
	if input.BreachAt != "" {
		breachAt, _ := time.Parse(time.RFC3339, input.BreachAt)
		if !breachAt.IsZero() && (now.After(breachAt) || now.Equal(breachAt)) {
			alertType = string(types.EventSLABreach)
		}
	}

	// Only publish a verdict if the pipeline was actually triggered.
	// If no trigger record exists, the pipeline never ran — publishing SLA_MET
	// would be misleading since the SLA wasn't "met" (nothing executed).
	publish := true
	if d.Store != nil {
		tr, err := d.Store.GetTrigger(ctx, input.PipelineID, input.ScheduleID, input.Date)
		if err != nil {
			d.Logger.WarnContext(ctx, "trigger lookup failed in cancel, proceeding with verdict",
				"pipeline", input.PipelineID, "error", err)
		} else if tr == nil {
			d.Logger.InfoContext(ctx, "skipping SLA verdict — pipeline was never triggered",
				"pipeline", input.PipelineID, "date", input.Date, "alertType", alertType)
			publish = false
		}
	}

	d.Logger.InfoContext(ctx, "cancelled SLA schedules",
		"pipeline", input.PipelineID,
		"alertType", alertType,
	)
	if publish {
		if err := PublishEvent(ctx, d, alertType, input.PipelineID, input.ScheduleID, input.Date,
			fmt.Sprintf("pipeline %s: %s", input.PipelineID, alertType)); err != nil {
			return SLAMonitorOutput{}, fmt.Errorf("publish SLA cancel verdict: %w", err)
		}
	}

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

// createSLASchedules creates warning and breach one-time schedules.
// Returns an error on the first schedule creation failure. If onConflictSkip
// is true, ConflictException errors are silently skipped (idempotent retries).
func createSLASchedules(ctx context.Context, d *Deps, pipelineID, scheduleID, date string, calc SLAMonitorOutput, onConflictSkip bool) error {
	for _, alert := range []struct {
		suffix    string
		alertType string
		timestamp string
	}{
		{"warning", "SLA_WARNING", calc.WarningAt},
		{"breach", "SLA_BREACH", calc.BreachAt},
	} {
		name := slaScheduleName(pipelineID, scheduleID, date, alert.suffix)
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

// handleSLAReconcile calculates deadlines and fires any alerts for deadlines
// that have already passed. Fallback for environments without EventBridge
// Scheduler configured.
func handleSLAReconcile(ctx context.Context, d *Deps, input SLAMonitorInput) (SLAMonitorOutput, error) {
	calc, err := handleSLACalculate(input, d.Now())
	if err != nil {
		return SLAMonitorOutput{}, fmt.Errorf("reconcile: %w", err)
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
		if err := PublishEvent(ctx, d, "SLA_BREACH", input.PipelineID, input.ScheduleID, input.Date,
			fmt.Sprintf("pipeline %s: SLA_BREACH", input.PipelineID), reconcileDetail); err != nil {
			d.Logger.WarnContext(ctx, "failed to publish event", "type", "SLA_BREACH", "error", err)
		}
		alertType = "SLA_BREACH"
	case now.After(warningAt) || now.Equal(warningAt):
		// Past warning but before breach — fire warning only
		if err := PublishEvent(ctx, d, "SLA_WARNING", input.PipelineID, input.ScheduleID, input.Date,
			fmt.Sprintf("pipeline %s: SLA_WARNING", input.PipelineID), reconcileDetail); err != nil {
			d.Logger.WarnContext(ctx, "failed to publish event", "type", "SLA_WARNING", "error", err)
		}
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

