package sla

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	schedulerTypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// handleSLACancel deletes unfired SLA schedules and determines the final SLA
// outcome. SLA_MET is published only for a COMPLETED trigger row; SLA_BREACH
// is published once the breach time has passed, for a row in any status or
// one that could not be read. A date its deadline does not fit gets no verdict,
// unless it has a relative SLA.
func handleSLACancel(ctx context.Context, d *lambda.Deps, input lambda.SLAMonitorInput) (lambda.SLAMonitorOutput, error) {
	// Schedule names need only the execution date, so delete them first: a
	// config read error below must not leave them to fire.
	if d.Scheduler != nil && !lambda.SkipScheduler() {
		for _, suffix := range []string{"warning", "breach"} {
			name := lambda.SLAScheduleName(input.PipelineID, input.ScheduleID, input.Date, suffix)
			_, err := d.Scheduler.DeleteSchedule(ctx, &scheduler.DeleteScheduleInput{
				Name:      aws.String(name),
				GroupName: aws.String(d.SchedulerGroupName),
			})
			if err != nil {
				var rnf *schedulerTypes.ResourceNotFoundException
				if !errors.As(err, &rnf) {
					d.Logger.WarnContext(ctx, "delete schedule failed", "name", name, "error", err)
				}
			}
		}
	} else if lambda.SkipScheduler() {
		d.Logger.InfoContext(ctx, "SKIP_SCHEDULER set, no-op DeleteSchedule for SLA cancel",
			"pipeline", input.PipelineID, "date", input.Date)
	}

	// Each branch adds what the verdict is judged against: a relative verdict
	// must not name the deadline.
	detail := map[string]interface{}{"source": "cancel"}
	if input.WarningAt == "" && input.BreachAt == "" {
		if input.MaxDuration != "" && input.SensorArrivalAt != "" {
			calc, err := lambda.HandleSLACalculate(input, d.Now())
			if err != nil {
				return lambda.SLAMonitorOutput{}, fmt.Errorf("cancel recalculate (relative): %w", err)
			}
			input.WarningAt = calc.WarningAt
			input.BreachAt = calc.BreachAt
			detail["maxDuration"] = input.MaxDuration
			detail["sensorArrivalAt"] = input.SensorArrivalAt
		} else if input.Deadline != "" && lambda.SLADeadlineFitsDate(input.Deadline, input.Date) {
			calcInput := input
			if d.Store != nil {
				cfg, cfgErr := d.Store.GetConfig(ctx, input.PipelineID)
				if cfgErr != nil {
					return lambda.SLAMonitorOutput{}, fmt.Errorf("cancel config lookup for %s: %w", input.PipelineID, cfgErr)
				}
				if cfg == nil {
					d.Logger.WarnContext(ctx, "config not found in cancel, using the execution date as the SLA date",
						"pipeline", input.PipelineID)
				}
				calcInput.Date = lambda.ResolveSLADate(cfg, input.Date)
			}
			calc, err := lambda.HandleSLACalculate(calcInput, d.Now())
			if err != nil {
				return lambda.SLAMonitorOutput{}, fmt.Errorf("cancel recalculate: %w", err)
			}
			input.WarningAt = calc.WarningAt
			input.BreachAt = calc.BreachAt
			addDeadline(detail, input)
		}
	} else {
		// The caller gave the times; the state machine never does.
		addDeadline(detail, input)
	}
	if input.BreachAt != "" {
		detail["breachAt"] = input.BreachAt
	}

	now := d.Now().UTC()
	alertType := string(types.EventSLAMet)
	if input.BreachAt != "" {
		breachAt, _ := time.Parse(time.RFC3339, input.BreachAt)
		if !breachAt.IsZero() && (now.After(breachAt) || now.Equal(breachAt)) {
			alertType = string(types.EventSLABreach)
		}
	}

	publish := true
	if input.WarningAt == "" && input.BreachAt == "" {
		d.Logger.WarnContext(ctx, "no SLA deadline could be determined, skipping verdict",
			"pipeline", input.PipelineID, "date", input.Date)
		publish = false
	}
	if publish && d.Store != nil {
		tr, err := d.Store.GetTrigger(ctx, input.PipelineID, input.ScheduleID, input.Date)
		if tr != nil {
			detail["status"] = tr.Status
		}
		switch {
		case err != nil:
			// An unknown status is not COMPLETED: withhold SLA_MET, keep SLA_BREACH.
			d.Logger.WarnContext(ctx, "trigger lookup failed in cancel, publishing SLA_BREACH only",
				"pipeline", input.PipelineID, "schedule", input.ScheduleID, "date", input.Date, "error", err)
			publish = alertType == string(types.EventSLABreach)
		case tr == nil:
			d.Logger.InfoContext(ctx, "skipping SLA verdict, pipeline was never triggered",
				"pipeline", input.PipelineID, "date", input.Date, "alertType", alertType)
			publish = false
		case alertType == string(types.EventSLAMet) && tr.Status != types.TriggerStatusCompleted:
			// A run that failed or never finished has not met its SLA.
			d.Logger.InfoContext(ctx, "skipping SLA_MET, the run did not complete",
				"pipeline", input.PipelineID, "date", input.Date, "status", tr.Status)
			publish = false
		}
	}

	d.Logger.InfoContext(ctx, "cancelled SLA schedules",
		"pipeline", input.PipelineID, "alertType", alertType, "published", publish)
	if publish {
		if err := lambda.PublishEvent(ctx, d, alertType, input.PipelineID, input.ScheduleID, input.Date,
			fmt.Sprintf("pipeline %s: %s", input.PipelineID, alertType), detail); err != nil {
			return lambda.SLAMonitorOutput{}, fmt.Errorf("publish SLA cancel verdict: %w", err)
		}
	}

	return lambda.SLAMonitorOutput{
		AlertType: alertType,
		WarningAt: input.WarningAt,
		BreachAt:  input.BreachAt,
		FiredAt:   now.Format(time.RFC3339),
	}, nil
}
