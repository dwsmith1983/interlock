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

// handleSLACancel deletes unfired SLA schedules and determines the final SLA outcome.
func handleSLACancel(ctx context.Context, d *lambda.Deps, input lambda.SLAMonitorInput) (lambda.SLAMonitorOutput, error) {
	if input.WarningAt == "" && input.BreachAt == "" {
		if input.MaxDuration != "" && input.SensorArrivalAt != "" {
			calc, err := lambda.HandleSLACalculate(lambda.SLAMonitorInput{
				Mode:             "calculate",
				MaxDuration:      input.MaxDuration,
				SensorArrivalAt:  input.SensorArrivalAt,
				ExpectedDuration: input.ExpectedDuration,
			}, d.Now())
			if err != nil {
				return lambda.SLAMonitorOutput{}, fmt.Errorf("cancel recalculate (relative): %w", err)
			}
			input.WarningAt = calc.WarningAt
			input.BreachAt = calc.BreachAt
		} else if input.Deadline != "" {
			calc, err := lambda.HandleSLACalculate(input, d.Now())
			if err != nil {
				return lambda.SLAMonitorOutput{}, fmt.Errorf("cancel recalculate: %w", err)
			}
			input.WarningAt = calc.WarningAt
			input.BreachAt = calc.BreachAt
		}
	}

	if d.Scheduler != nil {
		for _, suffix := range []string{"warning", "breach"} {
			name := SLAScheduleName(input.PipelineID, input.ScheduleID, input.Date, suffix)
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
		"pipeline", input.PipelineID, "alertType", alertType)
	if publish {
		if err := lambda.PublishEvent(ctx, d, alertType, input.PipelineID, input.ScheduleID, input.Date,
			fmt.Sprintf("pipeline %s: %s", input.PipelineID, alertType)); err != nil {
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

// SLAScheduleName returns a deterministic EventBridge Scheduler name for an SLA alert.
func SLAScheduleName(pipelineID, scheduleID, date, suffix string) string {
	return fmt.Sprintf("%s-%s-%s-sla-%s", pipelineID, scheduleID, date, suffix)
}
