package sla

import (
	"cmp"
	"context"
	"fmt"
	"time"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// handleSLAFireAlert publishes an SLA alert event to EventBridge.
func handleSLAFireAlert(ctx context.Context, d *lambda.Deps, input lambda.SLAMonitorInput) (lambda.SLAMonitorOutput, error) {
	var tr *types.ControlRecord
	if d.Store != nil {
		var err error
		tr, err = d.Store.GetTrigger(ctx, input.PipelineID, input.ScheduleID, input.Date)
		if err != nil {
			d.Logger.WarnContext(ctx, "trigger lookup failed in fire-alert, proceeding with alert",
				"pipeline", input.PipelineID, "error", err)
		} else if finished, reason := lambda.RunFinished(ctx, d, tr, input.PipelineID, input.ScheduleID, input.Date); finished {
			d.Logger.InfoContext(ctx, "suppressing SLA alert", "reason", reason,
				"pipeline", input.PipelineID, "date", input.Date, "alertType", input.AlertType)
			return lambda.SLAMonitorOutput{AlertType: input.AlertType, FiredAt: d.Now().UTC().Format(time.RFC3339)}, nil
		}
	}

	if input.AlertType == "SLA_WARNING" && input.BreachAt != "" {
		breachAt, err := time.Parse(time.RFC3339, input.BreachAt)
		if err == nil && !d.Now().UTC().Before(breachAt) {
			d.Logger.InfoContext(ctx, "suppressing SLA_WARNING (past breach time)",
				"pipeline", input.PipelineID, "breachAt", input.BreachAt)
			return lambda.SLAMonitorOutput{AlertType: input.AlertType, FiredAt: d.Now().UTC().Format(time.RFC3339)}, nil
		}
	}

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
	addDeadline(alertDetail, input)

	msg := fmt.Sprintf("pipeline %s: %s", input.PipelineID, input.AlertType)

	if err := lambda.PublishEvent(ctx, d, input.AlertType, input.PipelineID, input.ScheduleID, input.Date, msg, alertDetail); err != nil {
		return lambda.SLAMonitorOutput{}, fmt.Errorf("publish SLA event: %w", err)
	}

	return lambda.SLAMonitorOutput{
		AlertType: input.AlertType,
		FiredAt:   d.Now().UTC().Format(time.RFC3339),
	}, nil
}

// addDeadline adds the configured deadline, if any, and the time zone it is
// read in to an event detail.
func addDeadline(detail map[string]interface{}, input lambda.SLAMonitorInput) {
	if input.Deadline == "" {
		return
	}
	detail["deadline"] = input.Deadline
	detail["timezone"] = cmp.Or(input.Timezone, "UTC")
}
