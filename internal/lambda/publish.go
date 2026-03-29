package lambda

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebTypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// PublishEvent sends an event to EventBridge. It is safe to call when
// EventBridge is nil or EventBusName is empty (returns nil with no action).
func PublishEvent(ctx context.Context, d *Deps, eventType, pipelineID, schedule, date, message string, detail ...map[string]interface{}) error {
	if d.EventBridge == nil || d.EventBusName == "" {
		return nil
	}

	evt := types.InterlockEvent{
		PipelineID: pipelineID,
		ScheduleID: schedule,
		Date:       date,
		Message:    message,
		Timestamp:  d.Now(),
	}
	if len(detail) > 0 && detail[0] != nil {
		evt.Detail = detail[0]
	}
	detailJSON, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal event detail: %w", err)
	}

	source := types.EventSource
	detailStr := string(detailJSON)

	out, err := d.EventBridge.PutEvents(ctx, &eventbridge.PutEventsInput{
		Entries: []ebTypes.PutEventsRequestEntry{
			{
				Source:       &source,
				DetailType:   &eventType,
				Detail:       &detailStr,
				EventBusName: &d.EventBusName,
			},
		},
	})
	if err != nil {
		return fmt.Errorf("publish %s event: %w", eventType, err)
	}
	if out.FailedEntryCount > 0 {
		code, msg := "", ""
		if len(out.Entries) > 0 && out.Entries[0].ErrorCode != nil {
			code = *out.Entries[0].ErrorCode
			if out.Entries[0].ErrorMessage != nil {
				msg = *out.Entries[0].ErrorMessage
			}
		}
		return fmt.Errorf("publish %s event: partial failure (code=%s, message=%s)", eventType, code, msg)
	}
	return nil
}
