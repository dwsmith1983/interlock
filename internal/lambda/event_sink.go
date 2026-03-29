package lambda

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// Deprecated: Use sink.HandleEventSink instead. Retained for test compatibility.
func HandleEventSink(ctx context.Context, d *Deps, input EventBridgeInput) error {
	var detail types.InterlockEvent
	if err := json.Unmarshal(input.Detail, &detail); err != nil {
		return fmt.Errorf("unmarshal event detail: %w", err)
	}

	// Use the event's original timestamp when available; fall back to ingestion time.
	var tsMillis int64
	if !detail.Timestamp.IsZero() {
		tsMillis = detail.Timestamp.UnixMilli()
	} else {
		tsMillis = d.Now().UnixMilli()
	}

	now := d.Now()
	ttlDays := d.EventsTTLDays
	if ttlDays <= 0 {
		ttlDays = 90
	}
	ttl := now.Add(time.Duration(ttlDays) * 24 * time.Hour).Unix()
	sk := fmt.Sprintf("%d#%s", tsMillis, input.DetailType)

	item := map[string]ddbtypes.AttributeValue{
		"PK":         &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(detail.PipelineID)},
		"SK":         &ddbtypes.AttributeValueMemberS{Value: sk},
		"eventType":  &ddbtypes.AttributeValueMemberS{Value: input.DetailType},
		"pipelineId": &ddbtypes.AttributeValueMemberS{Value: detail.PipelineID},
		"scheduleId": &ddbtypes.AttributeValueMemberS{Value: detail.ScheduleID},
		"date":       &ddbtypes.AttributeValueMemberS{Value: detail.Date},
		"message":    &ddbtypes.AttributeValueMemberS{Value: detail.Message},
		"timestamp":  &ddbtypes.AttributeValueMemberN{Value: fmt.Sprintf("%d", tsMillis)},
		"ttl":        &ddbtypes.AttributeValueMemberN{Value: fmt.Sprintf("%d", ttl)},
	}

	_, err := d.Store.Client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &d.Store.EventsTable,
		Item:      item,
	})
	if err != nil {
		return fmt.Errorf("write event to events table (pipeline=%s type=%s): %w", detail.PipelineID, input.DetailType, err)
	}

	d.Logger.InfoContext(ctx, "event written", "pipeline", detail.PipelineID, "eventType", input.DetailType, "sk", sk)
	return nil
}
