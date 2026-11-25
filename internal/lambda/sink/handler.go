// Package sink implements the event-sink Lambda handler.
// It writes EventBridge events, details included, to DynamoDB for audit trail.
package sink

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// contentHashBytes is how much of the event's SHA-256 goes into its sort key.
const contentHashBytes = 8

// HandleEventSink writes an EventBridge event to the centralized events table
// under SK {tsMillis}#{detailType}#{content hash}. The hash covers the decoded
// event, so events that differ in content get their own rows, and two equal in
// every field, the time to the nanosecond included, are one row. An event
// without a timestamp is keyed on the time of the write instead, so delivering
// it again can add a row.
func HandleEventSink(ctx context.Context, d *lambda.Deps, input lambda.EventBridgeInput) error {
	var detail types.InterlockEvent
	if err := json.Unmarshal(input.Detail, &detail); err != nil {
		return fmt.Errorf("unmarshal event detail: %w", err)
	}

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

	// Hash the decoded event, so the layout of its JSON does not matter.
	canonical, err := json.Marshal(detail)
	if err != nil {
		return fmt.Errorf("marshal event (pipeline=%s type=%s): %w", detail.PipelineID, input.DetailType, err)
	}
	sum := sha256.Sum256(canonical)
	sk := fmt.Sprintf("%d#%s#%s", tsMillis, input.DetailType, hex.EncodeToString(sum[:contentHashBytes]))

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

	if len(detail.Detail) > 0 {
		item["detail"], err = attributevalue.Marshal(detail.Detail)
		if err != nil {
			return fmt.Errorf("marshal event detail (pipeline=%s type=%s): %w", detail.PipelineID, input.DetailType, err)
		}
	}

	_, err = d.Store.Client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &d.Store.EventsTable,
		Item:      item,
	})
	if err != nil {
		return fmt.Errorf("write event to events table (pipeline=%s type=%s): %w", detail.PipelineID, input.DetailType, err)
	}

	d.Logger.InfoContext(ctx, "event written", "pipeline", detail.PipelineID, "eventType", input.DetailType, "sk", sk)
	return nil
}
