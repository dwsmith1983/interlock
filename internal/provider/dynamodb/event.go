package dynamodb

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/interlock-systems/interlock/pkg/types"
)

// AppendEvent writes an event to the pipeline's event partition.
func (p *DynamoDBProvider) AppendEvent(ctx context.Context, event types.Event) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}

	sk := eventSK(event.Timestamp)
	_, err = p.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &p.tableName,
		Item: map[string]ddbtypes.AttributeValue{
			"PK":   &ddbtypes.AttributeValueMemberS{Value: pipelinePK(event.PipelineID)},
			"SK":   &ddbtypes.AttributeValueMemberS{Value: sk},
			"data": &ddbtypes.AttributeValueMemberS{Value: string(data)},
			"ttl":  &ddbtypes.AttributeValueMemberN{Value: fmt.Sprintf("%d", ttlEpoch(p.retentionTTL))},
		},
	})
	return err
}

// ListEvents returns recent events for a pipeline in chronological order.
func (p *DynamoDBProvider) ListEvents(ctx context.Context, pipelineID string, limit int) ([]types.Event, error) {
	if limit <= 0 {
		limit = 50
	}

	// Query newest-first, then reverse for chronological order.
	out, err := p.client.Query(ctx, &dynamodb.QueryInput{
		TableName:              &p.tableName,
		KeyConditionExpression: aws.String("PK = :pk AND begins_with(SK, :prefix)"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":pk":     &ddbtypes.AttributeValueMemberS{Value: pipelinePK(pipelineID)},
			":prefix": &ddbtypes.AttributeValueMemberS{Value: prefixEvent},
		},
		ScanIndexForward: aws.Bool(false),
		Limit:            aws.Int32(int32(limit)),
	})
	if err != nil {
		return nil, err
	}

	events := make([]types.Event, 0, len(out.Items))
	for i := len(out.Items) - 1; i >= 0; i-- {
		item := out.Items[i]
		ttlVal, _ := attributeInt(item, "ttl")
		if isExpired(ttlVal) {
			continue
		}
		data, err := attributeStr(item, "data")
		if err != nil {
			p.logger.Warn("skipping corrupt event data", "error", err)
			continue
		}
		var ev types.Event
		if err := json.Unmarshal([]byte(data), &ev); err != nil {
			p.logger.Warn("skipping corrupt event data", "error", err)
			continue
		}
		events = append(events, ev)
	}
	return events, nil
}

// ReadEventsSince reads events forward from after sinceID (exclusive).
// sinceID is the SK value from a previous EventRecord.StreamID.
// Use "" or "0-0" to read from the beginning.
func (p *DynamoDBProvider) ReadEventsSince(ctx context.Context, pipelineID string, sinceID string, count int64) ([]types.EventRecord, error) {
	if count <= 0 {
		count = 100
	}

	var keyCondition string
	exprValues := map[string]ddbtypes.AttributeValue{
		":pk": &ddbtypes.AttributeValueMemberS{Value: pipelinePK(pipelineID)},
	}

	if sinceID == "" || sinceID == "0-0" {
		keyCondition = "PK = :pk AND begins_with(SK, :prefix)"
		exprValues[":prefix"] = &ddbtypes.AttributeValueMemberS{Value: prefixEvent}
	} else {
		keyCondition = "PK = :pk AND SK > :sinceID"
		exprValues[":sinceID"] = &ddbtypes.AttributeValueMemberS{Value: sinceID}
	}

	out, err := p.client.Query(ctx, &dynamodb.QueryInput{
		TableName:                 &p.tableName,
		KeyConditionExpression:    aws.String(keyCondition),
		ExpressionAttributeValues: exprValues,
		ScanIndexForward:          aws.Bool(true),
		Limit:                     aws.Int32(int32(count)),
	})
	if err != nil {
		return nil, err
	}

	records := make([]types.EventRecord, 0, len(out.Items))
	for _, item := range out.Items {
		ttlVal, _ := attributeInt(item, "ttl")
		if isExpired(ttlVal) {
			continue
		}

		sk, err := attributeStr(item, "SK")
		if err != nil {
			continue
		}
		data, err := attributeStr(item, "data")
		if err != nil {
			p.logger.Warn("skipping corrupt event data", "error", err)
			continue
		}
		var ev types.Event
		if err := json.Unmarshal([]byte(data), &ev); err != nil {
			p.logger.Warn("skipping corrupt event data", "error", err)
			continue
		}
		records = append(records, types.EventRecord{
			StreamID: sk,
			Event:    ev,
		})
	}
	return records, nil
}
