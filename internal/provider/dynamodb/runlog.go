package dynamodb

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// PutRunLog stores a run log entry.
func (p *DynamoDBProvider) PutRunLog(ctx context.Context, entry types.RunLogEntry) error {
	if entry.ScheduleID == "" {
		entry.ScheduleID = types.DefaultScheduleID
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	_, err = p.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &p.tableName,
		Item: map[string]ddbtypes.AttributeValue{
			"PK":   &ddbtypes.AttributeValueMemberS{Value: pipelinePK(entry.PipelineID)},
			"SK":   &ddbtypes.AttributeValueMemberS{Value: runLogSK(entry.Date, entry.ScheduleID)},
			"data": &ddbtypes.AttributeValueMemberS{Value: string(data)},
			"ttl":  &ddbtypes.AttributeValueMemberN{Value: fmt.Sprintf("%d", ttlEpoch(p.retentionTTL))},
		},
	})
	return err
}

// GetRunLog retrieves a run log entry for a pipeline, date, and schedule.
func (p *DynamoDBProvider) GetRunLog(ctx context.Context, pipelineID, date, scheduleID string) (*types.RunLogEntry, error) {
	if scheduleID == "" {
		scheduleID = types.DefaultScheduleID
	}
	out, err := p.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: &p.tableName,
		Key: map[string]ddbtypes.AttributeValue{
			"PK": &ddbtypes.AttributeValueMemberS{Value: pipelinePK(pipelineID)},
			"SK": &ddbtypes.AttributeValueMemberS{Value: runLogSK(date, scheduleID)},
		},
	})
	if err != nil {
		return nil, err
	}
	if out.Item == nil {
		return nil, nil
	}

	ttlVal, _ := extractTTL(out.Item)
	if isExpired(ttlVal) {
		return nil, nil
	}

	data, err := attributeStr(out.Item, "data")
	if err != nil {
		return nil, err
	}
	var entry types.RunLogEntry
	if err := json.Unmarshal([]byte(data), &entry); err != nil {
		return nil, err
	}
	return &entry, nil
}

// ListRunLogs returns recent run log entries for a pipeline, newest first.
func (p *DynamoDBProvider) ListRunLogs(ctx context.Context, pipelineID string, limit int) ([]types.RunLogEntry, error) {
	if limit <= 0 {
		limit = 20
	}

	out, err := p.client.Query(ctx, &dynamodb.QueryInput{
		TableName:              &p.tableName,
		KeyConditionExpression: aws.String("PK = :pk AND begins_with(SK, :prefix)"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":pk":     &ddbtypes.AttributeValueMemberS{Value: pipelinePK(pipelineID)},
			":prefix": &ddbtypes.AttributeValueMemberS{Value: prefixRunLog},
		},
		ScanIndexForward: aws.Bool(false),
		Limit:            aws.Int32(int32(limit)),
	})
	if err != nil {
		return nil, err
	}

	var entries []types.RunLogEntry
	for _, item := range out.Items {
		ttlVal, _ := extractTTL(item)
		if isExpired(ttlVal) {
			continue
		}
		data, err := attributeStr(item, "data")
		if err != nil {
			p.logger.Warn("skipping corrupt run log data", "error", err)
			continue
		}
		var entry types.RunLogEntry
		if err := json.Unmarshal([]byte(data), &entry); err != nil {
			p.logger.Warn("skipping corrupt run log data", "error", err)
			continue
		}
		entries = append(entries, entry)
	}
	return entries, nil
}
