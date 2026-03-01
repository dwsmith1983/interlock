package store

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// WriteJobEvent appends a job event record to the job log table.
// Optional fields (runID, duration, errMsg) are omitted when empty/zero.
func (s *Store) WriteJobEvent(ctx context.Context, pipelineID, schedule, date, event, runID string, duration int64, errMsg string) error {
	timestamp := fmt.Sprintf("%d", time.Now().UTC().UnixMilli())
	sk := types.JobSK(schedule, date, timestamp)
	ttl := time.Now().Add(30 * 24 * time.Hour).Unix()

	item := map[string]ddbtypes.AttributeValue{
		"PK":    &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
		"SK":    &ddbtypes.AttributeValueMemberS{Value: sk},
		"event": &ddbtypes.AttributeValueMemberS{Value: event},
		"ttl":   &ddbtypes.AttributeValueMemberN{Value: fmt.Sprintf("%d", ttl)},
	}

	if runID != "" {
		item["runId"] = &ddbtypes.AttributeValueMemberS{Value: runID}
	}
	if duration != 0 {
		item["duration"] = &ddbtypes.AttributeValueMemberN{Value: fmt.Sprintf("%d", duration)}
	}
	if errMsg != "" {
		item["error"] = &ddbtypes.AttributeValueMemberS{Value: errMsg}
	}

	_, err := s.Client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &s.JobLogTable,
		Item:      item,
	})
	if err != nil {
		return fmt.Errorf("write job event %q/%s/%s: %w", pipelineID, schedule, date, err)
	}
	return nil
}

// GetLatestJobEvent retrieves the most recent job event for a pipeline,
// schedule, and date. Returns nil, nil if no events exist.
func (s *Store) GetLatestJobEvent(ctx context.Context, pipelineID, schedule, date string) (*types.JobLogRecord, error) {
	prefix := "JOB#" + schedule + "#" + date + "#"

	out, err := s.Client.Query(ctx, &dynamodb.QueryInput{
		TableName:              &s.JobLogTable,
		KeyConditionExpression: aws.String("PK = :pk AND begins_with(SK, :prefix)"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":pk":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
			":prefix": &ddbtypes.AttributeValueMemberS{Value: prefix},
		},
		ScanIndexForward: aws.Bool(false),
		Limit:            aws.Int32(1),
	})
	if err != nil {
		return nil, fmt.Errorf("get latest job event %q/%s/%s: %w", pipelineID, schedule, date, err)
	}
	if len(out.Items) == 0 {
		return nil, nil
	}

	var rec types.JobLogRecord
	if err := attributevalue.UnmarshalMap(out.Items[0], &rec); err != nil {
		return nil, fmt.Errorf("unmarshal job event %q/%s/%s: %w", pipelineID, schedule, date, err)
	}
	return &rec, nil
}
