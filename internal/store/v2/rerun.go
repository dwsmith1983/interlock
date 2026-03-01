package store

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	v2 "github.com/dwsmith1983/interlock/pkg/types/v2"
)

const rerunTTL = 30 * 24 * time.Hour // 30 days

// WriteRerun appends a new re-run record for a pipeline schedule/date.
// It determines the attempt number by counting existing reruns (0-indexed).
// Returns the attempt number assigned to this rerun.
func (s *Store) WriteRerun(ctx context.Context, pipelineID, schedule, date, reason, sourceJobEvent string) (int, error) {
	count, err := s.CountReruns(ctx, pipelineID, schedule, date)
	if err != nil {
		return 0, fmt.Errorf("write rerun %q/%s/%s: %w", pipelineID, schedule, date, err)
	}

	attempt := count
	now := time.Now()

	rec := v2.RerunRecord{
		PK:          v2.PipelinePK(pipelineID),
		SK:          v2.RerunSK(schedule, date, attempt),
		Reason:      reason,
		TriggeredAt: now,
		TTL:         now.Add(rerunTTL).Unix(),
	}
	if sourceJobEvent != "" {
		rec.SourceJobEvent = sourceJobEvent
	}

	item, err := attributevalue.MarshalMap(rec)
	if err != nil {
		return 0, fmt.Errorf("marshal rerun %q/%s/%s: %w", pipelineID, schedule, date, err)
	}

	_, err = s.Client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &s.RerunTable,
		Item:      item,
	})
	if err != nil {
		return 0, fmt.Errorf("put rerun %q/%s/%s: %w", pipelineID, schedule, date, err)
	}

	return attempt, nil
}

// CountReruns returns the number of rerun records for a pipeline schedule/date.
// It uses a count-only query (no item data returned) and handles pagination.
func (s *Store) CountReruns(ctx context.Context, pipelineID, schedule, date string) (int, error) {
	prefix := fmt.Sprintf("RERUN#%s#%s#", schedule, date)
	total := 0

	var startKey map[string]ddbtypes.AttributeValue
	for {
		out, err := s.Client.Query(ctx, &dynamodb.QueryInput{
			TableName:              &s.RerunTable,
			Select:                 ddbtypes.SelectCount,
			KeyConditionExpression: aws.String("PK = :pk AND begins_with(SK, :prefix)"),
			ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
				":pk":     &ddbtypes.AttributeValueMemberS{Value: v2.PipelinePK(pipelineID)},
				":prefix": &ddbtypes.AttributeValueMemberS{Value: prefix},
			},
			ExclusiveStartKey: startKey,
		})
		if err != nil {
			return 0, fmt.Errorf("count reruns %q/%s/%s: %w", pipelineID, schedule, date, err)
		}

		total += int(out.Count)

		if out.LastEvaluatedKey == nil {
			break
		}
		startKey = out.LastEvaluatedKey
	}

	return total, nil
}
