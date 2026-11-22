package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/interlock/pkg/types"
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
	now := s.now()

	rec := types.RerunRecord{
		PK:          types.PipelinePK(pipelineID),
		SK:          types.RerunSK(schedule, date, attempt),
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

// DeleteRerun removes the rerun row of one attempt, written by WriteRerun.
// An absent row is not an error.
func (s *Store) DeleteRerun(ctx context.Context, pipelineID, schedule, date string, attempt int) error {
	_, err := s.Client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: &s.RerunTable,
		Key: map[string]ddbtypes.AttributeValue{
			"PK": &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
			"SK": &ddbtypes.AttributeValueMemberS{Value: types.RerunSK(schedule, date, attempt)},
		},
	})
	if err != nil {
		return fmt.Errorf("delete rerun %q/%s/%s attempt %d: %w", pipelineID, schedule, date, attempt, err)
	}
	return nil
}

// WriteRerunOnce writes the rerun record for one execution, keyed by its
// name (SK RERUN#<schedule>#<date>#<execName>), unless that row already
// exists. It returns false when the row was already there, so a retried
// caller adds no second row. CountReruns and CountRerunsBySource count it like
// any other rerun row.
func (s *Store) WriteRerunOnce(ctx context.Context, pipelineID, schedule, date, execName, reason, sourceJobEvent string) (bool, error) {
	now := s.now()
	item, err := attributevalue.MarshalMap(types.RerunRecord{
		PK:             types.PipelinePK(pipelineID),
		SK:             "RERUN#" + schedule + "#" + date + "#" + execName,
		Reason:         reason,
		TriggeredAt:    now,
		SourceJobEvent: sourceJobEvent,
		TTL:            now.Add(rerunTTL).Unix(),
	})
	if err != nil {
		return false, fmt.Errorf("marshal rerun %q/%s/%s %s: %w", pipelineID, schedule, date, execName, err)
	}
	_, err = s.Client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:           &s.RerunTable,
		Item:                item,
		ConditionExpression: aws.String("attribute_not_exists(PK)"),
	})
	if err != nil {
		var ccfe *ddbtypes.ConditionalCheckFailedException
		if errors.As(err, &ccfe) {
			return false, nil
		}
		return false, fmt.Errorf("put rerun %q/%s/%s %s: %w", pipelineID, schedule, date, execName, err)
	}
	return true, nil
}

// CountReruns returns the number of rerun records for a pipeline schedule/date.
// It uses a count-only query (no item data returned) and handles pagination.
func (s *Store) CountReruns(ctx context.Context, pipelineID, schedule, date string) (int, error) {
	prefix := fmt.Sprintf("RERUN#%s#%s#", schedule, date)
	total, err := QueryCount(ctx, s.Client, func(startKey map[string]ddbtypes.AttributeValue) *dynamodb.QueryInput {
		return &dynamodb.QueryInput{
			TableName:              &s.RerunTable,
			Select:                 ddbtypes.SelectCount,
			KeyConditionExpression: aws.String("PK = :pk AND begins_with(SK, :prefix)"),
			ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
				":pk":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
				":prefix": &ddbtypes.AttributeValueMemberS{Value: prefix},
			},
			ExclusiveStartKey: startKey,
			ConsistentRead:    aws.Bool(true), // the count is the next rerun's attempt, which names its execution
		}
	})
	if err != nil {
		return 0, fmt.Errorf("count reruns %q/%s/%s: %w", pipelineID, schedule, date, err)
	}
	return total, nil
}

// CountRerunsBySource counts rerun records whose reason matches one of the
// given sources. Returns 0 immediately if sources is empty (avoids invalid
// DynamoDB FilterExpression).
func (s *Store) CountRerunsBySource(ctx context.Context, pipelineID, schedule, date string, sources []string) (int, error) {
	if len(sources) == 0 {
		return 0, nil
	}

	prefix := fmt.Sprintf("RERUN#%s#%s#", schedule, date)

	// Build FilterExpression: reason IN (:s0, :s1, ...)
	var placeholders []string
	filterValues := map[string]ddbtypes.AttributeValue{
		":pk":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
		":prefix": &ddbtypes.AttributeValueMemberS{Value: prefix},
	}
	for i, src := range sources {
		key := fmt.Sprintf(":s%d", i)
		placeholders = append(placeholders, key)
		filterValues[key] = &ddbtypes.AttributeValueMemberS{Value: src}
	}
	filterExpr := fmt.Sprintf("reason IN (%s)", strings.Join(placeholders, ", "))

	total, err := QueryCount(ctx, s.Client, func(startKey map[string]ddbtypes.AttributeValue) *dynamodb.QueryInput {
		return &dynamodb.QueryInput{
			TableName:                 &s.RerunTable,
			Select:                    ddbtypes.SelectCount,
			KeyConditionExpression:    aws.String("PK = :pk AND begins_with(SK, :prefix)"),
			FilterExpression:          aws.String(filterExpr),
			ExpressionAttributeValues: filterValues,
			ExclusiveStartKey:         startKey,
			ConsistentRead:            aws.Bool(true),
		}
	})
	if err != nil {
		return 0, fmt.Errorf("count reruns by source %q/%s/%s: %w", pipelineID, schedule, date, err)
	}
	return total, nil
}
