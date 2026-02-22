package dynamodb

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/interlock/internal/lifecycle"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// runKeyTTL returns the TTL for a run-related key based on status.
func (p *DynamoDBProvider) runKeyTTL(status types.RunStatus) time.Duration {
	if lifecycle.IsTerminal(status) {
		return p.retentionTTL
	}
	return p.retentionTTL + 24*time.Hour
}

// PutRunState stores a run state using dual-write: truth item + list copy.
func (p *DynamoDBProvider) PutRunState(ctx context.Context, run types.RunState) error {
	data, err := json.Marshal(run)
	if err != nil {
		return err
	}

	ttl := fmt.Sprintf("%d", ttlEpoch(p.runKeyTTL(run.Status)))

	_, err = p.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: []ddbtypes.TransactWriteItem{
			{
				Put: &ddbtypes.Put{
					TableName: &p.tableName,
					Item: map[string]ddbtypes.AttributeValue{
						"PK":      &ddbtypes.AttributeValueMemberS{Value: runPK(run.RunID)},
						"SK":      &ddbtypes.AttributeValueMemberS{Value: runTruthSK(run.RunID)},
						"data":    &ddbtypes.AttributeValueMemberS{Value: string(data)},
						"version": &ddbtypes.AttributeValueMemberN{Value: fmt.Sprintf("%d", run.Version)},
						"ttl":     &ddbtypes.AttributeValueMemberN{Value: ttl},
					},
				},
			},
			{
				Put: &ddbtypes.Put{
					TableName: &p.tableName,
					Item: map[string]ddbtypes.AttributeValue{
						"PK":   &ddbtypes.AttributeValueMemberS{Value: pipelinePK(run.PipelineID)},
						"SK":   &ddbtypes.AttributeValueMemberS{Value: runListSK(run.CreatedAt, run.RunID)},
						"data": &ddbtypes.AttributeValueMemberS{Value: string(data)},
						"ttl":  &ddbtypes.AttributeValueMemberN{Value: ttl},
					},
				},
			},
		},
	})
	return err
}

// GetRunState retrieves a run state from the truth item (strongly consistent).
func (p *DynamoDBProvider) GetRunState(ctx context.Context, runID string) (*types.RunState, error) {
	out, err := p.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      &p.tableName,
		ConsistentRead: aws.Bool(true),
		Key: map[string]ddbtypes.AttributeValue{
			"PK": &ddbtypes.AttributeValueMemberS{Value: runPK(runID)},
			"SK": &ddbtypes.AttributeValueMemberS{Value: runTruthSK(runID)},
		},
	})
	if err != nil {
		return nil, err
	}
	if out.Item == nil {
		return nil, fmt.Errorf("run %q not found", runID)
	}

	ttlVal, _ := attributeInt(out.Item, "ttl")
	if isExpired(ttlVal) {
		return nil, fmt.Errorf("run %q not found", runID)
	}

	data, err := attributeStr(out.Item, "data")
	if err != nil {
		return nil, err
	}
	var run types.RunState
	if err := json.Unmarshal([]byte(data), &run); err != nil {
		return nil, err
	}
	return &run, nil
}

// ListRuns returns recent runs for a pipeline, newest first.
func (p *DynamoDBProvider) ListRuns(ctx context.Context, pipelineID string, limit int) ([]types.RunState, error) {
	if limit <= 0 {
		limit = 10
	}

	out, err := p.client.Query(ctx, &dynamodb.QueryInput{
		TableName:              &p.tableName,
		KeyConditionExpression: aws.String("PK = :pk AND begins_with(SK, :prefix)"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":pk":     &ddbtypes.AttributeValueMemberS{Value: pipelinePK(pipelineID)},
			":prefix": &ddbtypes.AttributeValueMemberS{Value: prefixRun},
		},
		ScanIndexForward: aws.Bool(false),
		Limit:            aws.Int32(int32(limit)),
	})
	if err != nil {
		return nil, err
	}

	var runs []types.RunState
	for _, item := range out.Items {
		ttlVal, _ := attributeInt(item, "ttl")
		if isExpired(ttlVal) {
			continue
		}
		data, err := attributeStr(item, "data")
		if err != nil {
			p.logger.Warn("skipping corrupt run data", "error", err)
			continue
		}
		var run types.RunState
		if err := json.Unmarshal([]byte(data), &run); err != nil {
			p.logger.Warn("skipping corrupt run data", "error", err)
			continue
		}
		runs = append(runs, run)
	}
	return runs, nil
}

// CompareAndSwapRunState atomically updates a run state if the version matches.
func (p *DynamoDBProvider) CompareAndSwapRunState(ctx context.Context, runID string, expectedVersion int, newState types.RunState) (bool, error) {
	data, err := json.Marshal(newState)
	if err != nil {
		return false, err
	}

	ttl := fmt.Sprintf("%d", ttlEpoch(p.runKeyTTL(newState.Status)))

	// Update truth item with condition on version.
	_, err = p.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: &p.tableName,
		Key: map[string]ddbtypes.AttributeValue{
			"PK": &ddbtypes.AttributeValueMemberS{Value: runPK(runID)},
			"SK": &ddbtypes.AttributeValueMemberS{Value: runTruthSK(runID)},
		},
		UpdateExpression: aws.String("SET #data = :data, #version = :newVersion, #ttl = :ttl"),
		ConditionExpression: aws.String("#version = :expectedVersion"),
		ExpressionAttributeNames: map[string]string{
			"#data":    "data",
			"#version": "version",
			"#ttl":     "ttl",
		},
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":data":            &ddbtypes.AttributeValueMemberS{Value: string(data)},
			":newVersion":      &ddbtypes.AttributeValueMemberN{Value: fmt.Sprintf("%d", newState.Version)},
			":expectedVersion": &ddbtypes.AttributeValueMemberN{Value: fmt.Sprintf("%d", expectedVersion)},
			":ttl":             &ddbtypes.AttributeValueMemberN{Value: ttl},
		},
	})
	if err != nil {
		if isConditionalCheckFailed(err) {
			return false, nil
		}
		return false, err
	}

	// Best-effort update of the list copy.
	_, _ = p.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &p.tableName,
		Item: map[string]ddbtypes.AttributeValue{
			"PK":   &ddbtypes.AttributeValueMemberS{Value: pipelinePK(newState.PipelineID)},
			"SK":   &ddbtypes.AttributeValueMemberS{Value: runListSK(newState.CreatedAt, runID)},
			"data": &ddbtypes.AttributeValueMemberS{Value: string(data)},
			"ttl":  &ddbtypes.AttributeValueMemberN{Value: ttl},
		},
	})

	return true, nil
}
