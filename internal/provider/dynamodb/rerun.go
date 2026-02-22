package dynamodb

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// PutRerun stores a rerun record using dual-write: truth item + list copy.
func (p *DynamoDBProvider) PutRerun(ctx context.Context, record types.RerunRecord) error {
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}

	ttl := fmt.Sprintf("%d", ttlEpoch(30*24*time.Hour))
	gsi1sk := record.RequestedAt.UTC().Format(time.RFC3339Nano)

	_, err = p.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: []ddbtypes.TransactWriteItem{
			{
				Put: &ddbtypes.Put{
					TableName: &p.tableName,
					Item: map[string]ddbtypes.AttributeValue{
						"PK":     &ddbtypes.AttributeValueMemberS{Value: rerunPK(record.RerunID)},
						"SK":     &ddbtypes.AttributeValueMemberS{Value: rerunSK(record.RerunID)},
						"data":   &ddbtypes.AttributeValueMemberS{Value: string(data)},
						"GSI1PK": &ddbtypes.AttributeValueMemberS{Value: prefixType + "rerun"},
						"GSI1SK": &ddbtypes.AttributeValueMemberS{Value: gsi1sk},
						"ttl":    &ddbtypes.AttributeValueMemberN{Value: ttl},
					},
				},
			},
			{
				Put: &ddbtypes.Put{
					TableName: &p.tableName,
					Item: map[string]ddbtypes.AttributeValue{
						"PK":     &ddbtypes.AttributeValueMemberS{Value: pipelinePK(record.PipelineID)},
						"SK":     &ddbtypes.AttributeValueMemberS{Value: rerunSK(record.RerunID)},
						"data":   &ddbtypes.AttributeValueMemberS{Value: string(data)},
						"GSI1PK": &ddbtypes.AttributeValueMemberS{Value: prefixType + "rerun"},
						"GSI1SK": &ddbtypes.AttributeValueMemberS{Value: gsi1sk},
						"ttl":    &ddbtypes.AttributeValueMemberN{Value: ttl},
					},
				},
			},
		},
	})
	return err
}

// GetRerun retrieves a rerun record from the truth item.
func (p *DynamoDBProvider) GetRerun(ctx context.Context, rerunID string) (*types.RerunRecord, error) {
	out, err := p.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      &p.tableName,
		ConsistentRead: aws.Bool(true),
		Key: map[string]ddbtypes.AttributeValue{
			"PK": &ddbtypes.AttributeValueMemberS{Value: rerunPK(rerunID)},
			"SK": &ddbtypes.AttributeValueMemberS{Value: rerunSK(rerunID)},
		},
	})
	if err != nil {
		return nil, err
	}
	if out.Item == nil {
		return nil, nil
	}

	ttlVal, _ := attributeInt(out.Item, "ttl")
	if isExpired(ttlVal) {
		return nil, nil
	}

	data, err := attributeStr(out.Item, "data")
	if err != nil {
		return nil, err
	}
	var record types.RerunRecord
	if err := json.Unmarshal([]byte(data), &record); err != nil {
		return nil, err
	}
	return &record, nil
}

// ListReruns returns recent rerun records for a pipeline, newest first.
func (p *DynamoDBProvider) ListReruns(ctx context.Context, pipelineID string, limit int) ([]types.RerunRecord, error) {
	if limit <= 0 {
		limit = 20
	}

	out, err := p.client.Query(ctx, &dynamodb.QueryInput{
		TableName:              &p.tableName,
		KeyConditionExpression: aws.String("PK = :pk AND begins_with(SK, :prefix)"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":pk":     &ddbtypes.AttributeValueMemberS{Value: pipelinePK(pipelineID)},
			":prefix": &ddbtypes.AttributeValueMemberS{Value: prefixRerun},
		},
		ScanIndexForward: aws.Bool(false),
		Limit:            aws.Int32(int32(limit)),
	})
	if err != nil {
		return nil, err
	}

	return p.unmarshalReruns(out.Items)
}

// ListAllReruns returns recent rerun records across all pipelines via GSI1.
func (p *DynamoDBProvider) ListAllReruns(ctx context.Context, limit int) ([]types.RerunRecord, error) {
	if limit <= 0 {
		limit = 50
	}

	out, err := p.client.Query(ctx, &dynamodb.QueryInput{
		TableName:              &p.tableName,
		IndexName:              aws.String("GSI1"),
		KeyConditionExpression: aws.String("GSI1PK = :pk"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":pk": &ddbtypes.AttributeValueMemberS{Value: prefixType + "rerun"},
		},
		ScanIndexForward: aws.Bool(false),
		Limit:            aws.Int32(int32(limit)),
	})
	if err != nil {
		return nil, err
	}

	return p.unmarshalReruns(out.Items)
}

func (p *DynamoDBProvider) unmarshalReruns(items []map[string]ddbtypes.AttributeValue) ([]types.RerunRecord, error) {
	// Deduplicate: GSI1 query may return both truth + list copy items.
	seen := make(map[string]struct{})
	var records []types.RerunRecord

	for _, item := range items {
		ttlVal, _ := attributeInt(item, "ttl")
		if isExpired(ttlVal) {
			continue
		}
		data, err := attributeStr(item, "data")
		if err != nil {
			p.logger.Warn("skipping corrupt rerun data", "error", err)
			continue
		}
		var record types.RerunRecord
		if err := json.Unmarshal([]byte(data), &record); err != nil {
			p.logger.Warn("skipping corrupt rerun data", "error", err)
			continue
		}
		if _, dup := seen[record.RerunID]; dup {
			continue
		}
		seen[record.RerunID] = struct{}{}
		records = append(records, record)
	}
	return records, nil
}
