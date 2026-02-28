package dynamodb

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// PutQuarantineRecord stores a quarantine record.
func (p *DynamoDBProvider) PutQuarantineRecord(ctx context.Context, record types.QuarantineRecord) error {
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}

	_, err = p.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &p.tableName,
		Item: map[string]ddbtypes.AttributeValue{
			"PK":   &ddbtypes.AttributeValueMemberS{Value: pipelinePK(record.PipelineID)},
			"SK":   &ddbtypes.AttributeValueMemberS{Value: quarantineSK(record.Date, record.Hour)},
			"data": &ddbtypes.AttributeValueMemberS{Value: string(data)},
			"ttl":  &ddbtypes.AttributeValueMemberN{Value: fmt.Sprintf("%d", ttlEpoch(30*24*time.Hour))},
		},
	})
	return err
}

// GetQuarantineRecord retrieves a quarantine record for a pipeline, date, and hour.
func (p *DynamoDBProvider) GetQuarantineRecord(ctx context.Context, pipelineID, date, hour string) (*types.QuarantineRecord, error) {
	out, err := p.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: &p.tableName,
		Key: map[string]ddbtypes.AttributeValue{
			"PK": &ddbtypes.AttributeValueMemberS{Value: pipelinePK(pipelineID)},
			"SK": &ddbtypes.AttributeValueMemberS{Value: quarantineSK(date, hour)},
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
	var record types.QuarantineRecord
	if err := json.Unmarshal([]byte(data), &record); err != nil {
		return nil, err
	}
	return &record, nil
}
