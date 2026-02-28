package dynamodb

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// PutReadiness stores a readiness result with TTL.
func (p *DynamoDBProvider) PutReadiness(ctx context.Context, result types.ReadinessResult) error {
	data, err := json.Marshal(result)
	if err != nil {
		return err
	}

	_, err = p.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &p.tableName,
		Item: map[string]ddbtypes.AttributeValue{
			"PK":   &ddbtypes.AttributeValueMemberS{Value: pipelinePK(result.PipelineID)},
			"SK":   &ddbtypes.AttributeValueMemberS{Value: readinessSK()},
			"data": &ddbtypes.AttributeValueMemberS{Value: string(data)},
			"ttl":  &ddbtypes.AttributeValueMemberN{Value: fmt.Sprintf("%d", ttlEpoch(p.readinessTTL))},
		},
	})
	return err
}

// GetReadiness retrieves a cached readiness result.
func (p *DynamoDBProvider) GetReadiness(ctx context.Context, pipelineID string) (*types.ReadinessResult, error) {
	out, err := p.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: &p.tableName,
		Key: map[string]ddbtypes.AttributeValue{
			"PK": &ddbtypes.AttributeValueMemberS{Value: pipelinePK(pipelineID)},
			"SK": &ddbtypes.AttributeValueMemberS{Value: readinessSK()},
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
	var result types.ReadinessResult
	if err := json.Unmarshal([]byte(data), &result); err != nil {
		return nil, err
	}
	return &result, nil
}
