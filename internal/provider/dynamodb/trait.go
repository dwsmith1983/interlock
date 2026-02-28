package dynamodb

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// PutTrait stores a trait evaluation result with TTL.
func (p *DynamoDBProvider) PutTrait(ctx context.Context, pipelineID string, trait types.TraitEvaluation, ttl time.Duration) error {
	data, err := json.Marshal(trait)
	if err != nil {
		return err
	}

	item := map[string]ddbtypes.AttributeValue{
		"PK":   &ddbtypes.AttributeValueMemberS{Value: pipelinePK(pipelineID)},
		"SK":   &ddbtypes.AttributeValueMemberS{Value: traitSK(trait.TraitType)},
		"data": &ddbtypes.AttributeValueMemberS{Value: string(data)},
	}
	if ttl > 0 {
		item["ttl"] = &ddbtypes.AttributeValueMemberN{Value: fmt.Sprintf("%d", ttlEpoch(ttl))}
	}

	_, err = p.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &p.tableName,
		Item:      item,
	})
	if err != nil {
		return err
	}

	// Dual-write: append history record (best-effort, non-transactional).
	histItem := map[string]ddbtypes.AttributeValue{
		"PK":   &ddbtypes.AttributeValueMemberS{Value: pipelinePK(pipelineID)},
		"SK":   &ddbtypes.AttributeValueMemberS{Value: traitHistSK(trait.TraitType, trait.EvaluatedAt)},
		"data": &ddbtypes.AttributeValueMemberS{Value: string(data)},
	}
	if p.retentionTTL > 0 {
		histItem["ttl"] = &ddbtypes.AttributeValueMemberN{Value: fmt.Sprintf("%d", ttlEpoch(p.retentionTTL))}
	}
	if _, err := p.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &p.tableName,
		Item:      histItem,
	}); err != nil {
		p.logger.Warn("failed to write trait history", "pipeline", pipelineID, "trait", trait.TraitType, "error", err)
	}

	return nil
}

// GetTrait retrieves a single trait evaluation.
func (p *DynamoDBProvider) GetTrait(ctx context.Context, pipelineID, traitType string) (*types.TraitEvaluation, error) {
	out, err := p.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: &p.tableName,
		Key: map[string]ddbtypes.AttributeValue{
			"PK": &ddbtypes.AttributeValueMemberS{Value: pipelinePK(pipelineID)},
			"SK": &ddbtypes.AttributeValueMemberS{Value: traitSK(traitType)},
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
	var te types.TraitEvaluation
	if err := json.Unmarshal([]byte(data), &te); err != nil {
		return nil, err
	}
	return &te, nil
}

// GetTraits retrieves all trait evaluations for a pipeline.
func (p *DynamoDBProvider) GetTraits(ctx context.Context, pipelineID string) ([]types.TraitEvaluation, error) {
	out, err := p.client.Query(ctx, &dynamodb.QueryInput{
		TableName:              &p.tableName,
		KeyConditionExpression: aws.String("PK = :pk AND begins_with(SK, :prefix)"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":pk":     &ddbtypes.AttributeValueMemberS{Value: pipelinePK(pipelineID)},
			":prefix": &ddbtypes.AttributeValueMemberS{Value: prefixTrait},
		},
	})
	if err != nil {
		return nil, err
	}

	var traits []types.TraitEvaluation
	for _, item := range out.Items {
		ttlVal, _ := extractTTL(item)
		if isExpired(ttlVal) {
			continue
		}

		data, err := attributeStr(item, "data")
		if err != nil {
			p.logger.Warn("skipping corrupt trait data", "error", err)
			continue
		}

		// Verify SK has trait prefix to avoid matching non-trait items.
		sk, _ := attributeStr(item, "SK")
		if !strings.HasPrefix(sk, prefixTrait) {
			continue
		}

		var te types.TraitEvaluation
		if err := json.Unmarshal([]byte(data), &te); err != nil {
			p.logger.Warn("skipping corrupt trait data", "error", err)
			continue
		}
		traits = append(traits, te)
	}
	return traits, nil
}
