package dynamodb

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// RegisterPipeline stores a pipeline configuration.
func (p *DynamoDBProvider) RegisterPipeline(ctx context.Context, config types.PipelineConfig) error {
	data, err := json.Marshal(config)
	if err != nil {
		return fmt.Errorf("marshaling pipeline: %w", err)
	}

	_, err = p.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &p.tableName,
		Item: map[string]ddbtypes.AttributeValue{
			"PK":     &ddbtypes.AttributeValueMemberS{Value: pipelinePK(config.Name)},
			"SK":     &ddbtypes.AttributeValueMemberS{Value: configSK()},
			"GSI1PK": &ddbtypes.AttributeValueMemberS{Value: prefixType + "pipeline"},
			"GSI1SK": &ddbtypes.AttributeValueMemberS{Value: pipelinePK(config.Name)},
			"data":   &ddbtypes.AttributeValueMemberS{Value: string(data)},
		},
	})
	return err
}

// GetPipeline retrieves a pipeline configuration.
func (p *DynamoDBProvider) GetPipeline(ctx context.Context, id string) (*types.PipelineConfig, error) {
	out, err := p.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: &p.tableName,
		Key: map[string]ddbtypes.AttributeValue{
			"PK": &ddbtypes.AttributeValueMemberS{Value: pipelinePK(id)},
			"SK": &ddbtypes.AttributeValueMemberS{Value: configSK()},
		},
	})
	if err != nil {
		return nil, err
	}
	if out.Item == nil {
		return nil, fmt.Errorf("pipeline %q not found", id)
	}

	data, err := attributeStr(out.Item, "data")
	if err != nil {
		return nil, err
	}
	var config types.PipelineConfig
	if err := json.Unmarshal([]byte(data), &config); err != nil {
		return nil, err
	}
	return &config, nil
}

// ListPipelines returns all registered pipelines via GSI1.
func (p *DynamoDBProvider) ListPipelines(ctx context.Context) ([]types.PipelineConfig, error) {
	out, err := p.client.Query(ctx, &dynamodb.QueryInput{
		TableName:              &p.tableName,
		IndexName:              aws.String("GSI1"),
		KeyConditionExpression: aws.String("GSI1PK = :pk"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":pk": &ddbtypes.AttributeValueMemberS{Value: prefixType + "pipeline"},
		},
	})
	if err != nil {
		return nil, err
	}

	var pipelines []types.PipelineConfig
	for _, item := range out.Items {
		data, err := attributeStr(item, "data")
		if err != nil {
			p.logger.Warn("skipping corrupt pipeline entry", "error", err)
			continue
		}
		var config types.PipelineConfig
		if err := json.Unmarshal([]byte(data), &config); err != nil {
			p.logger.Warn("skipping corrupt pipeline data", "error", err)
			continue
		}
		pipelines = append(pipelines, config)
	}
	return pipelines, nil
}

// DeletePipeline removes a pipeline configuration.
func (p *DynamoDBProvider) DeletePipeline(ctx context.Context, id string) error {
	_, err := p.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: &p.tableName,
		Key: map[string]ddbtypes.AttributeValue{
			"PK": &ddbtypes.AttributeValueMemberS{Value: pipelinePK(id)},
			"SK": &ddbtypes.AttributeValueMemberS{Value: configSK()},
		},
	})
	return err
}

// attributeStr extracts a string attribute from a DynamoDB item.
func attributeStr(item map[string]ddbtypes.AttributeValue, key string) (string, error) {
	av, ok := item[key]
	if !ok {
		return "", fmt.Errorf("missing attribute %q", key)
	}
	var s string
	if err := attributevalue.Unmarshal(av, &s); err != nil {
		return "", fmt.Errorf("unmarshaling %q: %w", key, err)
	}
	return s, nil
}

// attributeInt extracts the "ttl" integer attribute from a DynamoDB item.
func attributeInt(item map[string]ddbtypes.AttributeValue) (int64, error) {
	av, ok := item["ttl"]
	if !ok {
		return 0, nil
	}
	var n int64
	if err := attributevalue.Unmarshal(av, &n); err != nil {
		return 0, fmt.Errorf("unmarshaling %q: %w", "ttl", err)
	}
	return n, nil
}
