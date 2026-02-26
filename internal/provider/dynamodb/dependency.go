package dynamodb

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// PutDependency records that downstreamID depends on upstreamID.
func (p *DynamoDBProvider) PutDependency(ctx context.Context, upstreamID, downstreamID string) error {
	_, err := p.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &p.tableName,
		Item: map[string]ddbtypes.AttributeValue{
			"PK": &ddbtypes.AttributeValueMemberS{Value: depPK(upstreamID)},
			"SK": &ddbtypes.AttributeValueMemberS{Value: depSK(downstreamID)},
		},
	})
	return err
}

// RemoveDependency deletes a dependency link.
func (p *DynamoDBProvider) RemoveDependency(ctx context.Context, upstreamID, downstreamID string) error {
	_, err := p.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: &p.tableName,
		Key: map[string]ddbtypes.AttributeValue{
			"PK": &ddbtypes.AttributeValueMemberS{Value: depPK(upstreamID)},
			"SK": &ddbtypes.AttributeValueMemberS{Value: depSK(downstreamID)},
		},
	})
	return err
}

// ListDependents returns all downstream pipeline IDs that depend on upstreamID.
func (p *DynamoDBProvider) ListDependents(ctx context.Context, upstreamID string) ([]string, error) {
	out, err := p.client.Query(ctx, &dynamodb.QueryInput{
		TableName:              &p.tableName,
		KeyConditionExpression: aws.String("PK = :pk AND begins_with(SK, :prefix)"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":pk":     &ddbtypes.AttributeValueMemberS{Value: depPK(upstreamID)},
			":prefix": &ddbtypes.AttributeValueMemberS{Value: prefixDep},
		},
	})
	if err != nil {
		return nil, err
	}

	var dependents []string
	for _, item := range out.Items {
		sk, err := attributeStr(item, "SK")
		if err != nil {
			continue
		}
		// SK is "DEP#<downstreamID>"
		if len(sk) > len(prefixDep) {
			dependents = append(dependents, sk[len(prefixDep):])
		}
	}
	return dependents, nil
}
