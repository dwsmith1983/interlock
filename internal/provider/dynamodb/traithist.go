package dynamodb

import (
	"context"
	"encoding/json"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// ListTraitHistory returns historical trait evaluations for a pipeline and trait type,
// newest first.
func (p *DynamoDBProvider) ListTraitHistory(ctx context.Context, pipelineID, traitType string, limit int) ([]types.TraitEvaluation, error) {
	prefix := prefixTraitHist + traitType + "#"
	out, err := p.client.Query(ctx, &dynamodb.QueryInput{
		TableName:              &p.tableName,
		KeyConditionExpression: aws.String("PK = :pk AND begins_with(SK, :prefix)"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":pk":     &ddbtypes.AttributeValueMemberS{Value: pipelinePK(pipelineID)},
			":prefix": &ddbtypes.AttributeValueMemberS{Value: prefix},
		},
		ScanIndexForward: aws.Bool(false),
		Limit:            aws.Int32(int32(limit)),
	})
	if err != nil {
		return nil, err
	}

	var evals []types.TraitEvaluation
	for _, item := range out.Items {
		data, err := attributeStr(item, "data")
		if err != nil {
			p.logger.Warn("skipping corrupt trait history entry", "error", err)
			continue
		}
		var te types.TraitEvaluation
		if err := json.Unmarshal([]byte(data), &te); err != nil {
			p.logger.Warn("skipping corrupt trait history entry", "error", err)
			continue
		}
		evals = append(evals, te)
	}
	return evals, nil
}
