package dynamodb

import (
	"context"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/interlock/pkg/types"
)

const (
	prefixControl = "CONTROL#"
	skStatus      = "STATUS"
)

// GetControlStatus reads the CONTROL# health record for a pipeline.
// Returns nil if no record exists (pipeline has never been monitored).
func (p *DynamoDBProvider) GetControlStatus(ctx context.Context, pipelineID string) (*types.ControlRecord, error) {
	out, err := p.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: &p.tableName,
		Key: map[string]ddbtypes.AttributeValue{
			"PK": &ddbtypes.AttributeValueMemberS{Value: prefixControl + pipelineID},
			"SK": &ddbtypes.AttributeValueMemberS{Value: skStatus},
		},
	})
	if err != nil {
		return nil, err
	}
	if out.Item == nil {
		return nil, nil
	}

	record := &types.ControlRecord{
		PipelineID: pipelineID,
		Enabled:    true, // default enabled if record exists
	}

	if v, ok := out.Item["consecutiveFailures"]; ok {
		if n, ok := v.(*ddbtypes.AttributeValueMemberN); ok {
			record.ConsecutiveFailures, _ = strconv.Atoi(n.Value)
		}
	}
	if v, ok := out.Item["lastStatus"]; ok {
		if s, ok := v.(*ddbtypes.AttributeValueMemberS); ok {
			record.LastStatus = s.Value
		}
	}
	if v, ok := out.Item["lastSuccessfulRun"]; ok {
		if s, ok := v.(*ddbtypes.AttributeValueMemberS); ok {
			record.LastSuccessfulRun = s.Value
		}
	}
	if v, ok := out.Item["lastFailedRun"]; ok {
		if s, ok := v.(*ddbtypes.AttributeValueMemberS); ok {
			record.LastFailedRun = s.Value
		}
	}
	if v, ok := out.Item["enabled"]; ok {
		if b, ok := v.(*ddbtypes.AttributeValueMemberBOOL); ok {
			record.Enabled = b.Value
		}
	}

	return record, nil
}
