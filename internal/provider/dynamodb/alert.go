package dynamodb

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// PutAlert persists an alert record.
func (p *DynamoDBProvider) PutAlert(ctx context.Context, alert types.Alert) error {
	if alert.AlertID == "" {
		alert.AlertID = fmt.Sprintf("%d", alert.Timestamp.UnixMilli())
	}
	data, err := json.Marshal(alert)
	if err != nil {
		return err
	}

	sk := alertSK(alert.Timestamp)
	item := map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: pipelinePK(alert.PipelineID)},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: sk},
		"GSI1PK": &ddbtypes.AttributeValueMemberS{Value: prefixType + "alert"},
		"GSI1SK": &ddbtypes.AttributeValueMemberS{Value: alert.Timestamp.UTC().Format("2006-01-02T15:04:05.000Z")},
		"data":   &ddbtypes.AttributeValueMemberS{Value: string(data)},
	}
	if p.retentionTTL > 0 {
		item["ttl"] = &ddbtypes.AttributeValueMemberN{Value: fmt.Sprintf("%d", ttlEpoch(p.retentionTTL))}
	}

	_, err = p.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &p.tableName,
		Item:      item,
	})
	return err
}

// ListAlerts returns recent alerts for a pipeline, newest first.
func (p *DynamoDBProvider) ListAlerts(ctx context.Context, pipelineID string, limit int) ([]types.Alert, error) {
	out, err := p.client.Query(ctx, &dynamodb.QueryInput{
		TableName:              &p.tableName,
		KeyConditionExpression: aws.String("PK = :pk AND begins_with(SK, :prefix)"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":pk":     &ddbtypes.AttributeValueMemberS{Value: pipelinePK(pipelineID)},
			":prefix": &ddbtypes.AttributeValueMemberS{Value: prefixAlert},
		},
		ScanIndexForward: aws.Bool(false),
		Limit:            aws.Int32(int32(limit)),
	})
	if err != nil {
		return nil, err
	}
	return unmarshalAlerts(out.Items, p)
}

// ListAllAlerts returns recent alerts across all pipelines, newest first.
func (p *DynamoDBProvider) ListAllAlerts(ctx context.Context, limit int) ([]types.Alert, error) {
	out, err := p.client.Query(ctx, &dynamodb.QueryInput{
		TableName:              &p.tableName,
		IndexName:              aws.String("GSI1"),
		KeyConditionExpression: aws.String("GSI1PK = :pk"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":pk": &ddbtypes.AttributeValueMemberS{Value: prefixType + "alert"},
		},
		ScanIndexForward: aws.Bool(false),
		Limit:            aws.Int32(int32(limit)),
	})
	if err != nil {
		return nil, err
	}
	return unmarshalAlerts(out.Items, p)
}

func unmarshalAlerts(items []map[string]ddbtypes.AttributeValue, p *DynamoDBProvider) ([]types.Alert, error) {
	var alerts []types.Alert
	for _, item := range items {
		data, err := attributeStr(item, "data")
		if err != nil {
			p.logger.Warn("skipping corrupt alert data", "error", err)
			continue
		}
		var a types.Alert
		if err := json.Unmarshal([]byte(data), &a); err != nil {
			p.logger.Warn("skipping corrupt alert data", "error", err)
			continue
		}
		alerts = append(alerts, a)
	}
	return alerts, nil
}
