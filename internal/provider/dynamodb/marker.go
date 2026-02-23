package dynamodb

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const prefixMarker = "MARKER#"

// WriteCascadeMarker writes a MARKER# record that the stream-router picks up
// to trigger downstream pipeline evaluation.
func (p *DynamoDBProvider) WriteCascadeMarker(ctx context.Context, pipelineID, scheduleID, date, source string) error {
	now := time.Now()
	millis := now.UnixMilli()
	sk := fmt.Sprintf("%scascade#%s#%d", prefixMarker, source, millis)
	ttl := fmt.Sprintf("%d", ttlEpoch(30*24*time.Hour))

	_, err := p.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &p.tableName,
		Item: map[string]ddbtypes.AttributeValue{
			"PK":         &ddbtypes.AttributeValueMemberS{Value: pipelinePK(pipelineID)},
			"SK":         &ddbtypes.AttributeValueMemberS{Value: sk},
			"scheduleID": &ddbtypes.AttributeValueMemberS{Value: scheduleID},
			"date":       &ddbtypes.AttributeValueMemberS{Value: date},
			"source":     &ddbtypes.AttributeValueMemberS{Value: source},
			"ttl":        &ddbtypes.AttributeValueMemberN{Value: ttl},
		},
	})
	return err
}
