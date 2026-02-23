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

const prefixLateArrival = "LATEARRIVAL#"

func lateArrivalSK(date, scheduleID string, ts time.Time) string {
	return fmt.Sprintf("%s%s#%s#%d", prefixLateArrival, date, scheduleID, ts.UnixMilli())
}

// PutLateArrival stores a late-arrival detection record.
func (p *DynamoDBProvider) PutLateArrival(ctx context.Context, entry types.LateArrival) error {
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	sk := lateArrivalSK(entry.Date, entry.ScheduleID, entry.DetectedAt)
	ttl := fmt.Sprintf("%d", ttlEpoch(30*24*time.Hour))

	_, err = p.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &p.tableName,
		Item: map[string]ddbtypes.AttributeValue{
			"PK":   &ddbtypes.AttributeValueMemberS{Value: pipelinePK(entry.PipelineID)},
			"SK":   &ddbtypes.AttributeValueMemberS{Value: sk},
			"data": &ddbtypes.AttributeValueMemberS{Value: string(data)},
			"ttl":  &ddbtypes.AttributeValueMemberN{Value: ttl},
		},
	})
	return err
}

// ListLateArrivals returns late-arrival records for a pipeline/date/schedule.
func (p *DynamoDBProvider) ListLateArrivals(ctx context.Context, pipelineID, date, scheduleID string) ([]types.LateArrival, error) {
	prefix := fmt.Sprintf("%s%s#%s#", prefixLateArrival, date, scheduleID)

	out, err := p.client.Query(ctx, &dynamodb.QueryInput{
		TableName:              &p.tableName,
		KeyConditionExpression: aws.String("PK = :pk AND begins_with(SK, :prefix)"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":pk":     &ddbtypes.AttributeValueMemberS{Value: pipelinePK(pipelineID)},
			":prefix": &ddbtypes.AttributeValueMemberS{Value: prefix},
		},
		ScanIndexForward: aws.Bool(false),
	})
	if err != nil {
		return nil, err
	}

	var arrivals []types.LateArrival
	for _, item := range out.Items {
		ttlVal, _ := attributeInt(item)
		if isExpired(ttlVal) {
			continue
		}
		data, err := attributeStr(item, "data")
		if err != nil {
			p.logger.Warn("skipping corrupt late arrival data", "error", err)
			continue
		}
		var la types.LateArrival
		if err := json.Unmarshal([]byte(data), &la); err != nil {
			p.logger.Warn("skipping corrupt late arrival data", "error", err)
			continue
		}
		arrivals = append(arrivals, la)
	}
	return arrivals, nil
}
