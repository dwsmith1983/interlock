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

const prefixReplay = "REPLAY#"

func replayPK(pipelineID, date, scheduleID string) string {
	return fmt.Sprintf("%s%s#%s#%s", prefixReplay, pipelineID, date, scheduleID)
}

func replayCreatedSK(ts time.Time) string {
	return fmt.Sprintf("CREATED#%d", ts.UnixMilli())
}

// PutReplay stores a replay request record.
func (p *DynamoDBProvider) PutReplay(ctx context.Context, entry types.ReplayRequest) error {
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	pk := replayPK(entry.PipelineID, entry.Date, entry.ScheduleID)
	sk := replayCreatedSK(entry.CreatedAt)
	ttl := fmt.Sprintf("%d", ttlEpoch(30*24*time.Hour))

	_, err = p.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &p.tableName,
		Item: map[string]ddbtypes.AttributeValue{
			"PK":     &ddbtypes.AttributeValueMemberS{Value: pk},
			"SK":     &ddbtypes.AttributeValueMemberS{Value: sk},
			"data":   &ddbtypes.AttributeValueMemberS{Value: string(data)},
			"GSI1PK": &ddbtypes.AttributeValueMemberS{Value: prefixType + "replay"},
			"GSI1SK": &ddbtypes.AttributeValueMemberS{Value: entry.CreatedAt.UTC().Format(time.RFC3339Nano)},
			"ttl":    &ddbtypes.AttributeValueMemberN{Value: ttl},
		},
	})
	return err
}

// GetReplay retrieves the most recent replay request for a pipeline/date/schedule.
func (p *DynamoDBProvider) GetReplay(ctx context.Context, pipelineID, date, scheduleID string) (*types.ReplayRequest, error) {
	pk := replayPK(pipelineID, date, scheduleID)

	out, err := p.client.Query(ctx, &dynamodb.QueryInput{
		TableName:              &p.tableName,
		KeyConditionExpression: aws.String("PK = :pk AND begins_with(SK, :prefix)"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":pk":     &ddbtypes.AttributeValueMemberS{Value: pk},
			":prefix": &ddbtypes.AttributeValueMemberS{Value: "CREATED#"},
		},
		ScanIndexForward: aws.Bool(false),
		Limit:            aws.Int32(1),
	})
	if err != nil {
		return nil, err
	}
	if len(out.Items) == 0 {
		return nil, nil
	}

	ttlVal, _ := extractTTL(out.Items[0])
	if isExpired(ttlVal) {
		return nil, nil
	}

	data, err := attributeStr(out.Items[0], "data")
	if err != nil {
		return nil, err
	}
	var req types.ReplayRequest
	if err := json.Unmarshal([]byte(data), &req); err != nil {
		return nil, err
	}
	return &req, nil
}

// ListReplays returns recent replay requests across all pipelines via GSI1.
func (p *DynamoDBProvider) ListReplays(ctx context.Context, limit int) ([]types.ReplayRequest, error) {
	if limit <= 0 {
		limit = 50
	}

	out, err := p.client.Query(ctx, &dynamodb.QueryInput{
		TableName:              &p.tableName,
		IndexName:              aws.String("GSI1"),
		KeyConditionExpression: aws.String("GSI1PK = :pk"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":pk": &ddbtypes.AttributeValueMemberS{Value: prefixType + "replay"},
		},
		ScanIndexForward: aws.Bool(false),
		Limit:            aws.Int32(int32(limit)),
	})
	if err != nil {
		return nil, err
	}

	var replays []types.ReplayRequest
	for _, item := range out.Items {
		ttlVal, _ := extractTTL(item)
		if isExpired(ttlVal) {
			continue
		}
		data, err := attributeStr(item, "data")
		if err != nil {
			p.logger.Warn("skipping corrupt replay data", "error", err)
			continue
		}
		var req types.ReplayRequest
		if err := json.Unmarshal([]byte(data), &req); err != nil {
			p.logger.Warn("skipping corrupt replay data", "error", err)
			continue
		}
		replays = append(replays, req)
	}
	return replays, nil
}
