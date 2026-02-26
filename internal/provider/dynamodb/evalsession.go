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

// PutEvaluationSession stores an evaluation session record.
func (p *DynamoDBProvider) PutEvaluationSession(ctx context.Context, session types.EvaluationSession) error {
	data, err := json.Marshal(session)
	if err != nil {
		return err
	}

	sk := evalSessionSK(session.SessionID, session.StartedAt)
	item := map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: pipelinePK(session.PipelineID)},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: sk},
		"GSI1PK": &ddbtypes.AttributeValueMemberS{Value: prefixType + "evalsession"},
		"GSI1SK": &ddbtypes.AttributeValueMemberS{Value: session.StartedAt.UTC().Format("2006-01-02T15:04:05.000Z")},
		"data":   &ddbtypes.AttributeValueMemberS{Value: string(data)},
		"sid":    &ddbtypes.AttributeValueMemberS{Value: session.SessionID},
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

// GetEvaluationSession retrieves a session by ID. This scans the GSI since
// we don't know the pipeline at lookup time.
func (p *DynamoDBProvider) GetEvaluationSession(ctx context.Context, sessionID string) (*types.EvaluationSession, error) {
	// Use GSI1 to find the session by scanning type#evalsession entries.
	// For production scale, a secondary index on sessionID would be better,
	// but for the expected cardinality this is acceptable.
	out, err := p.client.Query(ctx, &dynamodb.QueryInput{
		TableName:              &p.tableName,
		IndexName:              aws.String("GSI1"),
		KeyConditionExpression: aws.String("GSI1PK = :pk"),
		FilterExpression:       aws.String("sid = :sid"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":pk":  &ddbtypes.AttributeValueMemberS{Value: prefixType + "evalsession"},
			":sid": &ddbtypes.AttributeValueMemberS{Value: sessionID},
		},
		Limit: aws.Int32(1),
	})
	if err != nil {
		return nil, err
	}
	if len(out.Items) == 0 {
		return nil, nil
	}

	data, err := attributeStr(out.Items[0], "data")
	if err != nil {
		return nil, err
	}
	var session types.EvaluationSession
	if err := json.Unmarshal([]byte(data), &session); err != nil {
		return nil, err
	}
	return &session, nil
}

// ListEvaluationSessions returns recent sessions for a pipeline, newest first.
func (p *DynamoDBProvider) ListEvaluationSessions(ctx context.Context, pipelineID string, limit int) ([]types.EvaluationSession, error) {
	out, err := p.client.Query(ctx, &dynamodb.QueryInput{
		TableName:              &p.tableName,
		KeyConditionExpression: aws.String("PK = :pk AND begins_with(SK, :prefix)"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":pk":     &ddbtypes.AttributeValueMemberS{Value: pipelinePK(pipelineID)},
			":prefix": &ddbtypes.AttributeValueMemberS{Value: prefixEvalSession},
		},
		ScanIndexForward: aws.Bool(false),
		Limit:            aws.Int32(int32(limit)),
	})
	if err != nil {
		return nil, err
	}

	var sessions []types.EvaluationSession
	for _, item := range out.Items {
		data, err := attributeStr(item, "data")
		if err != nil {
			p.logger.Warn("skipping corrupt eval session", "error", err)
			continue
		}
		var s types.EvaluationSession
		if err := json.Unmarshal([]byte(data), &s); err != nil {
			p.logger.Warn("skipping corrupt eval session", "error", err)
			continue
		}
		sessions = append(sessions, s)
	}
	return sessions, nil
}
