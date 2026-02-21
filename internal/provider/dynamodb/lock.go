package dynamodb

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// AcquireLock attempts to acquire a distributed lock with the given key and TTL.
// Uses a conditional PutItem that succeeds only if the lock doesn't exist or has expired.
func (p *DynamoDBProvider) AcquireLock(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	now := fmt.Sprintf("%d", time.Now().Unix())
	ttlVal := fmt.Sprintf("%d", ttlEpoch(ttl))

	_, err := p.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &p.tableName,
		Item: map[string]ddbtypes.AttributeValue{
			"PK":  &ddbtypes.AttributeValueMemberS{Value: lockPK(key)},
			"SK":  &ddbtypes.AttributeValueMemberS{Value: lockSK()},
			"ttl": &ddbtypes.AttributeValueMemberN{Value: ttlVal},
		},
		ConditionExpression: aws.String("attribute_not_exists(PK) OR #ttl < :now"),
		ExpressionAttributeNames: map[string]string{
			"#ttl": "ttl",
		},
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":now": &ddbtypes.AttributeValueMemberN{Value: now},
		},
	})
	if err != nil {
		if isConditionalCheckFailed(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// ReleaseLock releases a distributed lock.
func (p *DynamoDBProvider) ReleaseLock(ctx context.Context, key string) error {
	_, err := p.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: &p.tableName,
		Key: map[string]ddbtypes.AttributeValue{
			"PK": &ddbtypes.AttributeValueMemberS{Value: lockPK(key)},
			"SK": &ddbtypes.AttributeValueMemberS{Value: lockSK()},
		},
	})
	return err
}
