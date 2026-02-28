package dynamodb

import (
	"context"
	"crypto/rand"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// generateToken produces a UUID v4 string using crypto/rand.
func generateToken() (string, error) {
	var uuid [16]byte
	if _, err := rand.Read(uuid[:]); err != nil {
		return "", err
	}
	uuid[6] = (uuid[6] & 0x0f) | 0x40 // version 4
	uuid[8] = (uuid[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:16]), nil
}

// AcquireLock attempts to acquire a distributed lock with the given key and TTL.
// Uses a conditional PutItem that succeeds only if the lock doesn't exist or has expired.
// Returns a non-empty owner token on success, or "" if the lock was not acquired.
func (p *DynamoDBProvider) AcquireLock(ctx context.Context, key string, ttl time.Duration) (string, error) {
	token, err := generateToken()
	if err != nil {
		return "", fmt.Errorf("generating lock token: %w", err)
	}

	now := fmt.Sprintf("%d", time.Now().Unix())
	ttlVal := fmt.Sprintf("%d", ttlEpoch(ttl))

	_, err = p.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &p.tableName,
		Item: map[string]ddbtypes.AttributeValue{
			"PK":    &ddbtypes.AttributeValueMemberS{Value: lockPK(key)},
			"SK":    &ddbtypes.AttributeValueMemberS{Value: lockSK()},
			"ttl":   &ddbtypes.AttributeValueMemberN{Value: ttlVal},
			"token": &ddbtypes.AttributeValueMemberS{Value: token},
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
			return "", nil
		}
		return "", err
	}
	return token, nil
}

// ReleaseLock releases a distributed lock only if the token matches the current owner.
// If the token does not match (e.g. lock expired and was re-acquired), this is a no-op.
func (p *DynamoDBProvider) ReleaseLock(ctx context.Context, key string, token string) error {
	_, err := p.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: &p.tableName,
		Key: map[string]ddbtypes.AttributeValue{
			"PK": &ddbtypes.AttributeValueMemberS{Value: lockPK(key)},
			"SK": &ddbtypes.AttributeValueMemberS{Value: lockSK()},
		},
		ConditionExpression: aws.String("#token = :token"),
		ExpressionAttributeNames: map[string]string{
			"#token": "token",
		},
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":token": &ddbtypes.AttributeValueMemberS{Value: token},
		},
	})
	if err != nil {
		if isConditionalCheckFailed(err) {
			p.logger.Warn("lock release skipped: token mismatch or lock expired",
				"key", key)
			return nil
		}
		return err
	}
	return nil
}
