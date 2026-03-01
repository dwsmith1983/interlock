package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	v2 "github.com/dwsmith1983/interlock/pkg/types/v2"
)

// GetConfig retrieves the pipeline configuration from the control table.
// Returns nil, nil if the item does not exist.
func (s *Store) GetConfig(ctx context.Context, pipelineID string) (*v2.PipelineConfig, error) {
	out, err := s.Client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: &s.ControlTable,
		Key: map[string]ddbtypes.AttributeValue{
			"PK": &ddbtypes.AttributeValueMemberS{Value: v2.PipelinePK(pipelineID)},
			"SK": &ddbtypes.AttributeValueMemberS{Value: v2.ConfigSK},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("get config for %q: %w", pipelineID, err)
	}
	if out.Item == nil {
		return nil, nil
	}

	configAttr, ok := out.Item["config"]
	if !ok {
		return nil, fmt.Errorf("config attribute missing for %q", pipelineID)
	}
	configStr, ok := configAttr.(*ddbtypes.AttributeValueMemberS)
	if !ok {
		return nil, fmt.Errorf("config attribute is not a string for %q", pipelineID)
	}

	var cfg v2.PipelineConfig
	if err := json.Unmarshal([]byte(configStr.Value), &cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config for %q: %w", pipelineID, err)
	}
	return &cfg, nil
}

// PutConfig writes a pipeline configuration to the control table.
func (s *Store) PutConfig(ctx context.Context, cfg v2.PipelineConfig) error {
	data, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshal config for %q: %w", cfg.Pipeline.ID, err)
	}

	_, err = s.Client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &s.ControlTable,
		Item: map[string]ddbtypes.AttributeValue{
			"PK":     &ddbtypes.AttributeValueMemberS{Value: v2.PipelinePK(cfg.Pipeline.ID)},
			"SK":     &ddbtypes.AttributeValueMemberS{Value: v2.ConfigSK},
			"config": &ddbtypes.AttributeValueMemberS{Value: string(data)},
		},
	})
	if err != nil {
		return fmt.Errorf("put config for %q: %w", cfg.Pipeline.ID, err)
	}
	return nil
}

// GetSensorData retrieves sensor data for a given pipeline and sensor key.
// Returns nil, nil if the item does not exist.
func (s *Store) GetSensorData(ctx context.Context, pipelineID, sensorKey string) (map[string]interface{}, error) {
	out, err := s.Client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      &s.ControlTable,
		ConsistentRead: aws.Bool(true),
		Key: map[string]ddbtypes.AttributeValue{
			"PK": &ddbtypes.AttributeValueMemberS{Value: v2.PipelinePK(pipelineID)},
			"SK": &ddbtypes.AttributeValueMemberS{Value: v2.SensorSK(sensorKey)},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("get sensor %q for %q: %w", sensorKey, pipelineID, err)
	}
	if out.Item == nil {
		return nil, nil
	}

	var rec v2.ControlRecord
	if err := attributevalue.UnmarshalMap(out.Item, &rec); err != nil {
		return nil, fmt.Errorf("unmarshal sensor %q for %q: %w", sensorKey, pipelineID, err)
	}
	return rec.Data, nil
}

// GetAllSensors retrieves all sensor rows for a pipeline.
// Returns a map of sensor key to data. The sensor key is the raw SK suffix
// (e.g. "SENSOR#upstream-complete" -> key "upstream-complete").
func (s *Store) GetAllSensors(ctx context.Context, pipelineID string) (map[string]map[string]interface{}, error) {
	out, err := s.Client.Query(ctx, &dynamodb.QueryInput{
		TableName:              &s.ControlTable,
		ConsistentRead:         aws.Bool(true),
		KeyConditionExpression: aws.String("PK = :pk AND begins_with(SK, :prefix)"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":pk":     &ddbtypes.AttributeValueMemberS{Value: v2.PipelinePK(pipelineID)},
			":prefix": &ddbtypes.AttributeValueMemberS{Value: "SENSOR#"},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("query sensors for %q: %w", pipelineID, err)
	}

	result := make(map[string]map[string]interface{}, len(out.Items))
	for _, item := range out.Items {
		var rec v2.ControlRecord
		if err := attributevalue.UnmarshalMap(item, &rec); err != nil {
			continue // skip corrupt rows
		}
		// Extract the sensor key from the SK: "SENSOR#<key>"
		const prefix = "SENSOR#"
		if len(rec.SK) > len(prefix) {
			key := rec.SK[len(prefix):]
			result[key] = rec.Data
		}
	}
	return result, nil
}

// AcquireTriggerLock attempts to acquire a trigger lock for a given pipeline,
// schedule, and date. Returns true if the lock was acquired, false if it is
// already held. The lock is set with a TTL for automatic expiration.
func (s *Store) AcquireTriggerLock(ctx context.Context, pipelineID, schedule, date string, ttl time.Duration) (bool, error) {
	ttlEpoch := time.Now().Add(ttl).Unix()

	_, err := s.Client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &s.ControlTable,
		Item: map[string]ddbtypes.AttributeValue{
			"PK":     &ddbtypes.AttributeValueMemberS{Value: v2.PipelinePK(pipelineID)},
			"SK":     &ddbtypes.AttributeValueMemberS{Value: v2.TriggerSK(schedule, date)},
			"status": &ddbtypes.AttributeValueMemberS{Value: v2.TriggerStatusRunning},
			"ttl":    &ddbtypes.AttributeValueMemberN{Value: fmt.Sprintf("%d", ttlEpoch)},
		},
		ConditionExpression: aws.String("attribute_not_exists(PK)"),
	})
	if err != nil {
		var ccfe *ddbtypes.ConditionalCheckFailedException
		if errors.As(err, &ccfe) {
			return false, nil
		}
		return false, fmt.Errorf("acquire trigger lock %q/%s/%s: %w", pipelineID, schedule, date, err)
	}
	return true, nil
}

// ReleaseTriggerLock removes the trigger lock row for a given pipeline, schedule, and date.
func (s *Store) ReleaseTriggerLock(ctx context.Context, pipelineID, schedule, date string) error {
	_, err := s.Client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: &s.ControlTable,
		Key: map[string]ddbtypes.AttributeValue{
			"PK": &ddbtypes.AttributeValueMemberS{Value: v2.PipelinePK(pipelineID)},
			"SK": &ddbtypes.AttributeValueMemberS{Value: v2.TriggerSK(schedule, date)},
		},
	})
	if err != nil {
		return fmt.Errorf("release trigger lock %q/%s/%s: %w", pipelineID, schedule, date, err)
	}
	return nil
}

// SetTriggerStatus writes or overwrites the trigger row with the given status.
func (s *Store) SetTriggerStatus(ctx context.Context, pipelineID, schedule, date, status string) error {
	_, err := s.Client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &s.ControlTable,
		Item: map[string]ddbtypes.AttributeValue{
			"PK":     &ddbtypes.AttributeValueMemberS{Value: v2.PipelinePK(pipelineID)},
			"SK":     &ddbtypes.AttributeValueMemberS{Value: v2.TriggerSK(schedule, date)},
			"status": &ddbtypes.AttributeValueMemberS{Value: status},
		},
	})
	if err != nil {
		return fmt.Errorf("set trigger status %q/%s/%s: %w", pipelineID, schedule, date, err)
	}
	return nil
}
