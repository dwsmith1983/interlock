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

	"github.com/dwsmith1983/interlock/pkg/types"
)

// ScanConfigs reads all CONFIG rows from the control table.
// Returns a map of pipeline ID to PipelineConfig.
func (s *Store) ScanConfigs(ctx context.Context) (map[string]*types.PipelineConfig, error) {
	configs := make(map[string]*types.PipelineConfig)

	err := ScanAll(ctx, s.Client, func(startKey map[string]ddbtypes.AttributeValue) *dynamodb.ScanInput {
		return &dynamodb.ScanInput{
			TableName:        &s.ControlTable,
			FilterExpression: aws.String("SK = :sk"),
			ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
				":sk": &ddbtypes.AttributeValueMemberS{Value: types.ConfigSK},
			},
			ExclusiveStartKey: startKey,
		}
	}, func(items []map[string]ddbtypes.AttributeValue) error {
		for _, item := range items {
			pkAttr, ok := item["PK"]
			if !ok {
				return fmt.Errorf("row missing PK attribute")
			}
			pkStr, ok := pkAttr.(*ddbtypes.AttributeValueMemberS)
			if !ok {
				return fmt.Errorf("PK is not a string")
			}

			const prefix = "PIPELINE#"
			if len(pkStr.Value) <= len(prefix) {
				return fmt.Errorf("invalid PK %q", pkStr.Value)
			}
			pipelineID := pkStr.Value[len(prefix):]

			configAttr, ok := item["config"]
			if !ok {
				return fmt.Errorf("config attribute missing for %q", pipelineID)
			}
			configStr, ok := configAttr.(*ddbtypes.AttributeValueMemberS)
			if !ok {
				return fmt.Errorf("config is not a string for %q", pipelineID)
			}

			var cfg types.PipelineConfig
			if err := json.Unmarshal([]byte(configStr.Value), &cfg); err != nil {
				return fmt.Errorf("unmarshal config for %q: %w", pipelineID, err)
			}
			configs[pipelineID] = &cfg
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("scan configs: %w", err)
	}
	return configs, nil
}

// GetConfig retrieves the pipeline configuration from the control table.
// Returns nil, nil if the item does not exist.
func (s *Store) GetConfig(ctx context.Context, pipelineID string) (*types.PipelineConfig, error) {
	out, err := s.Client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: &s.ControlTable,
		Key: map[string]ddbtypes.AttributeValue{
			"PK": &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
			"SK": &ddbtypes.AttributeValueMemberS{Value: types.ConfigSK},
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

	var cfg types.PipelineConfig
	if err := json.Unmarshal([]byte(configStr.Value), &cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config for %q: %w", pipelineID, err)
	}
	return &cfg, nil
}

// PutConfig writes a pipeline configuration to the control table.
func (s *Store) PutConfig(ctx context.Context, cfg types.PipelineConfig) error {
	data, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshal config for %q: %w", cfg.Pipeline.ID, err)
	}

	_, err = s.Client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &s.ControlTable,
		Item: map[string]ddbtypes.AttributeValue{
			"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(cfg.Pipeline.ID)},
			"SK":     &ddbtypes.AttributeValueMemberS{Value: types.ConfigSK},
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
			"PK": &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
			"SK": &ddbtypes.AttributeValueMemberS{Value: types.SensorSK(sensorKey)},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("get sensor %q for %q: %w", sensorKey, pipelineID, err)
	}
	if out.Item == nil {
		return nil, nil
	}

	var rec types.ControlRecord
	if err := attributevalue.UnmarshalMap(out.Item, &rec); err != nil {
		return nil, fmt.Errorf("unmarshal sensor %q for %q: %w", sensorKey, pipelineID, err)
	}
	return rec.Data, nil
}

// GetAllSensors retrieves all sensor rows for a pipeline.
// Returns a map of sensor key to data. The sensor key is the raw SK suffix
// (e.g. "SENSOR#upstream-complete" -> key "upstream-complete").
func (s *Store) GetAllSensors(ctx context.Context, pipelineID string) (map[string]map[string]interface{}, error) {
	result := make(map[string]map[string]interface{})

	err := QueryAll(ctx, s.Client, func(startKey map[string]ddbtypes.AttributeValue) *dynamodb.QueryInput {
		return &dynamodb.QueryInput{
			TableName:              &s.ControlTable,
			ConsistentRead:         aws.Bool(true),
			KeyConditionExpression: aws.String("PK = :pk AND begins_with(SK, :prefix)"),
			ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
				":pk":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
				":prefix": &ddbtypes.AttributeValueMemberS{Value: "SENSOR#"},
			},
			ExclusiveStartKey: startKey,
		}
	}, func(items []map[string]ddbtypes.AttributeValue) error {
		for _, item := range items {
			var rec types.ControlRecord
			if err := attributevalue.UnmarshalMap(item, &rec); err != nil {
				return fmt.Errorf("unmarshal sensor row: %w", err)
			}
			const prefix = "SENSOR#"
			if len(rec.SK) > len(prefix) {
				key := rec.SK[len(prefix):]
				result[key] = rec.Data
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("query sensors for %q: %w", pipelineID, err)
	}
	return result, nil
}

// WriteSensor writes (or overwrites) a sensor row in the control table.
func (s *Store) WriteSensor(ctx context.Context, pipelineID, sensorKey string, data map[string]interface{}) error {
	rec := types.ControlRecord{
		PK:   types.PipelinePK(pipelineID),
		SK:   types.SensorSK(sensorKey),
		Data: data,
	}

	item, err := attributevalue.MarshalMap(rec)
	if err != nil {
		return fmt.Errorf("marshal sensor %q for %q: %w", sensorKey, pipelineID, err)
	}

	_, err = s.Client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &s.ControlTable,
		Item:      item,
	})
	if err != nil {
		return fmt.Errorf("write sensor %q for %q: %w", sensorKey, pipelineID, err)
	}
	return nil
}

// DeleteSensor removes a sensor row from the control table.
func (s *Store) DeleteSensor(ctx context.Context, pipelineID, sensorKey string) error {
	_, err := s.Client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: &s.ControlTable,
		Key: map[string]ddbtypes.AttributeValue{
			"PK": &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
			"SK": &ddbtypes.AttributeValueMemberS{Value: types.SensorSK(sensorKey)},
		},
	})
	if err != nil {
		return fmt.Errorf("delete sensor %q for %q: %w", sensorKey, pipelineID, err)
	}
	return nil
}

// WriteRerunRequest writes a RERUN_REQUEST row to the control table.
// The stream-router picks this up via DynamoDB streams and validates
// via the circuit breaker before starting a new SFN execution.
func (s *Store) WriteRerunRequest(ctx context.Context, pipelineID, schedule, date, reason string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.Client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &s.ControlTable,
		Item: map[string]ddbtypes.AttributeValue{
			"PK":          &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
			"SK":          &ddbtypes.AttributeValueMemberS{Value: types.RerunRequestSK(schedule, date)},
			"reason":      &ddbtypes.AttributeValueMemberS{Value: reason},
			"requestedAt": &ddbtypes.AttributeValueMemberS{Value: now},
		},
	})
	if err != nil {
		return fmt.Errorf("write rerun request for %q/%s/%s: %w", pipelineID, schedule, date, err)
	}
	return nil
}

// AcquireTriggerLock attempts to acquire a trigger lock for a given pipeline,
// schedule, and date. Returns true if the lock was acquired, false if it is
// already held. The lock is set with a TTL for automatic expiration.
func (s *Store) AcquireTriggerLock(ctx context.Context, pipelineID, schedule, date string, ttl time.Duration) (bool, error) {
	ttlEpoch := time.Now().Add(ttl).Unix()

	_, err := s.Client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &s.ControlTable,
		Item: map[string]ddbtypes.AttributeValue{
			"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
			"SK":     &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK(schedule, date)},
			"status": &ddbtypes.AttributeValueMemberS{Value: types.TriggerStatusRunning},
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
			"PK": &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
			"SK": &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK(schedule, date)},
		},
	})
	if err != nil {
		return fmt.Errorf("release trigger lock %q/%s/%s: %w", pipelineID, schedule, date, err)
	}
	return nil
}

// ScanRunningTriggers returns all TRIGGER# rows with status=RUNNING.
func (s *Store) ScanRunningTriggers(ctx context.Context) ([]types.ControlRecord, error) {
	var records []types.ControlRecord

	err := ScanAll(ctx, s.Client, func(startKey map[string]ddbtypes.AttributeValue) *dynamodb.ScanInput {
		return &dynamodb.ScanInput{
			TableName:        &s.ControlTable,
			FilterExpression: aws.String("begins_with(SK, :prefix) AND #status = :running"),
			ExpressionAttributeNames: map[string]string{
				"#status": "status",
			},
			ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
				":prefix":  &ddbtypes.AttributeValueMemberS{Value: "TRIGGER#"},
				":running": &ddbtypes.AttributeValueMemberS{Value: types.TriggerStatusRunning},
			},
			ExclusiveStartKey: startKey,
		}
	}, func(items []map[string]ddbtypes.AttributeValue) error {
		for _, item := range items {
			var rec types.ControlRecord
			if err := attributevalue.UnmarshalMap(item, &rec); err != nil {
				return fmt.Errorf("unmarshal trigger row: %w", err)
			}
			records = append(records, rec)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("scan running triggers: %w", err)
	}
	return records, nil
}

// GetTrigger reads a TRIGGER# row for a specific pipeline/schedule/date.
// Returns nil, nil if the item does not exist.
func (s *Store) GetTrigger(ctx context.Context, pipelineID, schedule, date string) (*types.ControlRecord, error) {
	out, err := s.Client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: &s.ControlTable,
		Key: map[string]ddbtypes.AttributeValue{
			"PK": &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
			"SK": &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK(schedule, date)},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("get trigger %q/%s/%s: %w", pipelineID, schedule, date, err)
	}
	if out.Item == nil {
		return nil, nil
	}

	var rec types.ControlRecord
	if err := attributevalue.UnmarshalMap(out.Item, &rec); err != nil {
		return nil, fmt.Errorf("unmarshal trigger %q/%s/%s: %w", pipelineID, schedule, date, err)
	}
	return &rec, nil
}

// HasTriggerForDate returns true if any TRIGGER# row exists for the given
// pipeline, schedule, and date prefix. This handles both daily triggers
// (date="2026-03-04") and per-hour triggers (date="2026-03-04T00", etc.)
// by matching the SK prefix "TRIGGER#<schedule>#<datePrefix>".
func (s *Store) HasTriggerForDate(ctx context.Context, pipelineID, schedule, datePrefix string) (bool, error) {
	skPrefix := "TRIGGER#" + schedule + "#" + datePrefix
	out, err := s.Client.Query(ctx, &dynamodb.QueryInput{
		TableName:              &s.ControlTable,
		KeyConditionExpression: aws.String("PK = :pk AND begins_with(SK, :skPrefix)"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":pk":       &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
			":skPrefix": &ddbtypes.AttributeValueMemberS{Value: skPrefix},
		},
		Limit:  aws.Int32(1),
		Select: ddbtypes.SelectCount,
	})
	if err != nil {
		return false, fmt.Errorf("query triggers for %q/%s/%s: %w", pipelineID, schedule, datePrefix, err)
	}
	return out.Count > 0, nil
}

// ResetTriggerLock refreshes the TTL on an existing trigger lock without
// releasing it. This extends the lock window for long-running jobs that need
// more time than the original TTL allowed. Returns (true, nil) if the row
// existed and was updated, (false, nil) if the row does not exist.
func (s *Store) ResetTriggerLock(ctx context.Context, pipelineID, schedule, date string, ttl time.Duration) (bool, error) {
	ttlEpoch := fmt.Sprintf("%d", time.Now().Add(ttl).Unix())
	_, err := s.Client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: &s.ControlTable,
		Key: map[string]ddbtypes.AttributeValue{
			"PK": &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
			"SK": &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK(schedule, date)},
		},
		ConditionExpression: aws.String("attribute_exists(PK)"),
		UpdateExpression:    aws.String("SET #status = :running, #ttl = :ttl"),
		ExpressionAttributeNames: map[string]string{
			"#status": "status",
			"#ttl":    "ttl",
		},
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":running": &ddbtypes.AttributeValueMemberS{Value: types.TriggerStatusRunning},
			":ttl":     &ddbtypes.AttributeValueMemberN{Value: ttlEpoch},
		},
	})
	if err != nil {
		var ccfe *ddbtypes.ConditionalCheckFailedException
		if errors.As(err, &ccfe) {
			return false, nil
		}
		return false, fmt.Errorf("reset trigger lock %q/%s/%s: %w", pipelineID, schedule, date, err)
	}
	return true, nil
}

// SetTriggerStatus updates only the status attribute of an existing trigger row,
// preserving TTL and other attributes.
func (s *Store) SetTriggerStatus(ctx context.Context, pipelineID, schedule, date, status string) error {
	expr := "SET #status = :s"
	names := map[string]string{
		"#status": "status",
	}

	// Terminal statuses remove the TTL so DynamoDB doesn't auto-delete the
	// trigger record. Without this, TTL expiry removes the record and the
	// watchdog reconcile loop re-triggers completed pipelines.
	if status == types.TriggerStatusCompleted || status == types.TriggerStatusFailedFinal {
		expr = "SET #status = :s REMOVE #ttl"
		names["#ttl"] = "ttl"
	}

	_, err := s.Client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: &s.ControlTable,
		Key: map[string]ddbtypes.AttributeValue{
			"PK": &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
			"SK": &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK(schedule, date)},
		},
		UpdateExpression:         aws.String(expr),
		ExpressionAttributeNames: names,
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":s": &ddbtypes.AttributeValueMemberS{Value: status},
		},
	})
	if err != nil {
		return fmt.Errorf("set trigger status %q/%s/%s: %w", pipelineID, schedule, date, err)
	}
	return nil
}
