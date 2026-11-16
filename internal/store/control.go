package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/dwsmith1983/interlock/pkg/validation"
)

// RejectedConfig is a CONFIG row ScanConfigs could not load.
type RejectedConfig struct {
	PK         string // the row's raw partition key
	PipelineID string // empty when PK has no "PIPELINE#<id>" form
	Reason     string
	Hash       string // configHash of the raw config string; empty when there is none
}

// configHash is the first 16 hex characters of the SHA-256 of s: short, but
// enough to tell one version of a config from the next.
func configHash(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])[:16]
}

// ScanConfigs reads all CONFIG rows from the control table. A row that does
// not load (bad PK, missing or non-string config, JSON that does not
// unmarshal, a config that fails validation) is returned as a rejection, so
// one team's bad config never hides another's. Only a DynamoDB error fails
// the scan.
func (s *Store) ScanConfigs(ctx context.Context) (map[string]*types.PipelineConfig, []RejectedConfig, error) {
	configs := make(map[string]*types.PipelineConfig)
	var rejected []RejectedConfig

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
			cfg, r := loadConfigRow(item)
			if cfg == nil {
				rejected = append(rejected, r)
				continue
			}
			configs[r.PipelineID] = cfg
		}
		return nil
	})
	if err != nil {
		return nil, nil, fmt.Errorf("scan configs: %w", err)
	}
	return configs, rejected, nil
}

// loadConfigRow parses and validates one CONFIG row. It returns the config, or
// nil and the rejection; PK and PipelineID are filled in either way.
func loadConfigRow(item map[string]ddbtypes.AttributeValue) (*types.PipelineConfig, RejectedConfig) {
	var r RejectedConfig
	if pk, ok := item["PK"].(*ddbtypes.AttributeValueMemberS); ok {
		r.PK = pk.Value
	}
	if id, ok := strings.CutPrefix(r.PK, "PIPELINE#"); ok && id != "" {
		r.PipelineID = id
	}
	raw, isString := item["config"].(*ddbtypes.AttributeValueMemberS)
	if isString {
		r.Hash = configHash(raw.Value)
	}
	var cfg types.PipelineConfig
	switch _, present := item["config"]; {
	case r.PipelineID == "":
		r.Reason = fmt.Sprintf("invalid PK %q", r.PK)
	case !present:
		r.Reason = "config attribute missing"
	case !isString:
		r.Reason = "config is not a string"
	default:
		if err := json.Unmarshal([]byte(raw.Value), &cfg); err != nil {
			r.Reason = "unmarshal config: " + err.Error()
		} else if cfg.Pipeline.ID != r.PipelineID {
			// Handlers take the ID from the PK, so the rule on pipeline.id
			// covers the PK only when the two match.
			r.Reason = fmt.Sprintf("PK id %q does not match pipeline.id %q", r.PipelineID, cfg.Pipeline.ID)
		} else if errs := validation.ValidatePipelineConfig(&cfg); len(errs) > 0 {
			r.Reason = strings.Join(errs, "; ")
		} else {
			return &cfg, r
		}
	}
	// Cut to maxReasonBytes; ToValidUTF8 drops a rune the cut splits.
	r.Reason = strings.ToValidUTF8(r.Reason[:min(len(r.Reason), maxReasonBytes)], "")
	return nil, r
}

// maxReasonBytes bounds a rejection reason, which can quote the config.
const maxReasonBytes = 1024

// configInvalidKey is the key of rejection r's marker: r's own PK and SK
// CONFIG_INVALID#<hash>, the hash of the reason when r has no config string.
func configInvalidKey(r RejectedConfig) map[string]ddbtypes.AttributeValue {
	hash := r.Hash
	if hash == "" {
		hash = configHash(r.Reason)
	}
	return map[string]ddbtypes.AttributeValue{
		"PK": &ddbtypes.AttributeValueMemberS{Value: r.PK},
		"SK": &ddbtypes.AttributeValueMemberS{Value: "CONFIG_INVALID#" + hash},
	}
}

// MarkConfigInvalid records that rejection r was reported, one marker per
// broken config version, expiring at now+ttl. It returns false and writes
// nothing while an unexpired marker exists; the condition reads the ttl
// because DynamoDB deletes expired rows only eventually.
func (s *Store) MarkConfigInvalid(ctx context.Context, r RejectedConfig, now time.Time, ttl time.Duration) (bool, error) {
	item := configInvalidKey(r)
	item["ttl"] = &ddbtypes.AttributeValueMemberN{Value: fmt.Sprintf("%d", now.Add(ttl).Unix())}
	_, err := s.Client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:                 &s.ControlTable,
		Item:                      item,
		ConditionExpression:       aws.String("attribute_not_exists(PK) OR #ttl < :now"),
		ExpressionAttributeNames:  map[string]string{"#ttl": "ttl"},
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{":now": &ddbtypes.AttributeValueMemberN{Value: fmt.Sprintf("%d", now.Unix())}},
	})
	if err != nil {
		var ccfe *ddbtypes.ConditionalCheckFailedException
		if errors.As(err, &ccfe) {
			return false, nil
		}
		return false, fmt.Errorf("mark config invalid for %q: %w", r.PK, err)
	}
	return true, nil
}

// ClearConfigInvalid deletes rejection r's marker, so r is reported again.
func (s *Store) ClearConfigInvalid(ctx context.Context, r RejectedConfig) error {
	if _, err := s.Client.DeleteItem(ctx, &dynamodb.DeleteItemInput{TableName: &s.ControlTable, Key: configInvalidKey(r)}); err != nil {
		return fmt.Errorf("clear config invalid marker for %q: %w", r.PK, err)
	}
	return nil
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

// ListRunKeys returns the pipeline's expected run dates (RUN# rows) from
// fromDate through toDate, hourly keys on toDate included, in SK order with
// the RUN# prefix stripped. The keys are returned as written, unvalidated.
func (s *Store) ListRunKeys(ctx context.Context, pipelineID, fromDate, toDate string) ([]string, error) {
	var keys []string
	err := QueryAll(ctx, s.Client, func(startKey map[string]ddbtypes.AttributeValue) *dynamodb.QueryInput {
		return &dynamodb.QueryInput{
			TableName:              &s.ControlTable,
			KeyConditionExpression: aws.String("PK = :pk AND SK BETWEEN :from AND :to"),
			ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
				":pk":   &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
				":from": &ddbtypes.AttributeValueMemberS{Value: types.RunSK(fromDate)},
				":to":   &ddbtypes.AttributeValueMemberS{Value: types.RunSK(toDate) + "~"},
			},
			ExclusiveStartKey: startKey,
		}
	}, func(items []map[string]ddbtypes.AttributeValue) error {
		for _, item := range items {
			var rec types.ControlRecord
			if err := attributevalue.UnmarshalMap(item, &rec); err != nil {
				return fmt.Errorf("unmarshal run row: %w", err)
			}
			keys = append(keys, strings.TrimPrefix(rec.SK, types.RunSK("")))
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("query run keys for %q: %w", pipelineID, err)
	}
	return keys, nil
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

// WriteSensorIfAbsent writes a sensor row only if it does not already exist.
// Returns true if the write succeeded (key was absent), false if the key
// already existed. This is used for idempotent first-occurrence recording.
func (s *Store) WriteSensorIfAbsent(ctx context.Context, pipelineID, sensorKey string, data map[string]interface{}) (bool, error) {
	rec := types.ControlRecord{
		PK:   types.PipelinePK(pipelineID),
		SK:   types.SensorSK(sensorKey),
		Data: data,
	}

	item, err := attributevalue.MarshalMap(rec)
	if err != nil {
		return false, fmt.Errorf("marshal sensor %q for %q: %w", sensorKey, pipelineID, err)
	}

	_, err = s.Client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:           &s.ControlTable,
		Item:                item,
		ConditionExpression: aws.String("attribute_not_exists(PK)"),
	})
	if err != nil {
		var ccfe *ddbtypes.ConditionalCheckFailedException
		if errors.As(err, &ccfe) {
			return false, nil
		}
		return false, fmt.Errorf("write sensor if absent %q for %q: %w", sensorKey, pipelineID, err)
	}
	return true, nil
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
	now := s.now().UTC().Format(time.RFC3339)
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

// WriteDryRunMarker writes a DRY_RUN# marker row with a 7-day TTL.
// The write is conditional (attribute_not_exists) so repeated calls are
// idempotent — the original triggeredAt is preserved.
func (s *Store) WriteDryRunMarker(ctx context.Context, pipelineID, schedule, date string, triggeredAt time.Time) (bool, error) {
	ttlEpoch := s.now().Add(7 * 24 * time.Hour).Unix()
	_, err := s.Client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &s.ControlTable,
		Item: map[string]ddbtypes.AttributeValue{
			"PK": &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
			"SK": &ddbtypes.AttributeValueMemberS{Value: types.DryRunSK(schedule, date)},
			"data": &ddbtypes.AttributeValueMemberM{Value: map[string]ddbtypes.AttributeValue{
				"triggeredAt": &ddbtypes.AttributeValueMemberS{Value: triggeredAt.UTC().Format(time.RFC3339)},
			}},
			"ttl": &ddbtypes.AttributeValueMemberN{Value: fmt.Sprintf("%d", ttlEpoch)},
		},
		ConditionExpression: aws.String("attribute_not_exists(PK)"),
	})
	if err != nil {
		var ccfe *ddbtypes.ConditionalCheckFailedException
		if errors.As(err, &ccfe) {
			return false, nil
		}
		return false, fmt.Errorf("write dry-run marker %q/%s/%s: %w", pipelineID, schedule, date, err)
	}
	return true, nil
}

// GetDryRunMarker reads the DRY_RUN# marker for a given pipeline/schedule/date.
// Returns nil, nil if no marker exists.
func (s *Store) GetDryRunMarker(ctx context.Context, pipelineID, schedule, date string) (*types.ControlRecord, error) {
	out, err := s.Client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      &s.ControlTable,
		ConsistentRead: aws.Bool(true),
		Key: map[string]ddbtypes.AttributeValue{
			"PK": &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
			"SK": &ddbtypes.AttributeValueMemberS{Value: types.DryRunSK(schedule, date)},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("get dry-run marker %q/%s/%s: %w", pipelineID, schedule, date, err)
	}
	if out.Item == nil {
		return nil, nil
	}

	var rec types.ControlRecord
	if err := attributevalue.UnmarshalMap(out.Item, &rec); err != nil {
		return nil, fmt.Errorf("unmarshal dry-run marker %q/%s/%s: %w", pipelineID, schedule, date, err)
	}
	return &rec, nil
}

// AcquireTriggerLock attempts to acquire a trigger lock for a given pipeline,
// schedule, and date. Returns true if the lock was acquired, false if it is
// already held. The lock is set with a TTL for automatic expiration.
func (s *Store) AcquireTriggerLock(ctx context.Context, pipelineID, schedule, date string, ttl time.Duration) (bool, error) {
	ttlEpoch := s.now().Add(ttl).Unix()

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
	ttlEpoch := fmt.Sprintf("%d", s.now().Add(ttl).Unix())
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

// RefreshRunningTriggerLock gives a RUNNING trigger row a fresh TTL. Unlike
// ResetTriggerLock it never changes the status: it returns (false, nil) when
// the row is missing or already COMPLETED or FAILED_FINAL.
func (s *Store) RefreshRunningTriggerLock(ctx context.Context, pipelineID, schedule, date string, ttl time.Duration) (bool, error) {
	_, err := s.Client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: &s.ControlTable,
		Key: map[string]ddbtypes.AttributeValue{
			"PK": &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
			"SK": &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK(schedule, date)},
		},
		ConditionExpression:      aws.String("attribute_exists(PK) AND #status = :running"),
		UpdateExpression:         aws.String("SET #ttl = :ttl"),
		ExpressionAttributeNames: map[string]string{"#status": "status", "#ttl": "ttl"},
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":running": &ddbtypes.AttributeValueMemberS{Value: types.TriggerStatusRunning},
			":ttl":     &ddbtypes.AttributeValueMemberN{Value: fmt.Sprintf("%d", s.now().Add(ttl).Unix())},
		},
	})
	if err != nil {
		var ccfe *ddbtypes.ConditionalCheckFailedException
		if errors.As(err, &ccfe) {
			return false, nil
		}
		return false, fmt.Errorf("refresh trigger lock %q/%s/%s: %w", pipelineID, schedule, date, err)
	}
	return true, nil
}

// FailTriggerUnlessCompleted sets the trigger row to FAILED_FINAL (removing
// its TTL, creating it when absent) unless it is COMPLETED. It returns false,
// and writes nothing, for a COMPLETED row.
func (s *Store) FailTriggerUnlessCompleted(ctx context.Context, pipelineID, schedule, date string) (bool, error) {
	_, err := s.Client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: &s.ControlTable,
		Key: map[string]ddbtypes.AttributeValue{
			"PK": &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
			"SK": &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK(schedule, date)},
		},
		ConditionExpression:      aws.String("attribute_not_exists(PK) OR #status <> :completed"),
		UpdateExpression:         aws.String("SET #status = :s REMOVE #ttl"),
		ExpressionAttributeNames: map[string]string{"#status": "status", "#ttl": "ttl"},
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":s":         &ddbtypes.AttributeValueMemberS{Value: types.TriggerStatusFailedFinal},
			":completed": &ddbtypes.AttributeValueMemberS{Value: types.TriggerStatusCompleted},
		},
	})
	if err != nil {
		var ccfe *ddbtypes.ConditionalCheckFailedException
		if errors.As(err, &ccfe) {
			return false, nil
		}
		return false, fmt.Errorf("fail trigger %q/%s/%s: %w", pipelineID, schedule, date, err)
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

// HasTriggerForDates checks multiple dates for existing triggers and returns a
// map of date to existence. Internally calls HasTriggerForDate for each date.
func (s *Store) HasTriggerForDates(ctx context.Context, pipelineID, schedule string, dates []string) (map[string]bool, error) {
	result := make(map[string]bool, len(dates))
	for _, date := range dates {
		found, err := s.HasTriggerForDate(ctx, pipelineID, schedule, date)
		if err != nil {
			return nil, fmt.Errorf("check trigger for date %s: %w", date, err)
		}
		result[date] = found
	}
	return result, nil
}

// CreateTriggerIfAbsent writes a trigger row with the given status only if no
// trigger row exists for this pipeline/schedule/date. Returns true if the row
// was created, false if a row already existed (TOCTOU-safe via conditional put).
func (s *Store) CreateTriggerIfAbsent(ctx context.Context, pipelineID, schedule, date, status string) (bool, error) {
	_, err := s.Client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &s.ControlTable,
		Item: map[string]ddbtypes.AttributeValue{
			"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
			"SK":     &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK(schedule, date)},
			"status": &ddbtypes.AttributeValueMemberS{Value: status},
		},
		ConditionExpression: aws.String("attribute_not_exists(PK)"),
	})
	if err != nil {
		var condErr *ddbtypes.ConditionalCheckFailedException
		if errors.As(err, &condErr) {
			return false, nil
		}
		return false, fmt.Errorf("create trigger if absent %q/%s/%s: %w", pipelineID, schedule, date, err)
	}
	return true, nil
}
