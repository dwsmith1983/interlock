package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
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
		} else if errs := append(validation.ValidatePipelineConfig(&cfg), validation.ExecutionFitErrors(&cfg, executionTimeout())...); len(errs) > 0 {
			r.Reason = strings.Join(errs, "; ")
		} else {
			return &cfg, r
		}
	}
	r.Reason = CutReason(r.Reason)
	return nil, r
}

// executionTimeout is the Step Functions execution timeout from
// SFN_TIMEOUT_SECONDS, or 0 when it is unset, not a number or not positive.
func executionTimeout() time.Duration {
	sec, err := strconv.Atoi(os.Getenv("SFN_TIMEOUT_SECONDS"))
	if err != nil || sec <= 0 {
		return 0
	}
	return time.Duration(sec) * time.Second
}

// maxReasonBytes bounds a rejection reason, which can quote the config.
const maxReasonBytes = 1024

// CutReason cuts a config rejection reason to maxReasonBytes; ToValidUTF8
// drops a rune the cut splits.
func CutReason(reason string) string {
	return strings.ToValidUTF8(reason[:min(len(reason), maxReasonBytes)], "")
}

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

// GetSensorsBetween retrieves the pipeline's sensor rows from fromKey through
// toKey, keys that begin with toKey included. The result has the shape of
// GetAllSensors; the read is eventually consistent.
func (s *Store) GetSensorsBetween(ctx context.Context, pipelineID, fromKey, toKey string) (map[string]map[string]interface{}, error) {
	result := make(map[string]map[string]interface{})
	err := QueryAll(ctx, s.Client, func(startKey map[string]ddbtypes.AttributeValue) *dynamodb.QueryInput {
		return &dynamodb.QueryInput{
			TableName:              &s.ControlTable,
			KeyConditionExpression: aws.String("PK = :pk AND SK BETWEEN :from AND :to"),
			ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
				":pk":   &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
				":from": &ddbtypes.AttributeValueMemberS{Value: types.SensorSK(fromKey)},
				":to":   &ddbtypes.AttributeValueMemberS{Value: types.SensorSK(toKey) + "~"},
			},
			ExclusiveStartKey: startKey,
		}
	}, func(items []map[string]ddbtypes.AttributeValue) error {
		for _, item := range items {
			var rec types.ControlRecord
			if err := attributevalue.UnmarshalMap(item, &rec); err != nil {
				return fmt.Errorf("unmarshal sensor row: %w", err)
			}
			result[strings.TrimPrefix(rec.SK, types.SensorSK(""))] = rec.Data
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("query sensors %q to %q for %q: %w", fromKey, toKey, pipelineID, err)
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

// ownRowTTL is how long Interlock's own control rows (SENSOR# markers and
// baselines, the RERUN_REQUEST# rows it writes) live. The longest reader is
// the post-run baseline, written at completion and read until the watch
// window (at most MaxPostRunWatchWindow) after completedAt; a day of slack
// is added on top. A baseline that a rerun's completion did not rewrite (no
// post-run sensor then, or a failed capture) can expire inside the rerun's
// window; the next post-run write then seeds a new one.
const ownRowTTL = validation.MaxPostRunWatchWindow + 24*time.Hour

// WriteSensor writes (or overwrites) one of Interlock's own SENSOR# rows,
// expiring ownRowTTL after the write. User sensor rows are not written here.
func (s *Store) WriteSensor(ctx context.Context, pipelineID, sensorKey string, data map[string]interface{}) error {
	rec := types.ControlRecord{
		PK:   types.PipelinePK(pipelineID),
		SK:   types.SensorSK(sensorKey),
		Data: data,
		TTL:  s.now().Add(ownRowTTL).Unix(),
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

// WriteSensorIfAbsent writes one of Interlock's own SENSOR# rows, expiring
// ownRowTTL after the write, only if it does not already exist. Returns true
// if the write succeeded (key was absent), false if the key already existed.
// This is used for idempotent first-occurrence recording.
func (s *Store) WriteSensorIfAbsent(ctx context.Context, pipelineID, sensorKey string, data map[string]interface{}) (bool, error) {
	rec := types.ControlRecord{
		PK:   types.PipelinePK(pipelineID),
		SK:   types.SensorSK(sensorKey),
		Data: data,
		TTL:  s.now().Add(ownRowTTL).Unix(),
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

// WriteSensorFieldIfAbsent sets field to value inside the data map of an
// existing sensor row, only while the row exists and the field is not there.
// A missing row or a field already present is a false return with no error,
// and nothing is written. The row's data attribute must already be a map: a
// row without one, which WriteSensor writes for a nil map (data is
// omitempty), or one whose data is not a map, returns an error because
// DynamoDB rejects the document path. An empty non-nil map is stored as an
// empty data map and takes the field. The row's ttl is left as it was.
func (s *Store) WriteSensorFieldIfAbsent(ctx context.Context, pipelineID, sensorKey, field string, value map[string]interface{}) (bool, error) {
	av, err := attributevalue.Marshal(value)
	if err != nil {
		return false, fmt.Errorf("marshal field %q of sensor %q for %q: %w", field, sensorKey, pipelineID, err)
	}

	_, err = s.Client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: &s.ControlTable,
		Key: map[string]ddbtypes.AttributeValue{
			"PK": &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
			"SK": &ddbtypes.AttributeValueMemberS{Value: types.SensorSK(sensorKey)},
		},
		UpdateExpression:          aws.String("SET #data.#field = :value"),
		ConditionExpression:       aws.String("attribute_exists(PK) AND attribute_not_exists(#data.#field)"),
		ExpressionAttributeNames:  map[string]string{"#data": "data", "#field": field},
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{":value": av},
	})
	if err != nil {
		var ccfe *ddbtypes.ConditionalCheckFailedException
		if errors.As(err, &ccfe) {
			return false, nil
		}
		return false, fmt.Errorf("write field %q of sensor %q for %q: %w", field, sensorKey, pipelineID, err)
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

// WriteRerunRequest writes a RERUN_REQUEST row of Interlock's own (late-data,
// data-drift) to the control table, expiring ownRowTTL after the write.
// The stream-router picks this up via DynamoDB streams and validates
// via the circuit breaker before starting a new SFN execution.
func (s *Store) WriteRerunRequest(ctx context.Context, pipelineID, schedule, date, reason string) error {
	now := s.now()
	_, err := s.Client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &s.ControlTable,
		Item: map[string]ddbtypes.AttributeValue{
			"PK":          &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
			"SK":          &ddbtypes.AttributeValueMemberS{Value: types.RerunRequestSK(schedule, date)},
			"reason":      &ddbtypes.AttributeValueMemberS{Value: reason},
			"requestedAt": &ddbtypes.AttributeValueMemberS{Value: now.UTC().Format(time.RFC3339)},
			"ttl":         &ddbtypes.AttributeValueMemberN{Value: fmt.Sprintf("%d", now.Add(ownRowTTL).Unix())},
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
// already held. The row carries no ttl, so DynamoDB never deletes it; its
// staleAt (now plus staleAfter) tells the watchdog when the run is dead.
func (s *Store) AcquireTriggerLock(ctx context.Context, pipelineID, schedule, date string, staleAfter time.Duration) (bool, error) {
	_, err := s.Client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &s.ControlTable,
		Item: map[string]ddbtypes.AttributeValue{
			"PK":      &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
			"SK":      &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK(schedule, date)},
			"status":  &ddbtypes.AttributeValueMemberS{Value: types.TriggerStatusRunning},
			"staleAt": &ddbtypes.AttributeValueMemberN{Value: fmt.Sprintf("%d", s.now().Add(staleAfter).Unix())},
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
		ConsistentRead: aws.Bool(true),
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

// ResetTriggerLock takes a finished date's trigger row back for a rerun: it
// sets the row RUNNING with a fresh staleAt (now plus staleAfter) and removes
// any ttl. It returns (true, nil) when it did, and (false, nil) when the row
// is missing or already RUNNING, so a rerun never starts beside a run in
// progress.
func (s *Store) ResetTriggerLock(ctx context.Context, pipelineID, schedule, date string, staleAfter time.Duration) (bool, error) {
	_, err := s.Client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: &s.ControlTable,
		Key: map[string]ddbtypes.AttributeValue{
			"PK": &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
			"SK": &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK(schedule, date)},
		},
		ConditionExpression: aws.String("attribute_exists(PK) AND #status <> :running"),
		UpdateExpression:    aws.String("SET #status = :running, staleAt = :staleAt REMOVE #ttl"),
		ExpressionAttributeNames: map[string]string{
			"#status": "status",
			"#ttl":    "ttl",
		},
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":running": &ddbtypes.AttributeValueMemberS{Value: types.TriggerStatusRunning},
			":staleAt": &ddbtypes.AttributeValueMemberN{Value: fmt.Sprintf("%d", s.now().Add(staleAfter).Unix())},
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

// RefreshRunningTriggerLock gives a RUNNING trigger row a fresh staleAt (now
// plus staleAfter) and removes any ttl. Unlike ResetTriggerLock it never
// changes the status: it returns (false, nil) when the row is missing or
// already COMPLETED or FAILED_FINAL.
func (s *Store) RefreshRunningTriggerLock(ctx context.Context, pipelineID, schedule, date string, staleAfter time.Duration) (bool, error) {
	_, err := s.Client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: &s.ControlTable,
		Key: map[string]ddbtypes.AttributeValue{
			"PK": &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
			"SK": &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK(schedule, date)},
		},
		ConditionExpression:      aws.String("attribute_exists(PK) AND #status = :running"),
		UpdateExpression:         aws.String("SET staleAt = :staleAt REMOVE #ttl"),
		ExpressionAttributeNames: map[string]string{"#status": "status", "#ttl": "ttl"},
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":running": &ddbtypes.AttributeValueMemberS{Value: types.TriggerStatusRunning},
			":staleAt": &ddbtypes.AttributeValueMemberN{Value: fmt.Sprintf("%d", s.now().Add(staleAfter).Unix())},
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

// ErrTriggerNotFound means MarkTriggerStarted found no trigger row to mark.
var ErrTriggerNotFound = errors.New("trigger row not found")

// MarkTriggerStarted sets the trigger row's startedAt to at (RFC3339 UTC; a later start
// overwrites it). It never creates the row: without one it wraps ErrTriggerNotFound.
func (s *Store) MarkTriggerStarted(ctx context.Context, pipelineID, schedule, date string, at time.Time) error {
	_, err := s.Client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: &s.ControlTable,
		Key: map[string]ddbtypes.AttributeValue{
			"PK": &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
			"SK": &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK(schedule, date)},
		},
		ConditionExpression: aws.String("attribute_exists(PK)"),
		UpdateExpression:    aws.String("SET startedAt = :t"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":t": &ddbtypes.AttributeValueMemberS{Value: at.UTC().Format(time.RFC3339)},
		},
	})
	if err != nil {
		var ccfe *ddbtypes.ConditionalCheckFailedException
		if errors.As(err, &ccfe) {
			err = ErrTriggerNotFound
		}
		return fmt.Errorf("mark trigger started %q/%s/%s: %w", pipelineID, schedule, date, err)
	}
	return nil
}

// MarkTriggerCompleted sets the trigger row to COMPLETED with completedAt = at
// (RFC3339 UTC; a later completion overwrites it) and removes its staleAt and
// any ttl. Like SetTriggerStatus it creates the row when absent.
func (s *Store) MarkTriggerCompleted(ctx context.Context, pipelineID, schedule, date string, at time.Time) error {
	_, err := s.Client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: &s.ControlTable,
		Key: map[string]ddbtypes.AttributeValue{
			"PK": &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
			"SK": &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK(schedule, date)},
		},
		UpdateExpression:         aws.String("SET #status = :s, completedAt = :t REMOVE #ttl, staleAt"),
		ExpressionAttributeNames: map[string]string{"#status": "status", "#ttl": "ttl"},
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":s": &ddbtypes.AttributeValueMemberS{Value: types.TriggerStatusCompleted},
			":t": &ddbtypes.AttributeValueMemberS{Value: at.UTC().Format(time.RFC3339)},
		},
	})
	if err != nil {
		return fmt.Errorf("mark trigger completed %q/%s/%s: %w", pipelineID, schedule, date, err)
	}
	return nil
}

// FailTriggerUnlessCompleted sets the trigger row to FAILED_FINAL (removing
// its staleAt and any ttl, creating it when absent) unless it is COMPLETED. It
// returns false, and writes nothing, for a COMPLETED row.
func (s *Store) FailTriggerUnlessCompleted(ctx context.Context, pipelineID, schedule, date string) (bool, error) {
	_, err := s.Client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: &s.ControlTable,
		Key: map[string]ddbtypes.AttributeValue{
			"PK": &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
			"SK": &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK(schedule, date)},
		},
		ConditionExpression:      aws.String("attribute_not_exists(PK) OR #status <> :completed"),
		UpdateExpression:         aws.String("SET #status = :s REMOVE #ttl, staleAt"),
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

// SetTriggerStatus updates the status attribute of a trigger row, creating it
// when absent. COMPLETED and FAILED_FINAL also remove its staleAt and any ttl;
// any other status leaves every other attribute as it was.
func (s *Store) SetTriggerStatus(ctx context.Context, pipelineID, schedule, date, status string) error {
	expr := "SET #status = :s"
	names := map[string]string{
		"#status": "status",
	}

	// A finished run is no longer stale. Removing a ttl left by an older
	// writer keeps DynamoDB from deleting the row, after which the watchdog
	// reconcile loop would start the date again.
	if status == types.TriggerStatusCompleted || status == types.TriggerStatusFailedFinal {
		expr = "SET #status = :s REMOVE #ttl, staleAt"
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
