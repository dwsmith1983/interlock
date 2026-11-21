package store

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/interlock/pkg/types"
)

type jobEventOpts struct {
	category string
}

// JobEventOption configures optional fields on WriteJobEvent.
type JobEventOption func(*jobEventOpts)

// WithFailureCategory sets the failure category on the job event record.
func WithFailureCategory(c types.FailureCategory) JobEventOption {
	return func(o *jobEventOpts) {
		o.category = string(c)
	}
}

// WriteJobEvent appends a job event record to the job log table.
// Optional fields (runID, duration, errMsg) are omitted when empty/zero.
func (s *Store) WriteJobEvent(ctx context.Context, pipelineID, schedule, date, event, runID string, duration int64, errMsg string, opts ...JobEventOption) error {
	var o jobEventOpts
	for _, opt := range opts {
		opt(&o)
	}

	now := s.now()
	timestamp := fmt.Sprintf("%d", now.UTC().UnixMilli())
	sk := types.JobSK(schedule, date, timestamp)
	ttl := now.Add(30 * 24 * time.Hour).Unix()

	item := map[string]ddbtypes.AttributeValue{
		"PK":    &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
		"SK":    &ddbtypes.AttributeValueMemberS{Value: sk},
		"event": &ddbtypes.AttributeValueMemberS{Value: event},
		"ttl":   &ddbtypes.AttributeValueMemberN{Value: fmt.Sprintf("%d", ttl)},
	}

	if runID != "" {
		item["runId"] = &ddbtypes.AttributeValueMemberS{Value: runID}
	}
	if duration != 0 {
		item["duration"] = &ddbtypes.AttributeValueMemberN{Value: fmt.Sprintf("%d", duration)}
	}
	if errMsg != "" {
		item["error"] = &ddbtypes.AttributeValueMemberS{Value: errMsg}
	}
	if o.category != "" {
		item["category"] = &ddbtypes.AttributeValueMemberS{Value: o.category}
	}

	_, err := s.Client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &s.JobLogTable,
		Item:      item,
	})
	if err != nil {
		return fmt.Errorf("write job event %q/%s/%s: %w", pipelineID, schedule, date, err)
	}
	return nil
}

// GetLatestJobEvent retrieves the most recent job event for a pipeline,
// schedule, and date. Returns nil, nil if no events exist.
func (s *Store) GetLatestJobEvent(ctx context.Context, pipelineID, schedule, date string) (*types.JobLogRecord, error) {
	prefix := "JOB#" + schedule + "#" + date + "#"

	out, err := s.Client.Query(ctx, &dynamodb.QueryInput{
		TableName:              &s.JobLogTable,
		KeyConditionExpression: aws.String("PK = :pk AND begins_with(SK, :prefix)"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":pk":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
			":prefix": &ddbtypes.AttributeValueMemberS{Value: prefix},
		},
		ScanIndexForward: aws.Bool(false),
		Limit:            aws.Int32(1),
	})
	if err != nil {
		return nil, fmt.Errorf("get latest job event %q/%s/%s: %w", pipelineID, schedule, date, err)
	}
	if len(out.Items) == 0 {
		return nil, nil
	}

	var rec types.JobLogRecord
	if err := attributevalue.UnmarshalMap(out.Items[0], &rec); err != nil {
		return nil, fmt.Errorf("unmarshal job event %q/%s/%s: %w", pipelineID, schedule, date, err)
	}
	return &rec, nil
}

// GetJobResult returns the newest terminal job event (success, fail or
// timeout) whose runId is runID for a pipeline, schedule and date, or nil, nil
// when that run has none yet. It pages newest first and filters in Go: a
// FilterExpression combined with Limit would drop matches past the limit.
// runID is required: an empty one would match rows written without a runId.
func (s *Store) GetJobResult(ctx context.Context, pipelineID, schedule, date, runID string) (*types.JobLogRecord, error) {
	if runID == "" {
		return nil, fmt.Errorf("get job result %q/%s/%s: runID is required", pipelineID, schedule, date)
	}
	prefix := "JOB#" + schedule + "#" + date + "#"
	var startKey map[string]ddbtypes.AttributeValue
	for {
		out, err := s.Client.Query(ctx, &dynamodb.QueryInput{
			TableName:              &s.JobLogTable,
			KeyConditionExpression: aws.String("PK = :pk AND begins_with(SK, :prefix)"),
			ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
				":pk":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
				":prefix": &ddbtypes.AttributeValueMemberS{Value: prefix},
			},
			ConsistentRead:    aws.Bool(true),
			ScanIndexForward:  aws.Bool(false),
			ExclusiveStartKey: startKey,
		})
		if err != nil {
			return nil, fmt.Errorf("get job result %q/%s/%s run %q: %w", pipelineID, schedule, date, runID, err)
		}
		var recs []types.JobLogRecord
		if err := attributevalue.UnmarshalListOfMaps(out.Items, &recs); err != nil {
			return nil, fmt.Errorf("unmarshal job events %q/%s/%s run %q: %w", pipelineID, schedule, date, runID, err)
		}
		for i := range recs {
			if recs[i].RunID == runID && isTerminalJobEvent(recs[i].Event) {
				return &recs[i], nil
			}
		}
		if out.LastEvaluatedKey == nil {
			return nil, nil
		}
		startKey = out.LastEvaluatedKey
	}
}

func isTerminalJobEvent(event string) bool {
	switch event {
	case types.JobEventSuccess, types.JobEventFail, types.JobEventTimeout:
		return true
	}
	return false
}

// jobOutcomePageSize is how many job rows GetLatestJobOutcome reads per page.
// Rows the stream-router writes can pile up above the last run during a long
// watch window, and the rows are small.
const jobOutcomePageSize = 100

// GetLatestJobOutcome returns the newest job row for a pipeline, schedule and
// date that records what the date's run did, skipping the rows the
// stream-router writes about reruns and late data (see isAuditJobEvent).
// Returns nil, nil if there is none. Only the event attribute of a skipped row
// is read, so a malformed older row cannot fail the lookup.
func (s *Store) GetLatestJobOutcome(ctx context.Context, pipelineID, schedule, date string) (*types.JobLogRecord, error) {
	prefix := "JOB#" + schedule + "#" + date + "#"
	var startKey map[string]ddbtypes.AttributeValue
	for {
		out, err := s.Client.Query(ctx, &dynamodb.QueryInput{
			TableName:              &s.JobLogTable,
			KeyConditionExpression: aws.String("PK = :pk AND begins_with(SK, :prefix)"),
			ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
				":pk":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
				":prefix": &ddbtypes.AttributeValueMemberS{Value: prefix},
			},
			ScanIndexForward:  aws.Bool(false),
			Limit:             aws.Int32(jobOutcomePageSize),
			ExclusiveStartKey: startKey,
		})
		if err != nil {
			return nil, fmt.Errorf("get latest job outcome %q/%s/%s: %w", pipelineID, schedule, date, err)
		}
		for _, item := range out.Items {
			if ev, ok := item["event"].(*ddbtypes.AttributeValueMemberS); ok && isAuditJobEvent(ev.Value) {
				continue
			}
			var rec types.JobLogRecord
			if err := attributevalue.UnmarshalMap(item, &rec); err != nil {
				return nil, fmt.Errorf("unmarshal job outcome %q/%s/%s: %w", pipelineID, schedule, date, err)
			}
			return &rec, nil
		}
		if out.LastEvaluatedKey == nil {
			return nil, nil
		}
		startKey = out.LastEvaluatedKey
	}
}

// isAuditJobEvent reports whether a job row is one the stream-router writes
// about a rerun request or late data rather than a record of the run. Any
// other event, including one this version does not know, is an outcome.
func isAuditJobEvent(event string) bool {
	switch event {
	case types.JobEventRerunAccepted, types.JobEventRerunRejected, types.JobEventLateDataArrival:
		return true
	}
	return false
}
