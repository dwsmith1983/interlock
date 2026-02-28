// stream-router Lambda receives DynamoDB Stream events and starts Step Function executions.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-lambda-go/events"
	awslambda "github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	awssns "github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	intlambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/metrics"
	"github.com/dwsmith1983/interlock/pkg/types"
)

var (
	deps     *intlambda.Deps
	depsOnce sync.Once
	depsErr  error
)

func getDeps() (*intlambda.Deps, error) {
	depsOnce.Do(func() {
		deps, depsErr = intlambda.Init(context.Background())
	})
	return deps, depsErr
}

// handleStreamEvent processes DynamoDB Stream records, starts Step Function
// executions for MARKER# records, and publishes lifecycle events for RUNLOG#
// records that reach a terminal status.
func handleStreamEvent(ctx context.Context, d *intlambda.Deps, event intlambda.StreamEvent) error {
	logger := slog.Default()

	for _, record := range event.Records {
		if record.EventName != "INSERT" && record.EventName != "MODIFY" {
			continue
		}

		keys := record.Change.Keys
		pkAttr, hasPK := keys["PK"]
		skAttr, hasSK := keys["SK"]
		if !hasPK || !hasSK {
			logger.Warn("stream record missing PK/SK", "eventID", record.EventID)
			continue
		}

		pk := pkAttr.String()
		sk := skAttr.String()

		switch {
		case strings.HasPrefix(sk, "MARKER#"):
			if err := handleMarkerRecord(ctx, d, logger, pk, sk, record); err != nil {
				return err
			}
		case strings.HasPrefix(sk, "RUNLOG#"):
			publishLifecycleEvent(ctx, d, logger, pk, record)
		}

		// Publish to observability topic (best-effort, all eligible records)
		publishObservabilityEvent(ctx, d, logger, pk, sk, record)
	}

	return nil
}

// handleMarkerRecord starts a Step Function execution for a MARKER# DynamoDB stream record.
func handleMarkerRecord(ctx context.Context, d *intlambda.Deps, logger *slog.Logger, pk, sk string, record events.DynamoDBEventRecord) error {
	// PK format: PIPELINE#<pipelineID>
	pipelineID := strings.TrimPrefix(pk, "PIPELINE#")
	if pipelineID == pk {
		logger.Warn("unexpected PK format", "pk", pk)
		return nil
	}

	// Extract schedule from NewImage attribute, defaulting to "daily"
	scheduleID := "daily"
	if record.Change.NewImage != nil {
		if schedAttr, ok := record.Change.NewImage["scheduleID"]; ok {
			if s := schedAttr.String(); s != "" {
				scheduleID = s
			}
		}
	}

	markerParts := strings.SplitN(sk, "#", 3)
	markerSource := ""
	if len(markerParts) >= 2 {
		markerSource = markerParts[1]
	}

	date := time.Now().UTC().Format("2006-01-02")
	if record.Change.NewImage != nil {
		if dateAttr, ok := record.Change.NewImage["date"]; ok {
			if d := dateAttr.String(); d != "" {
				date = d
			}
		}
	}

	// Dedup execution name: pipelineID:date:scheduleID (replace invalid chars)
	execName := sanitizeExecName(fmt.Sprintf("%s:%s:%s", pipelineID, date, scheduleID))

	sfnInput, _ := json.Marshal(map[string]interface{}{
		"pipelineID":   pipelineID,
		"scheduleID":   scheduleID,
		"markerSource": markerSource,
		"date":         date,
	})

	_, err := d.SFNClient.StartExecution(ctx, &sfn.StartExecutionInput{
		StateMachineArn: &d.StateMachineARN,
		Name:            &execName,
		Input:           strPtr(string(sfnInput)),
	})
	if err != nil {
		if strings.Contains(err.Error(), "ExecutionAlreadyExists") {
			logger.Info("execution already exists (dedup)",
				"pipeline", pipelineID, "date", date, "schedule", scheduleID)
			// Check if previous run failed — if so, start a retry execution
			if d.Provider != nil {
				entry, getErr := d.Provider.GetRunLog(ctx, pipelineID, date, scheduleID)
				if getErr != nil {
					logger.Warn("failed to check run log for retry",
						"pipeline", pipelineID, "error", getErr)
				} else if entry != nil && entry.Status == types.RunFailed {
					retryName := sanitizeExecName(fmt.Sprintf("%s:%s:%s:a%d",
						pipelineID, date, scheduleID, entry.AttemptNumber+1))
					_, retryErr := d.SFNClient.StartExecution(ctx, &sfn.StartExecutionInput{
						StateMachineArn: &d.StateMachineARN,
						Name:            &retryName,
						Input:           strPtr(string(sfnInput)),
					})
					if retryErr != nil {
						if strings.Contains(retryErr.Error(), "ExecutionAlreadyExists") {
							logger.Info("retry execution already exists",
								"pipeline", pipelineID, "retryName", retryName)
						} else {
							logger.Error("failed to start retry execution",
								"pipeline", pipelineID, "retryName", retryName, "error", retryErr)
						}
					} else {
						logger.Info("started retry execution after failed run",
							"pipeline", pipelineID, "retryName", retryName,
							"attempt", entry.AttemptNumber+1)
					}
				}
			}
			return nil
		}
		logger.Error("failed to start execution",
			"pipeline", pipelineID,
			"error", err,
		)
		return fmt.Errorf("starting execution for %s: %w", pipelineID, err)
	}

	logger.Info("started step function execution",
		"pipeline", pipelineID,
		"date", date,
		"schedule", scheduleID,
		"execName", execName,
	)
	return nil
}

// publishLifecycleEvent publishes a lifecycle event to SNS when a RUNLOG# record
// transitions to COMPLETED or FAILED. Best-effort: errors are logged, not returned.
// No-op when LIFECYCLE_TOPIC_ARN is not configured.
func publishLifecycleEvent(ctx context.Context, d *intlambda.Deps, logger *slog.Logger, pk string, record events.DynamoDBEventRecord) {
	if d.SNSClient == nil || d.LifecycleTopicARN == "" {
		return
	}

	newImage := record.Change.NewImage
	if newImage == nil {
		return
	}

	// Extract status from NewImage
	statusAttr, ok := newImage["status"]
	if !ok {
		return
	}
	status := statusAttr.String()

	// Only publish for terminal statuses
	if status != string(types.RunCompleted) && status != string(types.RunFailed) {
		return
	}

	pipelineID := strings.TrimPrefix(pk, "PIPELINE#")
	if pipelineID == pk {
		return
	}

	eventType := types.EventPipelineCompleted
	if status == string(types.RunFailed) {
		eventType = types.EventPipelineFailed
	}

	// Extract optional fields from NewImage
	scheduleID := ""
	if attr, ok := newImage["scheduleID"]; ok {
		scheduleID = attr.String()
	}
	date := ""
	if attr, ok := newImage["date"]; ok {
		date = attr.String()
	}
	runID := ""
	if attr, ok := newImage["runId"]; ok {
		runID = attr.String()
	}

	evt := intlambda.LifecycleEvent{
		EventType:  eventType,
		PipelineID: pipelineID,
		ScheduleID: scheduleID,
		Date:       date,
		RunID:      runID,
		Status:     status,
		Timestamp:  time.Now(),
	}

	payload, err := json.Marshal(evt)
	if err != nil {
		logger.Error("failed to marshal lifecycle event",
			"pipeline", pipelineID, "error", err)
		return
	}

	msg := string(payload)
	_, err = d.SNSClient.Publish(ctx, &awssns.PublishInput{
		TopicArn: &d.LifecycleTopicARN,
		Message:  &msg,
	})
	if err != nil {
		logger.Error("failed to publish lifecycle event",
			"pipeline", pipelineID, "status", status, "error", err)
		return
	}

	metrics.LifecycleEventsPublished.Add(1)
	logger.Info("published lifecycle event",
		"pipeline", pipelineID, "status", status, "eventType", eventType)
}

// skippedSKPrefixes are SK prefixes that should not be published to the
// observability topic (noisy internal records).
var skippedSKPrefixes = []string{"LOCK#", "DEDUP#", "CONFIG", "READINESS"}

// skPrefixToRecordType maps DynamoDB SK prefix to observability record type.
var skPrefixToRecordType = map[string]string{
	"EVENT#":      "EVENT",
	"RUNLOG#":     "RUNLOG",
	"ALERT#":      "ALERT",
	"JOBLOG#":     "JOBLOG",
	"EVAL#":       "EVAL",
	"ERROR#":      "ERROR",
	"CHAOS#":      "CHAOS",
	"MARKER#":     "MARKER",
	"SENSOR#":     "SENSOR",
	"CONTROL#":    "CONTROL",
	"QUARANTINE#": "QUARANTINE",
}

// recordTypeToEventType maps record type to default event type when no specific
// attribute provides the event type.
var recordTypeToEventType = map[string]string{
	"RUNLOG":     "RUNLOG_UPDATED",
	"JOBLOG":     "JOB_LOG",
	"EVAL":       "EVALUATION_SESSION",
	"ERROR":      "ERROR_RECORDED",
	"CHAOS":      "CHAOS_EVENT",
	"MARKER":     "MARKER_WRITTEN",
	"SENSOR":     "SENSOR_DATA",
	"CONTROL":    "CONTROL_UPDATED",
	"QUARANTINE": "DATA_QUARANTINED",
}

// publishObservabilityEvent publishes a normalized event to the observability
// SNS topic for all eligible DynamoDB stream records. Best-effort: errors are
// logged, not returned. No-op when OBSERVABILITY_TOPIC_ARN is not configured.
func publishObservabilityEvent(ctx context.Context, d *intlambda.Deps, logger *slog.Logger, pk, sk string, record events.DynamoDBEventRecord) {
	if d.SNSClient == nil || d.ObservabilityTopicARN == "" {
		return
	}

	// Skip noisy internal record types
	for _, prefix := range skippedSKPrefixes {
		if strings.HasPrefix(sk, prefix) {
			return
		}
	}

	// Determine record type from SK prefix
	recordType := ""
	for prefix, rt := range skPrefixToRecordType {
		if strings.HasPrefix(sk, prefix) {
			recordType = rt
			break
		}
	}
	if recordType == "" {
		return // unknown record type, skip
	}

	pipelineID := strings.TrimPrefix(pk, "PIPELINE#")
	if pipelineID == pk {
		return // not a pipeline record
	}

	// Determine event type
	eventType := recordTypeToEventType[recordType]
	newImage := record.Change.NewImage

	// EVENT# records use "kind" attr, ALERT# records use "alertType" attr
	if newImage != nil {
		switch recordType {
		case "EVENT":
			if kindAttr, ok := newImage["kind"]; ok {
				if k := kindAttr.String(); k != "" {
					eventType = k
				}
			}
		case "ALERT":
			if atAttr, ok := newImage["alertType"]; ok {
				if at := atAttr.String(); at != "" {
					eventType = at
				}
			}
		}
	}

	// Build observability event
	evt := intlambda.ObservabilityEvent{
		EventID:    fmt.Sprintf("%s:%s", record.EventName, record.EventID),
		RecordType: recordType,
		EventType:  eventType,
		PipelineID: pipelineID,
		Timestamp:  time.Now(),
	}

	// Extract standard fields from NewImage
	if newImage != nil {
		if attr, ok := newImage["scheduleID"]; ok {
			evt.ScheduleID = attr.String()
		}
		if attr, ok := newImage["date"]; ok {
			evt.Date = attr.String()
		}
		if attr, ok := newImage["runId"]; ok {
			evt.RunID = attr.String()
		}
		if attr, ok := newImage["status"]; ok {
			evt.Status = attr.String()
		}
		if attr, ok := newImage["message"]; ok {
			evt.Message = attr.String()
		}
	}

	payload, err := json.Marshal(evt)
	if err != nil {
		logger.Error("failed to marshal observability event",
			"pipeline", pipelineID, "recordType", recordType, "error", err)
		return
	}

	// Truncate if > 256KB (SNS limit)
	msg := string(payload)
	if len(msg) > 256*1024 {
		msg = msg[:256*1024]
	}

	_, err = d.SNSClient.Publish(ctx, &awssns.PublishInput{
		TopicArn: &d.ObservabilityTopicARN,
		Message:  &msg,
		MessageAttributes: map[string]snstypes.MessageAttributeValue{
			"recordType": {
				DataType:    strPtr("String"),
				StringValue: &recordType,
			},
		},
	})
	if err != nil {
		logger.Error("failed to publish observability event",
			"pipeline", pipelineID, "recordType", recordType, "error", err)
		return
	}

	logger.Debug("published observability event",
		"pipeline", pipelineID, "recordType", recordType, "eventType", eventType)
}

func handler(ctx context.Context, event intlambda.StreamEvent) error {
	d, err := getDeps()
	if err != nil {
		return err
	}
	if d.SFNClient == nil {
		return fmt.Errorf("STATE_MACHINE_ARN environment variable required")
	}
	return handleStreamEvent(ctx, d, event)
}

// sanitizeExecName replaces characters invalid for SFN execution names.
// Valid: a-z, A-Z, 0-9, -, _  (max 80 chars)
func sanitizeExecName(name string) string {
	var b strings.Builder
	for _, c := range name {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '-', c == '_':
			b.WriteRune(c)
		default:
			b.WriteRune('_')
		}
	}
	s := b.String()
	if len(s) > 80 {
		s = s[:80]
	}
	return s
}

func strPtr(s string) *string { return &s }

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	awslambda.Start(handler)
}
