package lambda

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"
	awssns "github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
)

// SkippedSKPrefixes are SK prefixes that should not be published to the
// observability topic (noisy internal records).
var SkippedSKPrefixes = []string{"LOCK#", "DEDUP#", "CONFIG", "READINESS"}

// SKPrefixToRecordType maps DynamoDB SK prefix to observability record type.
var SKPrefixToRecordType = map[string]string{
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

// RecordTypeToEventType maps record type to default event type when no specific
// attribute provides the event type.
var RecordTypeToEventType = map[string]string{
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

// extractDataJSON parses the "data" JSON blob from a DynamoDB stream record
// and returns the parsed map. Returns nil if the attribute is missing or invalid.
func extractDataJSON(newImage map[string]events.DynamoDBAttributeValue) map[string]interface{} {
	dataAttr, ok := newImage["data"]
	if !ok {
		return nil
	}
	raw := dataAttr.String()
	if raw == "" {
		return nil
	}
	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		return nil
	}
	return parsed
}

// dataStr returns the string value for a key in a parsed JSON map, or "".
func dataStr(m map[string]interface{}, key string) string {
	if m == nil {
		return ""
	}
	v, _ := m[key].(string)
	return v
}

// PublishObservabilityEvent publishes a normalized event to the observability
// SNS topic for all eligible DynamoDB stream records. Best-effort: errors are
// logged, not returned. No-op when OBSERVABILITY_TOPIC_ARN is not configured.
func PublishObservabilityEvent(ctx context.Context, d *Deps, logger *slog.Logger, pk, sk string, record events.DynamoDBEventRecord) {
	if d.SNSClient == nil || d.ObservabilityTopicARN == "" {
		return
	}

	// Skip noisy internal record types
	for _, prefix := range SkippedSKPrefixes {
		if strings.HasPrefix(sk, prefix) {
			return
		}
	}

	// Determine record type from SK prefix
	recordType := ""
	for prefix, rt := range SKPrefixToRecordType {
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
	eventType := RecordTypeToEventType[recordType]
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

	// Extract standard fields from top-level NewImage attributes.
	var scheduleID, date, runID, status, message string
	if newImage != nil {
		if attr, ok := newImage["scheduleID"]; ok {
			scheduleID = attr.String()
		}
		if attr, ok := newImage["date"]; ok {
			date = attr.String()
		}
		if attr, ok := newImage["runId"]; ok {
			runID = attr.String()
		}
		if attr, ok := newImage["status"]; ok {
			status = attr.String()
		}
		if attr, ok := newImage["message"]; ok {
			message = attr.String()
		}

		// Fallback: parse "data" JSON blob for fields not found at top level.
		// RUNLOG and EVENT records store all rich fields inside "data".
		parsed := extractDataJSON(newImage)
		if parsed != nil {
			if scheduleID == "" {
				scheduleID = dataStr(parsed, "scheduleId")
			}
			if date == "" {
				date = dataStr(parsed, "date")
			}
			if runID == "" {
				runID = dataStr(parsed, "runId")
			}
			if status == "" {
				status = dataStr(parsed, "status")
			}
			if message == "" {
				message = dataStr(parsed, "message")
				if message == "" {
					message = dataStr(parsed, "failureMessage")
				}
			}
			// EVENT records: extract "kind" for eventType from data JSON
			if recordType == "EVENT" && eventType == RecordTypeToEventType["EVENT"] {
				if kind := dataStr(parsed, "kind"); kind != "" {
					eventType = kind
				}
			}
		}
	}

	// Build observability event
	evt := ObservabilityEvent{
		EventID:    fmt.Sprintf("%s:%s", record.EventName, record.EventID),
		RecordType: recordType,
		EventType:  eventType,
		PipelineID: pipelineID,
		ScheduleID: scheduleID,
		RunID:      runID,
		Date:       date,
		Status:     status,
		Message:    message,
		Timestamp:  time.Now(),
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

func strPtr(s string) *string { return &s }
