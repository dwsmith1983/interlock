package main

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	awssns "github.com/aws/aws-sdk-go-v2/service/sns"
	intlambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/testutil"
	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockSFN struct {
	executions []*sfn.StartExecutionInput
	err        error
}

func (m *mockSFN) StartExecution(_ context.Context, input *sfn.StartExecutionInput, _ ...func(*sfn.Options)) (*sfn.StartExecutionOutput, error) {
	m.executions = append(m.executions, input)
	return &sfn.StartExecutionOutput{}, m.err
}

type mockSNS struct {
	messages []*awssns.PublishInput
	err      error
}

func (m *mockSNS) Publish(_ context.Context, input *awssns.PublishInput, _ ...func(*awssns.Options)) (*awssns.PublishOutput, error) {
	m.messages = append(m.messages, input)
	return &awssns.PublishOutput{}, m.err
}

func testDeps(sfnMock *mockSFN, snsMock *mockSNS) *intlambda.Deps {
	d := &intlambda.Deps{
		SFNClient:       sfnMock,
		StateMachineARN: "arn:aws:states:us-east-1:123:stateMachine:interlock",
	}
	if snsMock != nil {
		d.SNSClient = snsMock
		d.LifecycleTopicARN = "arn:aws:sns:us-east-1:123:lifecycle"
	}
	return d
}

func makeRecord(pk, sk, eventName string) events.DynamoDBEventRecord {
	return events.DynamoDBEventRecord{
		EventName: eventName,
		Change: events.DynamoDBStreamRecord{
			Keys: map[string]events.DynamoDBAttributeValue{
				"PK": events.NewStringAttribute(pk),
				"SK": events.NewStringAttribute(sk),
			},
		},
	}
}

func makeRecordWithNewImage(pk, sk, eventName string, newImage map[string]events.DynamoDBAttributeValue) events.DynamoDBEventRecord {
	return events.DynamoDBEventRecord{
		EventName: eventName,
		Change: events.DynamoDBStreamRecord{
			Keys: map[string]events.DynamoDBAttributeValue{
				"PK": events.NewStringAttribute(pk),
				"SK": events.NewStringAttribute(sk),
			},
			NewImage: newImage,
		},
	}
}

// ---------------------------------------------------------------------------
// Existing MARKER# tests (refactored to use *Deps)
// ---------------------------------------------------------------------------

func TestHandleStreamEvent_MarkerRecord(t *testing.T) {
	sfnMock := &mockSFN{}
	d := testDeps(sfnMock, nil)
	event := intlambda.StreamEvent{
		Records: []events.DynamoDBEventRecord{
			makeRecord("PIPELINE#my-pipeline", "MARKER#freshness#2026-02-22T10:00:00Z", "INSERT"),
		},
	}

	err := handleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	require.Len(t, sfnMock.executions, 1)
	exec := sfnMock.executions[0]
	assert.Equal(t, d.StateMachineARN, *exec.StateMachineArn)
	assert.Contains(t, *exec.Name, "my-pipeline")

	var input map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(*exec.Input), &input))
	assert.Equal(t, "my-pipeline", input["pipelineID"])
	assert.Equal(t, "daily", input["scheduleID"])
	assert.Equal(t, "freshness", input["markerSource"])
}

func TestHandleStreamEvent_SkipsNonMarker(t *testing.T) {
	sfnMock := &mockSFN{}
	d := testDeps(sfnMock, nil)
	event := intlambda.StreamEvent{
		Records: []events.DynamoDBEventRecord{
			makeRecord("PIPELINE#my-pipeline", "TRAIT#freshness", "INSERT"),
			makeRecord("PIPELINE#my-pipeline", "RUN#run-1", "MODIFY"),
		},
	}

	err := handleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)
	assert.Empty(t, sfnMock.executions)
}

func TestHandleStreamEvent_SkipsDelete(t *testing.T) {
	sfnMock := &mockSFN{}
	d := testDeps(sfnMock, nil)
	event := intlambda.StreamEvent{
		Records: []events.DynamoDBEventRecord{
			makeRecord("PIPELINE#my-pipeline", "MARKER#freshness", "REMOVE"),
		},
	}

	err := handleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)
	assert.Empty(t, sfnMock.executions)
}

func TestHandleStreamEvent_MultipleRecords(t *testing.T) {
	sfnMock := &mockSFN{}
	d := testDeps(sfnMock, nil)
	event := intlambda.StreamEvent{
		Records: []events.DynamoDBEventRecord{
			makeRecord("PIPELINE#pipe-a", "MARKER#freshness", "INSERT"),
			makeRecord("PIPELINE#pipe-b", "MARKER#completeness", "INSERT"),
			makeRecord("PIPELINE#pipe-c", "TRAIT#something", "INSERT"), // skipped
		},
	}

	err := handleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)
	assert.Len(t, sfnMock.executions, 2)
}

func TestHandleStreamEvent_ScheduleIDFromNewImage(t *testing.T) {
	sfnMock := &mockSFN{}
	d := testDeps(sfnMock, nil)
	event := intlambda.StreamEvent{
		Records: []events.DynamoDBEventRecord{
			makeRecordWithNewImage(
				"PIPELINE#wikipedia-silver",
				"MARKER#ingest-complete#2026-02-22T14:05:00Z",
				"INSERT",
				map[string]events.DynamoDBAttributeValue{
					"PK":         events.NewStringAttribute("PIPELINE#wikipedia-silver"),
					"SK":         events.NewStringAttribute("MARKER#ingest-complete#2026-02-22T14:05:00Z"),
					"scheduleID": events.NewStringAttribute("h14"),
				},
			),
		},
	}

	err := handleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	require.Len(t, sfnMock.executions, 1)
	exec := sfnMock.executions[0]
	assert.Contains(t, *exec.Name, "wikipedia-silver")
	assert.Contains(t, *exec.Name, "h14")

	var input map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(*exec.Input), &input))
	assert.Equal(t, "wikipedia-silver", input["pipelineID"])
	assert.Equal(t, "h14", input["scheduleID"])
	assert.Equal(t, "ingest-complete", input["markerSource"])
}

func TestHandleStreamEvent_NoNewImage_DefaultsDaily(t *testing.T) {
	sfnMock := &mockSFN{}
	d := testDeps(sfnMock, nil)
	event := intlambda.StreamEvent{
		Records: []events.DynamoDBEventRecord{
			makeRecord("PIPELINE#my-pipeline", "MARKER#freshness#2026-02-22T10:00:00Z", "INSERT"),
		},
	}

	err := handleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	require.Len(t, sfnMock.executions, 1)
	exec := sfnMock.executions[0]
	var input map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(*exec.Input), &input))
	assert.Equal(t, "daily", input["scheduleID"])
}

func TestHandleStreamEvent_EmptyScheduleID_DefaultsDaily(t *testing.T) {
	sfnMock := &mockSFN{}
	d := testDeps(sfnMock, nil)
	event := intlambda.StreamEvent{
		Records: []events.DynamoDBEventRecord{
			makeRecordWithNewImage(
				"PIPELINE#my-pipeline",
				"MARKER#freshness#2026-02-22T10:00:00Z",
				"INSERT",
				map[string]events.DynamoDBAttributeValue{
					"PK":         events.NewStringAttribute("PIPELINE#my-pipeline"),
					"SK":         events.NewStringAttribute("MARKER#freshness#2026-02-22T10:00:00Z"),
					"scheduleID": events.NewStringAttribute(""),
				},
			),
		},
	}

	err := handleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	require.Len(t, sfnMock.executions, 1)
	exec := sfnMock.executions[0]
	var input map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(*exec.Input), &input))
	assert.Equal(t, "daily", input["scheduleID"])
}

func TestHandleStreamEvent_DateFromRecord(t *testing.T) {
	sfnMock := &mockSFN{}
	d := testDeps(sfnMock, nil)
	event := intlambda.StreamEvent{
		Records: []events.DynamoDBEventRecord{
			makeRecordWithNewImage(
				"PIPELINE#crypto-silver",
				"MARKER#crypto#complete#20260227#23",
				"INSERT",
				map[string]events.DynamoDBAttributeValue{
					"PK":         events.NewStringAttribute("PIPELINE#crypto-silver"),
					"SK":         events.NewStringAttribute("MARKER#crypto#complete#20260227#23"),
					"scheduleID": events.NewStringAttribute("h23"),
					"date":       events.NewStringAttribute("2026-02-27"),
				},
			),
		},
	}

	err := handleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	require.Len(t, sfnMock.executions, 1)
	exec := sfnMock.executions[0]

	var input map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(*exec.Input), &input))
	assert.Equal(t, "2026-02-27", input["date"], "should use date from record, not time.Now()")
	assert.Equal(t, "h23", input["scheduleID"])
	assert.Equal(t, "crypto-silver", input["pipelineID"])
	assert.Contains(t, *exec.Name, "2026-02-27", "exec name should contain record date")
}

func TestHandleStreamEvent_DateFallsBackToNow(t *testing.T) {
	sfnMock := &mockSFN{}
	d := testDeps(sfnMock, nil)
	today := time.Now().UTC().Format("2006-01-02")

	// Record without date field — should fall back to time.Now()
	event := intlambda.StreamEvent{
		Records: []events.DynamoDBEventRecord{
			makeRecordWithNewImage(
				"PIPELINE#earthquake-silver",
				"MARKER#earthquake#2026-02-28T04:00:00Z",
				"INSERT",
				map[string]events.DynamoDBAttributeValue{
					"PK":         events.NewStringAttribute("PIPELINE#earthquake-silver"),
					"SK":         events.NewStringAttribute("MARKER#earthquake#2026-02-28T04:00:00Z"),
					"scheduleID": events.NewStringAttribute("h04"),
				},
			),
		},
	}

	err := handleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	require.Len(t, sfnMock.executions, 1)
	var input map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(*sfnMock.executions[0].Input), &input))
	assert.Equal(t, today, input["date"], "should fall back to today when no date in record")
}

func TestSanitizeExecName(t *testing.T) {
	assert.Equal(t, "my-pipe_2026-02-22_daily", sanitizeExecName("my-pipe:2026-02-22:daily"))
	assert.Equal(t, "pipe_with_spaces", sanitizeExecName("pipe with spaces"))

	long := ""
	for i := 0; i < 100; i++ {
		long += "a"
	}
	assert.Len(t, sanitizeExecName(long), 80)
}

// ---------------------------------------------------------------------------
// Lifecycle event tests (RUNLOG#)
// ---------------------------------------------------------------------------

func TestLifecycleEvent_CompletedPublishes(t *testing.T) {
	sfnMock := &mockSFN{}
	snsMock := &mockSNS{}
	d := testDeps(sfnMock, snsMock)

	event := intlambda.StreamEvent{
		Records: []events.DynamoDBEventRecord{
			makeRecordWithNewImage(
				"PIPELINE#pipe-a",
				"RUNLOG#daily#2026-02-25",
				"MODIFY",
				map[string]events.DynamoDBAttributeValue{
					"status":     events.NewStringAttribute("COMPLETED"),
					"scheduleID": events.NewStringAttribute("daily"),
					"date":       events.NewStringAttribute("2026-02-25"),
					"runId":      events.NewStringAttribute("run-42"),
				},
			),
		},
	}

	err := handleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)
	assert.Empty(t, sfnMock.executions, "RUNLOG should not trigger SFN")
	require.Len(t, snsMock.messages, 1)

	var evt intlambda.LifecycleEvent
	require.NoError(t, json.Unmarshal([]byte(*snsMock.messages[0].Message), &evt))
	assert.Equal(t, types.EventPipelineCompleted, evt.EventType)
	assert.Equal(t, "pipe-a", evt.PipelineID)
	assert.Equal(t, "daily", evt.ScheduleID)
	assert.Equal(t, "2026-02-25", evt.Date)
	assert.Equal(t, "run-42", evt.RunID)
	assert.Equal(t, "COMPLETED", evt.Status)
}

func TestLifecycleEvent_FailedPublishes(t *testing.T) {
	sfnMock := &mockSFN{}
	snsMock := &mockSNS{}
	d := testDeps(sfnMock, snsMock)

	event := intlambda.StreamEvent{
		Records: []events.DynamoDBEventRecord{
			makeRecordWithNewImage(
				"PIPELINE#pipe-b",
				"RUNLOG#h14#2026-02-25",
				"MODIFY",
				map[string]events.DynamoDBAttributeValue{
					"status":     events.NewStringAttribute("FAILED"),
					"scheduleID": events.NewStringAttribute("h14"),
					"date":       events.NewStringAttribute("2026-02-25"),
					"runId":      events.NewStringAttribute("run-99"),
				},
			),
		},
	}

	err := handleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)
	require.Len(t, snsMock.messages, 1)

	var evt intlambda.LifecycleEvent
	require.NoError(t, json.Unmarshal([]byte(*snsMock.messages[0].Message), &evt))
	assert.Equal(t, types.EventPipelineFailed, evt.EventType)
	assert.Equal(t, "pipe-b", evt.PipelineID)
	assert.Equal(t, "FAILED", evt.Status)
}

func TestLifecycleEvent_PendingSkips(t *testing.T) {
	sfnMock := &mockSFN{}
	snsMock := &mockSNS{}
	d := testDeps(sfnMock, snsMock)

	event := intlambda.StreamEvent{
		Records: []events.DynamoDBEventRecord{
			makeRecordWithNewImage(
				"PIPELINE#pipe-a",
				"RUNLOG#daily#2026-02-25",
				"INSERT",
				map[string]events.DynamoDBAttributeValue{
					"status": events.NewStringAttribute("PENDING"),
				},
			),
		},
	}

	err := handleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)
	assert.Empty(t, snsMock.messages, "PENDING should not publish lifecycle event")
}

func TestLifecycleEvent_NoTopicSkips(t *testing.T) {
	sfnMock := &mockSFN{}
	d := testDeps(sfnMock, nil) // no SNS configured

	event := intlambda.StreamEvent{
		Records: []events.DynamoDBEventRecord{
			makeRecordWithNewImage(
				"PIPELINE#pipe-a",
				"RUNLOG#daily#2026-02-25",
				"MODIFY",
				map[string]events.DynamoDBAttributeValue{
					"status": events.NewStringAttribute("COMPLETED"),
				},
			),
		},
	}

	err := handleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)
	// No panic, no error — graceful no-op
}

func TestLifecycleEvent_SNSError_BestEffort(t *testing.T) {
	sfnMock := &mockSFN{}
	snsMock := &mockSNS{err: assert.AnError}
	d := testDeps(sfnMock, snsMock)

	event := intlambda.StreamEvent{
		Records: []events.DynamoDBEventRecord{
			makeRecordWithNewImage(
				"PIPELINE#pipe-a",
				"RUNLOG#daily#2026-02-25",
				"MODIFY",
				map[string]events.DynamoDBAttributeValue{
					"status": events.NewStringAttribute("COMPLETED"),
				},
			),
		},
	}

	err := handleStreamEvent(context.Background(), d, event)
	require.NoError(t, err, "SNS publish errors should not fail the batch")
	require.Len(t, snsMock.messages, 1, "should have attempted publish")
}

// mockSFNSequenced returns errors from a sequence; once exhausted, returns nil.
type mockSFNSequenced struct {
	executions []*sfn.StartExecutionInput
	errs       []error
	callIdx    int
}

func (m *mockSFNSequenced) StartExecution(_ context.Context, input *sfn.StartExecutionInput, _ ...func(*sfn.Options)) (*sfn.StartExecutionOutput, error) {
	m.executions = append(m.executions, input)
	var err error
	if m.callIdx < len(m.errs) {
		err = m.errs[m.callIdx]
	}
	m.callIdx++
	return &sfn.StartExecutionOutput{}, err
}

func testDepsWithProvider(sfnClient intlambda.SFNAPI, prov *testutil.MockProvider) *intlambda.Deps {
	return &intlambda.Deps{
		SFNClient:       sfnClient,
		StateMachineARN: "arn:aws:states:us-east-1:123:stateMachine:interlock",
		Provider:        prov,
	}
}

func TestLifecycleEvent_MixedBatch(t *testing.T) {
	sfnMock := &mockSFN{}
	snsMock := &mockSNS{}
	d := testDeps(sfnMock, snsMock)

	event := intlambda.StreamEvent{
		Records: []events.DynamoDBEventRecord{
			// MARKER record → SFN
			makeRecord("PIPELINE#pipe-a", "MARKER#freshness#2026-02-25T10:00:00Z", "INSERT"),
			// RUNLOG record → SNS lifecycle
			makeRecordWithNewImage(
				"PIPELINE#pipe-b",
				"RUNLOG#daily#2026-02-25",
				"MODIFY",
				map[string]events.DynamoDBAttributeValue{
					"status": events.NewStringAttribute("COMPLETED"),
					"runId":  events.NewStringAttribute("run-1"),
				},
			),
		},
	}

	err := handleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)
	assert.Len(t, sfnMock.executions, 1, "MARKER should trigger SFN")
	assert.Len(t, snsMock.messages, 1, "RUNLOG COMPLETED should publish lifecycle event")
}

// ---------------------------------------------------------------------------
// Retry on ExecutionAlreadyExists tests
// ---------------------------------------------------------------------------

func TestRetryOnFailedRun(t *testing.T) {
	today := time.Now().UTC().Format("2006-01-02")
	// First StartExecution → ExecutionAlreadyExists; second (retry) → success
	sfnMock := &mockSFNSequenced{
		errs: []error{
			fmt.Errorf("ExecutionAlreadyExists: execution already exists"),
			nil,
		},
	}
	prov := testutil.NewMockProvider()
	_ = prov.PutRunLog(context.Background(), types.RunLogEntry{
		PipelineID:    "earthquake-gold",
		Date:          today,
		ScheduleID:    "h16",
		Status:        types.RunFailed,
		AttemptNumber: 1,
	})
	d := testDepsWithProvider(sfnMock, prov)

	event := intlambda.StreamEvent{
		Records: []events.DynamoDBEventRecord{
			makeRecordWithNewImage(
				"PIPELINE#earthquake-gold",
				"MARKER#freshness#2026-02-27T16:00:00Z",
				"INSERT",
				map[string]events.DynamoDBAttributeValue{
					"PK":         events.NewStringAttribute("PIPELINE#earthquake-gold"),
					"SK":         events.NewStringAttribute("MARKER#freshness#2026-02-27T16:00:00Z"),
					"scheduleID": events.NewStringAttribute("h16"),
				},
			),
		},
	}

	err := handleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	require.Len(t, sfnMock.executions, 2, "should have base + retry execution")
	retryExec := sfnMock.executions[1]
	assert.Contains(t, *retryExec.Name, "_a2", "retry name should contain attempt suffix")
}

func TestNoRetryOnCompletedRun(t *testing.T) {
	today := time.Now().UTC().Format("2006-01-02")
	sfnMock := &mockSFNSequenced{
		errs: []error{
			fmt.Errorf("ExecutionAlreadyExists: execution already exists"),
		},
	}
	prov := testutil.NewMockProvider()
	_ = prov.PutRunLog(context.Background(), types.RunLogEntry{
		PipelineID:    "earthquake-gold",
		Date:          today,
		ScheduleID:    "h16",
		Status:        types.RunCompleted,
		AttemptNumber: 1,
	})
	d := testDepsWithProvider(sfnMock, prov)

	event := intlambda.StreamEvent{
		Records: []events.DynamoDBEventRecord{
			makeRecordWithNewImage(
				"PIPELINE#earthquake-gold",
				"MARKER#freshness#2026-02-27T16:00:00Z",
				"INSERT",
				map[string]events.DynamoDBAttributeValue{
					"PK":         events.NewStringAttribute("PIPELINE#earthquake-gold"),
					"SK":         events.NewStringAttribute("MARKER#freshness#2026-02-27T16:00:00Z"),
					"scheduleID": events.NewStringAttribute("h16"),
				},
			),
		},
	}

	err := handleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	assert.Len(t, sfnMock.executions, 1, "should not start retry for completed run")
}

func TestNoRetryOnRunning(t *testing.T) {
	sfnMock := &mockSFNSequenced{
		errs: []error{
			fmt.Errorf("ExecutionAlreadyExists: execution already exists"),
		},
	}
	// No run log entry → nil returned → no retry
	prov := testutil.NewMockProvider()
	d := testDepsWithProvider(sfnMock, prov)

	event := intlambda.StreamEvent{
		Records: []events.DynamoDBEventRecord{
			makeRecordWithNewImage(
				"PIPELINE#earthquake-gold",
				"MARKER#freshness#2026-02-27T16:00:00Z",
				"INSERT",
				map[string]events.DynamoDBAttributeValue{
					"PK":         events.NewStringAttribute("PIPELINE#earthquake-gold"),
					"SK":         events.NewStringAttribute("MARKER#freshness#2026-02-27T16:00:00Z"),
					"scheduleID": events.NewStringAttribute("h16"),
				},
			),
		},
	}

	err := handleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	assert.Len(t, sfnMock.executions, 1, "should not start retry when no run log exists")
}
