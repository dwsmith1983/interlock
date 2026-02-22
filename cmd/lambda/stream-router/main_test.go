package main

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	intlambda "github.com/interlock-systems/interlock/internal/lambda"
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

func TestHandleStreamEvent_MarkerRecord(t *testing.T) {
	mock := &mockSFN{}
	event := intlambda.StreamEvent{
		Records: []events.DynamoDBEventRecord{
			makeRecord("PIPELINE#my-pipeline", "MARKER#freshness#2026-02-22T10:00:00Z", "INSERT"),
		},
	}

	err := handleStreamEvent(context.Background(), mock, "arn:aws:states:us-east-1:123:stateMachine:interlock", event)
	require.NoError(t, err)

	require.Len(t, mock.executions, 1)
	exec := mock.executions[0]
	assert.Equal(t, "arn:aws:states:us-east-1:123:stateMachine:interlock", *exec.StateMachineArn)
	assert.Contains(t, *exec.Name, "my-pipeline")

	var input map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(*exec.Input), &input))
	assert.Equal(t, "my-pipeline", input["pipelineID"])
	assert.Equal(t, "daily", input["scheduleID"])
	assert.Equal(t, "freshness", input["markerSource"])
}

func TestHandleStreamEvent_SkipsNonMarker(t *testing.T) {
	mock := &mockSFN{}
	event := intlambda.StreamEvent{
		Records: []events.DynamoDBEventRecord{
			makeRecord("PIPELINE#my-pipeline", "TRAIT#freshness", "INSERT"),
			makeRecord("PIPELINE#my-pipeline", "RUN#run-1", "MODIFY"),
		},
	}

	err := handleStreamEvent(context.Background(), mock, "arn:aws:states:us-east-1:123:stateMachine:interlock", event)
	require.NoError(t, err)
	assert.Empty(t, mock.executions)
}

func TestHandleStreamEvent_SkipsDelete(t *testing.T) {
	mock := &mockSFN{}
	event := intlambda.StreamEvent{
		Records: []events.DynamoDBEventRecord{
			makeRecord("PIPELINE#my-pipeline", "MARKER#freshness", "REMOVE"),
		},
	}

	err := handleStreamEvent(context.Background(), mock, "arn:aws:states:us-east-1:123:stateMachine:interlock", event)
	require.NoError(t, err)
	assert.Empty(t, mock.executions)
}

func TestHandleStreamEvent_MultipleRecords(t *testing.T) {
	mock := &mockSFN{}
	event := intlambda.StreamEvent{
		Records: []events.DynamoDBEventRecord{
			makeRecord("PIPELINE#pipe-a", "MARKER#freshness", "INSERT"),
			makeRecord("PIPELINE#pipe-b", "MARKER#completeness", "INSERT"),
			makeRecord("PIPELINE#pipe-c", "TRAIT#something", "INSERT"), // skipped
		},
	}

	err := handleStreamEvent(context.Background(), mock, "arn:aws:states:us-east-1:123:stateMachine:interlock", event)
	require.NoError(t, err)
	assert.Len(t, mock.executions, 2)
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
