package dlq_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dwsmith1983/interlock/internal/dlq"
)

// --- mock SQS client ---

type mockSQSClient struct {
	sendFn func(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
}

func (m *mockSQSClient) SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	return m.sendFn(ctx, params, optFns...)
}

// --- mock counter ---

type mockCounter struct {
	count atomic.Int64
}

func (c *mockCounter) Inc() { c.count.Add(1) }

// --- helpers ---

func testRecord() dlq.Record {
	return dlq.Record{
		ID:             dlq.GenerateID(),
		OriginalRecord: json.RawMessage(`{"key":"value"}`),
		PipelineID:     "pipeline-42",
		StageID:        "stage-1",
		ErrorMessage:   "something broke",
		ErrorType:      dlq.ErrorTypeProvider,
		STAMPComponent: "evaluator",
		AttemptCount:   3,
		Timestamp:      time.Now().UTC(),
		CorrelationID:  "corr-abc-123",
	}
}

// --- tests ---

func TestSQSRouter_RouteSuccess(t *testing.T) {
	var captured *sqs.SendMessageInput

	client := &mockSQSClient{
		sendFn: func(_ context.Context, params *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
			captured = params
			return &sqs.SendMessageOutput{}, nil
		},
	}

	counter := &mockCounter{}
	router, err := dlq.NewSQSRouter(client, "https://sqs.us-east-1.amazonaws.com/123456789012/dlq", counter)
	require.NoError(t, err)

	rec := testRecord()
	err = router.Route(context.Background(), rec)
	require.NoError(t, err)

	// Verify the body is valid JSON matching the record.
	require.NotNil(t, captured)
	var decoded dlq.Record
	require.NoError(t, json.Unmarshal([]byte(*captured.MessageBody), &decoded))
	assert.Equal(t, rec.PipelineID, decoded.PipelineID)
	assert.Equal(t, rec.ErrorType, decoded.ErrorType)

	// Verify message attributes for filtering.
	require.Contains(t, captured.MessageAttributes, "PipelineID")
	assert.Equal(t, "pipeline-42", *captured.MessageAttributes["PipelineID"].StringValue)

	require.Contains(t, captured.MessageAttributes, "ErrorType")
	assert.Equal(t, "PROVIDER", *captured.MessageAttributes["ErrorType"].StringValue)
}

func TestSQSRouter_RouteFallbackOnSendFailure(t *testing.T) {
	client := &mockSQSClient{
		sendFn: func(context.Context, *sqs.SendMessageInput, ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
			return nil, errors.New("network timeout")
		},
	}

	// Capture slog output to verify the fallback log fires.
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelError}))
	restore := slog.Default()
	slog.SetDefault(logger)
	t.Cleanup(func() { slog.SetDefault(restore) })

	router, routerErr := dlq.NewSQSRouter(client, "https://sqs.example.com/dlq", nil)
	require.NoError(t, routerErr)

	rec := testRecord()
	err := router.Route(context.Background(), rec)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "send record")
	assert.Contains(t, err.Error(), "network timeout")

	// The full payload must appear in the error log.
	logged := buf.String()
	assert.Contains(t, logged, rec.PipelineID)
	assert.Contains(t, logged, "payload")
}

func TestSQSRouter_CounterIncrements(t *testing.T) {
	client := &mockSQSClient{
		sendFn: func(context.Context, *sqs.SendMessageInput, ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
			return &sqs.SendMessageOutput{}, nil
		},
	}

	counter := &mockCounter{}
	router, err := dlq.NewSQSRouter(client, "https://sqs.example.com/dlq", counter)
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		require.NoError(t, router.Route(context.Background(), testRecord()))
	}
	assert.Equal(t, int64(5), counter.count.Load())
}

func TestSQSRouter_CounterIncrementsOnFailure(t *testing.T) {
	client := &mockSQSClient{
		sendFn: func(context.Context, *sqs.SendMessageInput, ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
			return nil, errors.New("fail")
		},
	}

	// Suppress slog output for this test.
	slog.SetDefault(slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil)))

	counter := &mockCounter{}
	router, err := dlq.NewSQSRouter(client, "https://sqs.example.com/dlq", counter)
	require.NoError(t, err)

	_ = router.Route(context.Background(), testRecord())
	assert.Equal(t, int64(1), counter.count.Load(), "counter must increment even when SendMessage fails")
}

func TestNewSQSRouter_EmptyQueueURL(t *testing.T) {
	client := &mockSQSClient{}
	_, err := dlq.NewSQSRouter(client, "", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "queueURL must not be empty")
}

func TestNoopRouter_ReturnsNil(t *testing.T) {
	var router dlq.NoopRouter
	err := router.Route(context.Background(), testRecord())
	assert.NoError(t, err)
}
