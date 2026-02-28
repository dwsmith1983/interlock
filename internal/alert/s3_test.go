package alert

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dwsmith1983/interlock/pkg/types"
)

type mockS3Client struct {
	lastInput *s3.PutObjectInput
	err       error
}

func (m *mockS3Client) PutObject(_ context.Context, input *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	m.lastInput = input
	return &s3.PutObjectOutput{}, m.err
}

func TestS3Sink_Send(t *testing.T) {
	mock := &mockS3Client{}
	sink, err := NewS3Sink("my-bucket", "alerts", WithS3Client(mock))
	require.NoError(t, err)

	assert.Equal(t, "s3", sink.Name())

	now := time.Date(2026, 2, 23, 14, 30, 0, 0, time.UTC)
	err = sink.Send(context.Background(), types.Alert{
		Level:      types.AlertLevelWarning,
		PipelineID: "test-pipeline",
		Message:    "SLA breached",
		Timestamp:  now,
	})
	require.NoError(t, err)

	require.NotNil(t, mock.lastInput)
	assert.Equal(t, "my-bucket", *mock.lastInput.Bucket)
	assert.Contains(t, *mock.lastInput.Key, "alerts/2026-02-23/test-pipeline/")
	assert.Contains(t, *mock.lastInput.Key, "-warning.json")
	assert.Equal(t, "application/json", *mock.lastInput.ContentType)
}

func TestS3Sink_MissingBucket(t *testing.T) {
	_, err := NewS3Sink("", "prefix")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "bucket name required")
}

func TestS3Sink_EmptyPipelineID(t *testing.T) {
	mock := &mockS3Client{}
	sink, err := NewS3Sink("bucket", "alerts", WithS3Client(mock))
	require.NoError(t, err)

	err = sink.Send(context.Background(), types.Alert{
		Level:     types.AlertLevelError,
		Message:   "system error",
		Timestamp: time.Now(),
	})
	require.NoError(t, err)
	assert.Contains(t, *mock.lastInput.Key, "/system/")
}
