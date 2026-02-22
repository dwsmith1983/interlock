package alert

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockSNS struct {
	published []*sns.PublishInput
}

func (m *mockSNS) Publish(_ context.Context, input *sns.PublishInput, _ ...func(*sns.Options)) (*sns.PublishOutput, error) {
	m.published = append(m.published, input)
	return &sns.PublishOutput{}, nil
}

func TestSNSSink_Send(t *testing.T) {
	mock := &mockSNS{}
	sink, err := NewSNSSink("arn:aws:sns:us-east-1:123456789:alerts", WithSNSClient(mock))
	require.NoError(t, err)

	alert := types.Alert{
		Level:      types.AlertLevelError,
		PipelineID: "test-pipeline",
		Message:    "Pipeline blocked",
		Timestamp:  time.Date(2026, 2, 22, 10, 0, 0, 0, time.UTC),
	}

	err = sink.Send(alert)
	require.NoError(t, err)

	require.Len(t, mock.published, 1)
	pub := mock.published[0]
	assert.Equal(t, "arn:aws:sns:us-east-1:123456789:alerts", *pub.TopicArn)
	assert.Equal(t, "[error] test-pipeline", *pub.Subject)

	var decoded types.Alert
	require.NoError(t, json.Unmarshal([]byte(*pub.Message), &decoded))
	assert.Equal(t, types.AlertLevelError, decoded.Level)
	assert.Equal(t, "test-pipeline", decoded.PipelineID)
	assert.Equal(t, "Pipeline blocked", decoded.Message)
}

func TestSNSSink_Name(t *testing.T) {
	mock := &mockSNS{}
	sink, err := NewSNSSink("arn:aws:sns:us-east-1:123456789:alerts", WithSNSClient(mock))
	require.NoError(t, err)
	assert.Equal(t, "sns", sink.Name())
}

func TestSNSSink_EmptyTopicARN(t *testing.T) {
	_, err := NewSNSSink("")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "topic ARN required")
}

func TestSNSSink_SubjectTruncation(t *testing.T) {
	mock := &mockSNS{}
	sink, err := NewSNSSink("arn:aws:sns:us-east-1:123456789:alerts", WithSNSClient(mock))
	require.NoError(t, err)

	alert := types.Alert{
		Level:      types.AlertLevelWarning,
		PipelineID: "this-is-a-very-long-pipeline-name-that-exceeds-the-normal-subject-length-limit-for-sns-messages-in-practice",
		Message:    "test",
		Timestamp:  time.Now(),
	}

	err = sink.Send(alert)
	require.NoError(t, err)
	assert.LessOrEqual(t, len(*mock.published[0].Subject), 100)
}
