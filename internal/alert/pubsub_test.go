package alert

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockPubSub struct {
	published []*pubsub.Message
}

func (m *mockPubSub) Publish(_ context.Context, msg *pubsub.Message) (string, error) {
	m.published = append(m.published, msg)
	return "msg-123", nil
}

func TestPubSubSink_Send(t *testing.T) {
	mock := &mockPubSub{}
	sink, err := NewPubSubSink("", "alerts", WithPubSubClient(mock))
	require.NoError(t, err)

	a := types.Alert{
		Level:      types.AlertLevelError,
		PipelineID: "test-pipeline",
		Message:    "Pipeline blocked",
		Timestamp:  time.Date(2026, 2, 22, 10, 0, 0, 0, time.UTC),
	}

	err = sink.Send(a)
	require.NoError(t, err)

	require.Len(t, mock.published, 1)
	msg := mock.published[0]
	assert.Equal(t, "error", msg.Attributes["level"])
	assert.Equal(t, "test-pipeline", msg.Attributes["pipelineID"])

	var decoded types.Alert
	require.NoError(t, json.Unmarshal(msg.Data, &decoded))
	assert.Equal(t, types.AlertLevelError, decoded.Level)
	assert.Equal(t, "test-pipeline", decoded.PipelineID)
	assert.Equal(t, "Pipeline blocked", decoded.Message)
}

func TestPubSubSink_Name(t *testing.T) {
	mock := &mockPubSub{}
	sink, err := NewPubSubSink("", "alerts", WithPubSubClient(mock))
	require.NoError(t, err)
	assert.Equal(t, "pubsub", sink.Name())
}

func TestPubSubSink_EmptyTopicID(t *testing.T) {
	_, err := NewPubSubSink("project", "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "topic ID required")
}
