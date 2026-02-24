package alert

import (
	"context"
	"encoding/json"
	"fmt"

	"cloud.google.com/go/pubsub"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// PubSubAPI is the subset of the Pub/Sub client used by PubSubSink.
type PubSubAPI interface {
	Publish(ctx context.Context, msg *pubsub.Message) (string, error)
}

// pubsubTopicWrapper adapts a *pubsub.Topic to PubSubAPI.
type pubsubTopicWrapper struct {
	topic *pubsub.Topic
}

func (w *pubsubTopicWrapper) Publish(ctx context.Context, msg *pubsub.Message) (string, error) {
	result := w.topic.Publish(ctx, msg)
	return result.Get(ctx)
}

// PubSubSink publishes alerts to a Pub/Sub topic.
type PubSubSink struct {
	client PubSubAPI
}

// PubSubSinkOption configures a PubSubSink.
type PubSubSinkOption func(*PubSubSink)

// WithPubSubClient sets a custom Pub/Sub client (useful for testing).
func WithPubSubClient(c PubSubAPI) PubSubSinkOption {
	return func(s *PubSubSink) { s.client = c }
}

// NewPubSubSink creates a new Pub/Sub alert sink.
func NewPubSubSink(projectID, topicID string, opts ...PubSubSinkOption) (*PubSubSink, error) {
	if topicID == "" {
		return nil, fmt.Errorf("Pub/Sub topic ID required")
	}
	s := &PubSubSink{}
	for _, o := range opts {
		o(s)
	}
	if s.client == nil {
		if projectID == "" {
			return nil, fmt.Errorf("Pub/Sub project ID required")
		}
		client, err := pubsub.NewClient(context.Background(), projectID)
		if err != nil {
			return nil, fmt.Errorf("creating Pub/Sub client: %w", err)
		}
		s.client = &pubsubTopicWrapper{topic: client.Topic(topicID)}
	}
	return s, nil
}

// Name returns the sink identifier.
func (s *PubSubSink) Name() string { return "pubsub" }

// Send publishes the alert as JSON to the configured Pub/Sub topic.
func (s *PubSubSink) Send(alert types.Alert) error {
	data, err := json.Marshal(alert)
	if err != nil {
		return fmt.Errorf("marshaling alert: %w", err)
	}

	_, err = s.client.Publish(context.Background(), &pubsub.Message{
		Data: data,
		Attributes: map[string]string{
			"level":      string(alert.Level),
			"pipelineID": alert.PipelineID,
		},
	})
	if err != nil {
		return fmt.Errorf("publishing to Pub/Sub: %w", err)
	}

	return nil
}
