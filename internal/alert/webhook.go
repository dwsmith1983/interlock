package alert

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/interlock-systems/interlock/pkg/types"
)

// WebhookSink sends alerts as JSON POST requests to a URL.
type WebhookSink struct {
	url    string
	client *http.Client
}

// NewWebhookSink creates a new webhook alert sink.
func NewWebhookSink(url string) *WebhookSink {
	return &WebhookSink{
		url: url,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// Name returns the sink identifier.
func (s *WebhookSink) Name() string { return "webhook" }

// Send posts the alert as JSON to the configured webhook URL.
func (s *WebhookSink) Send(alert types.Alert) error {
	data, err := json.Marshal(alert)
	if err != nil {
		return err
	}

	resp, err := s.client.Post(s.url, "application/json", bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("webhook POST failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("webhook returned status %d", resp.StatusCode)
	}
	return nil
}
