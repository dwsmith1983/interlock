package alert

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// Webhook HTTP delivery defaults.
const (
	webhookTimeout    = 10 * time.Second
	webhookRetryDelay = 2 * time.Second
)

// WebhookSink sends alerts as JSON POST requests to a URL.
type WebhookSink struct {
	url    string
	client *http.Client
	logger *slog.Logger
}

// NewWebhookSink creates a new webhook alert sink.
func NewWebhookSink(url string) *WebhookSink {
	return &WebhookSink{
		url: url,
		client: &http.Client{
			Timeout: webhookTimeout,
		},
		logger: slog.Default(),
	}
}

// Name returns the sink identifier.
func (s *WebhookSink) Name() string { return "webhook" }

// Send posts the alert as JSON to the configured webhook URL.
// On failure, it retries once after a 2-second delay.
func (s *WebhookSink) Send(alert types.Alert) error {
	data, err := json.Marshal(alert)
	if err != nil {
		return err
	}

	err = s.doPost(data)
	if err != nil {
		s.logger.Warn("webhook first attempt failed, retrying", "url", s.url, "error", err)
		time.Sleep(webhookRetryDelay)
		err = s.doPost(data)
		if err != nil {
			return fmt.Errorf("webhook POST failed after retry: %w", err)
		}
	}
	return nil
}

func (s *WebhookSink) doPost(data []byte) error {
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
