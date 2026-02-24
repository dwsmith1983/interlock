// Package alert implements alert dispatching to multiple sinks.
package alert

import (
	"fmt"
	"log/slog"

	"github.com/dwsmith1983/interlock/internal/metrics"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// Sink is an alert destination.
type Sink interface {
	Send(alert types.Alert) error
	Name() string
}

// Dispatcher routes alerts to configured sinks.
type Dispatcher struct {
	sinks  []Sink
	logger *slog.Logger
}

// NewDispatcher creates a dispatcher from alert configs.
// If logger is nil, slog.Default() is used.
func NewDispatcher(configs []types.AlertConfig, logger *slog.Logger) (*Dispatcher, error) {
	if logger == nil {
		logger = slog.Default()
	}
	d := &Dispatcher{logger: logger}
	for _, cfg := range configs {
		sink, err := newSink(cfg)
		if err != nil {
			return nil, fmt.Errorf("creating %s sink: %w", cfg.Type, err)
		}
		d.sinks = append(d.sinks, sink)
	}
	return d, nil
}

// Dispatch sends an alert to all configured sinks.
func (d *Dispatcher) Dispatch(alert types.Alert) {
	for _, sink := range d.sinks {
		if err := sink.Send(alert); err != nil {
			metrics.AlertsFailed.Add(1)
			d.logger.Error("alert dispatch failed", "sink", sink.Name(), "error", err)
		} else {
			metrics.AlertsDispatched.Add(1)
		}
	}
}

// AddSink appends a sink to the dispatcher.
func (d *Dispatcher) AddSink(s Sink) {
	d.sinks = append(d.sinks, s)
}

// AlertFunc returns a function suitable for use as the engine's alert callback.
func (d *Dispatcher) AlertFunc() func(types.Alert) {
	return d.Dispatch
}

func newSink(cfg types.AlertConfig) (Sink, error) {
	switch cfg.Type {
	case types.AlertConsole:
		return NewConsoleSink(), nil
	case types.AlertWebhook:
		if cfg.URL == "" {
			return nil, fmt.Errorf("webhook URL required")
		}
		return NewWebhookSink(cfg.URL), nil
	case types.AlertFile:
		if cfg.Path == "" {
			return nil, fmt.Errorf("file path required")
		}
		return NewFileSink(cfg.Path)
	case types.AlertSNS:
		return NewSNSSink(cfg.TopicARN)
	case types.AlertS3:
		return NewS3Sink(cfg.BucketName, cfg.Prefix)
	case types.AlertPubSub:
		return NewPubSubSink(cfg.ProjectID, cfg.TopicID)
	default:
		return nil, fmt.Errorf("unknown alert type %q", cfg.Type)
	}
}
