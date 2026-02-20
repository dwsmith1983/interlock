// Package alert implements alert dispatching to multiple sinks.
package alert

import (
	"fmt"

	"github.com/interlock-systems/interlock/pkg/types"
)

// Sink is an alert destination.
type Sink interface {
	Send(alert types.Alert) error
	Name() string
}

// Dispatcher routes alerts to configured sinks.
type Dispatcher struct {
	sinks []Sink
}

// NewDispatcher creates a dispatcher from alert configs.
func NewDispatcher(configs []types.AlertConfig) (*Dispatcher, error) {
	d := &Dispatcher{}
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
			fmt.Printf("[alert] error sending to %s: %v\n", sink.Name(), err)
		}
	}
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
	default:
		return nil, fmt.Errorf("unknown alert type %q", cfg.Type)
	}
}
