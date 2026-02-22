package alert

import (
	"fmt"

	"github.com/fatih/color"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// ConsoleSink writes alerts to the terminal with color.
type ConsoleSink struct{}

// NewConsoleSink creates a new console alert sink.
func NewConsoleSink() *ConsoleSink {
	return &ConsoleSink{}
}

// Name returns the sink identifier.
func (s *ConsoleSink) Name() string { return "console" }

// Send writes an alert to the terminal with color-coded severity.
func (s *ConsoleSink) Send(alert types.Alert) error {
	var prefix string
	switch alert.Level {
	case types.AlertLevelError:
		prefix = color.RedString("[ERROR]")
	case types.AlertLevelWarning:
		prefix = color.YellowString("[WARN]")
	default:
		prefix = color.CyanString("[INFO]")
	}

	if alert.PipelineID != "" {
		fmt.Printf("%s [%s] %s\n", prefix, alert.PipelineID, alert.Message)
	} else {
		fmt.Printf("%s %s\n", prefix, alert.Message)
	}
	return nil
}
