package alert

import (
	"fmt"
	"strings"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// FormatAlertText builds the Slack message text from event detail.
func FormatAlertText(detailType string, detail types.InterlockEvent) string {
	emoji := alertEmoji(detailType)
	header := fmt.Sprintf("%s *%s* | %s | %s", emoji, detailType, detail.PipelineID, detail.Date)

	if len(detail.Detail) == 0 {
		return header + "\n" + detail.Message
	}

	var parts []string
	if v, ok := detail.Detail["deadline"]; ok {
		if breachAt, ok2 := detail.Detail["breachAt"]; ok2 {
			parts = append(parts, fmt.Sprintf("Deadline %v (%v)", v, breachAt))
		} else {
			parts = append(parts, fmt.Sprintf("Deadline %v", v))
		}
	}
	if v, ok := detail.Detail["status"]; ok {
		parts = append(parts, fmt.Sprintf("Status: %v", v))
	}
	if v, ok := detail.Detail["source"]; ok {
		parts = append(parts, fmt.Sprintf("Source: %v", v))
	}
	if v, ok := detail.Detail["cron"]; ok {
		parts = append(parts, fmt.Sprintf("Cron: %v", v))
	}

	text := header
	if len(parts) > 0 {
		text += "\n" + strings.Join(parts, " · ")
	}
	if hint, ok := detail.Detail["actionHint"]; ok {
		text += fmt.Sprintf("\n→ %v", hint)
	}
	return text
}

func alertEmoji(detailType string) string {
	switch detailType {
	case string(types.EventSLABreach), string(types.EventJobFailed),
		string(types.EventValidationExhausted), string(types.EventRetryExhausted),
		string(types.EventInfraFailure), string(types.EventSFNTimeout),
		string(types.EventScheduleMissed), string(types.EventDataDrift),
		string(types.EventJobPollExhausted):
		return "\xf0\x9f\x94\xb4" // red circle
	case string(types.EventSLAWarning):
		return "\xf0\x9f\x9f\xa1" // yellow circle
	case string(types.EventSLAMet):
		return "\xe2\x9c\x85" // check mark
	default:
		return "\xe2\x84\xb9\xef\xb8\x8f" // info
	}
}
