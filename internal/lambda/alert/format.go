package alert

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// mrkdwn escapes the characters Slack mrkdwn treats as control characters, so
// a value can neither mention (<!channel>) nor link.
var mrkdwn = strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;")

// esc makes v safe for Slack text: every control character (C0, DEL and C1,
// so newline, CR and tab too), line or paragraph separator and bidirectional
// control becomes a space, so a value can neither start a line that looks like
// another alert nor override the direction of the text after it, then mrkdwn is
// escaped. Other invisible characters, such as the zero-width joiner, are kept.
func esc(v interface{}) string {
	return mrkdwn.Replace(strings.Map(func(r rune) rune {
		if unicode.IsControl(r) || unicode.In(r, unicode.Zl, unicode.Zp, unicode.Bidi_Control) {
			return ' '
		}
		return r
	}, fmt.Sprint(v)))
}

// FormatAlertText builds the Slack message text from event detail: a header,
// the event's message, then any known detail fields. Every value is escaped.
func FormatAlertText(detailType string, detail types.InterlockEvent) string {
	emoji := alertEmoji(detailType)
	text := fmt.Sprintf("%s *%s* | %s | %s", emoji, detailType, esc(detail.PipelineID), esc(detail.Date))
	if detail.Message != "" || len(detail.Detail) == 0 {
		text += "\n" + esc(detail.Message)
	}
	if detailType == string(types.EventManualRerunRequired) {
		return capText(text + manualRerunText(detail.Detail))
	}

	var parts []string
	if v, ok := detail.Detail["deadline"]; ok {
		deadline := "Deadline " + esc(v)
		if timezone, ok2 := detail.Detail["timezone"]; ok2 {
			deadline += " " + esc(timezone)
		}
		if breachAt, ok2 := detail.Detail["breachAt"]; ok2 {
			deadline += " (" + esc(breachAt) + ")"
		}
		parts = append(parts, deadline)
	}
	for _, f := range []struct{ key, label string }{
		{"status", "Status"}, {"source", "Source"}, {"cron", "Cron"}, {"cutoff", "Cutoff"}, {"cutoffAt", "Cutoff at"}, {"hash", "Config hash"},
	} {
		if v, ok := detail.Detail[f.key]; ok {
			parts = append(parts, f.label+": "+esc(v))
		}
	}

	if len(parts) > 0 {
		text += "\n" + strings.Join(parts, " · ")
	}
	if hint, ok := detail.Detail["actionHint"]; ok {
		text += "\n→ " + esc(hint)
	}
	return capText(text)
}

// manualRerunText is the body of a MANUAL_RERUN_REQUIRED alert: why, the job
// and its last run, when the run finished, when the late data arrived or the
// request was written, the sensor key, then the action hint. Sensor data, any
// baseline and the request row stay in the event.
func manualRerunText(d map[string]interface{}) string {
	str := func(k string) string {
		if v, ok := d[k]; ok {
			return esc(v)
		}
		return ""
	}
	reason, finished, job := str("reason"), str("completedAt"), str("jobType")
	if by := str("blockedBy"); reason != "" && by != "" {
		reason += " (" + by + ")"
	}
	if ago := str("finishedAgo"); finished != "" && ago != "" {
		finished += " (" + ago + " ago)"
	}
	if ids, ok := d["job"].(map[string]interface{}); ok {
		for _, k := range slices.Sorted(maps.Keys(ids)) {
			job += " " + esc(k) + "=" + esc(ids[k])
		}
	}
	var text string
	for _, f := range [][2]string{{"Reason", reason}, {"Job", strings.TrimSpace(job)}, {"Last run", str("lastRunId")},
		{"Run finished", finished}, {"Arrived", str("arrivedAt")}, {"Requested", str("requestedAt")}, {"Sensor key", str("sensorKey")}} {
		if f[1] != "" {
			text += "\n" + f[0] + ": " + f[1]
		}
	}
	if hint := str("actionHint"); hint != "" {
		text += "\n→ " + hint
	}
	return text
}

// maxSectionChars keeps the text under Slack's 3000-character section limit.
const maxSectionChars = 2900

// capText cuts text to maxSectionChars characters, marking the cut with
// "...". It never splits a rune or an entity such as &lt;: every & left in
// escaped text starts one.
func capText(text string) string {
	if utf8.RuneCountInString(text) <= maxSectionChars {
		return text
	}
	const mark = "..."
	kept, n := text, 0
	for i := range text {
		if n == maxSectionChars-len(mark) {
			kept = text[:i]
			break
		}
		n++
	}
	if amp := strings.LastIndexByte(kept, '&'); amp >= 0 && !strings.Contains(kept[amp:], ";") {
		kept = kept[:amp]
	}
	return kept + mark
}

func alertEmoji(detailType string) string {
	switch detailType {
	case string(types.EventSLABreach), string(types.EventJobFailed),
		string(types.EventValidationExhausted), string(types.EventRetryExhausted),
		string(types.EventInfraFailure), string(types.EventSFNTimeout),
		string(types.EventScheduleMissed), string(types.EventDataDrift),
		string(types.EventConfigInvalid), string(types.EventSensorDeadlineExpired),
		string(types.EventRelativeSLABreach), string(types.EventIrregularScheduleMissed),
		string(types.EventPostRunFailed), string(types.EventPostRunDrift),
		string(types.EventBaselineCaptureFailed), string(types.EventInfraAlarm),
		string(types.EventManualRerunRequired):
		return "\xf0\x9f\x94\xb4" // red circle
	case string(types.EventSLAWarning), string(types.EventRelativeSLAWarning):
		return "\xf0\x9f\x9f\xa1" // yellow circle
	case string(types.EventSLAMet):
		return "\xe2\x9c\x85" // check mark
	default:
		return "\xe2\x84\xb9\xef\xb8\x8f" // info
	}
}
