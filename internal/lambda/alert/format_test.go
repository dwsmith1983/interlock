package alert_test

import (
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/dwsmith1983/interlock/internal/lambda/alert"
	"github.com/dwsmith1983/interlock/pkg/types"
)

func TestFormatAlertText_MessageAndEscaping(t *testing.T) {
	tests := []struct {
		name   string
		detail types.InterlockEvent
		want   []string
		reject []string
	}{
		{
			name: "detail keeps the message",
			detail: types.InterlockEvent{PipelineID: "p", Date: "2026-03-01", Message: "retry limit reached (2/2) for p",
				Detail: map[string]interface{}{"retries": 2, "maxRetries": 2}},
			want: []string{"*RETRY_EXHAUSTED* | p | 2026-03-01", "\nretry limit reached (2/2) for p"},
		},
		{
			name: "mrkdwn control characters are escaped everywhere",
			detail: types.InterlockEvent{PipelineID: "a&b", Date: "2026-03-01", Message: "job fail: <!channel> & co",
				Detail: map[string]interface{}{"status": "<ok>", "actionHint": ">run it"}},
			want:   []string{"| a&amp;b |", "job fail: &lt;!channel&gt; &amp; co", "Status: &lt;ok&gt;", "→ &gt;run it"},
			reject: []string{"<!channel>", "a&b", "<ok>"},
		},
		{
			name: "deadline, breachAt, source and cron are escaped",
			detail: types.InterlockEvent{PipelineID: "p", Message: "m",
				Detail: map[string]interface{}{"deadline": "<8>", "breachAt": "a&b", "source": "<src>", "cron": "0 > 1"}},
			want:   []string{"Deadline &lt;8&gt; (a&amp;b)", "Source: &lt;src&gt;", "Cron: 0 &gt; 1"},
			reject: []string{"<8>", "a&b", "<src>", "0 > 1"},
		},
		{
			name: "deadline without breachAt",
			detail: types.InterlockEvent{PipelineID: "bronze-cdr", Date: "2026-03-06", Message: "test",
				Detail: map[string]interface{}{"deadline": ":30"}},
			want:   []string{"\nDeadline :30"},
			reject: []string{"("},
		},
		{
			name:   "no detail is escaped too",
			detail: types.InterlockEvent{PipelineID: "p", Message: "x<y"},
			want:   []string{"\nx&lt;y"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			text := alert.FormatAlertText(string(types.EventRetryExhausted), tt.detail)
			for _, w := range tt.want {
				if !strings.Contains(text, w) {
					t.Errorf("text %q, want it to contain %q", text, w)
				}
			}
			for _, r := range tt.reject {
				if strings.Contains(text, r) {
					t.Errorf("text %q contains unescaped %q", text, r)
				}
			}
		})
	}
}

// TestFormatAlertText_CapsLength: Slack section text has a hard limit, so
// the escaped text is cut to maxSectionChars characters, never inside an
// entity or a rune, and the cut is marked.
func TestFormatAlertText_CapsLength(t *testing.T) {
	for name, msg := range map[string]string{
		"2 KB of <": strings.Repeat("<", 2048),
		"runes":     strings.Repeat("é", 4000),
		"mixed":     strings.Repeat("a&", 1500),
	} {
		t.Run(name, func(t *testing.T) {
			text := alert.FormatAlertText(string(types.EventJobFailed), types.InterlockEvent{PipelineID: "p", Message: msg})
			if n := utf8.RuneCountInString(text); n > 2900 || !utf8.ValidString(text) || !strings.HasSuffix(text, "...") {
				t.Fatalf("text has %d characters (valid UTF-8 %v), ends %q; want at most 2900 ending in ...", n, utf8.ValidString(text), text[len(text)-12:])
			}
			body := strings.TrimSuffix(text, "...")
			if amp := strings.LastIndex(body, "&"); amp >= 0 && !strings.Contains(body[amp:], ";") {
				t.Errorf("text is cut inside an entity: ...%q", body[amp:])
			}
		})
	}
}

// TestFormatAlertText_ConfigInvalid: a rejected config stops that pipeline,
// so the alert is red and names the config version.
func TestFormatAlertText_ConfigInvalid(t *testing.T) {
	text := alert.FormatAlertText(string(types.EventConfigInvalid), types.InterlockEvent{
		PipelineID: "q", Message: "config for q rejected: <bad>",
		Detail: map[string]interface{}{"reason": "<bad>", "hash": "0123456789abcdef"},
	})
	want := "\xf0\x9f\x94\xb4 *CONFIG_INVALID* | q | \nconfig for q rejected: &lt;bad&gt;\nConfig hash: 0123456789abcdef"
	if text != want {
		t.Fatalf("text = %q, want %q", text, want)
	}
}

// TestFormatAlertText_ControlCharacters: a value cannot start a new line (and
// so fake a second alert) or carry any other control character.
func TestFormatAlertText_ControlCharacters(t *testing.T) {
	text := alert.FormatAlertText(string(types.EventConfigInvalid), types.InterlockEvent{
		PipelineID: "q\n\xf0\x9f\x94\xb4 *SLA_BREACH* | prod", Message: "a\tb\r\nc\x00d\x7fe",
		Detail: map[string]interface{}{"hash": "h\x1b"},
	})
	want := "\xf0\x9f\x94\xb4 *CONFIG_INVALID* | q \xf0\x9f\x94\xb4 *SLA_BREACH* | prod | \na b  c d e\nConfig hash: h "
	if text != want {
		t.Fatalf("text = %q, want %q", text, want)
	}
}

// TestFormatAlertText_UnicodeControls: characters that may break a line or
// reorder the text after them become one space each; other text is unchanged.
func TestFormatAlertText_UnicodeControls(t *testing.T) {
	message := func(msg string) string {
		text := alert.FormatAlertText("JOB_FAILED", types.InterlockEvent{PipelineID: "p", Date: "2026-03-01", Message: msg})
		_, line, _ := strings.Cut(text, "\n")
		return line
	}
	for _, r := range []rune{
		'\u0080', '\u0085', '\u009f', // C1 controls
		'\u2028', '\u2029', // line and paragraph separators
		'\u061c', '\u200e', '\u200f', '\u202a', '\u202e', '\u2066', '\u2069', // bidi controls
	} {
		t.Run(fmt.Sprintf("replaced %U", r), func(t *testing.T) {
			if got, want := message("a"+string(r)+"b"), "a b"; got != want {
				t.Errorf("message = %q, want %q", got, want)
			}
		})
	}
	for name, msg := range map[string]string{
		"zero-width joiner emoji": "\U0001f468\u200d\U0001f469\u200d\U0001f467",
		"accented letter":         "caf\u00e9",
		"non-breaking space":      "a\u00a0b",
		"formatting marks":        "*a* _b_ ~c~ `d`",
	} {
		t.Run("kept "+name, func(t *testing.T) {
			if got := message(msg); got != msg {
				t.Errorf("message = %q, want %q", got, msg)
			}
		})
	}
}

// TestFormatAlertText_CutoffPassed: a run date closed at its cutoff is a
// final failure, so the alert is red and shows the cutoff and when it fell.
func TestFormatAlertText_CutoffPassed(t *testing.T) {
	text := alert.FormatAlertText(string(types.EventSensorDeadlineExpired), types.InterlockEvent{
		PipelineID: "p", Date: "2026-03-10", Message: "m",
		Detail: map[string]interface{}{"source": "watchdog", "cutoff": "14:00", "cutoffAt": "2026-03-10T14:00:00Z", "actionHint": "rerun"},
	})
	want := "\xf0\x9f\x94\xb4 *SENSOR_DEADLINE_EXPIRED* | p | 2026-03-10\nm\nSource: watchdog · Cutoff: 14:00 · Cutoff at: 2026-03-10T14:00:00Z\n→ rerun"
	if text != want {
		t.Fatalf("text = %q, want %q", text, want)
	}
}

// TestFormatAlertText_SLADeadline: an SLA alert shows its configured deadline
// with the time zone it is in, and the UTC instant that was. Every value is
// escaped, and a verdict that has no deadline shows none.
func TestFormatAlertText_SLADeadline(t *testing.T) {
	type m = map[string]interface{}
	for _, tt := range []struct {
		name, detailType string
		detail           m
		want             string
	}{
		{"scheduled breach", "SLA_BREACH", m{"status": "RUNNING", "source": "schedule", "actionHint": "h", "deadline": "06:00", "timezone": "America/New_York", "breachAt": "2026-03-10T10:00:00Z"},
			"Deadline 06:00 America/New_York (2026-03-10T10:00:00Z) · Status: RUNNING · Source: schedule\n→ h"},
		{"met", "SLA_MET", m{"status": "COMPLETED", "source": "cancel", "deadline": ":30", "timezone": "UTC", "breachAt": "2026-03-10T08:30:00Z"},
			"Deadline :30 UTC (2026-03-10T08:30:00Z) · Status: COMPLETED · Source: cancel"},
		{"no timezone", "SLA_WARNING", m{"deadline": ":30", "breachAt": "b"}, "Deadline :30 (b)"},
		{"no breachAt", "SLA_WARNING", m{"deadline": ":30", "timezone": "UTC"}, "Deadline :30 UTC"},
		{"no deadline", "SLA_MET", m{"source": "cancel", "maxDuration": "2h", "timezone": "UTC", "breachAt": "b"}, "Source: cancel"},
		{"escaped", "SLA_WARNING", m{"deadline": "<8>", "timezone": "<\nz>", "breachAt": "a&b"}, "Deadline &lt;8&gt; &lt; z&gt; (a&amp;b)"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			text := alert.FormatAlertText(tt.detailType, types.InterlockEvent{PipelineID: "p", Message: "m", Detail: tt.detail})
			if _, got, _ := strings.Cut(text, "\nm\n"); got != tt.want {
				t.Errorf("text = %q, want it to end %q", text, tt.want)
			}
		})
	}
}

// TestFormatAlertText_ManualRerunRequired: the alert names why, the job, its
// last run, when the run finished and the late data arrived or the request
// was written, the sensor key and what to do. Sensor data, baselines and the
// request row stay in the event, and every value is escaped.
func TestFormatAlertText_ManualRerunRequired(t *testing.T) {
	type m = map[string]interface{}
	common := func(extra m) m {
		d := m{"reason": "late-data", "blockedBy": "job.maxAutoRerunAge", "jobType": "airflow",
			"job":       m{"url": "https://airflow.example.com", "dagId": "daily-orders"},
			"lastRunId": "run-1", "completedAt": "2026-03-10T12:00:00Z", "finishedAgo": "25h0m0s",
			"baseline":     m{"count": "baseline-7"},
			"rerunRequest": m{"table": "control", "PK": "PIPELINE#pk-secret", "SK": "RERUN_REQUEST#daily#2026-03-10", "reason": "manual"},
			"actionHint":   "rerun by hand"}
		for k, v := range extra {
			d[k] = v
		}
		return d
	}
	const head = "\xf0\x9f\x94\xb4 *MANUAL_RERUN_REQUIRED* | p | 2026-03-10\nm\nReason: late-data (job.maxAutoRerunAge)" +
		"\nJob: airflow dagId=daily-orders url=https://airflow.example.com\nLast run: run-1" +
		"\nRun finished: 2026-03-10T12:00:00Z (25h0m0s ago)"
	for _, tt := range []struct {
		name   string
		detail m
		want   string
	}{
		{"sensor", common(m{"source": "sensor", "arrivedAt": "2026-03-11T12:59:30Z", "sensorKey": "upstream",
			"sensorData": m{"count": "value-42"}}),
			head + "\nArrived: 2026-03-11T12:59:30Z\nSensor key: upstream\n\u2192 rerun by hand"},
		{"rerun request", common(m{"source": "rerun-request", "reason": "data-drift", "blockedBy": "postRun.watchWindow",
			"requestedAt": "2026-03-11T13:00:00Z"}),
			strings.Replace(head, "late-data (job.maxAutoRerunAge)", "data-drift (postRun.watchWindow)", 1) +
				"\nRequested: 2026-03-11T13:00:00Z\n\u2192 rerun by hand"},
		{"escaped", m{"reason": "late<!channel>", "jobType": "http", "job": m{"url": "https://a.example.com/x&y", "method": "PO\nST"},
			"lastRunId": "run\u2028<2>", "sensorKey": "k\t>"},
			"\xf0\x9f\x94\xb4 *MANUAL_RERUN_REQUIRED* | p | 2026-03-10\nm\nReason: late&lt;!channel&gt;" +
				"\nJob: http method=PO ST url=https://a.example.com/x&amp;y\nLast run: run &lt;2&gt;\nSensor key: k &gt;"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			text := alert.FormatAlertText(string(types.EventManualRerunRequired), types.InterlockEvent{
				PipelineID: "p", Date: "2026-03-10", Message: "m", Detail: tt.detail})
			if text != tt.want {
				t.Errorf("text = %q, want %q", text, tt.want)
			}
			for _, leak := range []string{"value-42", "baseline-7", "pk-secret", "Source"} {
				if strings.Contains(text, leak) {
					t.Errorf("text %q contains %q", text, leak)
				}
			}
		})
	}
	t.Run("capped", func(t *testing.T) {
		text := alert.FormatAlertText(string(types.EventManualRerunRequired), types.InterlockEvent{
			PipelineID: "p", Date: "2026-03-10", Message: "m", Detail: common(m{"sensorKey": strings.Repeat("<", 2048)})})
		if n := utf8.RuneCountInString(text); n > 2900 || !strings.HasSuffix(text, "...") {
			t.Fatalf("text has %d characters, ends %q; want at most 2900 ending in ...", n, text[len(text)-12:])
		}
		if body := strings.TrimSuffix(text, "..."); !strings.HasSuffix(body, "&lt;") {
			t.Errorf("text is cut inside an entity: ...%q", body[len(body)-12:])
		}
	})
}

// TestAlertEmoji: failures are red, warnings yellow, a met SLA a check mark
// and everything else info, and the emoji leads the header.
func TestAlertEmoji(t *testing.T) {
	for emoji, detailTypes := range map[string][]string{
		"\xf0\x9f\x94\xb4": {"SLA_BREACH", "JOB_FAILED", "VALIDATION_EXHAUSTED", "RETRY_EXHAUSTED", "INFRA_FAILURE",
			"SFN_TIMEOUT", "SCHEDULE_MISSED", "DATA_DRIFT", "CONFIG_INVALID", "SENSOR_DEADLINE_EXPIRED",
			"RELATIVE_SLA_BREACH", "IRREGULAR_SCHEDULE_MISSED", "POST_RUN_FAILED", "POST_RUN_DRIFT",
			"BASELINE_CAPTURE_FAILED", "INFRA_ALARM", "MANUAL_RERUN_REQUIRED"},
		"\xf0\x9f\x9f\xa1": {"SLA_WARNING", "RELATIVE_SLA_WARNING"},
		"\xe2\x9c\x85":     {"SLA_MET"},
		"\xe2\x84\xb9\xef\xb8\x8f": {"UNKNOWN_TYPE", "JOB_TRIGGERED", "JOB_COMPLETED", "TRIGGER_RECOVERED",
			"POST_RUN_DRIFT_INFLIGHT", "POST_RUN_SENSOR_MISSING", "RERUN_REJECTED", "DRY_RUN_DRIFT"},
	} {
		for _, detailType := range detailTypes {
			t.Run(detailType, func(t *testing.T) {
				text := alert.FormatAlertText(detailType, types.InterlockEvent{PipelineID: "p", Date: "2026-03-04", Message: "m"})
				if want := emoji + " *" + detailType + "*"; !strings.HasPrefix(text, want) {
					t.Errorf("text %q, want prefix %q", text, want)
				}
			})
		}
	}
}
