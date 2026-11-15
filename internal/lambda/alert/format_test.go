package alert_test

import (
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
