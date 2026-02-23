package schedule

import "testing"

func TestLockKey(t *testing.T) {
	tests := []struct {
		pipelineID string
		scheduleID string
		want       string
	}{
		{"my-pipeline", "daily", "eval:my-pipeline:daily"},
		{"gharchive-silver", "h14", "eval:gharchive-silver:h14"},
		{"p", "", "eval:p:"},
	}
	for _, tt := range tests {
		got := LockKey(tt.pipelineID, tt.scheduleID)
		if got != tt.want {
			t.Errorf("LockKey(%q, %q) = %q, want %q", tt.pipelineID, tt.scheduleID, got, tt.want)
		}
	}
}
