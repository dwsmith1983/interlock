package lambda

import (
	"fmt"
	"strings"
	"testing"
)

func TestNextExecName(t *testing.T) {
	long := strings.Repeat("x", 60) + "-s-2026-03-01-1709280000" // 84 characters before truncation
	firstRun := TruncateExecName(fmt.Sprintf("%s-%s-%s-%d", "p", "s", "2026-03-01", 1709280000))
	requested := TruncateExecName(fmt.Sprintf("%s-%s-%s-%s-rerun-%d", "p", "s", "2026-03-01", "manual", 1709280000))
	tests := []struct {
		name, exec  string
		wantRetries int
		wantNext    string
	}{
		{"first run starts at 0", firstRun, 0, firstRun + "-r1"},
		{"requested rerun starts at 0", requested, 0, requested + "-r1"},
		{"retry suffix is replaced", firstRun + "-r1", 1, firstRun + "-r2"},
		{"multi-digit suffix", firstRun + "-r9", 9, firstRun + "-r10"},
		{"truncation keeps the suffix", TruncateExecName(long), 0, TruncateExecName(long + "-r1")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := RetriesSoFar(tt.exec); got != tt.wantRetries {
				t.Errorf("RetriesSoFar(%q) = %d, want %d", tt.exec, got, tt.wantRetries)
			}
			got := NextExecName(tt.exec)
			if got != tt.wantNext || len(got) > SFNExecNameMaxLen || !strings.HasSuffix(got, fmt.Sprintf("-r%d", tt.wantRetries+1)) {
				t.Errorf("NextExecName(%q) = %q, want %q", tt.exec, got, tt.wantNext)
			}
		})
	}
}
