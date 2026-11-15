package lambda

import "testing"

func TestIsValidExecutionDate(t *testing.T) {
	for date, want := range map[string]bool{
		"2026-03-07": true, "2026-03-07T00": true, "2026-03-07T23": true,
		"2026-13-45": false, "2026-03-07T24": false, "2026-03-07T5": false, "2026-03-07T+7": false, "2026-03-07T-1": false, "20260307": false, "": false, "x": false,
	} {
		if got := IsValidExecutionDate(date); got != want {
			t.Errorf("IsValidExecutionDate(%q) = %v, want %v", date, got, want)
		}
	}
}
