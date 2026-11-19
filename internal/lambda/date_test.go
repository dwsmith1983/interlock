package lambda

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

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

func TestResolveExecutionDate_WithDateAndHour(t *testing.T) {
	data := map[string]interface{}{"date": "20260303", "hour": "10", "complete": true}
	got := ResolveExecutionDate(data, time.Now())
	if got != "2026-03-03T10" {
		t.Errorf("got %q, want %q", got, "2026-03-03T10")
	}
}

func TestResolveExecutionDate_DashedDate(t *testing.T) {
	data := map[string]interface{}{"date": "2026-03-03", "hour": "10"}
	got := ResolveExecutionDate(data, time.Now())
	if got != "2026-03-03T10" {
		t.Errorf("got %q, want %q", got, "2026-03-03T10")
	}
}

func TestResolveExecutionDate_DateOnly(t *testing.T) {
	data := map[string]interface{}{"date": "20260303"}
	got := ResolveExecutionDate(data, time.Now())
	if got != "2026-03-03" {
		t.Errorf("got %q, want %q", got, "2026-03-03")
	}
}

func TestResolveExecutionDate_NoFields(t *testing.T) {
	data := map[string]interface{}{"complete": true}
	now := time.Date(2026, 3, 5, 12, 0, 0, 0, time.UTC)
	got := ResolveExecutionDate(data, now)
	want := now.Format("2006-01-02")
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestResolveExecutionDate_HourWithLeadingZero(t *testing.T) {
	data := map[string]interface{}{"date": "20260303", "hour": "03"}
	got := ResolveExecutionDate(data, time.Now())
	if got != "2026-03-03T03" {
		t.Errorf("got %q, want %q", got, "2026-03-03T03")
	}
}

func TestResolveExecutionDate_InvalidDate(t *testing.T) {
	data := map[string]interface{}{"date": "not-a-date"}
	now := time.Date(2026, 3, 5, 12, 0, 0, 0, time.UTC)
	got := ResolveExecutionDate(data, now)
	// Should fall back to today's date.
	want := now.Format("2006-01-02")
	if got != want {
		t.Errorf("got %q, want today %q", got, want)
	}
}

func TestResolveExecutionDate_InvalidHour(t *testing.T) {
	data := map[string]interface{}{"date": "20260303", "hour": "25"}
	got := ResolveExecutionDate(data, time.Now())
	// Invalid hour should be ignored; return date only.
	if got != "2026-03-03" {
		t.Errorf("got %q, want %q", got, "2026-03-03")
	}
}

func TestResolveExecutionDate_SignedHour(t *testing.T) {
	if got := ResolveExecutionDate(map[string]interface{}{"date": "20260303", "hour": "+5"}, time.Now()); got != "2026-03-03" {
		t.Errorf("hour +5: got %q, want the date without an hour", got)
	}
}

func TestResolveExecutionDate_NonNumericHour(t *testing.T) {
	data := map[string]interface{}{"date": "20260303", "hour": "ab"}
	got := ResolveExecutionDate(data, time.Now())
	if got != "2026-03-03" {
		t.Errorf("got %q, want %q", got, "2026-03-03")
	}
}

// normalizeDate is tested indirectly through ResolveExecutionDate.

func TestNormalizeDate_AlreadyNormalized(t *testing.T) {
	data := map[string]interface{}{"date": "2026-03-03"}
	got := ResolveExecutionDate(data, time.Now())
	assert.Equal(t, "2026-03-03", got)
}

func TestNormalizeDate_Compact(t *testing.T) {
	data := map[string]interface{}{"date": "20260303"}
	got := ResolveExecutionDate(data, time.Now())
	assert.Equal(t, "2026-03-03", got)
}

func TestNormalizeDate_CompactWithHour(t *testing.T) {
	data := map[string]interface{}{"date": "20260303", "hour": "07"}
	got := ResolveExecutionDate(data, time.Now())
	assert.Equal(t, "2026-03-03T07", got)
}
