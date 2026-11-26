package lambda_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/dwsmith1983/interlock/internal/lambda"
)

func TestPastInclusionDates_AllFuture(t *testing.T) {
	now := time.Date(2026, 3, 10, 14, 0, 0, 0, time.UTC)
	dates := []string{"2026-03-11", "2026-04-01", "2026-06-30"}

	got := lambda.PastInclusionDates(dates, now, time.UTC)
	assert.Empty(t, got, "all future dates should return empty slice")
}

func TestPastInclusionDates_MultiplePast(t *testing.T) {
	now := time.Date(2026, 3, 10, 14, 0, 0, 0, time.UTC)
	dates := []string{"2026-03-02", "2026-03-04", "2026-03-05"}

	got := lambda.PastInclusionDates(dates, now, time.UTC)
	assert.Equal(t, []string{"2026-03-05", "2026-03-04", "2026-03-02"}, got,
		"should return all past dates, most recent first")
}

func TestPastInclusionDates_MixedPastFuture(t *testing.T) {
	now := time.Date(2026, 3, 10, 14, 0, 0, 0, time.UTC)
	dates := []string{"2026-03-03", "2026-03-08", "2026-03-12", "2026-04-01"}

	got := lambda.PastInclusionDates(dates, now, time.UTC)
	assert.Equal(t, []string{"2026-03-08", "2026-03-03"}, got,
		"should only return past dates, most recent first")
}

func TestPastInclusionDates_TodayIncluded(t *testing.T) {
	now := time.Date(2026, 3, 10, 14, 0, 0, 0, time.UTC)
	dates := []string{"2026-03-05", "2026-03-10", "2026-03-15"}

	got := lambda.PastInclusionDates(dates, now, time.UTC)
	assert.Equal(t, []string{"2026-03-10", "2026-03-05"}, got,
		"today's date should be included in results")
}

func TestPastInclusionDates_CappedAt3(t *testing.T) {
	now := time.Date(2026, 3, 10, 14, 0, 0, 0, time.UTC)
	dates := []string{
		"2026-03-02", "2026-03-03", "2026-03-04", "2026-03-05", "2026-03-06",
	}

	got := lambda.PastInclusionDates(dates, now, time.UTC)
	assert.Len(t, got, 3, "result should be capped at 3 entries")
	assert.Equal(t, []string{"2026-03-06", "2026-03-05", "2026-03-04"}, got,
		"should return the 3 most recent past dates, most recent first")
}

// TestPastInclusionDates_SkipsDatesOlderThanLookback: only dates from the
// last 8 days of now's date in loc qualify. At 23:00 on 2026-03-09 in New
// York it is already 2026-03-10 in UTC, so 2026-03-01 is in the window only
// when the bound is taken in loc.
func TestPastInclusionDates_SkipsDatesOlderThanLookback(t *testing.T) {
	newYork, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("load zone: %v", err)
	}
	now := time.Date(2026, 3, 10, 3, 0, 0, 0, time.UTC)
	dates := []string{"2026-02-28", "2026-03-01", "2026-03-08"}

	got := lambda.PastInclusionDates(dates, now, newYork)
	assert.Equal(t, []string{"2026-03-08", "2026-03-01"}, got,
		"dates 1 and 8 days back qualify, 9 days back does not")
}

// TestPastInclusionDates_UsesLocation: a date is past once it has begun in
// loc, whatever the UTC date is.
func TestPastInclusionDates_UsesLocation(t *testing.T) {
	load := func(name string) *time.Location {
		loc, err := time.LoadLocation(name)
		if err != nil {
			t.Fatalf("load %s: %v", name, err)
		}
		return loc
	}
	dates := []string{"2026-03-10"}
	tests := []struct {
		name string
		now  time.Time
		loc  *time.Location
		want []string
	}{
		{"New York, the evening before", time.Date(2026, 3, 10, 0, 0, 0, 0, time.UTC), load("America/New_York"), nil},
		{"UTC midnight", time.Date(2026, 3, 10, 0, 0, 0, 0, time.UTC), time.UTC, dates},
		{"Tokyo, date begun", time.Date(2026, 3, 9, 15, 30, 0, 0, time.UTC), load("Asia/Tokyo"), dates},
		{"UTC, the day before", time.Date(2026, 3, 9, 15, 30, 0, 0, time.UTC), time.UTC, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, lambda.PastInclusionDates(dates, tt.now, tt.loc))
		})
	}
}
