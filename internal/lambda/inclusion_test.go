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

	got := lambda.PastInclusionDates(dates, now)
	assert.Empty(t, got, "all future dates should return empty slice")
}

func TestPastInclusionDates_MultiplePast(t *testing.T) {
	now := time.Date(2026, 3, 10, 14, 0, 0, 0, time.UTC)
	dates := []string{"2026-01-15", "2026-02-28", "2026-03-05"}

	got := lambda.PastInclusionDates(dates, now)
	assert.Equal(t, []string{"2026-03-05", "2026-02-28", "2026-01-15"}, got,
		"should return all past dates, most recent first")
}

func TestPastInclusionDates_MixedPastFuture(t *testing.T) {
	now := time.Date(2026, 3, 10, 14, 0, 0, 0, time.UTC)
	dates := []string{"2026-01-01", "2026-03-08", "2026-03-12", "2026-04-01"}

	got := lambda.PastInclusionDates(dates, now)
	assert.Equal(t, []string{"2026-03-08", "2026-01-01"}, got,
		"should only return past dates, most recent first")
}

func TestPastInclusionDates_TodayIncluded(t *testing.T) {
	now := time.Date(2026, 3, 10, 14, 0, 0, 0, time.UTC)
	dates := []string{"2026-03-05", "2026-03-10", "2026-03-15"}

	got := lambda.PastInclusionDates(dates, now)
	assert.Equal(t, []string{"2026-03-10", "2026-03-05"}, got,
		"today's date should be included in results")
}

func TestPastInclusionDates_CappedAt3(t *testing.T) {
	now := time.Date(2026, 3, 10, 14, 0, 0, 0, time.UTC)
	dates := []string{
		"2026-01-01", "2026-01-15", "2026-02-01", "2026-02-15", "2026-03-01",
	}

	got := lambda.PastInclusionDates(dates, now)
	assert.Len(t, got, 3, "result should be capped at 3 entries")
	assert.Equal(t, []string{"2026-03-01", "2026-02-15", "2026-02-01"}, got,
		"should return the 3 most recent past dates, most recent first")
}
