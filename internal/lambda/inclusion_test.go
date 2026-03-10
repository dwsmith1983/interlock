package lambda_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/dwsmith1983/interlock/internal/lambda"
)

func TestMostRecentInclusionDate(t *testing.T) {
	// Fixed reference time: 2026-03-10T14:00:00Z
	now := time.Date(2026, 3, 10, 14, 0, 0, 0, time.UTC)

	tests := []struct {
		name     string
		dates    []string
		now      time.Time
		wantDate string
		wantOK   bool
	}{
		{
			name:   "empty dates list",
			dates:  nil,
			now:    now,
			wantOK: false,
		},
		{
			name:   "all dates in the future",
			dates:  []string{"2026-03-11", "2026-04-01", "2026-06-30"},
			now:    now,
			wantOK: false,
		},
		{
			name:     "single past date",
			dates:    []string{"2026-03-01"},
			now:      now,
			wantDate: "2026-03-01",
			wantOK:   true,
		},
		{
			name:     "multiple past dates picks most recent",
			dates:    []string{"2026-01-15", "2026-02-28", "2026-03-05"},
			now:      now,
			wantDate: "2026-03-05",
			wantOK:   true,
		},
		{
			name:     "today's date is included (on or before now)",
			dates:    []string{"2026-03-05", "2026-03-10", "2026-03-15"},
			now:      now,
			wantDate: "2026-03-10",
			wantOK:   true,
		},
		{
			name:     "mix of past and future picks most recent past",
			dates:    []string{"2026-01-01", "2026-03-08", "2026-03-12", "2026-04-01"},
			now:      now,
			wantDate: "2026-03-08",
			wantOK:   true,
		},
		{
			name:   "invalid date format is skipped",
			dates:  []string{"not-a-date", "2026-13-01"},
			now:    now,
			wantOK: false,
		},
		{
			name:     "invalid dates mixed with valid date",
			dates:    []string{"bad", "2026-03-01", "also-bad"},
			now:      now,
			wantDate: "2026-03-01",
			wantOK:   true,
		},
		{
			name:     "now at midnight still includes today",
			dates:    []string{"2026-03-10"},
			now:      time.Date(2026, 3, 10, 0, 0, 0, 0, time.UTC),
			wantDate: "2026-03-10",
			wantOK:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotDate, gotOK := lambda.MostRecentInclusionDate(tt.dates, tt.now)
			assert.Equal(t, tt.wantOK, gotOK)
			if tt.wantOK {
				assert.Equal(t, tt.wantDate, gotDate)
			}
		})
	}
}
