package sla_test

import (
	"testing"
	"time"

	"github.com/dwsmith1983/interlock/pkg/sla"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCalculateAbsoluteDeadline(t *testing.T) {
	tests := []struct {
		name             string
		date             string
		deadline         string
		expectedDuration string
		timezone         string
		now              time.Time
		wantBreach       time.Time
		wantWarning      time.Time
		wantErr          bool
	}{
		{
			name:             "daily pipeline UTC",
			date:             "2026-03-28",
			deadline:         "08:00",
			expectedDuration: "30m",
			timezone:         "UTC",
			now:              time.Date(2026, 3, 28, 6, 0, 0, 0, time.UTC),
			wantBreach:       time.Date(2026, 3, 28, 8, 0, 0, 0, time.UTC),
			wantWarning:      time.Date(2026, 3, 28, 7, 30, 0, 0, time.UTC),
		},
		{
			name:             "hourly pipeline with offset deadline",
			date:             "2026-03-28T10",
			deadline:         ":30",
			expectedDuration: "15m",
			timezone:         "UTC",
			now:              time.Date(2026, 3, 28, 10, 0, 0, 0, time.UTC),
			wantBreach:       time.Date(2026, 3, 28, 11, 30, 0, 0, time.UTC),
			wantWarning:      time.Date(2026, 3, 28, 11, 15, 0, 0, time.UTC),
		},
		{
			name:             "daily pipeline America/New_York",
			date:             "2026-03-28",
			deadline:         "09:00",
			expectedDuration: "1h",
			timezone:         "America/New_York",
			now:              time.Date(2026, 3, 28, 6, 0, 0, 0, time.UTC),
			wantBreach: func() time.Time {
				loc, _ := time.LoadLocation("America/New_York")
				return time.Date(2026, 3, 28, 9, 0, 0, 0, loc)
			}(),
			wantWarning: func() time.Time {
				loc, _ := time.LoadLocation("America/New_York")
				return time.Date(2026, 3, 28, 8, 0, 0, 0, loc)
			}(),
		},
		{
			name:             "invalid deadline format",
			date:             "2026-03-28",
			deadline:         "8pm",
			expectedDuration: "30m",
			timezone:         "UTC",
			now:              time.Date(2026, 3, 28, 6, 0, 0, 0, time.UTC),
			wantErr:          true,
		},
		{
			name:             "invalid timezone",
			date:             "2026-03-28",
			deadline:         "08:00",
			expectedDuration: "30m",
			timezone:         "Mars/Olympus_Mons",
			now:              time.Date(2026, 3, 28, 6, 0, 0, 0, time.UTC),
			wantErr:          true,
		},
		{
			name:             "invalid expectedDuration",
			date:             "2026-03-28",
			deadline:         "08:00",
			expectedDuration: "not-a-duration",
			timezone:         "UTC",
			now:              time.Date(2026, 3, 28, 6, 0, 0, 0, time.UTC),
			wantErr:          true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			breach, warning, err := sla.CalculateAbsoluteDeadline(
				tt.date, tt.deadline, tt.expectedDuration, tt.timezone, tt.now,
			)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.True(t, tt.wantBreach.Equal(breach),
				"breach: want %v, got %v", tt.wantBreach, breach)
			assert.True(t, tt.wantWarning.Equal(warning),
				"warning: want %v, got %v", tt.wantWarning, warning)
		})
	}
}

func TestCalculateRelativeDeadline(t *testing.T) {
	baseArrival := time.Date(2026, 3, 28, 10, 0, 0, 0, time.UTC)

	tests := []struct {
		name             string
		arrivalAt        string
		maxDuration      string
		expectedDuration string
		wantBreach       time.Time
		wantWarning      time.Time
		wantErr          bool
	}{
		{
			name:             "with explicit expectedDuration",
			arrivalAt:        baseArrival.Format(time.RFC3339),
			maxDuration:      "2h",
			expectedDuration: "30m",
			wantBreach:       baseArrival.Add(2 * time.Hour),           // 12:00
			wantWarning:      baseArrival.Add(2*time.Hour - 30*time.Minute), // 11:30
		},
		{
			name:             "without expectedDuration uses 25% of maxDuration",
			arrivalAt:        baseArrival.Format(time.RFC3339),
			maxDuration:      "2h",
			expectedDuration: "",
			wantBreach:       baseArrival.Add(2 * time.Hour),           // 12:00
			wantWarning:      baseArrival.Add(2*time.Hour - 30*time.Minute), // 12:00 - 25% of 2h = 11:30
		},
		{
			name:             "invalid arrivalAt",
			arrivalAt:        "not-a-timestamp",
			maxDuration:      "2h",
			expectedDuration: "",
			wantErr:          true,
		},
		{
			name:             "invalid maxDuration",
			arrivalAt:        baseArrival.Format(time.RFC3339),
			maxDuration:      "bogus",
			expectedDuration: "",
			wantErr:          true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			breach, warning, err := sla.CalculateRelativeDeadline(
				tt.arrivalAt, tt.maxDuration, tt.expectedDuration,
			)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.True(t, tt.wantBreach.Equal(breach),
				"breach: want %v, got %v", tt.wantBreach, breach)
			assert.True(t, tt.wantWarning.Equal(warning),
				"warning: want %v, got %v", tt.wantWarning, warning)
		})
	}
}
