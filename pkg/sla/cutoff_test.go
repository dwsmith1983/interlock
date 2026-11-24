package sla_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dwsmith1983/interlock/pkg/sla"
)

func TestCutoffAt(t *testing.T) {
	newYork, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	utc := func(s string) time.Time {
		t.Helper()
		v, err := time.Parse(time.RFC3339, s)
		require.NoError(t, err)
		return v
	}
	tests := []struct {
		name   string
		runKey string
		cutoff string
		offset int
		loc    *time.Location
		want   time.Time
	}{
		{"daily cutoff, daily key", "2026-03-10", "14:00", 0, time.UTC, utc("2026-03-10T14:00:00Z")},
		{"daily cutoff, daily key, offset", "2026-03-10", "02:00", 1, time.UTC, utc("2026-03-11T02:00:00Z")},
		{"daily cutoff, daily key, zone", "2026-03-10", "14:00", 0, newYork, utc("2026-03-10T18:00:00Z")},
		{"hourly cutoff, hourly key", "2026-03-10T07", ":15", 0, time.UTC, utc("2026-03-10T08:15:00Z")},
		{"hourly cutoff, last hour of the day", "2026-03-10T23", ":15", 0, time.UTC, utc("2026-03-11T00:15:00Z")},
		{"daily cutoff, hourly key", "2026-03-10T10", "14:00", 0, time.UTC, utc("2026-03-10T14:00:00Z")},
		{"daily cutoff, hourly key, offset", "2026-03-10T23", "02:00", 1, time.UTC, utc("2026-03-11T02:00:00Z")},
		{"hourly cutoff, daily key", "2026-03-10", ":15", 0, time.UTC, utc("2026-03-11T00:15:00Z")},
		{"hourly cutoff, daily key, offset not applied", "2026-03-10", ":15", 3, time.UTC, utc("2026-03-11T00:15:00Z")},
		{"hourly cutoff, daily key, zone", "2026-03-10", ":15", 0, newYork, utc("2026-03-11T04:15:00Z")},
		{"spring-forward gap gives the gap's end", "2026-03-08", "02:30", 0, newYork, utc("2026-03-08T07:00:00Z")},
		{"fall-back repeat gives the first", "2026-11-01", "01:30", 0, newYork, utc("2026-11-01T05:30:00Z")},
		{"empty cutoff", "2026-03-10", "", 0, time.UTC, time.Time{}},
		{"cutoff without colon", "2026-03-10", "14", 0, time.UTC, time.Time{}},
		{"cutoff hour not a number", "2026-03-10", "ab:00", 0, time.UTC, time.Time{}},
		{"cutoff minute not a number", "2026-03-10T07", ":xx", 0, time.UTC, time.Time{}},
		{"empty key", "", "14:00", 0, time.UTC, time.Time{}},
		{"key without dashes", "20260310", "14:00", 0, time.UTC, time.Time{}},
		{"hour 24", "2026-03-10T24", ":15", 0, time.UTC, time.Time{}},
		{"one-digit hour", "2026-03-10T7", ":15", 0, time.UTC, time.Time{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sla.CutoffAt(tt.runKey, tt.cutoff, tt.offset, tt.loc)
			assert.True(t, tt.want.Equal(got), "want %v, got %v", tt.want, got)
			assert.Equal(t, tt.want.IsZero(), got.IsZero())
		})
	}
}
