//go:build e2e

package e2e

import (
	"testing"
	"time"
)

func TestParseScheduleTime(t *testing.T) {
	newYork, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("load zone: %v", err)
	}
	tests := []struct {
		name    string
		expr    string
		tz      string
		want    time.Time
		wantErr bool
	}{
		{name: "no timezone is UTC", expr: "at(2026-03-03T10:30:00)", want: time.Date(2026, 3, 3, 10, 30, 0, 0, time.UTC)},
		{name: "explicit UTC", expr: "at(2026-03-03T10:30:00)", tz: "UTC", want: time.Date(2026, 3, 3, 10, 30, 0, 0, time.UTC)},
		{name: "named zone", expr: "at(2026-03-03T10:30:00)", tz: "America/New_York", want: time.Date(2026, 3, 3, 10, 30, 0, 0, newYork)},
		{name: "rate expression", expr: "rate(5 minutes)", wantErr: true},
		{name: "missing close", expr: "at(2026-03-03T10:30:00", wantErr: true},
		{name: "trailing Z", expr: "at(2026-03-03T10:30:00Z)", wantErr: true},
		{name: "date only", expr: "at(2026-03-03)", wantErr: true},
		{name: "unknown zone", expr: "at(2026-03-03T10:30:00)", tz: "Mars/Olympus", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseScheduleTime(tc.expr, tc.tz)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parseScheduleTime(%q, %q) = %s, want an error", tc.expr, tc.tz, got)
				}
				return
			}
			if err != nil || !got.Equal(tc.want) {
				t.Fatalf("parseScheduleTime(%q, %q) = %s, %v; want %s", tc.expr, tc.tz, got, err, tc.want)
			}
		})
	}
}
