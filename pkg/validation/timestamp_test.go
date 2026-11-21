package validation

import (
	"math"
	"testing"
	"time"
)

func TestParseTimestamp(t *testing.T) {
	const (
		millis  = int64(1773133200000) // 2026-03-10T09:00:00Z
		seconds = int64(1773133200)
	)
	tests := []struct {
		name   string
		in     interface{}
		want   time.Time
		wantOK bool
	}{
		{name: "RFC3339 Z", in: "2026-03-10T09:00:00Z", want: time.Unix(seconds, 0), wantOK: true},
		{name: "RFC3339 offset", in: "2026-03-10T10:00:00+01:00", want: time.Unix(seconds, 0), wantOK: true},
		{name: "RFC3339 fractional", in: "2026-03-10T09:00:00.250Z", want: time.UnixMilli(millis + 250), wantOK: true},
		{name: "millis float64", in: float64(millis), want: time.UnixMilli(millis), wantOK: true},
		{name: "millis int64", in: millis, want: time.UnixMilli(millis), wantOK: true},
		{name: "millis string", in: "1773133200000", want: time.UnixMilli(millis), wantOK: true},
		{name: "seconds float64", in: float64(seconds), want: time.Unix(seconds, 0), wantOK: true},
		{name: "seconds int", in: int(seconds), want: time.Unix(seconds, 0), wantOK: true},
		{name: "seconds int32", in: int32(seconds), want: time.Unix(seconds, 0), wantOK: true},
		{name: "seconds float32", in: float32(1e9), want: time.Unix(1e9, 0), wantOK: true},
		{name: "seconds string", in: "1773133200", want: time.Unix(seconds, 0), wantOK: true},
		{name: "largest seconds", in: float64(999999999999), want: time.Unix(999999999999, 0), wantOK: true},
		{name: "largest seconds string", in: "999999999999", want: time.Unix(999999999999, 0), wantOK: true},
		{name: "smallest millis", in: float64(1000000000000), want: time.UnixMilli(1000000000000), wantOK: true},
		{name: "smallest millis string", in: "1000000000000", want: time.UnixMilli(1000000000000), wantOK: true},
		{name: "not a time", in: "not-a-time"},
		{name: "empty string", in: ""},
		{name: "nil", in: nil},
		{name: "bool", in: true},
		{name: "zero", in: 0},
		{name: "zero string", in: "0"},
		{name: "negative", in: -5},
		{name: "negative float", in: -5.0},
		{name: "NaN", in: math.NaN()},
		{name: "infinity", in: math.Inf(1)},
		{name: "float beyond int64", in: 1e19},
		{name: "map", in: map[string]interface{}{"t": 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := ParseTimestamp(tt.in)
			if ok != tt.wantOK || !got.Equal(tt.want) {
				t.Errorf("ParseTimestamp(%#v) = %v, %v; want %v, %v", tt.in, got, ok, tt.want, tt.wantOK)
			}
		})
	}
}
