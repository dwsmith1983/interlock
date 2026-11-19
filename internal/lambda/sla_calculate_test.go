package lambda_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dwsmith1983/interlock/internal/lambda"
)

// TestHandleSLACalculate pins the shared SLA calculation that the watchdog,
// the dry-run projection and the cancel path all call: absolute and relative
// deadlines come back as UTC RFC3339, and every parse failure is reported.
func TestHandleSLACalculate(t *testing.T) {
	now := time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)
	type in = lambda.SLAMonitorInput
	type out = lambda.SLAMonitorOutput

	for _, tt := range []struct {
		name  string
		input in
		want  out
	}{
		{"time zone deadline converted to UTC", in{Date: "2030-06-15", Deadline: "14:00", ExpectedDuration: "30m", Timezone: "America/New_York"}, out{BreachAt: "2030-06-15T18:00:00Z", WarningAt: "2030-06-15T17:30:00Z"}},
		{"relative deadline warns at a quarter of maxDuration", in{MaxDuration: "4h", SensorArrivalAt: "2026-03-10T10:00:00Z"}, out{BreachAt: "2026-03-10T14:00:00Z", WarningAt: "2026-03-10T13:00:00Z"}},
		{"relative deadline warns at expectedDuration", in{MaxDuration: "2h", SensorArrivalAt: "2026-03-10T14:00:00Z", ExpectedDuration: "30m"}, out{BreachAt: "2026-03-10T16:00:00Z", WarningAt: "2026-03-10T15:30:00Z"}},
		{"offset arrival converted to UTC", in{MaxDuration: "2h", SensorArrivalAt: "2026-03-10T14:00:00+02:00"}, out{BreachAt: "2026-03-10T14:00:00Z", WarningAt: "2026-03-10T13:30:00Z"}},
		{"maxDuration without arrival uses the absolute deadline", in{MaxDuration: "2h", Date: "2030-03-01", Deadline: "08:00", ExpectedDuration: "30m"}, out{BreachAt: "2030-03-01T08:00:00Z", WarningAt: "2030-03-01T07:30:00Z"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := lambda.HandleSLACalculate(tt.input, now)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}

	for _, tt := range []struct {
		input   in
		wantErr string
	}{
		{in{Date: "2030-03-01", Deadline: "not-a-time", ExpectedDuration: "30m"}, "parse deadline"},
		{in{Date: "2030-03-01", Deadline: "08:00", ExpectedDuration: "not-a-duration"}, "parse expectedDuration"},
		{in{Date: "2030-03-01", Deadline: "08:00", ExpectedDuration: "30m", Timezone: "Mars/Olympus_Mons"}, "load timezone"},
		{in{MaxDuration: "not-a-duration", SensorArrivalAt: "2026-03-10T10:00:00Z"}, "parse maxDuration"},
		{in{MaxDuration: "2h", SensorArrivalAt: "not-a-time"}, "parse arrivalAt"},
	} {
		t.Run(tt.wantErr, func(t *testing.T) {
			got, err := lambda.HandleSLACalculate(tt.input, now)
			require.ErrorContains(t, err, tt.wantErr)
			assert.Zero(t, got)
		})
	}
}
