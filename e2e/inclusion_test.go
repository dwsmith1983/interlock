//go:build e2e

package e2e

import (
	"testing"

	"github.com/dwsmith1983/interlock/internal/store"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// An inclusion calendar lists dates 9 and 8 days before the scenario's day,
// and neither has a trigger. The watchdog looks back 8 days, so its pass
// publishes IRREGULAR_SCHEDULE_MISSED for the 8-day date only. The watchdog
// writes a date's marker right after publishing for it, so once the pass
// returns the missing marker shows the 9-day date was never published.
func TestScenarioInclusionLookback(t *testing.T) {
	const id = "inclusion-lookback"
	start, _ := startDay(t)
	tooOld := start.AddDate(0, 0, -9).Format(smokeDateLayout)
	oldest := start.AddDate(0, 0, -8).Format(smokeDateLayout)
	cfg := scenarioConfig(id)
	cfg.Schedule.Include = &types.InclusionConfig{Dates: []string{tooOld, oldest}}
	putConfig(t, cfg)

	invokeWatchdog(t)

	s := &store.Store{Client: ddb, ControlTable: controlTable}
	ctx, cancel := callCtx()
	defer cancel()
	for date, want := range map[string]bool{tooOld: false, oldest: true} {
		marker, err := s.GetSensorData(ctx, id, "irregular-missed-check#"+date)
		if err != nil {
			t.Fatalf("read marker for %s: %v", date, err)
		}
		if (marker != nil) != want {
			t.Fatalf("marker for %s = %v, want present %v", date, marker, want)
		}
	}
	missed := string(types.EventIrregularScheduleMissed)
	waitEvents(t, id, oldest, map[string]int{missed: 1})
	waitAlerts(t, id, alert{id, oldest, missed})
	assertLaunches(t, id, 0)
}
