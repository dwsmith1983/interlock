//go:build e2e

package e2e

import (
	"testing"
	"time"

	"github.com/dwsmith1983/interlock/pkg/types"
)

const (
	smokePipeline   = "smoke"
	smokeSensor     = "smoke-ready"
	settleTimeout   = 3 * time.Minute // first run: includes Lambda cold starts
	streamSchedule  = "stream"        // lambda.ResolveScheduleID for a pipeline without cron
	smokeDateLayout = "2006-01-02"
)

// smokeConfig is a sensor-triggered pipeline whose http job is the mock.
func smokeConfig() types.PipelineConfig {
	return types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: smokePipeline},
		Schedule: types.ScheduleConfig{
			Trigger: &types.TriggerCondition{
				Key: smokeSensor, Check: types.CheckEquals, Field: "status", Value: "ready",
			},
			Evaluation: types.EvaluationWindow{Window: "60s", Interval: "5s"},
		},
		Validation: types.ValidationConfig{
			Trigger: "ALL",
			Rules:   []types.ValidationRule{{Key: smokeSensor, Check: types.CheckExists}},
		},
		Job: types.JobConfig{
			Type:   types.TriggerHTTP,
			Config: map[string]interface{}{"method": "POST", "url": mockURLForLambdas + "/jobs/" + smokePipeline + "/run"},
		},
	}
}

func TestHarnessSmoke(t *testing.T) {
	day := mockNow(t).UTC()
	date := day.Format(smokeDateLayout)

	// The config goes in first: the stream-router's config cache is pinned
	// to the simulated clock, so it must see the config on its first load.
	putConfig(t, smokeConfig())
	writeSensor(t, smokePipeline, smokeSensor, map[string]interface{}{"status": "ready", "date": date})

	waitFor(t, settleTimeout, "the mock job to be launched", func() bool {
		return launches(t, smokePipeline) >= 1
	})
	waitFor(t, settleTimeout, "the trigger row to reach COMPLETED", func() bool {
		return triggerStatus(t, smokePipeline, streamSchedule, date) == types.TriggerStatusCompleted
	})
	if n := launches(t, smokePipeline); n != 1 {
		t.Fatalf("mock job launches = %d, want exactly 1", n)
	}
}
