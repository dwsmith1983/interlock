package lambda_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/sfn"
	"github.com/stretchr/testify/assert"

	"github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// aslDereferencedTopLevelKeys and aslDereferencedSLAKeys mirror the JSONPath
// references in deploy/statemachine.asl.json. A missing key raises
// States.Runtime, which Catch: States.ALL cannot intercept.
var (
	aslDereferencedTopLevelKeys = []string{"pipelineId", "scheduleId", "date", "sensorArrivalAt", "config"}
	aslDereferencedConfigKeys   = []string{
		"evaluationIntervalSeconds", "evaluationWindowSeconds",
		"jobCheckIntervalSeconds", "jobPollWindowSeconds",
	}
	aslDereferencedSLAKeys = []string{"deadline", "expectedDuration", "maxDuration", "timezone"}
)

func baseConfig() *types.PipelineConfig {
	return &types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "gold-orders"},
		Schedule: types.ScheduleConfig{
			Evaluation: types.EvaluationWindow{Window: "1h", Interval: "5m"},
		},
		Job: types.JobConfig{Type: types.TriggerGlue, Config: map[string]interface{}{"jobName": "etl"}},
	}
}

func TestBuildSFNInput_AlwaysEmitsDereferencedKeys(t *testing.T) {
	tests := []struct {
		name            string
		sla             *types.SLAConfig
		sensorArrivalAt string
		wantSLA         bool
	}{
		{
			name:    "absolute SLA only",
			sla:     &types.SLAConfig{Deadline: "08:00", ExpectedDuration: "30m"},
			wantSLA: true,
		},
		{
			name:            "relative SLA with sensor arrival",
			sla:             &types.SLAConfig{MaxDuration: "2h"},
			sensorArrivalAt: "2026-03-01T06:00:00Z",
			wantSLA:         true,
		},
		{
			name:    "relative SLA without sensor arrival",
			sla:     &types.SLAConfig{MaxDuration: "2h"},
			wantSLA: true,
		},
		{
			name:    "no SLA",
			sla:     nil,
			wantSLA: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := baseConfig()
			cfg.SLA = tt.sla

			data, err := json.Marshal(lambda.BuildSFNInput(cfg, "gold-orders", "daily", "2026-03-01", tt.sensorArrivalAt))
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			var doc map[string]interface{}
			if err := json.Unmarshal(data, &doc); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}

			for _, key := range aslDereferencedTopLevelKeys {
				if _, ok := doc[key]; !ok {
					t.Errorf("SFN input is missing %q: %s", key, data)
				}
			}

			cfgMap, ok := doc["config"].(map[string]interface{})
			if !ok {
				t.Fatalf("config is not an object: %s", data)
			}
			for _, key := range aslDereferencedConfigKeys {
				if _, ok := cfgMap[key]; !ok {
					t.Errorf("SFN config is missing %q: %s", key, data)
				}
			}

			slaMap, hasSLA := cfgMap["sla"].(map[string]interface{})
			if hasSLA != tt.wantSLA {
				t.Fatalf("config.sla present = %v, want %v (CheckCancelSLA uses IsPresent): %s", hasSLA, tt.wantSLA, data)
			}
			if !tt.wantSLA {
				return
			}
			for _, key := range aslDereferencedSLAKeys {
				if _, ok := slaMap[key]; !ok {
					t.Errorf("SFN config.sla is missing %q: %s", key, data)
				}
			}
			if slaMap["timezone"] != "UTC" {
				t.Errorf("config.sla.timezone = %v, want UTC default", slaMap["timezone"])
			}
		})
	}
}

func TestBuildSFNConfig_JobCheckIntervalEnv(t *testing.T) {
	tests := []struct {
		name   string
		value  string
		set    bool
		wantJC int
	}{
		{name: "valid override", value: "5", set: true, wantJC: 5},
		{name: "unset", set: false, wantJC: lambda.DefaultJobCheckIntervalSec},
		{name: "empty", value: "", set: true, wantJC: lambda.DefaultJobCheckIntervalSec},
		{name: "zero", value: "0", set: true, wantJC: lambda.DefaultJobCheckIntervalSec},
		{name: "negative", value: "-3", set: true, wantJC: lambda.DefaultJobCheckIntervalSec},
		{name: "non-integer", value: "abc", set: true, wantJC: lambda.DefaultJobCheckIntervalSec},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange: t.Setenv also restores the variable after the subtest,
			// so "unset" clears it explicitly for a clean starting state.
			t.Setenv(lambda.JobCheckIntervalEnv, "")
			if !tt.set {
				if err := os.Unsetenv(lambda.JobCheckIntervalEnv); err != nil {
					t.Fatalf("unset %s: %v", lambda.JobCheckIntervalEnv, err)
				}
			} else {
				t.Setenv(lambda.JobCheckIntervalEnv, tt.value)
			}

			// Act
			got := lambda.BuildSFNConfig(baseConfig())

			// Assert
			if got.JobCheckIntervalSeconds != tt.wantJC {
				t.Errorf("JobCheckIntervalSeconds = %d, want %d", got.JobCheckIntervalSeconds, tt.wantJC)
			}
			if got.EvaluationIntervalSeconds != 300 {
				t.Errorf("EvaluationIntervalSeconds = %d, want 300", got.EvaluationIntervalSeconds)
			}
			if got.EvaluationWindowSeconds != 3600 {
				t.Errorf("EvaluationWindowSeconds = %d, want 3600", got.EvaluationWindowSeconds)
			}
			if got.JobPollWindowSeconds != types.DefaultJobPollWindowSeconds {
				t.Errorf("JobPollWindowSeconds = %d, want %d", got.JobPollWindowSeconds, types.DefaultJobPollWindowSeconds)
			}
		})
	}
}

func TestBuildSFNConfig(t *testing.T) {
	poll := func(sec int) *int { return &sec }
	tests := []struct {
		name   string
		mutate func(*types.PipelineConfig)
		check  func(*testing.T, lambda.SFNConfig)
	}{
		{"CustomTimings", func(c *types.PipelineConfig) {
			c.Schedule.Evaluation = types.EvaluationWindow{Interval: "2m", Window: "30m"}
		}, func(t *testing.T, sc lambda.SFNConfig) {
			assert.Equal(t, 120, sc.EvaluationIntervalSeconds)
			assert.Equal(t, 1800, sc.EvaluationWindowSeconds)
		}},
		{"1h/5m", func(c *types.PipelineConfig) {
			c.Schedule.Evaluation = types.EvaluationWindow{Window: "1h", Interval: "5m"}
		}, func(t *testing.T, sc lambda.SFNConfig) {
			assert.Equal(t, 3600, sc.EvaluationWindowSeconds)
			assert.Equal(t, 300, sc.EvaluationIntervalSeconds)
		}},
		{"UnsetTimings", func(c *types.PipelineConfig) { c.Schedule.Evaluation = types.EvaluationWindow{} }, func(t *testing.T, sc lambda.SFNConfig) {
			assert.Equal(t, 3600, sc.EvaluationWindowSeconds)
			assert.Equal(t, 300, sc.EvaluationIntervalSeconds)
		}},
		// An unvalidated sub-second interval must not reach the state machine
		// as 0, which would evaluate in a tight loop.
		{"SubSecondInterval", func(c *types.PipelineConfig) { c.Schedule.Evaluation.Interval = "500ms" }, func(t *testing.T, sc lambda.SFNConfig) {
			assert.Equal(t, 300, sc.EvaluationIntervalSeconds)
		}},
		{"PollOverride", func(c *types.PipelineConfig) { c.Job.JobPollWindowSeconds = poll(7200) }, func(t *testing.T, sc lambda.SFNConfig) {
			assert.Equal(t, 7200, sc.JobPollWindowSeconds)
		}},
		{"PollZero", func(c *types.PipelineConfig) { c.Job.JobPollWindowSeconds = poll(0) }, func(t *testing.T, sc lambda.SFNConfig) {
			assert.Equal(t, 3600, sc.JobPollWindowSeconds)
		}},
		{"WithSLA", func(c *types.PipelineConfig) {
			c.SLA = &types.SLAConfig{Deadline: "08:00", ExpectedDuration: "30m", Critical: true}
		}, func(t *testing.T, sc lambda.SFNConfig) {
			assert.Equal(t, &lambda.SFNSLA{Deadline: "08:00", ExpectedDuration: "30m", MaxDuration: "", Timezone: "UTC", Critical: true}, sc.SLA)
		}},
		{"BadInterval", func(c *types.PipelineConfig) { c.Schedule.Evaluation.Interval = "bogus" }, func(t *testing.T, sc lambda.SFNConfig) {
			assert.Equal(t, 300, sc.EvaluationIntervalSeconds)
		}},
		// "120" has no unit, so it does not parse: the default applies, not 120s.
		// The window still parses, so the fallback is per field.
		{"UnitlessInterval", func(c *types.PipelineConfig) {
			c.Schedule.Evaluation = types.EvaluationWindow{Interval: "120", Window: "2m"}
		}, func(t *testing.T, sc lambda.SFNConfig) {
			assert.Equal(t, 300, sc.EvaluationIntervalSeconds)
			assert.Equal(t, 120, sc.EvaluationWindowSeconds)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(lambda.JobCheckIntervalEnv, "")
			cfg := baseConfig()
			tt.mutate(cfg)
			tt.check(t, lambda.BuildSFNConfig(cfg))
		})
	}
}

// failingSFN refuses every StartExecution with err.
type failingSFN struct{ err error }

func (f failingSFN) StartExecution(context.Context, *sfn.StartExecutionInput, ...func(*sfn.Options)) (*sfn.StartExecutionOutput, error) {
	return nil, f.err
}

func TestStartSFNWithName_WrapsStartError(t *testing.T) {
	limit := errors.New("execution limit exceeded")
	d := &lambda.Deps{SFNClient: failingSFN{err: limit}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	err := lambda.StartSFNWithName(context.Background(), d, baseConfig(), "gold-orders", "stream", "2026-03-03", "gold-orders-stream-2026-03-03-1")
	if !errors.Is(err, limit) {
		t.Errorf("err = %v, want it to wrap %v", err, limit)
	}
}
