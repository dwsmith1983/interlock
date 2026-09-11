package lambda_test

import (
	"encoding/json"
	"testing"

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
