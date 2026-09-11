package orchestrator_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/lambda/orchestrator"
	"github.com/dwsmith1983/interlock/internal/store/storetest"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// configItem builds the control-table row that store.GetConfig expects.
// Every test in this file uses pipeline "p".
func configItem(t *testing.T, cfg types.PipelineConfig) map[string]ddbtypes.AttributeValue {
	t.Helper()
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	return map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("p")},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: types.ConfigSK},
		"config": &ddbtypes.AttributeValueMemberS{Value: string(data)},
	}
}

func testDeps(fake *storetest.FakeDynamo) *lambda.Deps {
	return &lambda.Deps{
		Store:  storetest.NewStore(fake),
		Logger: slog.Default(),
	}
}

// fakeExecutor is a lambda.TriggerExecutor double.
type fakeExecutor struct {
	meta map[string]interface{}
	err  error
}

func (f *fakeExecutor) Execute(context.Context, *types.TriggerConfig) (map[string]interface{}, error) {
	return f.meta, f.err
}

// TestEvaluate_AlwaysEmitsStatus guards the IsReady Choice state, which reads
// $.evaluateResult.status with no IsPresent guard. An absent status raises
// States.Runtime, which Catch: States.ALL cannot intercept.
func TestEvaluate_AlwaysEmitsStatus(t *testing.T) {
	goodCfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "p"},
		Validation: types.ValidationConfig{
			Trigger: "ALL",
			Rules:   []types.ValidationRule{{Key: "upstream", Check: types.CheckExists}},
		},
	}

	tests := []struct {
		name       string
		fake       *storetest.FakeDynamo
		wantStatus string
		wantErrSub string
	}{
		{
			name: "GetConfig failure",
			fake: &storetest.FakeDynamo{
				GetItemFn: func(context.Context, *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
					return nil, errors.New("dynamodb: internal error")
				},
			},
			wantStatus: "error",
			wantErrSub: "dynamodb: internal error",
		},
		{
			name:       "config not found",
			fake:       &storetest.FakeDynamo{},
			wantStatus: "error",
			wantErrSub: "config not found",
		},
		{
			name: "GetAllSensors failure",
			fake: &storetest.FakeDynamo{
				GetItemFn: func(context.Context, *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
					return &dynamodb.GetItemOutput{Item: configItem(t, goodCfg)}, nil
				},
				QueryFn: func(context.Context, *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
					return nil, errors.New("dynamodb: request limit exceeded")
				},
			},
			wantStatus: "error",
			wantErrSub: "request limit exceeded",
		},
		{
			name: "rules not satisfied",
			fake: &storetest.FakeDynamo{
				GetItemFn: func(context.Context, *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
					return &dynamodb.GetItemOutput{Item: configItem(t, goodCfg)}, nil
				},
			},
			wantStatus: "not_ready",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := orchestrator.HandleOrchestrator(context.Background(), testDeps(tt.fake), lambda.OrchestratorInput{
				Mode: "evaluate", PipelineID: "p", ScheduleID: "s", Date: "2026-03-01",
			})
			if err != nil {
				t.Fatalf("evaluate must not return a Lambda error, got %v", err)
			}
			if out.Status != tt.wantStatus {
				t.Errorf("status = %q, want %q", out.Status, tt.wantStatus)
			}
			if tt.wantErrSub != "" && !strings.Contains(out.Error, tt.wantErrSub) {
				t.Errorf("error = %q, want it to contain %q", out.Error, tt.wantErrSub)
			}

			data, mErr := json.Marshal(out)
			if mErr != nil {
				t.Fatalf("marshal: %v", mErr)
			}
			var decoded map[string]interface{}
			if err := json.Unmarshal(data, &decoded); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if _, ok := decoded["status"]; !ok {
				t.Errorf("marshaled output = %s, want a status key for $.evaluateResult.status", data)
			}
		})
	}
}

// TestTrigger_FailuresReturnLambdaError guards HasTriggerResult/CheckJob.
// A nil error with a partial triggerResult makes IsPresent true and CheckJob
// dereference a missing runId; returning a Go error routes to
// TriggerRetryExhausted instead, which releases the trigger lock.
func TestTrigger_FailuresReturnLambdaError(t *testing.T) {
	badTypeCfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "p"},
		Job: types.JobConfig{
			Type:   types.TriggerType("bogus"),
			Config: map[string]interface{}{"jobName": "x"},
		},
	}

	tests := []struct {
		name       string
		fake       *storetest.FakeDynamo
		wantErrSub string
	}{
		{
			name: "GetConfig failure",
			fake: &storetest.FakeDynamo{
				GetItemFn: func(context.Context, *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
					return nil, errors.New("dynamodb: internal error")
				},
			},
			wantErrSub: "dynamodb: internal error",
		},
		{
			name:       "config not found",
			fake:       &storetest.FakeDynamo{},
			wantErrSub: "config not found",
		},
		{
			name: "unsupported trigger type",
			fake: &storetest.FakeDynamo{
				GetItemFn: func(context.Context, *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
					return &dynamodb.GetItemOutput{Item: configItem(t, badTypeCfg)}, nil
				},
			},
			wantErrSub: "unsupported trigger type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := testDeps(tt.fake)
			d.TriggerRunner = &fakeExecutor{}
			out, err := orchestrator.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
				Mode: "trigger", PipelineID: "p", ScheduleID: "s", Date: "2026-03-01",
			})
			if err == nil {
				t.Fatalf("trigger must return a Lambda error, got output %+v", out)
			}
			if !strings.Contains(err.Error(), tt.wantErrSub) {
				t.Errorf("err = %v, want it to contain %q", err, tt.wantErrSub)
			}
			if out.Mode != "" || out.RunID != "" || out.Metadata != nil {
				t.Errorf("output = %+v, want the zero value so ResultPath is never written", out)
			}
		})
	}
}

// TestTrigger_SuccessAlwaysCarriesRunIDAndMetadata guards the CheckJob
// Parameters, which dereference both $.triggerResult.runId and
// $.triggerResult.metadata.
func TestTrigger_SuccessAlwaysCarriesRunIDAndMetadata(t *testing.T) {
	glueCfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "p"},
		Job: types.JobConfig{
			Type:   types.TriggerGlue,
			Config: map[string]interface{}{"jobName": "my-etl"},
		},
	}
	httpCfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "p"},
		Job: types.JobConfig{
			Type:   types.TriggerHTTP,
			Config: map[string]interface{}{"url": "https://example.com/trigger", "method": "POST"},
		},
	}

	tests := []struct {
		name      string
		cfg       types.PipelineConfig
		meta      map[string]interface{}
		wantRunID string
	}{
		{
			name:      "polling trigger",
			cfg:       glueCfg,
			meta:      map[string]interface{}{"glue_job_name": "my-etl", "glue_job_run_id": "jr_1"},
			wantRunID: "jr_1",
		},
		{
			name:      "sync trigger with nil metadata",
			cfg:       httpCfg,
			meta:      nil,
			wantRunID: "sync",
		},
		{
			name:      "sync trigger with empty metadata",
			cfg:       httpCfg,
			meta:      map[string]interface{}{},
			wantRunID: "sync",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.cfg
			fake := &storetest.FakeDynamo{
				GetItemFn: func(context.Context, *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
					return &dynamodb.GetItemOutput{Item: configItem(t, cfg)}, nil
				},
			}
			d := testDeps(fake)
			d.TriggerRunner = &fakeExecutor{meta: tt.meta}

			out, err := orchestrator.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
				Mode: "trigger", PipelineID: "p", ScheduleID: "s", Date: "2026-03-01",
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if out.RunID != tt.wantRunID {
				t.Errorf("runID = %q, want %q", out.RunID, tt.wantRunID)
			}
			if len(out.Metadata) == 0 {
				t.Errorf("metadata = %v, want a non-empty map for $.triggerResult.metadata", out.Metadata)
			}
		})
	}
}
