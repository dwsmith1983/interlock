package lambda_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/store"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// ---------------------------------------------------------------------------
// Mock DynamoDB client (simple, with configurable callbacks)
// ---------------------------------------------------------------------------

type mockDynamo struct {
	store.DynamoAPI
	getItemFn func(ctx context.Context, input *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	queryFn   func(ctx context.Context, input *dynamodb.QueryInput, opts ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
}

func (m *mockDynamo) GetItem(ctx context.Context, input *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	if m.getItemFn != nil {
		return m.getItemFn(ctx, input, opts...)
	}
	return &dynamodb.GetItemOutput{}, nil
}

func (m *mockDynamo) Query(ctx context.Context, input *dynamodb.QueryInput, opts ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	if m.queryFn != nil {
		return m.queryFn(ctx, input, opts...)
	}
	return &dynamodb.QueryOutput{}, nil
}

func (m *mockDynamo) PutItem(_ context.Context, _ *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	return &dynamodb.PutItemOutput{}, nil
}
func (m *mockDynamo) UpdateItem(_ context.Context, _ *dynamodb.UpdateItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	return &dynamodb.UpdateItemOutput{}, nil
}
func (m *mockDynamo) DeleteItem(_ context.Context, _ *dynamodb.DeleteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	return &dynamodb.DeleteItemOutput{}, nil
}
func (m *mockDynamo) Scan(_ context.Context, _ *dynamodb.ScanInput, _ ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	return &dynamodb.ScanOutput{}, nil
}

// ---------------------------------------------------------------------------
// Mock trigger executor
// ---------------------------------------------------------------------------

type mockTriggerExecutor struct {
	result map[string]interface{}
	err    error
	called bool
	cfg    *types.TriggerConfig
}

func (m *mockTriggerExecutor) Execute(_ context.Context, cfg *types.TriggerConfig) (map[string]interface{}, error) {
	m.called = true
	m.cfg = cfg
	return m.result, m.err
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

func configItem(pipelineID string, cfg types.PipelineConfig) map[string]ddbtypes.AttributeValue {
	data, _ := json.Marshal(cfg)
	return map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: types.ConfigSK},
		"config": &ddbtypes.AttributeValueMemberS{Value: string(data)},
	}
}

func sensorItem(pipelineID, sensorKey string, data map[string]ddbtypes.AttributeValue) map[string]ddbtypes.AttributeValue {
	item := map[string]ddbtypes.AttributeValue{
		"PK": &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
		"SK": &ddbtypes.AttributeValueMemberS{Value: types.SensorSK(sensorKey)},
	}
	if len(data) > 0 {
		item["data"] = &ddbtypes.AttributeValueMemberM{Value: data}
	}
	return item
}

func jobItem(pipelineID, schedule, date, timestamp, event string) map[string]ddbtypes.AttributeValue {
	return map[string]ddbtypes.AttributeValue{
		"PK":    &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
		"SK":    &ddbtypes.AttributeValueMemberS{Value: types.JobSK(schedule, date, timestamp)},
		"event": &ddbtypes.AttributeValueMemberS{Value: event},
	}
}

func newTestDeps(ddb store.DynamoAPI) *lambda.Deps {
	return &lambda.Deps{
		Store: &store.Store{
			Client:       ddb,
			ControlTable: "control",
			JobLogTable:  "joblog",
		},
		EventBridge:  &mockEventBridge{},
		EventBusName: "test-bus",
		Logger:       slog.Default(),
	}
}

// ---------------------------------------------------------------------------
// Evaluate tests
// ---------------------------------------------------------------------------

func TestOrchestrator_Evaluate_AllPass(t *testing.T) {
	pipelineID := "gold-orders"
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: pipelineID},
		Validation: types.ValidationConfig{
			Trigger: "ALL",
			Rules: []types.ValidationRule{
				{Key: "upstream-complete", Check: types.CheckEquals, Field: "status", Value: "done"},
				{Key: "quality-score", Check: types.CheckGTE, Field: "score", Value: float64(90)},
			},
		},
	}

	ddb := &mockDynamo{
		getItemFn: func(_ context.Context, input *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: configItem(pipelineID, cfg)}, nil
		},
		queryFn: func(_ context.Context, input *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return &dynamodb.QueryOutput{
				Items: []map[string]ddbtypes.AttributeValue{
					sensorItem(pipelineID, "upstream-complete", map[string]ddbtypes.AttributeValue{
						"status": &ddbtypes.AttributeValueMemberS{Value: "done"},
					}),
					sensorItem(pipelineID, "quality-score", map[string]ddbtypes.AttributeValue{
						"score": &ddbtypes.AttributeValueMemberN{Value: "95"},
					}),
				},
			}, nil
		},
	}

	d := newTestDeps(ddb)
	out, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode:       "evaluate",
		PipelineID: pipelineID,
		ScheduleID: "daily",
		Date:       "2026-03-01",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Mode != "evaluate" {
		t.Errorf("mode = %q, want %q", out.Mode, "evaluate")
	}
	if out.Status != "passed" {
		t.Errorf("status = %q, want %q; results = %v", out.Status, "passed", out.Results)
	}

	eb := d.EventBridge.(*mockEventBridge)
	if len(eb.events) != 1 {
		t.Fatalf("expected 1 EventBridge call, got %d", len(eb.events))
	}
}

func TestOrchestrator_Evaluate_OneFails(t *testing.T) {
	pipelineID := "gold-orders"
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: pipelineID},
		Validation: types.ValidationConfig{
			Trigger: "ALL",
			Rules: []types.ValidationRule{
				{Key: "upstream-complete", Check: types.CheckExists},
				{Key: "quality-score", Check: types.CheckExists},
			},
		},
	}

	ddb := &mockDynamo{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: configItem(pipelineID, cfg)}, nil
		},
		queryFn: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return &dynamodb.QueryOutput{
				Items: []map[string]ddbtypes.AttributeValue{
					sensorItem(pipelineID, "upstream-complete", map[string]ddbtypes.AttributeValue{
						"status": &ddbtypes.AttributeValueMemberS{Value: "done"},
					}),
				},
			}, nil
		},
	}

	d := newTestDeps(ddb)
	out, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode:       "evaluate",
		PipelineID: pipelineID,
		ScheduleID: "daily",
		Date:       "2026-03-01",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Status == "passed" {
		t.Error("status = passed, want not_ready (one sensor missing)")
	}

	eb := d.EventBridge.(*mockEventBridge)
	if len(eb.events) != 0 {
		t.Errorf("expected 0 EventBridge calls, got %d", len(eb.events))
	}
}

func TestOrchestrator_Evaluate_ConfigNotFound(t *testing.T) {
	ddb := &mockDynamo{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: nil}, nil
		},
	}

	d := newTestDeps(ddb)
	out, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode:       "evaluate",
		PipelineID: "missing-pipeline",
		ScheduleID: "daily",
		Date:       "2026-03-01",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Error == "" {
		t.Error("expected non-empty error in output")
	}
}

// ---------------------------------------------------------------------------
// Trigger tests
// ---------------------------------------------------------------------------

func TestOrchestrator_Trigger(t *testing.T) {
	pipelineID := "gold-orders"
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: pipelineID},
		Job: types.JobConfig{
			Type: types.TriggerGlue,
			Config: map[string]interface{}{
				"jobName": "gold-etl",
			},
		},
	}

	ddb := &mockDynamo{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: configItem(pipelineID, cfg)}, nil
		},
	}

	executor := &mockTriggerExecutor{
		result: map[string]interface{}{"jobRunId": "jr-12345"},
	}

	d := newTestDeps(ddb)
	d.TriggerRunner = executor

	out, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode:       "trigger",
		PipelineID: pipelineID,
		ScheduleID: "daily",
		Date:       "2026-03-01",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Mode != "trigger" {
		t.Errorf("mode = %q, want %q", out.Mode, "trigger")
	}
	if out.RunID != "jr-12345" {
		t.Errorf("runID = %q, want %q", out.RunID, "jr-12345")
	}
	if out.JobType != "glue" {
		t.Errorf("jobType = %q, want %q", out.JobType, "glue")
	}
	if !executor.called {
		t.Error("trigger executor was not called")
	}
	if executor.cfg == nil || executor.cfg.Type != types.TriggerGlue {
		t.Error("trigger config type mismatch")
	}
	if executor.cfg.Glue == nil || executor.cfg.Glue.JobName != "gold-etl" {
		t.Errorf("glue job name = %v, want %q", executor.cfg.Glue, "gold-etl")
	}
}

func TestOrchestrator_Trigger_Error(t *testing.T) {
	pipelineID := "gold-orders"
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: pipelineID},
		Job: types.JobConfig{
			Type:   types.TriggerGlue,
			Config: map[string]interface{}{"jobName": "gold-etl"},
		},
	}

	ddb := &mockDynamo{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: configItem(pipelineID, cfg)}, nil
		},
	}

	executor := &mockTriggerExecutor{
		err: fmt.Errorf("glue: throttling"),
	}

	d := newTestDeps(ddb)
	d.TriggerRunner = executor

	out, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode:       "trigger",
		PipelineID: pipelineID,
		ScheduleID: "daily",
		Date:       "2026-03-01",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Error == "" {
		t.Error("expected error in output for failed trigger")
	}
}

// ---------------------------------------------------------------------------
// Check-job tests
// ---------------------------------------------------------------------------

func TestOrchestrator_CheckJob_Found(t *testing.T) {
	pipelineID := "gold-orders"

	ddb := &mockDynamo{
		queryFn: func(_ context.Context, input *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return &dynamodb.QueryOutput{
				Items: []map[string]ddbtypes.AttributeValue{
					jobItem(pipelineID, "daily", "2026-03-01", "1709280000000", "success"),
				},
			}, nil
		},
	}

	d := newTestDeps(ddb)
	out, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode:       "check-job",
		PipelineID: pipelineID,
		ScheduleID: "daily",
		Date:       "2026-03-01",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Mode != "check-job" {
		t.Errorf("mode = %q, want %q", out.Mode, "check-job")
	}
	if out.Event != "success" {
		t.Errorf("event = %q, want %q", out.Event, "success")
	}
}

func TestOrchestrator_CheckJob_NotFound(t *testing.T) {
	ddb := &mockDynamo{
		queryFn: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return &dynamodb.QueryOutput{Items: nil}, nil
		},
	}

	d := newTestDeps(ddb)
	out, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode:       "check-job",
		PipelineID: "gold-orders",
		ScheduleID: "daily",
		Date:       "2026-03-01",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Event != "" {
		t.Errorf("event = %q, want empty", out.Event)
	}
}

// ---------------------------------------------------------------------------
// Post-run tests
// ---------------------------------------------------------------------------

func TestOrchestrator_PostRun_NoConfig(t *testing.T) {
	pipelineID := "gold-orders"
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: pipelineID},
	}

	ddb := &mockDynamo{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: configItem(pipelineID, cfg)}, nil
		},
	}

	d := newTestDeps(ddb)
	out, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode:       "post-run",
		PipelineID: pipelineID,
		ScheduleID: "daily",
		Date:       "2026-03-01",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Status != "passed" {
		t.Errorf("status = %q, want %q when no PostRun config", out.Status, "passed")
	}
}

func TestOrchestrator_PostRun_RulesPass(t *testing.T) {
	pipelineID := "gold-orders"
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: pipelineID},
		PostRun: &types.PostRunConfig{
			Rules: []types.ValidationRule{
				{Key: "row-count", Check: types.CheckGTE, Field: "count", Value: float64(100)},
			},
		},
	}

	ddb := &mockDynamo{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: configItem(pipelineID, cfg)}, nil
		},
		queryFn: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return &dynamodb.QueryOutput{
				Items: []map[string]ddbtypes.AttributeValue{
					sensorItem(pipelineID, "row-count", map[string]ddbtypes.AttributeValue{
						"count": &ddbtypes.AttributeValueMemberN{Value: "500"},
					}),
				},
			}, nil
		},
	}

	d := newTestDeps(ddb)
	out, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode:       "post-run",
		PipelineID: pipelineID,
		ScheduleID: "daily",
		Date:       "2026-03-01",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Status != "passed" {
		t.Errorf("status = %q, want %q; results = %v", out.Status, "passed", out.Results)
	}
}

// ---------------------------------------------------------------------------
// Unknown mode test
// ---------------------------------------------------------------------------

func TestOrchestrator_UnknownMode(t *testing.T) {
	d := newTestDeps(&mockDynamo{})
	_, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode:       "explode",
		PipelineID: "gold-orders",
	})
	if err == nil {
		t.Fatal("expected error for unknown mode")
	}
}
