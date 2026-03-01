package v2

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	store "github.com/dwsmith1983/interlock/internal/store/v2"
	"github.com/dwsmith1983/interlock/pkg/types"
	v2types "github.com/dwsmith1983/interlock/pkg/types/v2"
)

// ---------------------------------------------------------------------------
// Mock DynamoDB client
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

// Satisfy the remaining DynamoAPI interface methods with no-ops.
func (m *mockDynamo) PutItem(ctx context.Context, input *dynamodb.PutItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	return &dynamodb.PutItemOutput{}, nil
}
func (m *mockDynamo) UpdateItem(ctx context.Context, input *dynamodb.UpdateItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	return &dynamodb.UpdateItemOutput{}, nil
}
func (m *mockDynamo) DeleteItem(ctx context.Context, input *dynamodb.DeleteItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	return &dynamodb.DeleteItemOutput{}, nil
}
func (m *mockDynamo) Scan(ctx context.Context, input *dynamodb.ScanInput, opts ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	return &dynamodb.ScanOutput{}, nil
}

// ---------------------------------------------------------------------------
// Mock EventBridge client
// ---------------------------------------------------------------------------

type mockEventBridge struct {
	calls []eventbridge.PutEventsInput
}

func (m *mockEventBridge) PutEvents(ctx context.Context, input *eventbridge.PutEventsInput, opts ...func(*eventbridge.Options)) (*eventbridge.PutEventsOutput, error) {
	m.calls = append(m.calls, *input)
	return &eventbridge.PutEventsOutput{}, nil
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

func (m *mockTriggerExecutor) Execute(ctx context.Context, cfg *types.TriggerConfig) (map[string]interface{}, error) {
	m.called = true
	m.cfg = cfg
	return m.result, m.err
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// configItem builds a DynamoDB item for a pipeline config.
func configItem(pipelineID string, cfg v2types.PipelineConfig) map[string]ddbtypes.AttributeValue {
	data, _ := json.Marshal(cfg)
	return map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: v2types.PipelinePK(pipelineID)},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: v2types.ConfigSK},
		"config": &ddbtypes.AttributeValueMemberS{Value: string(data)},
	}
}

// sensorItem builds a DynamoDB item for a sensor row with arbitrary data fields.
func sensorItem(pipelineID, sensorKey string, data map[string]ddbtypes.AttributeValue) map[string]ddbtypes.AttributeValue {
	item := map[string]ddbtypes.AttributeValue{
		"PK": &ddbtypes.AttributeValueMemberS{Value: v2types.PipelinePK(pipelineID)},
		"SK": &ddbtypes.AttributeValueMemberS{Value: v2types.SensorSK(sensorKey)},
	}
	// Build a data map attribute
	if len(data) > 0 {
		item["data"] = &ddbtypes.AttributeValueMemberM{Value: data}
	}
	return item
}

// jobItem builds a DynamoDB item for a job log record.
func jobItem(pipelineID, schedule, date, timestamp, event string) map[string]ddbtypes.AttributeValue {
	return map[string]ddbtypes.AttributeValue{
		"PK":    &ddbtypes.AttributeValueMemberS{Value: v2types.PipelinePK(pipelineID)},
		"SK":    &ddbtypes.AttributeValueMemberS{Value: v2types.JobSK(schedule, date, timestamp)},
		"event": &ddbtypes.AttributeValueMemberS{Value: event},
	}
}

func newTestDeps(ddb store.DynamoAPI) *Deps {
	return &Deps{
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
	cfg := v2types.PipelineConfig{
		Pipeline: v2types.PipelineIdentity{ID: pipelineID},
		Validation: v2types.ValidationConfig{
			Trigger: "ALL",
			Rules: []v2types.ValidationRule{
				{Key: "upstream-complete", Check: v2types.CheckEquals, Field: "status", Value: "done"},
				{Key: "quality-score", Check: v2types.CheckGTE, Field: "score", Value: float64(90)},
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
	out, err := HandleOrchestrator(context.Background(), d, OrchestratorInput{
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
	if !out.Passed {
		t.Errorf("passed = false, want true; results = %v", out.Results)
	}

	// Should have published VALIDATION_PASSED event.
	eb := d.EventBridge.(*mockEventBridge)
	if len(eb.calls) != 1 {
		t.Fatalf("expected 1 EventBridge call, got %d", len(eb.calls))
	}
}

func TestOrchestrator_Evaluate_OneFails(t *testing.T) {
	pipelineID := "gold-orders"
	cfg := v2types.PipelineConfig{
		Pipeline: v2types.PipelineIdentity{ID: pipelineID},
		Validation: v2types.ValidationConfig{
			Trigger: "ALL",
			Rules: []v2types.ValidationRule{
				{Key: "upstream-complete", Check: v2types.CheckExists},
				{Key: "quality-score", Check: v2types.CheckExists},
			},
		},
	}

	ddb := &mockDynamo{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: configItem(pipelineID, cfg)}, nil
		},
		queryFn: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			// Only one sensor exists — "quality-score" is missing.
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
	out, err := HandleOrchestrator(context.Background(), d, OrchestratorInput{
		Mode:       "evaluate",
		PipelineID: pipelineID,
		ScheduleID: "daily",
		Date:       "2026-03-01",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Passed {
		t.Error("passed = true, want false (one sensor missing)")
	}

	// Should NOT have published VALIDATION_PASSED event.
	eb := d.EventBridge.(*mockEventBridge)
	if len(eb.calls) != 0 {
		t.Errorf("expected 0 EventBridge calls, got %d", len(eb.calls))
	}
}

func TestOrchestrator_Evaluate_ConfigNotFound(t *testing.T) {
	ddb := &mockDynamo{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: nil}, nil // no config
		},
	}

	d := newTestDeps(ddb)
	out, err := HandleOrchestrator(context.Background(), d, OrchestratorInput{
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
	cfg := v2types.PipelineConfig{
		Pipeline: v2types.PipelineIdentity{ID: pipelineID},
		Job: v2types.JobConfig{
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

	out, err := HandleOrchestrator(context.Background(), d, OrchestratorInput{
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
	cfg := v2types.PipelineConfig{
		Pipeline: v2types.PipelineIdentity{ID: pipelineID},
		Job: v2types.JobConfig{
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

	out, err := HandleOrchestrator(context.Background(), d, OrchestratorInput{
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
	out, err := HandleOrchestrator(context.Background(), d, OrchestratorInput{
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
	out, err := HandleOrchestrator(context.Background(), d, OrchestratorInput{
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
	cfg := v2types.PipelineConfig{
		Pipeline: v2types.PipelineIdentity{ID: pipelineID},
		// PostRun is nil
	}

	ddb := &mockDynamo{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: configItem(pipelineID, cfg)}, nil
		},
	}

	d := newTestDeps(ddb)
	out, err := HandleOrchestrator(context.Background(), d, OrchestratorInput{
		Mode:       "post-run",
		PipelineID: pipelineID,
		ScheduleID: "daily",
		Date:       "2026-03-01",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !out.Passed {
		t.Error("passed = false, want true when no PostRun config")
	}
}

func TestOrchestrator_PostRun_RulesPass(t *testing.T) {
	pipelineID := "gold-orders"
	cfg := v2types.PipelineConfig{
		Pipeline: v2types.PipelineIdentity{ID: pipelineID},
		PostRun: &v2types.PostRunConfig{
			Rules: []v2types.ValidationRule{
				{Key: "row-count", Check: v2types.CheckGTE, Field: "count", Value: float64(100)},
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
	out, err := HandleOrchestrator(context.Background(), d, OrchestratorInput{
		Mode:       "post-run",
		PipelineID: pipelineID,
		ScheduleID: "daily",
		Date:       "2026-03-01",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !out.Passed {
		t.Errorf("passed = false, want true; results = %v", out.Results)
	}
}

// ---------------------------------------------------------------------------
// Unknown mode test
// ---------------------------------------------------------------------------

func TestOrchestrator_UnknownMode(t *testing.T) {
	d := newTestDeps(&mockDynamo{})
	_, err := HandleOrchestrator(context.Background(), d, OrchestratorInput{
		Mode:       "explode",
		PipelineID: "gold-orders",
	})
	if err == nil {
		t.Fatal("expected error for unknown mode")
	}
}
