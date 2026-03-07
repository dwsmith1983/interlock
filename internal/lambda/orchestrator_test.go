package lambda_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
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
	putItemFn func(ctx context.Context, input *dynamodb.PutItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
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

func (m *mockDynamo) PutItem(ctx context.Context, input *dynamodb.PutItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	if m.putItemFn != nil {
		return m.putItemFn(ctx, input, opts...)
	}
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
// Mock status checker
// ---------------------------------------------------------------------------

type mockStatusChecker struct {
	result lambda.StatusResult
	err    error
	called bool
}

func (m *mockStatusChecker) CheckStatus(_ context.Context, _ types.TriggerType, _ map[string]interface{}, _ map[string]string) (lambda.StatusResult, error) {
	m.called = true
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

func jobItem(pipelineID, schedule, date, event string) map[string]ddbtypes.AttributeValue {
	return map[string]ddbtypes.AttributeValue{
		"PK":    &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
		"SK":    &ddbtypes.AttributeValueMemberS{Value: types.JobSK(schedule, date, "1709280000000")},
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

func TestOrchestrator_Evaluate_PerPeriodSensorKey(t *testing.T) {
	pipelineID := "silver-cdr-hour"
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: pipelineID},
		Validation: types.ValidationConfig{
			Trigger: "ALL",
			Rules: []types.ValidationRule{
				{Key: "hourly-status", Check: types.CheckEquals, Field: "complete", Value: true},
				{Key: "hourly-status", Check: types.CheckGTE, Field: "pct_of_expected", Value: float64(0.7)},
			},
		},
	}

	ddb := &mockDynamo{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: configItem(pipelineID, cfg)}, nil
		},
		queryFn: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			// Sensor key uses compact date (from sensor.py): hourly-status#20260303T07
			return &dynamodb.QueryOutput{
				Items: []map[string]ddbtypes.AttributeValue{
					sensorItem(pipelineID, "hourly-status#20260303T07", map[string]ddbtypes.AttributeValue{
						"complete":        &ddbtypes.AttributeValueMemberBOOL{Value: true},
						"pct_of_expected": &ddbtypes.AttributeValueMemberN{Value: "0.95"},
					}),
				},
			}, nil
		},
	}

	d := newTestDeps(ddb)
	out, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode:       "evaluate",
		PipelineID: pipelineID,
		ScheduleID: "stream",
		Date:       "2026-03-03T07",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Status != "passed" {
		t.Errorf("status = %q, want %q — per-period sensor key should be remapped to base key; results = %v", out.Status, "passed", out.Results)
	}
}

func TestOrchestrator_Evaluate_PerPeriodDailySensorKey(t *testing.T) {
	pipelineID := "silver-cdr-day"
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: pipelineID},
		Validation: types.ValidationConfig{
			Trigger: "ALL",
			Rules: []types.ValidationRule{
				{Key: "daily-status", Check: types.CheckEquals, Field: "all_hours_complete", Value: true},
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
					sensorItem(pipelineID, "daily-status#20260303", map[string]ddbtypes.AttributeValue{
						"all_hours_complete": &ddbtypes.AttributeValueMemberBOOL{Value: true},
					}),
				},
			}, nil
		},
	}

	d := newTestDeps(ddb)
	out, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode:       "evaluate",
		PipelineID: pipelineID,
		ScheduleID: "stream",
		Date:       "2026-03-03",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Status != "passed" {
		t.Errorf("status = %q, want %q — daily per-period sensor should be remapped; results = %v", out.Status, "passed", out.Results)
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
		result: map[string]interface{}{"glue_job_name": "gold-etl", "glue_job_run_id": "jr-12345"},
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
	if out.Metadata == nil {
		t.Error("metadata should not be nil")
	}
	if out.Metadata["glue_job_run_id"] != "jr-12345" {
		t.Errorf("metadata[glue_job_run_id] = %v, want %q", out.Metadata["glue_job_run_id"], "jr-12345")
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

func TestOrchestrator_Trigger_InfraFailure(t *testing.T) {
	pipelineID := "gold-orders"
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: pipelineID},
		Job: types.JobConfig{
			Type:   types.TriggerGlue,
			Config: map[string]interface{}{"jobName": "gold-etl"},
		},
	}

	var putItemCalled bool
	var capturedEvent string
	var capturedError string

	ddb := &mockDynamo{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: configItem(pipelineID, cfg)}, nil
		},
		putItemFn: func(_ context.Context, input *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			putItemCalled = true
			if ev, ok := input.Item["event"].(*ddbtypes.AttributeValueMemberS); ok {
				capturedEvent = ev.Value
			}
			if em, ok := input.Item["error"].(*ddbtypes.AttributeValueMemberS); ok {
				capturedError = em.Value
			}
			return &dynamodb.PutItemOutput{}, nil
		},
	}

	executor := &mockTriggerExecutor{
		err: fmt.Errorf("glue trigger: StartJobRun failed: ConcurrentRunsExceededException"),
	}

	d := newTestDeps(ddb)
	d.TriggerRunner = executor

	_, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode:       "trigger",
		PipelineID: pipelineID,
		ScheduleID: "daily",
		Date:       "2026-03-01",
	})
	if err == nil {
		t.Fatal("expected Lambda error for infra trigger failure")
	}
	if !strings.Contains(err.Error(), "trigger execute") {
		t.Errorf("error = %q, want it to contain 'trigger execute'", err.Error())
	}
	if !putItemCalled {
		t.Error("expected joblog PutItem to be called")
	}
	if capturedEvent != types.JobEventInfraTriggerFailure {
		t.Errorf("joblog event = %q, want %q", capturedEvent, types.JobEventInfraTriggerFailure)
	}
	if capturedError == "" {
		t.Error("expected joblog error message to be non-empty")
	}
}

func TestOrchestrator_Trigger_InfraFailure_JoblogWriteFails(t *testing.T) {
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
		putItemFn: func(_ context.Context, _ *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			return nil, fmt.Errorf("dynamodb: provisioned throughput exceeded")
		},
	}

	executor := &mockTriggerExecutor{
		err: fmt.Errorf("glue trigger: ConcurrentRunsExceededException"),
	}

	d := newTestDeps(ddb)
	d.TriggerRunner = executor

	_, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode:       "trigger",
		PipelineID: pipelineID,
		ScheduleID: "daily",
		Date:       "2026-03-01",
	})
	if err == nil {
		t.Fatal("expected Lambda error even when joblog write fails")
	}
	if !strings.Contains(err.Error(), "trigger execute") {
		t.Errorf("error = %q, want it to contain 'trigger execute'", err.Error())
	}
}

func TestOrchestrator_Trigger_ConfigError(t *testing.T) {
	ddb := &mockDynamo{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{}, nil // no config found
		},
	}

	d := newTestDeps(ddb)

	out, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode:       "trigger",
		PipelineID: "nonexistent",
		ScheduleID: "daily",
		Date:       "2026-03-01",
	})
	if err != nil {
		t.Fatalf("config errors should be payload errors, not Lambda errors: %v", err)
	}
	if out.Error == "" {
		t.Error("expected payload error for missing config")
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
					jobItem(pipelineID, "daily", "2026-03-01", "success"),
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

func TestOrchestrator_CheckJob_PollsGlueSucceeded(t *testing.T) {
	pipelineID := "silver-cdr-hour"
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: pipelineID},
		Job:      types.JobConfig{Type: types.TriggerGlue},
	}

	ddb := &mockDynamo{
		getItemFn: func(_ context.Context, input *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: configItem(pipelineID, cfg)}, nil
		},
		queryFn: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return &dynamodb.QueryOutput{Items: nil}, nil
		},
	}

	checker := &mockStatusChecker{
		result: lambda.StatusResult{State: "succeeded", Message: "SUCCEEDED"},
	}

	d := newTestDeps(ddb)
	d.StatusChecker = checker

	out, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode:       "check-job",
		PipelineID: pipelineID,
		ScheduleID: "hourly",
		Date:       "2026-03-02",
		RunID:      "jr-999",
		Metadata:   map[string]interface{}{"glue_job_name": "cdr-agg-hour", "glue_job_run_id": "jr-999"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Event != "success" {
		t.Errorf("event = %q, want %q", out.Event, "success")
	}
	if !checker.called {
		t.Error("status checker was not called")
	}
}

func TestOrchestrator_CheckJob_PollsGlueFailed(t *testing.T) {
	pipelineID := "silver-cdr-hour"
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: pipelineID},
		Job:      types.JobConfig{Type: types.TriggerGlue},
	}

	ddb := &mockDynamo{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: configItem(pipelineID, cfg)}, nil
		},
		queryFn: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return &dynamodb.QueryOutput{Items: nil}, nil
		},
	}

	checker := &mockStatusChecker{
		result: lambda.StatusResult{State: "failed", Message: "OOM killed"},
	}

	d := newTestDeps(ddb)
	d.StatusChecker = checker

	out, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode:       "check-job",
		PipelineID: pipelineID,
		ScheduleID: "hourly",
		Date:       "2026-03-02",
		Metadata:   map[string]interface{}{"glue_job_name": "cdr-agg-hour", "glue_job_run_id": "jr-999"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Event != "fail" {
		t.Errorf("event = %q, want %q", out.Event, "fail")
	}
}

func TestOrchestrator_CheckJob_PollsRunning(t *testing.T) {
	pipelineID := "silver-cdr-hour"
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: pipelineID},
		Job:      types.JobConfig{Type: types.TriggerGlue},
	}

	ddb := &mockDynamo{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: configItem(pipelineID, cfg)}, nil
		},
		queryFn: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return &dynamodb.QueryOutput{Items: nil}, nil
		},
	}

	checker := &mockStatusChecker{
		result: lambda.StatusResult{State: "running", Message: "RUNNING"},
	}

	d := newTestDeps(ddb)
	d.StatusChecker = checker

	out, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode:       "check-job",
		PipelineID: pipelineID,
		ScheduleID: "hourly",
		Date:       "2026-03-02",
		Metadata:   map[string]interface{}{"glue_job_name": "cdr-agg-hour", "glue_job_run_id": "jr-999"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Event != "" {
		t.Errorf("event = %q, want empty (still running)", out.Event)
	}
}

func TestOrchestrator_CheckJob_SkipsInfraFailureEvent(t *testing.T) {
	pipelineID := "silver-cdr-hour"
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: pipelineID},
		Job:      types.JobConfig{Type: types.TriggerGlue},
	}

	ddb := &mockDynamo{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: configItem(pipelineID, cfg)}, nil
		},
		queryFn: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			// Return an infra-trigger-failure event as the latest joblog entry.
			return &dynamodb.QueryOutput{
				Items: []map[string]ddbtypes.AttributeValue{
					jobItem(pipelineID, "hourly", "2026-03-02", types.JobEventInfraTriggerFailure),
				},
			}, nil
		},
	}

	checker := &mockStatusChecker{
		result: lambda.StatusResult{State: "succeeded", Message: "SUCCEEDED"},
	}

	d := newTestDeps(ddb)
	d.StatusChecker = checker

	out, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode:       "check-job",
		PipelineID: pipelineID,
		ScheduleID: "hourly",
		Date:       "2026-03-02",
		RunID:      "jr-999",
		Metadata:   map[string]interface{}{"glue_job_name": "cdr-agg-hour", "glue_job_run_id": "jr-999"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Event != "success" {
		t.Errorf("event = %q, want %q — infra-trigger-failure should be skipped", out.Event, "success")
	}
	if !checker.called {
		t.Error("StatusChecker should be called when only infra-trigger-failure events exist")
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

func TestPostRun_PassedStatus(t *testing.T) {
	pipelineID := "gold-orders"
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: pipelineID},
		PostRun: &types.PostRunConfig{
			Rules: []types.ValidationRule{
				{Key: "audit-result", Check: types.CheckEquals, Field: "match", Value: true},
			},
		},
	}

	ddb := newMockDDB()
	ddb.putRaw("control", configItem(pipelineID, cfg))
	ddb.putRaw("control", sensorItem(pipelineID, "audit-result", map[string]ddbtypes.AttributeValue{
		"match":        &ddbtypes.AttributeValueMemberBOOL{Value: true},
		"sensor_count": &ddbtypes.AttributeValueMemberN{Value: "100"},
	}))

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

	// Verify baseline was written (first iteration, no prior baseline).
	baselineKey := ddbItemKey("control", types.PipelinePK(pipelineID), types.SensorSK("postrun-baseline"))
	if _, ok := ddb.items[baselineKey]; !ok {
		t.Error("expected postrun-baseline sensor to be written on first iteration")
	}
}

func TestPostRun_DriftDetected(t *testing.T) {
	pipelineID := "gold-orders"
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: pipelineID},
		PostRun: &types.PostRunConfig{
			Rules: []types.ValidationRule{
				{Key: "audit-result", Check: types.CheckEquals, Field: "match", Value: true},
			},
		},
	}

	ddb := newMockDDB()
	ddb.putRaw("control", configItem(pipelineID, cfg))
	// Current audit result has a different count than baseline.
	ddb.putRaw("control", sensorItem(pipelineID, "audit-result", map[string]ddbtypes.AttributeValue{
		"match":        &ddbtypes.AttributeValueMemberBOOL{Value: true},
		"sensor_count": &ddbtypes.AttributeValueMemberN{Value: "120"},
	}))
	// Baseline saved from first iteration.
	ddb.putRaw("control", sensorItem(pipelineID, "postrun-baseline", map[string]ddbtypes.AttributeValue{
		"sensor_count": &ddbtypes.AttributeValueMemberN{Value: "100"},
	}))

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
	if out.Status != "drift" {
		t.Errorf("status = %q, want %q", out.Status, "drift")
	}

	// Verify DATA_DRIFT event was published.
	eb := d.EventBridge.(*mockEventBridge)
	if len(eb.events) != 1 {
		t.Fatalf("expected 1 EventBridge event, got %d", len(eb.events))
	}
	entry := eb.events[0].Entries[0]
	if *entry.DetailType != string(types.EventDataDrift) {
		t.Errorf("event type = %q, want %q", *entry.DetailType, string(types.EventDataDrift))
	}
}

func TestPostRun_DriftWritesRerunRequest(t *testing.T) {
	pipelineID := "gold-orders"
	scheduleID := "daily"
	date := "2026-03-01"
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: pipelineID},
		PostRun: &types.PostRunConfig{
			Rules: []types.ValidationRule{
				{Key: "audit-result", Check: types.CheckEquals, Field: "match", Value: true},
			},
		},
	}

	ddb := newMockDDB()
	ddb.putRaw("control", configItem(pipelineID, cfg))
	// Current audit result with sensor_count=150.
	ddb.putRaw("control", sensorItem(pipelineID, "audit-result", map[string]ddbtypes.AttributeValue{
		"match":        &ddbtypes.AttributeValueMemberBOOL{Value: true},
		"sensor_count": &ddbtypes.AttributeValueMemberN{Value: "150"},
	}))
	// Baseline with sensor_count=100 (different count → drift).
	ddb.putRaw("control", sensorItem(pipelineID, "postrun-baseline", map[string]ddbtypes.AttributeValue{
		"sensor_count": &ddbtypes.AttributeValueMemberN{Value: "100"},
	}))

	d := newTestDeps(ddb)
	out, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode:       "post-run",
		PipelineID: pipelineID,
		ScheduleID: scheduleID,
		Date:       date,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Status != "drift" {
		t.Errorf("status = %q, want %q", out.Status, "drift")
	}

	// Verify RERUN_REQUEST was written to the control table.
	wantPK := types.PipelinePK(pipelineID)
	wantSK := types.RerunRequestSK(scheduleID, date)
	key := ddbItemKey("control", wantPK, wantSK)

	ddb.mu.Lock()
	item, ok := ddb.items[key]
	ddb.mu.Unlock()

	if !ok {
		t.Fatalf("expected RERUN_REQUEST item at PK=%q SK=%q, but not found", wantPK, wantSK)
	}

	reason, _ := item["reason"].(*ddbtypes.AttributeValueMemberS)
	if reason == nil || reason.Value != "data-drift" {
		t.Errorf("reason = %v, want %q", reason, "data-drift")
	}
}

func TestPostRun_NoRules(t *testing.T) {
	pipelineID := "gold-orders"
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: pipelineID},
		// No PostRun config at all.
	}

	ddb := newMockDDB()
	ddb.putRaw("control", configItem(pipelineID, cfg))

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
		t.Errorf("status = %q, want %q when no PostRun rules", out.Status, "passed")
	}
}

// ---------------------------------------------------------------------------
// Validation-exhausted tests
// ---------------------------------------------------------------------------

func TestOrchestrator_ValidationExhausted(t *testing.T) {
	d := newTestDeps(&mockDynamo{})
	out, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode:       "validation-exhausted",
		PipelineID: "gold-orders",
		ScheduleID: "daily",
		Date:       "2026-03-01",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Mode != "validation-exhausted" {
		t.Errorf("mode = %q, want %q", out.Mode, "validation-exhausted")
	}
	if out.Status != "exhausted" {
		t.Errorf("status = %q, want %q", out.Status, "exhausted")
	}

	eb := d.EventBridge.(*mockEventBridge)
	if len(eb.events) != 1 {
		t.Fatalf("expected 1 EventBridge call, got %d", len(eb.events))
	}
}

func TestOrchestrator_ValidationExhausted_WritesJoblog(t *testing.T) {
	ddb := newMockDDB()
	d := newTestDeps(ddb)

	out, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode:       "validation-exhausted",
		PipelineID: "gold-orders",
		ScheduleID: "daily",
		Date:       "2026-03-01",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Mode != "validation-exhausted" {
		t.Errorf("mode = %q, want %q", out.Mode, "validation-exhausted")
	}
	if out.Status != "exhausted" {
		t.Errorf("status = %q, want %q", out.Status, "exhausted")
	}

	// Verify joblog entry was written (query for JOB# prefix).
	jobItems, _ := ddb.Query(context.Background(), &dynamodb.QueryInput{
		TableName:              strPtr("joblog"),
		KeyConditionExpression: strPtr("PK = :pk AND begins_with(SK, :prefix)"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":pk":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("gold-orders")},
			":prefix": &ddbtypes.AttributeValueMemberS{Value: "JOB#daily#2026-03-01#"},
		},
	})
	if len(jobItems.Items) != 1 {
		t.Fatalf("expected 1 joblog entry, got %d", len(jobItems.Items))
	}
	jobEvent := jobItems.Items[0]["event"].(*ddbtypes.AttributeValueMemberS).Value
	if jobEvent != types.JobEventValidationExhausted {
		t.Errorf("joblog event = %q, want %q", jobEvent, types.JobEventValidationExhausted)
	}

	// Verify EventBridge event is still published.
	eb := d.EventBridge.(*mockEventBridge)
	if len(eb.events) != 1 {
		t.Fatalf("expected 1 EventBridge call, got %d", len(eb.events))
	}
}

// ---------------------------------------------------------------------------
// Trigger date-arg injection tests
// ---------------------------------------------------------------------------

func TestOrchestrator_Trigger_InjectsHourlyGlueArgs(t *testing.T) {
	pipelineID := "silver-cdr-hour"
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: pipelineID},
		Job: types.JobConfig{
			Type: types.TriggerGlue,
			Config: map[string]interface{}{
				"jobName":   "cdr-agg-hour",
				"arguments": map[string]interface{}{"--s3_bucket": "my-bucket"},
			},
		},
	}

	ddb := &mockDynamo{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: configItem(pipelineID, cfg)}, nil
		},
	}

	executor := &mockTriggerExecutor{
		result: map[string]interface{}{"glue_job_run_id": "jr-100"},
	}

	d := newTestDeps(ddb)
	d.TriggerRunner = executor

	_, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode:       "trigger",
		PipelineID: pipelineID,
		ScheduleID: "stream",
		Date:       "2026-03-03T10",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if executor.cfg == nil || executor.cfg.Glue == nil {
		t.Fatal("expected glue config")
	}
	args := executor.cfg.Glue.Arguments
	if args["--par_day"] != "20260303" {
		t.Errorf("--par_day = %q, want %q", args["--par_day"], "20260303")
	}
	if args["--par_hour"] != "10" {
		t.Errorf("--par_hour = %q, want %q", args["--par_hour"], "10")
	}
	if args["--s3_bucket"] != "my-bucket" {
		t.Errorf("--s3_bucket = %q, want %q", args["--s3_bucket"], "my-bucket")
	}
}

func TestOrchestrator_Trigger_InjectsDailyGlueArgs(t *testing.T) {
	pipelineID := "silver-cdr-day"
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: pipelineID},
		Job: types.JobConfig{
			Type: types.TriggerGlue,
			Config: map[string]interface{}{
				"jobName": "cdr-agg-day",
			},
		},
	}

	ddb := &mockDynamo{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: configItem(pipelineID, cfg)}, nil
		},
	}

	executor := &mockTriggerExecutor{
		result: map[string]interface{}{"glue_job_run_id": "jr-200"},
	}

	d := newTestDeps(ddb)
	d.TriggerRunner = executor

	_, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode:       "trigger",
		PipelineID: pipelineID,
		ScheduleID: "cron",
		Date:       "2026-03-03",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	args := executor.cfg.Glue.Arguments
	if args["--par_day"] != "20260303" {
		t.Errorf("--par_day = %q, want %q", args["--par_day"], "20260303")
	}
	if _, ok := args["--par_hour"]; ok {
		t.Error("--par_hour should not be set for daily pipeline")
	}
}

func TestOrchestrator_Trigger_NonGlueSkipsInjection(t *testing.T) {
	pipelineID := "bronze-cdr"
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: pipelineID},
		Job: types.JobConfig{
			Type: types.TriggerHTTP,
			Config: map[string]interface{}{
				"url":    "https://example.com/audit",
				"method": "POST",
			},
		},
	}

	ddb := &mockDynamo{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: configItem(pipelineID, cfg)}, nil
		},
	}

	executor := &mockTriggerExecutor{
		result: map[string]interface{}{"statusCode": 200},
	}

	d := newTestDeps(ddb)
	d.TriggerRunner = executor

	_, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode:       "trigger",
		PipelineID: pipelineID,
		ScheduleID: "cron",
		Date:       "2026-03-03T10",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !executor.called {
		t.Error("executor not called")
	}
}

// ---------------------------------------------------------------------------
// Trigger-exhausted tests
// ---------------------------------------------------------------------------

func TestOrchestrator_TriggerExhausted(t *testing.T) {
	ddb := newMockDDB()

	// Pre-acquire trigger lock to simulate stream-router behavior.
	ddb.putRaw("control", map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("pipe-1")},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK("stream", "2026-03-01")},
		"status": &ddbtypes.AttributeValueMemberS{Value: types.TriggerStatusRunning},
	})

	d := newTestDeps(ddb)

	out, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode:       "trigger-exhausted",
		PipelineID: "pipe-1",
		ScheduleID: "stream",
		Date:       "2026-03-01",
		ErrorInfo: map[string]interface{}{
			"Error": "States.TaskFailed",
			"Cause": "ConcurrentRunsExceededException: Concurrent runs exceeded.",
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Mode != "trigger-exhausted" {
		t.Errorf("mode = %q, want %q", out.Mode, "trigger-exhausted")
	}
	if out.Status != "exhausted" {
		t.Errorf("status = %q, want %q", out.Status, "exhausted")
	}

	// Verify joblog entry was written (query for JOB# prefix).
	jobItems, _ := ddb.Query(context.Background(), &dynamodb.QueryInput{
		TableName:              strPtr("joblog"),
		KeyConditionExpression: strPtr("PK = :pk AND begins_with(SK, :prefix)"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":pk":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("pipe-1")},
			":prefix": &ddbtypes.AttributeValueMemberS{Value: "JOB#stream#2026-03-01#"},
		},
	})
	if len(jobItems.Items) != 1 {
		t.Fatalf("expected 1 joblog entry, got %d", len(jobItems.Items))
	}
	jobEvent := jobItems.Items[0]["event"].(*ddbtypes.AttributeValueMemberS).Value
	if jobEvent != types.JobEventInfraTriggerExhausted {
		t.Errorf("joblog event = %q, want %q", jobEvent, types.JobEventInfraTriggerExhausted)
	}
	jobError := jobItems.Items[0]["error"].(*ddbtypes.AttributeValueMemberS).Value
	if !strings.Contains(jobError, "ConcurrentRunsExceededException") {
		t.Errorf("joblog error = %q, want it to contain ConcurrentRunsExceededException", jobError)
	}

	// Verify EventBridge event was published.
	eb := d.EventBridge.(*mockEventBridge)
	if len(eb.events) != 1 {
		t.Fatalf("expected 1 EventBridge call, got %d", len(eb.events))
	}

	// Verify trigger lock was released — re-acquire should succeed.
	lockKey := ddbItemKey("control", types.PipelinePK("pipe-1"), types.TriggerSK("stream", "2026-03-01"))
	if _, exists := ddb.items[lockKey]; exists {
		t.Error("trigger lock should be released after exhaustion")
	}
}

func TestOrchestrator_TriggerExhausted_NoErrorInfo(t *testing.T) {
	ddb := newMockDDB()
	d := newTestDeps(ddb)

	out, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode:       "trigger-exhausted",
		PipelineID: "pipe-1",
		ScheduleID: "stream",
		Date:       "2026-03-01",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Status != "exhausted" {
		t.Errorf("status = %q, want %q", out.Status, "exhausted")
	}

	// Joblog should still be written with empty error.
	jobItems, _ := ddb.Query(context.Background(), &dynamodb.QueryInput{
		TableName:              strPtr("joblog"),
		KeyConditionExpression: strPtr("PK = :pk AND begins_with(SK, :prefix)"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":pk":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("pipe-1")},
			":prefix": &ddbtypes.AttributeValueMemberS{Value: "JOB#stream#2026-03-01#"},
		},
	})
	if len(jobItems.Items) != 1 {
		t.Fatalf("expected 1 joblog entry, got %d", len(jobItems.Items))
	}
}

// ---------------------------------------------------------------------------
// Complete-trigger tests
// ---------------------------------------------------------------------------

func TestOrchestrator_CompleteTrigger_Success(t *testing.T) {
	ddb := newMockDDB()

	// Pre-create trigger lock (simulates stream-router acquiring it).
	ddb.putRaw("control", map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("pipe-1")},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK("stream", "2026-03-06T15")},
		"status": &ddbtypes.AttributeValueMemberS{Value: types.TriggerStatusRunning},
	})

	d := newTestDeps(ddb)
	out, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode:       "complete-trigger",
		PipelineID: "pipe-1",
		ScheduleID: "stream",
		Date:       "2026-03-06T15",
		Event:      "success",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Mode != "complete-trigger" {
		t.Errorf("mode = %q, want %q", out.Mode, "complete-trigger")
	}
	if out.Status != types.TriggerStatusCompleted {
		t.Errorf("status = %q, want %q", out.Status, types.TriggerStatusCompleted)
	}

	// Verify trigger row was updated to COMPLETED.
	key := ddbItemKey("control", types.PipelinePK("pipe-1"), types.TriggerSK("stream", "2026-03-06T15"))
	item, exists := ddb.items[key]
	if !exists {
		t.Fatal("trigger row should still exist")
	}
	status := item["status"].(*ddbtypes.AttributeValueMemberS).Value
	if status != types.TriggerStatusCompleted {
		t.Errorf("trigger status = %q, want %q", status, types.TriggerStatusCompleted)
	}
}

func TestOrchestrator_CompleteTrigger_Fail(t *testing.T) {
	ddb := newMockDDB()
	ddb.putRaw("control", map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("pipe-1")},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK("stream", "2026-03-06T15")},
		"status": &ddbtypes.AttributeValueMemberS{Value: types.TriggerStatusRunning},
	})

	d := newTestDeps(ddb)
	out, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode:       "complete-trigger",
		PipelineID: "pipe-1",
		ScheduleID: "stream",
		Date:       "2026-03-06T15",
		Event:      "fail",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Status != types.TriggerStatusFailedFinal {
		t.Errorf("status = %q, want %q", out.Status, types.TriggerStatusFailedFinal)
	}

	key := ddbItemKey("control", types.PipelinePK("pipe-1"), types.TriggerSK("stream", "2026-03-06T15"))
	status := ddb.items[key]["status"].(*ddbtypes.AttributeValueMemberS).Value
	if status != types.TriggerStatusFailedFinal {
		t.Errorf("trigger status = %q, want %q", status, types.TriggerStatusFailedFinal)
	}
}

func TestOrchestrator_CompleteTrigger_Timeout(t *testing.T) {
	ddb := newMockDDB()
	ddb.putRaw("control", map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("pipe-1")},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK("stream", "2026-03-06")},
		"status": &ddbtypes.AttributeValueMemberS{Value: types.TriggerStatusRunning},
	})

	d := newTestDeps(ddb)
	out, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode:       "complete-trigger",
		PipelineID: "pipe-1",
		ScheduleID: "stream",
		Date:       "2026-03-06",
		Event:      "timeout",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Status != types.TriggerStatusFailedFinal {
		t.Errorf("status = %q, want %q", out.Status, types.TriggerStatusFailedFinal)
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

func TestInjectDateArgs_HTTP_HourlyDate(t *testing.T) {
	tc := types.TriggerConfig{
		Type: types.TriggerHTTP,
		HTTP: &types.HTTPTriggerConfig{
			Method: "POST",
			URL:    "https://example.com/audit",
		},
	}
	lambda.InjectDateArgs(&tc, "2026-03-06T16")
	if tc.HTTP.Body == "" {
		t.Fatal("HTTP body not injected")
	}
	var body map[string]string
	if err := json.Unmarshal([]byte(tc.HTTP.Body), &body); err != nil {
		t.Fatalf("invalid JSON body: %v", err)
	}
	if body["par_day"] != "20260306" {
		t.Errorf("par_day = %q, want %q", body["par_day"], "20260306")
	}
	if body["par_hour"] != "16" {
		t.Errorf("par_hour = %q, want %q", body["par_hour"], "16")
	}
}

func TestInjectDateArgs_HTTP_DailyDate(t *testing.T) {
	tc := types.TriggerConfig{
		Type: types.TriggerHTTP,
		HTTP: &types.HTTPTriggerConfig{
			Method: "POST",
			URL:    "https://example.com/audit",
		},
	}
	lambda.InjectDateArgs(&tc, "2026-03-06")
	var body map[string]string
	if err := json.Unmarshal([]byte(tc.HTTP.Body), &body); err != nil {
		t.Fatalf("invalid JSON body: %v", err)
	}
	if body["par_day"] != "20260306" {
		t.Errorf("par_day = %q, want %q", body["par_day"], "20260306")
	}
	if _, hasHour := body["par_hour"]; hasHour {
		t.Error("par_hour should not be present for daily date")
	}
}

func TestInjectDateArgs_HTTP_PreservesExistingBody(t *testing.T) {
	tc := types.TriggerConfig{
		Type: types.TriggerHTTP,
		HTTP: &types.HTTPTriggerConfig{
			Method: "POST",
			URL:    "https://example.com/custom",
			Body:   `{"custom": "payload"}`,
		},
	}
	lambda.InjectDateArgs(&tc, "2026-03-06T16")
	if tc.HTTP.Body != `{"custom": "payload"}` {
		t.Errorf("existing body overwritten: %q", tc.HTTP.Body)
	}
}
