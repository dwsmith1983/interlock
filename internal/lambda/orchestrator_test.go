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

// ---------------------------------------------------------------------------
// InjectDateArgs — additional coverage
// ---------------------------------------------------------------------------

func TestInjectDateArgs_GlueOnly(t *testing.T) {
	tc := types.TriggerConfig{
		Type: types.TriggerGlue,
		Glue: &types.GlueTriggerConfig{
			JobName:   "etl-job",
			Arguments: map[string]string{"--s3_bucket": "bucket"},
		},
	}
	lambda.InjectDateArgs(&tc, "2026-03-03T10")
	if tc.Glue.Arguments["--par_day"] != "20260303" {
		t.Errorf("--par_day = %q, want %q", tc.Glue.Arguments["--par_day"], "20260303")
	}
	if tc.Glue.Arguments["--par_hour"] != "10" {
		t.Errorf("--par_hour = %q, want %q", tc.Glue.Arguments["--par_hour"], "10")
	}
	// HTTP is nil — should not panic.
}

func TestInjectDateArgs_BothGlueAndHTTP(t *testing.T) {
	tc := types.TriggerConfig{
		Type: types.TriggerGlue,
		Glue: &types.GlueTriggerConfig{JobName: "etl-job"},
		HTTP: &types.HTTPTriggerConfig{Method: "POST", URL: "https://example.com/hook"},
	}
	lambda.InjectDateArgs(&tc, "2026-03-05T14")
	if tc.Glue.Arguments["--par_day"] != "20260305" {
		t.Errorf("glue --par_day = %q, want %q", tc.Glue.Arguments["--par_day"], "20260305")
	}
	if tc.Glue.Arguments["--par_hour"] != "14" {
		t.Errorf("glue --par_hour = %q, want %q", tc.Glue.Arguments["--par_hour"], "14")
	}
	var body map[string]string
	if err := json.Unmarshal([]byte(tc.HTTP.Body), &body); err != nil {
		t.Fatalf("invalid JSON body: %v", err)
	}
	if body["par_day"] != "20260305" {
		t.Errorf("http par_day = %q, want %q", body["par_day"], "20260305")
	}
	if body["par_hour"] != "14" {
		t.Errorf("http par_hour = %q, want %q", body["par_hour"], "14")
	}
}

func TestInjectDateArgs_NeitherGlueNorHTTP(t *testing.T) {
	tc := types.TriggerConfig{Type: types.TriggerCommand}
	// Neither Glue nor HTTP set — should be a no-op without panic.
	lambda.InjectDateArgs(&tc, "2026-03-05")
	if tc.Glue != nil {
		t.Error("Glue should remain nil")
	}
	if tc.HTTP != nil {
		t.Error("HTTP should remain nil")
	}
}

func TestInjectDateArgs_GlueNilArguments(t *testing.T) {
	tc := types.TriggerConfig{
		Type: types.TriggerGlue,
		Glue: &types.GlueTriggerConfig{JobName: "etl-job"},
	}
	lambda.InjectDateArgs(&tc, "2026-03-03")
	if tc.Glue.Arguments == nil {
		t.Fatal("Arguments should be created when nil")
	}
	if tc.Glue.Arguments["--par_day"] != "20260303" {
		t.Errorf("--par_day = %q, want %q", tc.Glue.Arguments["--par_day"], "20260303")
	}
	if _, ok := tc.Glue.Arguments["--par_hour"]; ok {
		t.Error("--par_hour should not be set for daily date")
	}
}

// ---------------------------------------------------------------------------
// extractFloat — tested indirectly through post-run with audit-result values
// ---------------------------------------------------------------------------

func TestExtractFloat_Float64(t *testing.T) {
	// Sensor with sensor_count stored as DynamoDB Number (float64 after JSON unmarshal).
	pipelineID := "ef-float64"
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: pipelineID},
		PostRun: &types.PostRunConfig{
			Rules: []types.ValidationRule{
				{Key: "audit-result", Check: types.CheckExists},
			},
		},
	}

	ddb := newMockDDB()
	ddb.putRaw("control", configItem(pipelineID, cfg))
	ddb.putRaw("control", sensorItem(pipelineID, "audit-result", map[string]ddbtypes.AttributeValue{
		"sensor_count": &ddbtypes.AttributeValueMemberN{Value: "42.5"},
	}))
	// Baseline with different float count to trigger drift detection.
	ddb.putRaw("control", sensorItem(pipelineID, "postrun-baseline", map[string]ddbtypes.AttributeValue{
		"sensor_count": &ddbtypes.AttributeValueMemberN{Value: "100"},
	}))

	d := newTestDeps(ddb)
	out, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode: "post-run", PipelineID: pipelineID, ScheduleID: "daily", Date: "2026-03-01",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// 42.5 != 100 → drift detected. This confirms extractFloat correctly
	// parsed the float64 value from the sensor data map.
	if out.Status != "drift" {
		t.Errorf("status = %q, want %q — extractFloat should parse float64", out.Status, "drift")
	}
}

func TestExtractFloat_String(t *testing.T) {
	// Sensor with sensor_count stored as a string "3.14".
	pipelineID := "ef-string"
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: pipelineID},
		PostRun: &types.PostRunConfig{
			Rules: []types.ValidationRule{
				{Key: "audit-result", Check: types.CheckExists},
			},
		},
	}

	ddb := newMockDDB()
	ddb.putRaw("control", configItem(pipelineID, cfg))
	ddb.putRaw("control", sensorItem(pipelineID, "audit-result", map[string]ddbtypes.AttributeValue{
		"sensor_count": &ddbtypes.AttributeValueMemberS{Value: "3.14"},
	}))
	ddb.putRaw("control", sensorItem(pipelineID, "postrun-baseline", map[string]ddbtypes.AttributeValue{
		"sensor_count": &ddbtypes.AttributeValueMemberN{Value: "100"},
	}))

	d := newTestDeps(ddb)
	out, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode: "post-run", PipelineID: pipelineID, ScheduleID: "daily", Date: "2026-03-01",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// 3.14 != 100 → drift. Confirms extractFloat parses string values.
	if out.Status != "drift" {
		t.Errorf("status = %q, want %q — extractFloat should parse string", out.Status, "drift")
	}
}

func TestExtractFloat_InvalidString(t *testing.T) {
	// Sensor with sensor_count = "notanumber" → extractFloat returns 0 → skips drift.
	pipelineID := "ef-invalid"
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: pipelineID},
		PostRun: &types.PostRunConfig{
			Rules: []types.ValidationRule{
				{Key: "audit-result", Check: types.CheckExists},
			},
		},
	}

	ddb := newMockDDB()
	ddb.putRaw("control", configItem(pipelineID, cfg))
	ddb.putRaw("control", sensorItem(pipelineID, "audit-result", map[string]ddbtypes.AttributeValue{
		"sensor_count": &ddbtypes.AttributeValueMemberS{Value: "notanumber"},
	}))
	ddb.putRaw("control", sensorItem(pipelineID, "postrun-baseline", map[string]ddbtypes.AttributeValue{
		"sensor_count": &ddbtypes.AttributeValueMemberN{Value: "100"},
	}))

	d := newTestDeps(ddb)
	out, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode: "post-run", PipelineID: pipelineID, ScheduleID: "daily", Date: "2026-03-01",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// currCount=0 (invalid string) → skips drift check → evaluates rules normally.
	if out.Status == "drift" {
		t.Error("status = drift, but invalid string should yield 0 and skip drift check")
	}
}

func TestExtractFloat_MissingKey(t *testing.T) {
	// Sensor without "sensor_count" field → extractFloat returns 0 → skips drift.
	pipelineID := "ef-missing"
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: pipelineID},
		PostRun: &types.PostRunConfig{
			Rules: []types.ValidationRule{
				{Key: "audit-result", Check: types.CheckExists},
			},
		},
	}

	ddb := newMockDDB()
	ddb.putRaw("control", configItem(pipelineID, cfg))
	ddb.putRaw("control", sensorItem(pipelineID, "audit-result", map[string]ddbtypes.AttributeValue{
		"other_field": &ddbtypes.AttributeValueMemberS{Value: "hello"},
	}))
	ddb.putRaw("control", sensorItem(pipelineID, "postrun-baseline", map[string]ddbtypes.AttributeValue{
		"sensor_count": &ddbtypes.AttributeValueMemberN{Value: "100"},
	}))

	d := newTestDeps(ddb)
	out, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode: "post-run", PipelineID: pipelineID, ScheduleID: "daily", Date: "2026-03-01",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Missing key → extractFloat returns 0 → currCount=0 → skips drift check.
	if out.Status == "drift" {
		t.Error("status = drift, but missing sensor_count should yield 0 and skip drift check")
	}
}

func TestExtractFloat_UnsupportedType(t *testing.T) {
	// Sensor with sensor_count stored as a BOOL → extractFloat returns 0 → skips drift.
	pipelineID := "ef-unsupported"
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: pipelineID},
		PostRun: &types.PostRunConfig{
			Rules: []types.ValidationRule{
				{Key: "audit-result", Check: types.CheckExists},
			},
		},
	}

	ddb := newMockDDB()
	ddb.putRaw("control", configItem(pipelineID, cfg))
	ddb.putRaw("control", sensorItem(pipelineID, "audit-result", map[string]ddbtypes.AttributeValue{
		"sensor_count": &ddbtypes.AttributeValueMemberBOOL{Value: true},
	}))
	ddb.putRaw("control", sensorItem(pipelineID, "postrun-baseline", map[string]ddbtypes.AttributeValue{
		"sensor_count": &ddbtypes.AttributeValueMemberN{Value: "100"},
	}))

	d := newTestDeps(ddb)
	out, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode: "post-run", PipelineID: pipelineID, ScheduleID: "daily", Date: "2026-03-01",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Bool type → extractFloat returns 0 → currCount=0 → skips drift check.
	if out.Status == "drift" {
		t.Error("status = drift, but unsupported type (bool) should yield 0 and skip drift check")
	}
}

// ---------------------------------------------------------------------------
// handlePostRun — additional branch coverage
// ---------------------------------------------------------------------------

func TestPostRun_BaselineWriteError(t *testing.T) {
	// WriteSensor fails on first iteration → logged but doesn't abort.
	pipelineID := "pr-baseline-err"
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: pipelineID},
		PostRun: &types.PostRunConfig{
			Rules: []types.ValidationRule{
				{Key: "audit-result", Check: types.CheckExists},
			},
		},
	}

	// Use mockDynamo (not newMockDDB) so we can inject a PutItem error
	// specifically for the baseline sensor write.
	var putCount int
	ddb := &mockDynamo{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: configItem(pipelineID, cfg)}, nil
		},
		queryFn: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			// Return audit-result but NO postrun-baseline → triggers baseline write.
			return &dynamodb.QueryOutput{
				Items: []map[string]ddbtypes.AttributeValue{
					sensorItem(pipelineID, "audit-result", map[string]ddbtypes.AttributeValue{
						"sensor_count": &ddbtypes.AttributeValueMemberN{Value: "100"},
					}),
				},
			}, nil
		},
		putItemFn: func(_ context.Context, _ *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			putCount++
			return nil, fmt.Errorf("dynamodb: provisioned throughput exceeded")
		},
	}

	d := newTestDeps(ddb)
	out, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode: "post-run", PipelineID: pipelineID, ScheduleID: "daily", Date: "2026-03-01",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Baseline write failed, but rules still evaluated.
	if out.Mode != "post-run" {
		t.Errorf("mode = %q, want %q", out.Mode, "post-run")
	}
	// Should not be an error — the baseline write failure is non-fatal.
	if out.Error != "" {
		t.Errorf("unexpected error in output: %q", out.Error)
	}
}

func TestPostRun_DriftWriteRerunError(t *testing.T) {
	// WriteRerunRequest fails on drift → logged non-fatally, still returns "drift".
	pipelineID := "pr-rerun-err"
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: pipelineID},
		PostRun: &types.PostRunConfig{
			Rules: []types.ValidationRule{
				{Key: "audit-result", Check: types.CheckExists},
			},
		},
	}

	var putCallCount int
	ddb := &mockDynamo{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: configItem(pipelineID, cfg)}, nil
		},
		queryFn: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return &dynamodb.QueryOutput{
				Items: []map[string]ddbtypes.AttributeValue{
					sensorItem(pipelineID, "audit-result", map[string]ddbtypes.AttributeValue{
						"sensor_count": &ddbtypes.AttributeValueMemberN{Value: "150"},
					}),
					sensorItem(pipelineID, "postrun-baseline", map[string]ddbtypes.AttributeValue{
						"sensor_count": &ddbtypes.AttributeValueMemberN{Value: "100"},
					}),
				},
			}, nil
		},
		putItemFn: func(_ context.Context, _ *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			putCallCount++
			return nil, fmt.Errorf("dynamodb: write rerun request failed")
		},
	}

	d := newTestDeps(ddb)
	out, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode: "post-run", PipelineID: pipelineID, ScheduleID: "daily", Date: "2026-03-01",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Status != "drift" {
		t.Errorf("status = %q, want %q — rerun write failure should not change result", out.Status, "drift")
	}
}

func TestPostRun_ZeroPrevCount_NoDrift(t *testing.T) {
	pipelineID := "pr-zero-prev"
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: pipelineID},
		PostRun: &types.PostRunConfig{
			Rules: []types.ValidationRule{
				{Key: "audit-result", Check: types.CheckExists},
			},
		},
	}

	ddb := newMockDDB()
	ddb.putRaw("control", configItem(pipelineID, cfg))
	ddb.putRaw("control", sensorItem(pipelineID, "audit-result", map[string]ddbtypes.AttributeValue{
		"sensor_count": &ddbtypes.AttributeValueMemberN{Value: "100"},
	}))
	// Baseline with sensor_count=0.
	ddb.putRaw("control", sensorItem(pipelineID, "postrun-baseline", map[string]ddbtypes.AttributeValue{
		"sensor_count": &ddbtypes.AttributeValueMemberN{Value: "0"},
	}))

	d := newTestDeps(ddb)
	out, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode: "post-run", PipelineID: pipelineID, ScheduleID: "daily", Date: "2026-03-01",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// prevCount=0 → skips drift check (prevCount <= 0 guard).
	if out.Status == "drift" {
		t.Error("status = drift, but prevCount=0 should skip drift check")
	}
}

func TestPostRun_ZeroCurrCount_NoDrift(t *testing.T) {
	pipelineID := "pr-zero-curr"
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: pipelineID},
		PostRun: &types.PostRunConfig{
			Rules: []types.ValidationRule{
				{Key: "audit-result", Check: types.CheckExists},
			},
		},
	}

	ddb := newMockDDB()
	ddb.putRaw("control", configItem(pipelineID, cfg))
	// audit-result with sensor_count=0.
	ddb.putRaw("control", sensorItem(pipelineID, "audit-result", map[string]ddbtypes.AttributeValue{
		"sensor_count": &ddbtypes.AttributeValueMemberN{Value: "0"},
	}))
	ddb.putRaw("control", sensorItem(pipelineID, "postrun-baseline", map[string]ddbtypes.AttributeValue{
		"sensor_count": &ddbtypes.AttributeValueMemberN{Value: "100"},
	}))

	d := newTestDeps(ddb)
	out, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode: "post-run", PipelineID: pipelineID, ScheduleID: "daily", Date: "2026-03-01",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// currCount=0 → skips drift check.
	if out.Status == "drift" {
		t.Error("status = drift, but currCount=0 should skip drift check")
	}
}

func TestPostRun_SameCounts_NoDrift(t *testing.T) {
	pipelineID := "pr-same-counts"
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: pipelineID},
		PostRun: &types.PostRunConfig{
			Rules: []types.ValidationRule{
				{Key: "audit-result", Check: types.CheckExists},
			},
		},
	}

	ddb := newMockDDB()
	ddb.putRaw("control", configItem(pipelineID, cfg))
	ddb.putRaw("control", sensorItem(pipelineID, "audit-result", map[string]ddbtypes.AttributeValue{
		"sensor_count": &ddbtypes.AttributeValueMemberN{Value: "100"},
	}))
	ddb.putRaw("control", sensorItem(pipelineID, "postrun-baseline", map[string]ddbtypes.AttributeValue{
		"sensor_count": &ddbtypes.AttributeValueMemberN{Value: "100"},
	}))

	d := newTestDeps(ddb)
	out, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode: "post-run", PipelineID: pipelineID, ScheduleID: "daily", Date: "2026-03-01",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Same counts → no drift, evaluates rules normally.
	if out.Status == "drift" {
		t.Error("status = drift, but same counts should not trigger drift")
	}
}

func TestPostRun_NegativeDrift(t *testing.T) {
	// Current count < baseline count → still drift (currCount != prevCount).
	pipelineID := "pr-neg-drift"
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: pipelineID},
		PostRun: &types.PostRunConfig{
			Rules: []types.ValidationRule{
				{Key: "audit-result", Check: types.CheckExists},
			},
		},
	}

	ddb := newMockDDB()
	ddb.putRaw("control", configItem(pipelineID, cfg))
	ddb.putRaw("control", sensorItem(pipelineID, "audit-result", map[string]ddbtypes.AttributeValue{
		"sensor_count": &ddbtypes.AttributeValueMemberN{Value: "80"},
	}))
	ddb.putRaw("control", sensorItem(pipelineID, "postrun-baseline", map[string]ddbtypes.AttributeValue{
		"sensor_count": &ddbtypes.AttributeValueMemberN{Value: "100"},
	}))

	d := newTestDeps(ddb)
	out, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode: "post-run", PipelineID: pipelineID, ScheduleID: "daily", Date: "2026-03-01",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Status != "drift" {
		t.Errorf("status = %q, want %q — negative drift (80 < 100) should still trigger drift", out.Status, "drift")
	}
}

func TestPostRun_GetAllSensorsError(t *testing.T) {
	pipelineID := "pr-sensor-err"
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: pipelineID},
		PostRun: &types.PostRunConfig{
			Rules: []types.ValidationRule{
				{Key: "audit-result", Check: types.CheckExists},
			},
		},
	}

	ddb := &mockDynamo{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: configItem(pipelineID, cfg)}, nil
		},
		queryFn: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return nil, fmt.Errorf("dynamodb: request limit exceeded")
		},
	}

	d := newTestDeps(ddb)
	out, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode: "post-run", PipelineID: pipelineID, ScheduleID: "daily", Date: "2026-03-01",
	})
	if err != nil {
		t.Fatalf("unexpected Lambda error: %v", err)
	}
	if out.Error == "" {
		t.Error("expected error in output when GetAllSensors fails")
	}
}

func TestPostRun_RulesNotReady(t *testing.T) {
	pipelineID := "pr-not-ready"
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: pipelineID},
		PostRun: &types.PostRunConfig{
			Rules: []types.ValidationRule{
				{Key: "audit-result", Check: types.CheckGTE, Field: "count", Value: float64(1000)},
			},
		},
	}

	ddb := newMockDDB()
	ddb.putRaw("control", configItem(pipelineID, cfg))
	ddb.putRaw("control", sensorItem(pipelineID, "audit-result", map[string]ddbtypes.AttributeValue{
		"count": &ddbtypes.AttributeValueMemberN{Value: "50"},
	}))

	d := newTestDeps(ddb)
	out, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode: "post-run", PipelineID: pipelineID, ScheduleID: "daily", Date: "2026-03-01",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Status != "not_ready" {
		t.Errorf("status = %q, want %q — count 50 < 1000 should not pass", out.Status, "not_ready")
	}
}

// ---------------------------------------------------------------------------
// handleCheckJob — additional branch coverage
// ---------------------------------------------------------------------------

func TestCheckJob_StatusCheckError(t *testing.T) {
	pipelineID := "cj-status-err"
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
		err: fmt.Errorf("glue: access denied"),
	}

	d := newTestDeps(ddb)
	d.StatusChecker = checker

	out, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode:       "check-job",
		PipelineID: pipelineID,
		ScheduleID: "daily",
		Date:       "2026-03-01",
		Metadata:   map[string]interface{}{"glue_job_run_id": "jr-999"},
	})
	if err != nil {
		t.Fatalf("unexpected Lambda error: %v", err)
	}
	// StatusChecker error → empty event returned (treated as "still running").
	if out.Event != "" {
		t.Errorf("event = %q, want empty when status check fails", out.Event)
	}
}

func TestCheckJob_GetConfigError(t *testing.T) {
	ddb := &mockDynamo{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return nil, fmt.Errorf("dynamodb: internal error")
		},
		queryFn: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return &dynamodb.QueryOutput{Items: nil}, nil
		},
	}

	checker := &mockStatusChecker{}
	d := newTestDeps(ddb)
	d.StatusChecker = checker

	out, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode:       "check-job",
		PipelineID: "any-pipe",
		ScheduleID: "daily",
		Date:       "2026-03-01",
		Metadata:   map[string]interface{}{"runId": "abc"},
	})
	if err != nil {
		t.Fatalf("unexpected Lambda error: %v", err)
	}
	if out.Error == "" {
		t.Error("expected error in output when GetConfig fails")
	}
}

func TestCheckJob_ConfigNilWithStatusChecker(t *testing.T) {
	// Config not found but StatusChecker is set — returns empty event (no error).
	ddb := &mockDynamo{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: nil}, nil
		},
		queryFn: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return &dynamodb.QueryOutput{Items: nil}, nil
		},
	}

	checker := &mockStatusChecker{}
	d := newTestDeps(ddb)
	d.StatusChecker = checker

	out, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode:       "check-job",
		PipelineID: "missing-pipe",
		ScheduleID: "daily",
		Date:       "2026-03-01",
		Metadata:   map[string]interface{}{"runId": "abc"},
	})
	if err != nil {
		t.Fatalf("unexpected Lambda error: %v", err)
	}
	if out.Event != "" {
		t.Errorf("event = %q, want empty when config not found", out.Event)
	}
}

func TestCheckJob_FailedJobEvent(t *testing.T) {
	pipelineID := "cj-fail"

	ddb := &mockDynamo{
		queryFn: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return &dynamodb.QueryOutput{
				Items: []map[string]ddbtypes.AttributeValue{
					jobItem(pipelineID, "daily", "2026-03-01", "fail"),
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
	if out.Event != "fail" {
		t.Errorf("event = %q, want %q", out.Event, "fail")
	}
}

func TestCheckJob_TimeoutJobEvent(t *testing.T) {
	pipelineID := "cj-timeout"

	ddb := &mockDynamo{
		queryFn: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return &dynamodb.QueryOutput{
				Items: []map[string]ddbtypes.AttributeValue{
					jobItem(pipelineID, "daily", "2026-03-01", "timeout"),
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
	if out.Event != "timeout" {
		t.Errorf("event = %q, want %q", out.Event, "timeout")
	}
}

func TestCheckJob_NilStatusCheckerAndNoMetadata(t *testing.T) {
	// No terminal joblog, nil StatusChecker, no metadata → empty event.
	ddb := &mockDynamo{
		queryFn: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return &dynamodb.QueryOutput{Items: nil}, nil
		},
	}

	d := newTestDeps(ddb)
	// d.StatusChecker is nil by default.

	out, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode:       "check-job",
		PipelineID: "any-pipe",
		ScheduleID: "daily",
		Date:       "2026-03-01",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Event != "" {
		t.Errorf("event = %q, want empty when no StatusChecker and no metadata", out.Event)
	}
}

func TestCheckJob_GetLatestJobEventError(t *testing.T) {
	ddb := &mockDynamo{
		queryFn: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return nil, fmt.Errorf("dynamodb: query error")
		},
	}

	d := newTestDeps(ddb)
	out, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode:       "check-job",
		PipelineID: "any-pipe",
		ScheduleID: "daily",
		Date:       "2026-03-01",
	})
	if err != nil {
		t.Fatalf("unexpected Lambda error: %v", err)
	}
	if out.Error == "" {
		t.Error("expected error in output when GetLatestJobEvent fails")
	}
}

// ---------------------------------------------------------------------------
// handleCompleteTrigger — additional branch coverage
// ---------------------------------------------------------------------------

// updateItemFailingDDB embeds mockDynamo but overrides UpdateItem to always fail.
type updateItemFailingDDB struct {
	*mockDynamo
}

func (u *updateItemFailingDDB) UpdateItem(_ context.Context, _ *dynamodb.UpdateItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	return nil, fmt.Errorf("dynamodb: conditional check failed")
}

func TestCompleteTrigger_SetStatusError(t *testing.T) {
	// SetTriggerStatus calls UpdateItem. Override to return an error.
	baseDDB := &mockDynamo{}
	failDDB := &updateItemFailingDDB{mockDynamo: baseDDB}

	d := newTestDeps(failDDB)
	_, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode:       "complete-trigger",
		PipelineID: "pipe-1",
		ScheduleID: "stream",
		Date:       "2026-03-06",
		Event:      "success",
	})
	if err == nil {
		t.Fatal("expected Lambda error when SetTriggerStatus fails")
	}
	if !strings.Contains(err.Error(), "set trigger status") {
		t.Errorf("error = %q, want it to contain 'set trigger status'", err.Error())
	}
}

// ---------------------------------------------------------------------------
// handleTriggerExhausted — additional branch coverage
// ---------------------------------------------------------------------------

func TestTriggerExhausted_WriteJobEventError(t *testing.T) {
	// WriteJobEvent fails → returns Lambda error (not a payload error).
	ddb := &mockDynamo{
		putItemFn: func(_ context.Context, _ *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			return nil, fmt.Errorf("dynamodb: write capacity exceeded")
		},
	}

	d := newTestDeps(ddb)
	_, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode:       "trigger-exhausted",
		PipelineID: "pipe-1",
		ScheduleID: "stream",
		Date:       "2026-03-01",
		ErrorInfo: map[string]interface{}{
			"Error": "States.TaskFailed",
			"Cause": "some cause",
		},
	})
	if err == nil {
		t.Fatal("expected Lambda error when WriteJobEvent fails")
	}
	if !strings.Contains(err.Error(), "write trigger-exhausted joblog") {
		t.Errorf("error = %q, want it to contain 'write trigger-exhausted joblog'", err.Error())
	}
}

// ---------------------------------------------------------------------------
// handleValidationExhausted — additional branch coverage
// ---------------------------------------------------------------------------

func TestValidationExhausted_WriteJobEventError(t *testing.T) {
	ddb := &mockDynamo{
		putItemFn: func(_ context.Context, _ *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			return nil, fmt.Errorf("dynamodb: write capacity exceeded")
		},
	}

	d := newTestDeps(ddb)
	_, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode:       "validation-exhausted",
		PipelineID: "pipe-1",
		ScheduleID: "daily",
		Date:       "2026-03-01",
	})
	if err == nil {
		t.Fatal("expected Lambda error when WriteJobEvent fails")
	}
	if !strings.Contains(err.Error(), "write validation-exhausted joblog") {
		t.Errorf("error = %q, want it to contain 'write validation-exhausted joblog'", err.Error())
	}
}

// ---------------------------------------------------------------------------
// remapPerPeriodSensors — tested indirectly through evaluate mode
// ---------------------------------------------------------------------------

func TestRemapPerPeriod_EmptyDate(t *testing.T) {
	// Empty date → no remapping. Sensor key with suffix should not be accessible as base.
	pipelineID := "remap-empty"
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: pipelineID},
		Validation: types.ValidationConfig{
			Trigger: "ALL",
			Rules: []types.ValidationRule{
				{Key: "hourly-status", Check: types.CheckExists},
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
					sensorItem(pipelineID, "hourly-status#20260303T07", map[string]ddbtypes.AttributeValue{
						"complete": &ddbtypes.AttributeValueMemberBOOL{Value: true},
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
		Date:       "", // Empty date.
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Without remapping, "hourly-status" key won't exist → rule fails.
	if out.Status == "passed" {
		t.Error("status = passed, but empty date should prevent remapping")
	}
}

func TestRemapPerPeriod_NoMatch(t *testing.T) {
	// Sensor key does not match date suffix → no remapping.
	pipelineID := "remap-nomatch"
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: pipelineID},
		Validation: types.ValidationConfig{
			Trigger: "ALL",
			Rules: []types.ValidationRule{
				{Key: "hourly-status", Check: types.CheckExists},
			},
		},
	}

	ddb := &mockDynamo{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: configItem(pipelineID, cfg)}, nil
		},
		queryFn: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			// Sensor for a different hour.
			return &dynamodb.QueryOutput{
				Items: []map[string]ddbtypes.AttributeValue{
					sensorItem(pipelineID, "hourly-status#20260303T08", map[string]ddbtypes.AttributeValue{
						"complete": &ddbtypes.AttributeValueMemberBOOL{Value: true},
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
		Date:       "2026-03-03T07", // Looking for T07, sensor is T08.
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Status == "passed" {
		t.Error("status = passed, but sensor suffix doesn't match date")
	}
}

func TestRemapPerPeriod_BaseKeyExists(t *testing.T) {
	// If base key "hourly-status" already exists, don't overwrite with per-period sensor.
	pipelineID := "remap-base-exists"
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: pipelineID},
		Validation: types.ValidationConfig{
			Trigger: "ALL",
			Rules: []types.ValidationRule{
				{Key: "hourly-status", Check: types.CheckEquals, Field: "source", Value: "base"},
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
					// Base key already exists with source="base".
					sensorItem(pipelineID, "hourly-status", map[string]ddbtypes.AttributeValue{
						"source": &ddbtypes.AttributeValueMemberS{Value: "base"},
					}),
					// Per-period key with source="per-period".
					sensorItem(pipelineID, "hourly-status#20260303T07", map[string]ddbtypes.AttributeValue{
						"source": &ddbtypes.AttributeValueMemberS{Value: "per-period"},
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
	// Rule checks for source="base" — the base key should NOT be overwritten.
	if out.Status != "passed" {
		t.Errorf("status = %q, want %q — base key should not be overwritten by per-period sensor", out.Status, "passed")
	}
}

func TestRemapPerPeriod_CompactDate(t *testing.T) {
	// Sensor with compact date suffix (no hyphens) matches compact form of normalized date.
	pipelineID := "remap-compact"
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: pipelineID},
		Validation: types.ValidationConfig{
			Trigger: "ALL",
			Rules: []types.ValidationRule{
				{Key: "hourly-status", Check: types.CheckExists},
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
					sensorItem(pipelineID, "hourly-status#20260303T07", map[string]ddbtypes.AttributeValue{
						"complete": &ddbtypes.AttributeValueMemberBOOL{Value: true},
					}),
				},
			}, nil
		},
	}

	d := newTestDeps(ddb)
	// Use compact date form directly — tests compact suffix matching.
	out, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode:       "evaluate",
		PipelineID: pipelineID,
		ScheduleID: "stream",
		Date:       "20260303T07",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Status != "passed" {
		t.Errorf("status = %q, want %q — compact date suffix should match", out.Status, "passed")
	}
}

// ---------------------------------------------------------------------------
// buildTriggerConfig — tested indirectly through trigger mode
// ---------------------------------------------------------------------------

func TestBuildTriggerConfig_HTTP(t *testing.T) {
	pipelineID := "btc-http"
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: pipelineID},
		Job: types.JobConfig{
			Type: types.TriggerHTTP,
			Config: map[string]interface{}{
				"url":    "https://example.com/run",
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
		result: map[string]interface{}{"statusCode": float64(200)},
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
	if out.JobType != "http" {
		t.Errorf("jobType = %q, want %q", out.JobType, "http")
	}
	if !executor.called {
		t.Error("executor not called")
	}
	if executor.cfg == nil || executor.cfg.HTTP == nil {
		t.Fatal("expected HTTP config to be populated")
	}
	if executor.cfg.HTTP.URL != "https://example.com/run" {
		t.Errorf("url = %q, want %q", executor.cfg.HTTP.URL, "https://example.com/run")
	}
}

func TestBuildTriggerConfig_Command(t *testing.T) {
	pipelineID := "btc-cmd"
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: pipelineID},
		Job: types.JobConfig{
			Type: types.TriggerCommand,
			Config: map[string]interface{}{
				"command": "python3 /opt/scripts/etl.py",
			},
		},
	}

	ddb := &mockDynamo{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: configItem(pipelineID, cfg)}, nil
		},
	}

	executor := &mockTriggerExecutor{
		result: map[string]interface{}{"exitCode": float64(0)},
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
	if out.JobType != "command" {
		t.Errorf("jobType = %q, want %q", out.JobType, "command")
	}
	if executor.cfg == nil || executor.cfg.Command == nil {
		t.Fatal("expected Command config to be populated")
	}
	if executor.cfg.Command.Command != "python3 /opt/scripts/etl.py" {
		t.Errorf("command = %q, want %q", executor.cfg.Command.Command, "python3 /opt/scripts/etl.py")
	}
}

func TestBuildTriggerConfig_Unknown(t *testing.T) {
	pipelineID := "btc-unknown"
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: pipelineID},
		Job: types.JobConfig{
			Type:   "invalid-type",
			Config: map[string]interface{}{"key": "value"},
		},
	}

	ddb := &mockDynamo{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: configItem(pipelineID, cfg)}, nil
		},
	}

	d := newTestDeps(ddb)
	d.TriggerRunner = &mockTriggerExecutor{}

	out, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode:       "trigger",
		PipelineID: pipelineID,
		ScheduleID: "daily",
		Date:       "2026-03-01",
	})
	if err != nil {
		t.Fatalf("config errors should be payload errors, not Lambda errors: %v", err)
	}
	if out.Error == "" {
		t.Error("expected error in output for unsupported trigger type")
	}
	if !strings.Contains(out.Error, "unsupported trigger type") {
		t.Errorf("error = %q, want it to contain 'unsupported trigger type'", out.Error)
	}
}

func TestBuildTriggerConfig_EmptyConfig(t *testing.T) {
	// Empty job.Config map → returns TriggerConfig with type set but no sub-config.
	pipelineID := "btc-empty"
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: pipelineID},
		Job: types.JobConfig{
			Type: types.TriggerGlue,
			// Config is nil/empty.
		},
	}

	ddb := &mockDynamo{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: configItem(pipelineID, cfg)}, nil
		},
	}

	executor := &mockTriggerExecutor{
		result: map[string]interface{}{"glue_job_run_id": "jr-empty"},
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
	if out.JobType != "glue" {
		t.Errorf("jobType = %q, want %q", out.JobType, "glue")
	}
	// With empty config, Glue sub-config should be nil.
	if executor.cfg.Glue != nil {
		t.Error("Glue config should be nil when job.Config is empty")
	}
}

// ---------------------------------------------------------------------------
// extractRunID — tested indirectly through trigger mode
// ---------------------------------------------------------------------------

func TestExtractRunID_RunIdKey(t *testing.T) {
	pipelineID := "eri-runid"
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: pipelineID},
		Job: types.JobConfig{
			Type: types.TriggerHTTP,
			Config: map[string]interface{}{
				"url":    "https://example.com/trigger",
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
		result: map[string]interface{}{"runId": "abc-123"},
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
	if out.RunID != "abc-123" {
		t.Errorf("runID = %q, want %q", out.RunID, "abc-123")
	}
}

func TestExtractRunID_NilMetadata(t *testing.T) {
	pipelineID := "eri-nil"
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: pipelineID},
		Job: types.JobConfig{
			Type: types.TriggerHTTP,
			Config: map[string]interface{}{
				"url":    "https://example.com/trigger",
				"method": "POST",
			},
		},
	}

	ddb := &mockDynamo{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: configItem(pipelineID, cfg)}, nil
		},
	}

	// Execute returns nil metadata.
	executor := &mockTriggerExecutor{
		result: nil,
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
	if out.RunID != "" {
		t.Errorf("runID = %q, want empty when metadata is nil", out.RunID)
	}
}

func TestExtractRunID_NoMatchingKey(t *testing.T) {
	pipelineID := "eri-nomatch"
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: pipelineID},
		Job: types.JobConfig{
			Type: types.TriggerHTTP,
			Config: map[string]interface{}{
				"url":    "https://example.com/trigger",
				"method": "POST",
			},
		},
	}

	ddb := &mockDynamo{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: configItem(pipelineID, cfg)}, nil
		},
	}

	// Metadata has no recognized run ID keys.
	executor := &mockTriggerExecutor{
		result: map[string]interface{}{"statusCode": float64(200), "responseBody": "OK"},
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
	if out.RunID != "" {
		t.Errorf("runID = %q, want empty when no matching metadata key", out.RunID)
	}
}

func TestExtractRunID_EmptyString(t *testing.T) {
	// metadata["runId"]="" → should skip to next key.
	pipelineID := "eri-empty"
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: pipelineID},
		Job: types.JobConfig{
			Type: types.TriggerHTTP,
			Config: map[string]interface{}{
				"url":    "https://example.com/trigger",
				"method": "POST",
			},
		},
	}

	ddb := &mockDynamo{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: configItem(pipelineID, cfg)}, nil
		},
	}

	// runId is empty string, but glue_job_run_id has a value.
	executor := &mockTriggerExecutor{
		result: map[string]interface{}{"runId": "", "glue_job_run_id": "jr-fallback"},
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
	// Empty runId should be skipped, falls through to glue_job_run_id.
	if out.RunID != "jr-fallback" {
		t.Errorf("runID = %q, want %q — empty runId should fall through", out.RunID, "jr-fallback")
	}
}

func TestExtractRunID_ExecutionArnKey(t *testing.T) {
	pipelineID := "eri-arn"
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: pipelineID},
		Job: types.JobConfig{
			Type: types.TriggerStepFunction,
			Config: map[string]interface{}{
				"stateMachineArn": "arn:aws:states:us-east-1:123456:stateMachine:my-sfn",
			},
		},
	}

	ddb := &mockDynamo{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: configItem(pipelineID, cfg)}, nil
		},
	}

	executor := &mockTriggerExecutor{
		result: map[string]interface{}{"executionArn": "arn:aws:states:us-east-1:123456:execution:my-sfn:run-1"},
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
	if out.RunID != "arn:aws:states:us-east-1:123456:execution:my-sfn:run-1" {
		t.Errorf("runID = %q, want executionArn value", out.RunID)
	}
}

// ---------------------------------------------------------------------------
// handleEvaluate — additional branch coverage
// ---------------------------------------------------------------------------

func TestEvaluate_GetAllSensorsError(t *testing.T) {
	pipelineID := "eval-sensor-err"
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: pipelineID},
		Validation: types.ValidationConfig{
			Trigger: "ALL",
			Rules: []types.ValidationRule{
				{Key: "upstream", Check: types.CheckExists},
			},
		},
	}

	ddb := &mockDynamo{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: configItem(pipelineID, cfg)}, nil
		},
		queryFn: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return nil, fmt.Errorf("dynamodb: request limit exceeded")
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
		t.Fatalf("unexpected Lambda error: %v", err)
	}
	if out.Error == "" {
		t.Error("expected error in output when GetAllSensors fails")
	}
}

func TestEvaluate_GetConfigError(t *testing.T) {
	ddb := &mockDynamo{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return nil, fmt.Errorf("dynamodb: internal error")
		},
	}

	d := newTestDeps(ddb)
	out, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode:       "evaluate",
		PipelineID: "any-pipe",
		ScheduleID: "daily",
		Date:       "2026-03-01",
	})
	if err != nil {
		t.Fatalf("unexpected Lambda error: %v", err)
	}
	if out.Error == "" {
		t.Error("expected error in output when GetConfig fails")
	}
}

// ---------------------------------------------------------------------------
// handleTrigger — additional branch coverage
// ---------------------------------------------------------------------------

func TestTrigger_PublishesJobTriggeredEvent(t *testing.T) {
	pipelineID := "trig-event"
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: pipelineID},
		Job: types.JobConfig{
			Type: types.TriggerGlue,
			Config: map[string]interface{}{
				"jobName": "my-etl",
			},
		},
	}

	ddb := &mockDynamo{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: configItem(pipelineID, cfg)}, nil
		},
	}

	executor := &mockTriggerExecutor{
		result: map[string]interface{}{"glue_job_run_id": "jr-event"},
	}

	d := newTestDeps(ddb)
	d.TriggerRunner = executor

	_, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode:       "trigger",
		PipelineID: pipelineID,
		ScheduleID: "daily",
		Date:       "2026-03-01",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	eb := d.EventBridge.(*mockEventBridge)
	if len(eb.events) != 1 {
		t.Fatalf("expected 1 EventBridge call, got %d", len(eb.events))
	}
	entry := eb.events[0].Entries[0]
	if *entry.DetailType != string(types.EventJobTriggered) {
		t.Errorf("event type = %q, want %q", *entry.DetailType, string(types.EventJobTriggered))
	}
}

func TestTrigger_GetConfigError(t *testing.T) {
	ddb := &mockDynamo{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return nil, fmt.Errorf("dynamodb: service unavailable")
		},
	}

	d := newTestDeps(ddb)
	d.TriggerRunner = &mockTriggerExecutor{}

	out, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode:       "trigger",
		PipelineID: "any-pipe",
		ScheduleID: "daily",
		Date:       "2026-03-01",
	})
	if err != nil {
		t.Fatalf("config errors should be payload errors: %v", err)
	}
	if out.Error == "" {
		t.Error("expected error in output when GetConfig fails")
	}
}

// ---------------------------------------------------------------------------
// PostRun — GetConfig error and config not found
// ---------------------------------------------------------------------------

func TestPostRun_GetConfigError(t *testing.T) {
	ddb := &mockDynamo{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return nil, fmt.Errorf("dynamodb: internal error")
		},
	}

	d := newTestDeps(ddb)
	out, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode:       "post-run",
		PipelineID: "any-pipe",
		ScheduleID: "daily",
		Date:       "2026-03-01",
	})
	if err != nil {
		t.Fatalf("unexpected Lambda error: %v", err)
	}
	if out.Error == "" {
		t.Error("expected error in output when GetConfig fails")
	}
}

func TestPostRun_ConfigNotFound(t *testing.T) {
	ddb := &mockDynamo{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: nil}, nil
		},
	}

	d := newTestDeps(ddb)
	out, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode:       "post-run",
		PipelineID: "missing-pipe",
		ScheduleID: "daily",
		Date:       "2026-03-01",
	})
	if err != nil {
		t.Fatalf("unexpected Lambda error: %v", err)
	}
	if out.Error == "" {
		t.Error("expected error in output for missing config")
	}
	if !strings.Contains(out.Error, "config not found") {
		t.Errorf("error = %q, want it to contain 'config not found'", out.Error)
	}
}

// ---------------------------------------------------------------------------
// ParseExecutionDate tests
// ---------------------------------------------------------------------------

func TestParseExecutionDate_Hourly(t *testing.T) {
	datePart, hourPart := lambda.ParseExecutionDate("2026-03-03T10")
	if datePart != "2026-03-03" {
		t.Errorf("datePart = %q, want %q", datePart, "2026-03-03")
	}
	if hourPart != "10" {
		t.Errorf("hourPart = %q, want %q", hourPart, "10")
	}
}

func TestParseExecutionDate_Daily(t *testing.T) {
	datePart, hourPart := lambda.ParseExecutionDate("2026-03-03")
	if datePart != "2026-03-03" {
		t.Errorf("datePart = %q, want %q", datePart, "2026-03-03")
	}
	if hourPart != "" {
		t.Errorf("hourPart = %q, want empty", hourPart)
	}
}
