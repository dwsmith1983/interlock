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
// CompleteTrigger — post-run baseline capture tests
// ---------------------------------------------------------------------------

func TestCompleteTrigger_Success_CapturesBaseline(t *testing.T) {
	pipelineID := "pipe-baseline"
	date := "2026-03-08T06"
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: pipelineID},
		PostRun: &types.PostRunConfig{
			Rules: []types.ValidationRule{
				{Key: "audit-result", Check: types.CheckGTE, Field: "sensor_count", Value: float64(100)},
			},
		},
	}

	ddb := newMockDDB()
	ddb.putRaw("control", configItem(pipelineID, cfg))
	ddb.putRaw("control", map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK("stream", date)},
		"status": &ddbtypes.AttributeValueMemberS{Value: types.TriggerStatusRunning},
	})
	ddb.putRaw("control", sensorItem(pipelineID, "audit-result", map[string]ddbtypes.AttributeValue{
		"sensor_count": &ddbtypes.AttributeValueMemberN{Value: "150"},
	}))

	d := newTestDeps(ddb)
	out, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode:       "complete-trigger",
		PipelineID: pipelineID,
		ScheduleID: "stream",
		Date:       date,
		Event:      "success",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Status != types.TriggerStatusCompleted {
		t.Errorf("status = %q, want %q", out.Status, types.TriggerStatusCompleted)
	}

	// Verify date-scoped baseline was written.
	baselineKey := ddbItemKey("control", types.PipelinePK(pipelineID), types.SensorSK("postrun-baseline#"+date))
	ddb.mu.Lock()
	_, exists := ddb.items[baselineKey]
	ddb.mu.Unlock()
	if !exists {
		t.Error("expected date-scoped postrun-baseline sensor to be written on success")
	}

	// Verify POST_RUN_BASELINE_CAPTURED event was published.
	eb := d.EventBridge.(*mockEventBridge)
	eb.mu.Lock()
	defer eb.mu.Unlock()
	found := false
	for _, evt := range eb.events {
		if *evt.Entries[0].DetailType == string(types.EventPostRunBaselineCaptured) {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected POST_RUN_BASELINE_CAPTURED event")
	}
}

func TestCompleteTrigger_Fail_NoBaseline(t *testing.T) {
	pipelineID := "pipe-no-baseline"
	date := "2026-03-08T06"
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
	ddb.putRaw("control", map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK("stream", date)},
		"status": &ddbtypes.AttributeValueMemberS{Value: types.TriggerStatusRunning},
	})

	d := newTestDeps(ddb)
	out, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode:       "complete-trigger",
		PipelineID: pipelineID,
		ScheduleID: "stream",
		Date:       date,
		Event:      "fail",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Status != types.TriggerStatusFailedFinal {
		t.Errorf("status = %q, want %q", out.Status, types.TriggerStatusFailedFinal)
	}

	// Verify NO baseline was written on failure.
	baselineKey := ddbItemKey("control", types.PipelinePK(pipelineID), types.SensorSK("postrun-baseline#"+date))
	ddb.mu.Lock()
	_, exists := ddb.items[baselineKey]
	ddb.mu.Unlock()
	if exists {
		t.Error("postrun-baseline should NOT be written on failure")
	}
}

func TestCompleteTrigger_Success_NoPostRun_NoBaseline(t *testing.T) {
	pipelineID := "pipe-no-postrun"
	date := "2026-03-08"
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: pipelineID},
		// No PostRun config.
	}

	ddb := newMockDDB()
	ddb.putRaw("control", configItem(pipelineID, cfg))
	ddb.putRaw("control", map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK("stream", date)},
		"status": &ddbtypes.AttributeValueMemberS{Value: types.TriggerStatusRunning},
	})

	d := newTestDeps(ddb)
	out, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode:       "complete-trigger",
		PipelineID: pipelineID,
		ScheduleID: "stream",
		Date:       date,
		Event:      "success",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Status != types.TriggerStatusCompleted {
		t.Errorf("status = %q, want %q", out.Status, types.TriggerStatusCompleted)
	}

	// No PostRun config → no baseline written.
	baselineKey := ddbItemKey("control", types.PipelinePK(pipelineID), types.SensorSK("postrun-baseline#"+date))
	ddb.mu.Lock()
	_, exists := ddb.items[baselineKey]
	ddb.mu.Unlock()
	if exists {
		t.Error("postrun-baseline should NOT be written when no PostRun config")
	}
}

func TestCompleteTrigger_DateScopedBaseline(t *testing.T) {
	pipelineID := "pipe-date-scope"
	date := "2026-03-08T06"
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: pipelineID},
		PostRun: &types.PostRunConfig{
			Rules: []types.ValidationRule{
				{Key: "audit-result", Check: types.CheckGTE, Field: "sensor_count", Value: float64(1)},
			},
		},
	}

	ddb := newMockDDB()
	ddb.putRaw("control", configItem(pipelineID, cfg))
	ddb.putRaw("control", map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK("stream", date)},
		"status": &ddbtypes.AttributeValueMemberS{Value: types.TriggerStatusRunning},
	})
	// Per-period sensor that will be remapped to "audit-result".
	ddb.putRaw("control", sensorItem(pipelineID, "audit-result#20260308T06", map[string]ddbtypes.AttributeValue{
		"sensor_count": &ddbtypes.AttributeValueMemberN{Value: "42"},
	}))

	d := newTestDeps(ddb)
	_, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode:       "complete-trigger",
		PipelineID: pipelineID,
		ScheduleID: "stream",
		Date:       date,
		Event:      "success",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Baseline should be date-scoped.
	baselineKey := ddbItemKey("control", types.PipelinePK(pipelineID), types.SensorSK("postrun-baseline#"+date))
	ddb.mu.Lock()
	item, exists := ddb.items[baselineKey]
	ddb.mu.Unlock()
	if !exists {
		t.Fatal("expected date-scoped postrun-baseline")
	}

	// Verify the baseline captured sensor_count from the remapped sensor.
	if data, ok := item["data"]; ok {
		if dataMap, ok := data.(*ddbtypes.AttributeValueMemberM); ok {
			if sc, ok := dataMap.Value["sensor_count"]; ok {
				if scN, ok := sc.(*ddbtypes.AttributeValueMemberN); ok {
					if scN.Value != "42" {
						t.Errorf("baseline sensor_count = %q, want %q", scN.Value, "42")
					}
				}
			}
		}
	}
}

// ---------------------------------------------------------------------------
// CompleteTrigger — baseline capture failure event tests
// ---------------------------------------------------------------------------

// TestCompleteTrigger_BaselineCaptureFailed_PublishesEvent verifies that when
// capturePostRunBaseline fails (sensor query error), a BASELINE_CAPTURE_FAILED
// event is published to EventBridge and the trigger is still marked COMPLETED.
func TestCompleteTrigger_BaselineCaptureFailed_PublishesEvent(t *testing.T) {
	pipelineID := "pipe-baseline-fail"
	date := "2026-03-08T06"
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: pipelineID},
		PostRun: &types.PostRunConfig{
			Rules: []types.ValidationRule{
				{Key: "audit-result", Check: types.CheckGTE, Field: "sensor_count", Value: float64(100)},
			},
		},
	}

	callCount := 0
	ddb := &mockDynamo{
		// First GetItem call → config. Second GetItem call (from capturePostRunBaseline) → also config.
		getItemFn: func(_ context.Context, input *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: configItem(pipelineID, cfg)}, nil
		},
		// Query for sensors fails — this causes capturePostRunBaseline to return an error.
		queryFn: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			callCount++
			return nil, fmt.Errorf("dynamodb: provisioned throughput exceeded")
		},
	}

	d := newTestDeps(ddb)
	out, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode:       "complete-trigger",
		PipelineID: pipelineID,
		ScheduleID: "stream",
		Date:       date,
		Event:      "success",
	})
	if err != nil {
		t.Fatalf("unexpected Lambda error: baseline capture failure should not fail the handler: %v", err)
	}

	// Trigger must still be marked COMPLETED — baseline failure is non-fatal.
	if out.Status != types.TriggerStatusCompleted {
		t.Errorf("status = %q, want %q — baseline failure must not affect trigger completion", out.Status, types.TriggerStatusCompleted)
	}

	// BASELINE_CAPTURE_FAILED event must be published to EventBridge.
	eb := d.EventBridge.(*mockEventBridge)
	eb.mu.Lock()
	defer eb.mu.Unlock()
	found := false
	for _, evt := range eb.events {
		if evt.Entries[0].DetailType != nil && *evt.Entries[0].DetailType == string(types.EventBaselineCaptureFailed) {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected BASELINE_CAPTURE_FAILED event to be published; got %d events: %v", len(eb.events), eb.events)
	}
}

// TestCompleteTrigger_BaselineCaptureFailed_NoEvent_WhenNoPostRunConfig verifies
// that no BASELINE_CAPTURE_FAILED event is published when the pipeline has no
// PostRun config (capturePostRunBaseline returns nil early).
func TestCompleteTrigger_BaselineCaptureFailed_NoEvent_WhenNoPostRunConfig(t *testing.T) {
	pipelineID := "pipe-no-postrun-fail"
	date := "2026-03-08"
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: pipelineID},
		// No PostRun config — capturePostRunBaseline returns nil immediately.
	}

	ddb := &mockDynamo{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: configItem(pipelineID, cfg)}, nil
		},
		queryFn: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return nil, fmt.Errorf("dynamodb: should not be called")
		},
	}

	d := newTestDeps(ddb)
	_, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode:       "complete-trigger",
		PipelineID: pipelineID,
		ScheduleID: "stream",
		Date:       date,
		Event:      "success",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// No BASELINE_CAPTURE_FAILED event — capturePostRunBaseline returned nil (no PostRun).
	eb := d.EventBridge.(*mockEventBridge)
	eb.mu.Lock()
	defer eb.mu.Unlock()
	for _, evt := range eb.events {
		if evt.Entries[0].DetailType != nil && *evt.Entries[0].DetailType == string(types.EventBaselineCaptureFailed) {
			t.Error("BASELINE_CAPTURE_FAILED must NOT be published when pipeline has no PostRun config")
		}
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
// ExtractFloat — direct unit tests
// ---------------------------------------------------------------------------

func TestExtractFloat_Float64(t *testing.T) {
	data := map[string]interface{}{"sensor_count": float64(42.5)}
	got := lambda.ExtractFloat(data, "sensor_count")
	if got != 42.5 {
		t.Errorf("ExtractFloat = %v, want 42.5", got)
	}
}

func TestExtractFloat_String(t *testing.T) {
	data := map[string]interface{}{"sensor_count": "3.14"}
	got := lambda.ExtractFloat(data, "sensor_count")
	if got != 3.14 {
		t.Errorf("ExtractFloat = %v, want 3.14", got)
	}
}

func TestExtractFloat_InvalidString(t *testing.T) {
	data := map[string]interface{}{"sensor_count": "notanumber"}
	got := lambda.ExtractFloat(data, "sensor_count")
	if got != 0 {
		t.Errorf("ExtractFloat = %v, want 0 for invalid string", got)
	}
}

func TestExtractFloat_MissingKey(t *testing.T) {
	data := map[string]interface{}{"other_field": "hello"}
	got := lambda.ExtractFloat(data, "sensor_count")
	if got != 0 {
		t.Errorf("ExtractFloat = %v, want 0 for missing key", got)
	}
}

func TestExtractFloat_UnsupportedType(t *testing.T) {
	data := map[string]interface{}{"sensor_count": true}
	got := lambda.ExtractFloat(data, "sensor_count")
	if got != 0 {
		t.Errorf("ExtractFloat = %v, want 0 for unsupported type (bool)", got)
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
	// Per-period sensor ALWAYS overwrites the base key so the evaluation
	// uses the date-specific value instead of a stale base-key value.
	pipelineID := "remap-base-exists"
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: pipelineID},
		Validation: types.ValidationConfig{
			Trigger: "ALL",
			Rules: []types.ValidationRule{
				{Key: "hourly-status", Check: types.CheckEquals, Field: "source", Value: "per-period"},
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
	// Per-period sensor overwrites base key — rule checks source="per-period".
	if out.Status != "passed" {
		t.Errorf("status = %q, want %q — per-period sensor should overwrite base key", out.Status, "passed")
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
	// Non-polling triggers (nil metadata) now get sentinel runId="sync"
	// so the SFN CheckJob JSONPath resolves.
	if out.RunID != "sync" {
		t.Errorf("runID = %q, want \"sync\" for non-polling trigger", out.RunID)
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

// ---------------------------------------------------------------------------
// handleJobPollExhausted — happy path + error branch
// ---------------------------------------------------------------------------

func TestOrchestrator_JobPollExhausted_WritesJoblog(t *testing.T) {
	ddb := newMockDDB()
	d := newTestDeps(ddb)

	out, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode:       "job-poll-exhausted",
		PipelineID: "gold-orders",
		ScheduleID: "daily",
		Date:       "2026-03-01",
		RunID:      "jr_abc123",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Mode != "job-poll-exhausted" {
		t.Errorf("mode = %q, want %q", out.Mode, "job-poll-exhausted")
	}
	if out.Status != "exhausted" {
		t.Errorf("status = %q, want %q", out.Status, "exhausted")
	}

	// Verify joblog entry was written.
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
	if jobEvent != types.JobEventJobPollExhausted {
		t.Errorf("joblog event = %q, want %q", jobEvent, types.JobEventJobPollExhausted)
	}
	// Verify runId is stored.
	runID := jobItems.Items[0]["runId"].(*ddbtypes.AttributeValueMemberS).Value
	if runID != "jr_abc123" {
		t.Errorf("joblog runId = %q, want %q", runID, "jr_abc123")
	}

	// Verify EventBridge event was published.
	eb := d.EventBridge.(*mockEventBridge)
	if len(eb.events) != 1 {
		t.Fatalf("expected 1 EventBridge call, got %d", len(eb.events))
	}
}

func TestJobPollExhausted_WriteJobEventError(t *testing.T) {
	ddb := &mockDynamo{
		putItemFn: func(_ context.Context, _ *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			return nil, fmt.Errorf("dynamodb: write capacity exceeded")
		},
	}

	d := newTestDeps(ddb)
	_, err := lambda.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode:       "job-poll-exhausted",
		PipelineID: "pipe-1",
		ScheduleID: "stream",
		Date:       "2026-03-01",
		RunID:      "jr_xyz",
	})
	if err == nil {
		t.Fatal("expected Lambda error when WriteJobEvent fails")
	}
	if !strings.Contains(err.Error(), "write job-poll-exhausted joblog") {
		t.Errorf("error = %q, want it to contain 'write job-poll-exhausted joblog'", err.Error())
	}
}

func TestCheckJob_FailedWithCategory(t *testing.T) {
	pipelineID := "silver-cdr-hour"
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: pipelineID},
		Job:      types.JobConfig{Type: types.TriggerGlue},
	}

	var capturedItem map[string]ddbtypes.AttributeValue
	ddb := &mockDynamo{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: configItem(pipelineID, cfg)}, nil
		},
		queryFn: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return &dynamodb.QueryOutput{Items: nil}, nil
		},
		putItemFn: func(_ context.Context, input *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			if *input.TableName == "joblog" {
				capturedItem = input.Item
			}
			return &dynamodb.PutItemOutput{}, nil
		},
	}

	checker := &mockStatusChecker{
		result: lambda.StatusResult{
			State:           "failed",
			Message:         "OOM killed",
			FailureCategory: types.FailurePermanent,
		},
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
	if out.Event != "fail" {
		t.Errorf("event = %q, want %q", out.Event, "fail")
	}
	if capturedItem == nil {
		t.Fatal("expected joblog PutItem to be called")
	}
	catAttr, ok := capturedItem["category"]
	if !ok {
		t.Fatal("expected category attribute in joblog item")
	}
	catVal := catAttr.(*ddbtypes.AttributeValueMemberS).Value
	if catVal != "PERMANENT" {
		t.Errorf("category = %q, want %q", catVal, "PERMANENT")
	}
}

func TestCheckJob_FailedWithoutCategory(t *testing.T) {
	pipelineID := "silver-cdr-hour"
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: pipelineID},
		Job:      types.JobConfig{Type: types.TriggerGlue},
	}

	var capturedItem map[string]ddbtypes.AttributeValue
	ddb := &mockDynamo{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: configItem(pipelineID, cfg)}, nil
		},
		queryFn: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return &dynamodb.QueryOutput{Items: nil}, nil
		},
		putItemFn: func(_ context.Context, input *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			if *input.TableName == "joblog" {
				capturedItem = input.Item
			}
			return &dynamodb.PutItemOutput{}, nil
		},
	}

	checker := &mockStatusChecker{
		result: lambda.StatusResult{
			State:           "failed",
			Message:         "OOM killed",
			FailureCategory: "",
		},
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
	if out.Event != "fail" {
		t.Errorf("event = %q, want %q", out.Event, "fail")
	}
	if capturedItem == nil {
		t.Fatal("expected joblog PutItem to be called")
	}
	if _, ok := capturedItem["category"]; ok {
		t.Error("expected category attribute to be absent when FailureCategory is empty")
	}
}
