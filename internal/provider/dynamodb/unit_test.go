package dynamodb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// mockDDB is a minimal mock of the DDBAPI interface for unit testing.
type mockDDB struct {
	putItemFn           func(ctx context.Context, input *dynamodb.PutItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	getItemFn           func(ctx context.Context, input *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	queryFn             func(ctx context.Context, input *dynamodb.QueryInput, opts ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
	updateItemFn        func(ctx context.Context, input *dynamodb.UpdateItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
	deleteItemFn        func(ctx context.Context, input *dynamodb.DeleteItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
	transactWriteItemFn func(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
	describeTableFn     func(ctx context.Context, input *dynamodb.DescribeTableInput, opts ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error)
	createTableFn       func(ctx context.Context, input *dynamodb.CreateTableInput, opts ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error)
	updateTTLFn         func(ctx context.Context, input *dynamodb.UpdateTimeToLiveInput, opts ...func(*dynamodb.Options)) (*dynamodb.UpdateTimeToLiveOutput, error)
	deleteTableFn       func(ctx context.Context, input *dynamodb.DeleteTableInput, opts ...func(*dynamodb.Options)) (*dynamodb.DeleteTableOutput, error)
}

func (m *mockDDB) PutItem(ctx context.Context, input *dynamodb.PutItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	if m.putItemFn != nil {
		return m.putItemFn(ctx, input, opts...)
	}
	return &dynamodb.PutItemOutput{}, nil
}

func (m *mockDDB) GetItem(ctx context.Context, input *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	if m.getItemFn != nil {
		return m.getItemFn(ctx, input, opts...)
	}
	return &dynamodb.GetItemOutput{}, nil
}

func (m *mockDDB) Query(ctx context.Context, input *dynamodb.QueryInput, opts ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	if m.queryFn != nil {
		return m.queryFn(ctx, input, opts...)
	}
	return &dynamodb.QueryOutput{}, nil
}

func (m *mockDDB) UpdateItem(ctx context.Context, input *dynamodb.UpdateItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	if m.updateItemFn != nil {
		return m.updateItemFn(ctx, input, opts...)
	}
	return &dynamodb.UpdateItemOutput{}, nil
}

func (m *mockDDB) DeleteItem(ctx context.Context, input *dynamodb.DeleteItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	if m.deleteItemFn != nil {
		return m.deleteItemFn(ctx, input, opts...)
	}
	return &dynamodb.DeleteItemOutput{}, nil
}

func (m *mockDDB) TransactWriteItems(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
	if m.transactWriteItemFn != nil {
		return m.transactWriteItemFn(ctx, input, opts...)
	}
	return &dynamodb.TransactWriteItemsOutput{}, nil
}

func (m *mockDDB) DescribeTable(ctx context.Context, input *dynamodb.DescribeTableInput, opts ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	if m.describeTableFn != nil {
		return m.describeTableFn(ctx, input, opts...)
	}
	return &dynamodb.DescribeTableOutput{}, nil
}

func (m *mockDDB) CreateTable(ctx context.Context, input *dynamodb.CreateTableInput, opts ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error) {
	if m.createTableFn != nil {
		return m.createTableFn(ctx, input, opts...)
	}
	return &dynamodb.CreateTableOutput{}, nil
}

func (m *mockDDB) UpdateTimeToLive(ctx context.Context, input *dynamodb.UpdateTimeToLiveInput, opts ...func(*dynamodb.Options)) (*dynamodb.UpdateTimeToLiveOutput, error) {
	if m.updateTTLFn != nil {
		return m.updateTTLFn(ctx, input, opts...)
	}
	return &dynamodb.UpdateTimeToLiveOutput{}, nil
}

func (m *mockDDB) DeleteTable(ctx context.Context, input *dynamodb.DeleteTableInput, opts ...func(*dynamodb.Options)) (*dynamodb.DeleteTableOutput, error) {
	if m.deleteTableFn != nil {
		return m.deleteTableFn(ctx, input, opts...)
	}
	return &dynamodb.DeleteTableOutput{}, nil
}

func newTestProvider(mock *mockDDB) *DynamoDBProvider {
	return &DynamoDBProvider{
		client:       mock,
		tableName:    "test-table",
		logger:       slog.Default(),
		readinessTTL: 5 * time.Minute,
		retentionTTL: 7 * 24 * time.Hour,
	}
}

// ---------------------------------------------------------------------------
// Pipeline marshaling tests
// ---------------------------------------------------------------------------

func TestRegisterPipeline_MarshaledData(t *testing.T) {
	var captured *dynamodb.PutItemInput
	mock := &mockDDB{
		putItemFn: func(_ context.Context, input *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			captured = input
			return &dynamodb.PutItemOutput{}, nil
		},
	}
	p := newTestProvider(mock)

	cfg := types.PipelineConfig{Name: "my-pipeline", Archetype: "batch", Tier: 2}
	err := p.RegisterPipeline(context.Background(), cfg)
	if err != nil {
		t.Fatalf("RegisterPipeline: %v", err)
	}

	if captured == nil {
		t.Fatal("PutItem was not called")
	}
	if *captured.TableName != "test-table" {
		t.Errorf("table = %q, want %q", *captured.TableName, "test-table")
	}

	// Verify PK and SK
	pk := captured.Item["PK"].(*ddbtypes.AttributeValueMemberS).Value
	sk := captured.Item["SK"].(*ddbtypes.AttributeValueMemberS).Value
	if pk != "PIPELINE#my-pipeline" {
		t.Errorf("PK = %q, want %q", pk, "PIPELINE#my-pipeline")
	}
	if sk != "CONFIG" {
		t.Errorf("SK = %q, want %q", sk, "CONFIG")
	}

	// Verify data round-trips through JSON
	dataStr := captured.Item["data"].(*ddbtypes.AttributeValueMemberS).Value
	var roundTrip types.PipelineConfig
	if err := json.Unmarshal([]byte(dataStr), &roundTrip); err != nil {
		t.Fatalf("unmarshal data: %v", err)
	}
	if roundTrip.Name != "my-pipeline" {
		t.Errorf("name = %q, want %q", roundTrip.Name, "my-pipeline")
	}
	if roundTrip.Archetype != "batch" {
		t.Errorf("archetype = %q, want %q", roundTrip.Archetype, "batch")
	}
}

func TestGetPipeline_RoundTrip(t *testing.T) {
	cfg := types.PipelineConfig{Name: "test-pipe", Archetype: "streaming", Tier: 1}
	data, _ := json.Marshal(cfg)

	mock := &mockDDB{
		getItemFn: func(_ context.Context, input *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{
				Item: map[string]ddbtypes.AttributeValue{
					"PK":   &ddbtypes.AttributeValueMemberS{Value: "PIPELINE#test-pipe"},
					"SK":   &ddbtypes.AttributeValueMemberS{Value: "CONFIG"},
					"data": &ddbtypes.AttributeValueMemberS{Value: string(data)},
				},
			}, nil
		},
	}
	p := newTestProvider(mock)

	got, err := p.GetPipeline(context.Background(), "test-pipe")
	if err != nil {
		t.Fatalf("GetPipeline: %v", err)
	}
	if got.Name != "test-pipe" {
		t.Errorf("name = %q, want %q", got.Name, "test-pipe")
	}
	if got.Archetype != "streaming" {
		t.Errorf("archetype = %q, want %q", got.Archetype, "streaming")
	}
}

func TestGetPipeline_NotFound(t *testing.T) {
	mock := &mockDDB{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: nil}, nil
		},
	}
	p := newTestProvider(mock)

	_, err := p.GetPipeline(context.Background(), "nonexistent")
	if err == nil {
		t.Fatal("expected error for missing pipeline")
	}
}

func TestGetPipeline_CorruptData(t *testing.T) {
	mock := &mockDDB{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{
				Item: map[string]ddbtypes.AttributeValue{
					"PK":   &ddbtypes.AttributeValueMemberS{Value: "PIPELINE#bad"},
					"SK":   &ddbtypes.AttributeValueMemberS{Value: "CONFIG"},
					"data": &ddbtypes.AttributeValueMemberS{Value: "not-json{{{"},
				},
			}, nil
		},
	}
	p := newTestProvider(mock)

	_, err := p.GetPipeline(context.Background(), "bad")
	if err == nil {
		t.Fatal("expected error for corrupt JSON data")
	}
}

// ---------------------------------------------------------------------------
// Lock conditional expression tests
// ---------------------------------------------------------------------------

func TestAcquireLock_Success(t *testing.T) {
	var captured *dynamodb.PutItemInput
	mock := &mockDDB{
		putItemFn: func(_ context.Context, input *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			captured = input
			return &dynamodb.PutItemOutput{}, nil
		},
	}
	p := newTestProvider(mock)

	token, err := p.AcquireLock(context.Background(), "my-lock", 30*time.Second)
	if err != nil {
		t.Fatalf("AcquireLock: %v", err)
	}
	if token == "" {
		t.Error("expected non-empty token on acquire")
	}

	// Verify conditional expression is set
	if captured.ConditionExpression == nil {
		t.Fatal("expected ConditionExpression to be set")
	}
	if *captured.ConditionExpression != "attribute_not_exists(PK) OR #ttl < :now" {
		t.Errorf("condition = %q, want %q", *captured.ConditionExpression, "attribute_not_exists(PK) OR #ttl < :now")
	}

	// Verify PK format
	pk := captured.Item["PK"].(*ddbtypes.AttributeValueMemberS).Value
	if pk != "LOCK#my-lock" {
		t.Errorf("PK = %q, want %q", pk, "LOCK#my-lock")
	}

	// Verify token attribute is stored
	tokenAttr := captured.Item["token"].(*ddbtypes.AttributeValueMemberS).Value
	if tokenAttr != token {
		t.Errorf("stored token = %q, want %q", tokenAttr, token)
	}
}

func TestAcquireLock_AlreadyHeld(t *testing.T) {
	mock := &mockDDB{
		putItemFn: func(_ context.Context, _ *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			return nil, &ddbtypes.ConditionalCheckFailedException{
				Message: strPtr("The conditional request failed"),
			}
		},
	}
	p := newTestProvider(mock)

	token, err := p.AcquireLock(context.Background(), "held-lock", time.Minute)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if token != "" {
		t.Error("expected empty token when lock NOT acquired")
	}
}

func TestAcquireLock_OtherError(t *testing.T) {
	mock := &mockDDB{
		putItemFn: func(_ context.Context, _ *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			return nil, fmt.Errorf("network timeout")
		},
	}
	p := newTestProvider(mock)

	_, err := p.AcquireLock(context.Background(), "lock", time.Minute)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestReleaseLock_DeletesCorrectKey(t *testing.T) {
	var captured *dynamodb.DeleteItemInput
	mock := &mockDDB{
		deleteItemFn: func(_ context.Context, input *dynamodb.DeleteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
			captured = input
			return &dynamodb.DeleteItemOutput{}, nil
		},
	}
	p := newTestProvider(mock)

	err := p.ReleaseLock(context.Background(), "my-lock", "test-token")
	if err != nil {
		t.Fatalf("ReleaseLock: %v", err)
	}

	pk := captured.Key["PK"].(*ddbtypes.AttributeValueMemberS).Value
	sk := captured.Key["SK"].(*ddbtypes.AttributeValueMemberS).Value
	if pk != "LOCK#my-lock" {
		t.Errorf("PK = %q, want %q", pk, "LOCK#my-lock")
	}
	if sk != "LOCK" {
		t.Errorf("SK = %q, want %q", sk, "LOCK")
	}

	// Verify condition expression checks token
	if captured.ConditionExpression == nil {
		t.Fatal("expected ConditionExpression to be set")
	}
	if *captured.ConditionExpression != "#token = :token" {
		t.Errorf("condition = %q, want %q", *captured.ConditionExpression, "#token = :token")
	}
	tokenVal := captured.ExpressionAttributeValues[":token"].(*ddbtypes.AttributeValueMemberS).Value
	if tokenVal != "test-token" {
		t.Errorf("token value = %q, want %q", tokenVal, "test-token")
	}
}

func TestReleaseLock_TokenMismatch(t *testing.T) {
	mock := &mockDDB{
		deleteItemFn: func(_ context.Context, _ *dynamodb.DeleteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
			return nil, &ddbtypes.ConditionalCheckFailedException{
				Message: strPtr("The conditional request failed"),
			}
		},
	}
	p := newTestProvider(mock)

	err := p.ReleaseLock(context.Background(), "my-lock", "wrong-token")
	if err != nil {
		t.Fatalf("expected no error on token mismatch, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// CompareAndSwapRunState tests
// ---------------------------------------------------------------------------

func TestCompareAndSwapRunState_Success(t *testing.T) {
	var capturedUpdate *dynamodb.UpdateItemInput
	mock := &mockDDB{
		updateItemFn: func(_ context.Context, input *dynamodb.UpdateItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
			capturedUpdate = input
			return &dynamodb.UpdateItemOutput{}, nil
		},
		putItemFn: func(_ context.Context, _ *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			return &dynamodb.PutItemOutput{}, nil // best-effort list copy
		},
	}
	p := newTestProvider(mock)

	newState := types.RunState{
		RunID:      "run-1",
		PipelineID: "pipe-1",
		Status:     types.RunRunning,
		Version:    2,
		CreatedAt:  time.Now(),
	}

	swapped, err := p.CompareAndSwapRunState(context.Background(), "run-1", 1, newState)
	if err != nil {
		t.Fatalf("CAS: %v", err)
	}
	if !swapped {
		t.Error("expected swap to succeed")
	}

	// Verify condition expression checks version
	if capturedUpdate.ConditionExpression == nil {
		t.Fatal("expected ConditionExpression")
	}
	if *capturedUpdate.ConditionExpression != "#version = :expectedVersion" {
		t.Errorf("condition = %q, want %q", *capturedUpdate.ConditionExpression, "#version = :expectedVersion")
	}

	// Verify expected version value
	expectedV := capturedUpdate.ExpressionAttributeValues[":expectedVersion"].(*ddbtypes.AttributeValueMemberN).Value
	if expectedV != "1" {
		t.Errorf("expectedVersion = %q, want %q", expectedV, "1")
	}
	newV := capturedUpdate.ExpressionAttributeValues[":newVersion"].(*ddbtypes.AttributeValueMemberN).Value
	if newV != "2" {
		t.Errorf("newVersion = %q, want %q", newV, "2")
	}
}

func TestCompareAndSwapRunState_VersionMismatch(t *testing.T) {
	mock := &mockDDB{
		updateItemFn: func(_ context.Context, _ *dynamodb.UpdateItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
			return nil, &ddbtypes.ConditionalCheckFailedException{
				Message: strPtr("The conditional request failed"),
			}
		},
	}
	p := newTestProvider(mock)

	newState := types.RunState{RunID: "run-1", PipelineID: "pipe-1", Status: types.RunRunning, Version: 2}
	swapped, err := p.CompareAndSwapRunState(context.Background(), "run-1", 1, newState)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if swapped {
		t.Error("expected swap to fail on version mismatch")
	}
}

// ---------------------------------------------------------------------------
// Alert marshaling tests
// ---------------------------------------------------------------------------

func TestPutAlert_DefaultsAlertID(t *testing.T) {
	var captured *dynamodb.PutItemInput
	mock := &mockDDB{
		putItemFn: func(_ context.Context, input *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			captured = input
			return &dynamodb.PutItemOutput{}, nil
		},
	}
	p := newTestProvider(mock)

	ts := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)
	alert := types.Alert{
		Level:      types.AlertLevelWarning,
		PipelineID: "pipe-a",
		Message:    "SLA breached",
		Timestamp:  ts,
	}

	err := p.PutAlert(context.Background(), alert)
	if err != nil {
		t.Fatalf("PutAlert: %v", err)
	}

	// Verify GSI1 keys for cross-pipeline queries
	gsi1pk := captured.Item["GSI1PK"].(*ddbtypes.AttributeValueMemberS).Value
	if gsi1pk != "TYPE#alert" {
		t.Errorf("GSI1PK = %q, want %q", gsi1pk, "TYPE#alert")
	}

	// Verify data contains the alert
	dataStr := captured.Item["data"].(*ddbtypes.AttributeValueMemberS).Value
	var roundTrip types.Alert
	if err := json.Unmarshal([]byte(dataStr), &roundTrip); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if roundTrip.Message != "SLA breached" {
		t.Errorf("message = %q, want %q", roundTrip.Message, "SLA breached")
	}
	// AlertID should have been defaulted to timestamp millis
	if roundTrip.AlertID == "" {
		t.Error("expected AlertID to be populated")
	}
}

func TestListAlerts_UnmarshalsItems(t *testing.T) {
	alert1 := types.Alert{AlertID: "1", Level: types.AlertLevelError, PipelineID: "p", Message: "first"}
	alert2 := types.Alert{AlertID: "2", Level: types.AlertLevelInfo, PipelineID: "p", Message: "second"}
	data1, _ := json.Marshal(alert1)
	data2, _ := json.Marshal(alert2)

	mock := &mockDDB{
		queryFn: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return &dynamodb.QueryOutput{
				Items: []map[string]ddbtypes.AttributeValue{
					{"data": &ddbtypes.AttributeValueMemberS{Value: string(data1)}},
					{"data": &ddbtypes.AttributeValueMemberS{Value: string(data2)}},
				},
			}, nil
		},
	}
	p := newTestProvider(mock)

	alerts, err := p.ListAlerts(context.Background(), "p", 10)
	if err != nil {
		t.Fatalf("ListAlerts: %v", err)
	}
	if len(alerts) != 2 {
		t.Fatalf("len(alerts) = %d, want 2", len(alerts))
	}
	if alerts[0].Message != "first" {
		t.Errorf("alerts[0].Message = %q, want %q", alerts[0].Message, "first")
	}
}

func TestListAlerts_SkipsCorruptData(t *testing.T) {
	goodAlert := types.Alert{AlertID: "1", Level: types.AlertLevelError, PipelineID: "p", Message: "good"}
	goodData, _ := json.Marshal(goodAlert)

	mock := &mockDDB{
		queryFn: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return &dynamodb.QueryOutput{
				Items: []map[string]ddbtypes.AttributeValue{
					{"data": &ddbtypes.AttributeValueMemberS{Value: "not-json{"}},
					{"data": &ddbtypes.AttributeValueMemberS{Value: string(goodData)}},
				},
			}, nil
		},
	}
	p := newTestProvider(mock)

	alerts, err := p.ListAlerts(context.Background(), "p", 10)
	if err != nil {
		t.Fatalf("ListAlerts: %v", err)
	}
	if len(alerts) != 1 {
		t.Fatalf("len(alerts) = %d, want 1 (corrupt item should be skipped)", len(alerts))
	}
}

// ---------------------------------------------------------------------------
// Event marshaling tests
// ---------------------------------------------------------------------------

func TestAppendEvent_MarshaledCorrectly(t *testing.T) {
	var captured *dynamodb.PutItemInput
	mock := &mockDDB{
		putItemFn: func(_ context.Context, input *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			captured = input
			return &dynamodb.PutItemOutput{}, nil
		},
	}
	p := newTestProvider(mock)

	ev := types.Event{
		Kind:       types.EventTraitEvaluated,
		PipelineID: "pipe-1",
		TraitType:  "freshness",
		Status:     "PASS",
		Timestamp:  time.Now(),
	}

	err := p.AppendEvent(context.Background(), ev)
	if err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}

	pk := captured.Item["PK"].(*ddbtypes.AttributeValueMemberS).Value
	if pk != "PIPELINE#pipe-1" {
		t.Errorf("PK = %q, want %q", pk, "PIPELINE#pipe-1")
	}

	// SK should start with EVENT# prefix
	sk := captured.Item["SK"].(*ddbtypes.AttributeValueMemberS).Value
	if len(sk) < 6 || sk[:6] != "EVENT#" {
		t.Errorf("SK = %q, want prefix %q", sk, "EVENT#")
	}

	// Verify data round-trips
	dataStr := captured.Item["data"].(*ddbtypes.AttributeValueMemberS).Value
	var roundTrip types.Event
	if err := json.Unmarshal([]byte(dataStr), &roundTrip); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if roundTrip.Kind != types.EventTraitEvaluated {
		t.Errorf("kind = %q, want %q", roundTrip.Kind, types.EventTraitEvaluated)
	}
}

// ---------------------------------------------------------------------------
// Trait TTL tests
// ---------------------------------------------------------------------------

func TestPutTrait_SetsTTL(t *testing.T) {
	calls := 0
	var firstPut *dynamodb.PutItemInput
	mock := &mockDDB{
		putItemFn: func(_ context.Context, input *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			if calls == 0 {
				firstPut = input
			}
			calls++
			return &dynamodb.PutItemOutput{}, nil
		},
	}
	p := newTestProvider(mock)

	trait := types.TraitEvaluation{
		PipelineID:  "pipe-1",
		TraitType:   "freshness",
		Status:      types.TraitPass,
		EvaluatedAt: time.Now(),
	}

	err := p.PutTrait(context.Background(), "pipe-1", trait, 10*time.Minute)
	if err != nil {
		t.Fatalf("PutTrait: %v", err)
	}

	// First PutItem is the current trait, second is history
	if calls != 2 {
		t.Fatalf("expected 2 PutItem calls (current + history), got %d", calls)
	}

	// Verify TTL is set on the current trait item
	ttlAttr, ok := firstPut.Item["ttl"]
	if !ok {
		t.Fatal("expected ttl attribute on current trait item")
	}
	ttlVal := ttlAttr.(*ddbtypes.AttributeValueMemberN).Value
	if ttlVal == "" || ttlVal == "0" {
		t.Error("expected non-zero TTL value")
	}
}

func TestPutTrait_ZeroTTL_NoTTLAttribute(t *testing.T) {
	var firstPut *dynamodb.PutItemInput
	calls := 0
	mock := &mockDDB{
		putItemFn: func(_ context.Context, input *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			if calls == 0 {
				firstPut = input
			}
			calls++
			return &dynamodb.PutItemOutput{}, nil
		},
	}
	p := newTestProvider(mock)

	trait := types.TraitEvaluation{
		PipelineID:  "pipe-1",
		TraitType:   "freshness",
		Status:      types.TraitPass,
		EvaluatedAt: time.Now(),
	}

	err := p.PutTrait(context.Background(), "pipe-1", trait, 0)
	if err != nil {
		t.Fatalf("PutTrait: %v", err)
	}

	if _, ok := firstPut.Item["ttl"]; ok {
		t.Error("expected no ttl attribute when TTL is 0")
	}
}

func TestGetTrait_ExpiredTTL(t *testing.T) {
	trait := types.TraitEvaluation{
		PipelineID: "pipe-1",
		TraitType:  "freshness",
		Status:     types.TraitPass,
	}
	data, _ := json.Marshal(trait)

	// TTL in the past = expired
	expiredTTL := fmt.Sprintf("%d", time.Now().Add(-1*time.Hour).Unix())

	mock := &mockDDB{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{
				Item: map[string]ddbtypes.AttributeValue{
					"PK":   &ddbtypes.AttributeValueMemberS{Value: "PIPELINE#pipe-1"},
					"SK":   &ddbtypes.AttributeValueMemberS{Value: "TRAIT#freshness"},
					"data": &ddbtypes.AttributeValueMemberS{Value: string(data)},
					"ttl":  &ddbtypes.AttributeValueMemberN{Value: expiredTTL},
				},
			}, nil
		},
	}
	p := newTestProvider(mock)

	got, err := p.GetTrait(context.Background(), "pipe-1", "freshness")
	if err != nil {
		t.Fatalf("GetTrait: %v", err)
	}
	if got != nil {
		t.Error("expected nil for expired trait")
	}
}

// ---------------------------------------------------------------------------
// Quarantine record tests
// ---------------------------------------------------------------------------

func TestPutQuarantineRecord_KeyFormat(t *testing.T) {
	var captured *dynamodb.PutItemInput
	mock := &mockDDB{
		putItemFn: func(_ context.Context, input *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			captured = input
			return &dynamodb.PutItemOutput{}, nil
		},
	}
	p := newTestProvider(mock)

	rec := types.QuarantineRecord{
		PipelineID:     "pipe-1",
		Date:           "2025-01-15",
		Hour:           "10",
		Count:          42,
		QuarantinePath: "s3://bucket/quarantine/pipe-1/2025-01-15/10",
		Reasons:        []string{"schema_mismatch", "null_key"},
		Timestamp:      time.Now(),
	}

	err := p.PutQuarantineRecord(context.Background(), rec)
	if err != nil {
		t.Fatalf("PutQuarantineRecord: %v", err)
	}

	pk := captured.Item["PK"].(*ddbtypes.AttributeValueMemberS).Value
	sk := captured.Item["SK"].(*ddbtypes.AttributeValueMemberS).Value
	if pk != "PIPELINE#pipe-1" {
		t.Errorf("PK = %q, want %q", pk, "PIPELINE#pipe-1")
	}
	if sk != "QUARANTINE#2025-01-15#10" {
		t.Errorf("SK = %q, want %q", sk, "QUARANTINE#2025-01-15#10")
	}

	// Verify round-trip
	dataStr := captured.Item["data"].(*ddbtypes.AttributeValueMemberS).Value
	var roundTrip types.QuarantineRecord
	if err := json.Unmarshal([]byte(dataStr), &roundTrip); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if roundTrip.Count != 42 {
		t.Errorf("count = %d, want %d", roundTrip.Count, 42)
	}
	if len(roundTrip.Reasons) != 2 {
		t.Errorf("reasons len = %d, want 2", len(roundTrip.Reasons))
	}
}

func TestGetQuarantineRecord_NotFound(t *testing.T) {
	mock := &mockDDB{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: nil}, nil
		},
	}
	p := newTestProvider(mock)

	got, err := p.GetQuarantineRecord(context.Background(), "pipe-1", "2025-01-15", "10")
	if err != nil {
		t.Fatalf("GetQuarantineRecord: %v", err)
	}
	if got != nil {
		t.Error("expected nil for missing quarantine record")
	}
}

// ---------------------------------------------------------------------------
// Error classification tests
// ---------------------------------------------------------------------------

func TestIsConditionalCheckFailed(t *testing.T) {
	ccfe := &ddbtypes.ConditionalCheckFailedException{Message: strPtr("failed")}
	if !isConditionalCheckFailed(ccfe) {
		t.Error("expected true for ConditionalCheckFailedException")
	}

	wrapped := fmt.Errorf("wrapped: %w", ccfe)
	if !isConditionalCheckFailed(wrapped) {
		t.Error("expected true for wrapped ConditionalCheckFailedException")
	}

	other := errors.New("some other error")
	if isConditionalCheckFailed(other) {
		t.Error("expected false for non-conditional error")
	}
}

// ---------------------------------------------------------------------------
// Ping / ensureTable tests
// ---------------------------------------------------------------------------

func TestPing_PropagatesError(t *testing.T) {
	mock := &mockDDB{
		describeTableFn: func(_ context.Context, _ *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
			return nil, fmt.Errorf("table not found")
		},
	}
	p := newTestProvider(mock)

	err := p.Ping(context.Background())
	if err == nil {
		t.Fatal("expected error from Ping")
	}
}

func TestEnsureTable_AlreadyExists(t *testing.T) {
	mock := &mockDDB{
		createTableFn: func(_ context.Context, _ *dynamodb.CreateTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error) {
			return nil, &ddbtypes.ResourceInUseException{Message: strPtr("already exists")}
		},
	}
	p := newTestProvider(mock)

	err := p.ensureTable(context.Background())
	if err != nil {
		t.Fatalf("ensureTable should ignore ResourceInUseException, got: %v", err)
	}
}

func strPtr(s string) *string { return &s }
