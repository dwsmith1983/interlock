package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// seedConfig inserts a CONFIG row into the mock for the given pipeline.
func seedConfig(mock *mockDDB, cfg types.PipelineConfig) {
	data, _ := json.Marshal(cfg)
	mock.putRaw("control", map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(cfg.Pipeline.ID)},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: types.ConfigSK},
		"config": &ddbtypes.AttributeValueMemberS{Value: string(data)},
	})
}

func TestScanConfigs_MultipleConfigs(t *testing.T) {
	mock := newMockDDB()
	s := newTestStore(mock)

	seedConfig(mock, types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "pipeline-a", Owner: "team-a"},
	})
	seedConfig(mock, types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "pipeline-b", Owner: "team-b"},
	})

	// Also seed a non-CONFIG row to verify filtering.
	mock.putRaw("control", map[string]ddbtypes.AttributeValue{
		"PK": &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("pipeline-a")},
		"SK": &ddbtypes.AttributeValueMemberS{Value: types.SensorSK("upstream")},
		"data": &ddbtypes.AttributeValueMemberM{Value: map[string]ddbtypes.AttributeValue{
			"status": &ddbtypes.AttributeValueMemberS{Value: "done"},
		}},
	})

	configs, err := s.ScanConfigs(context.Background())
	if err != nil {
		t.Fatalf("ScanConfigs: %v", err)
	}
	if len(configs) != 2 {
		t.Fatalf("len(configs) = %d, want 2", len(configs))
	}
	if configs["pipeline-a"].Pipeline.Owner != "team-a" {
		t.Errorf("pipeline-a owner = %q, want %q", configs["pipeline-a"].Pipeline.Owner, "team-a")
	}
	if configs["pipeline-b"].Pipeline.Owner != "team-b" {
		t.Errorf("pipeline-b owner = %q, want %q", configs["pipeline-b"].Pipeline.Owner, "team-b")
	}
}

func TestScanConfigs_Empty(t *testing.T) {
	mock := newMockDDB()
	s := newTestStore(mock)

	configs, err := s.ScanConfigs(context.Background())
	if err != nil {
		t.Fatalf("ScanConfigs: %v", err)
	}
	if len(configs) != 0 {
		t.Fatalf("len(configs) = %d, want 0", len(configs))
	}
}

func TestScanConfigs_DynamoError(t *testing.T) {
	mock := newMockDDB()
	s := newTestStore(mock)

	injected := errors.New("scan throttled")
	mock.errFn = errOnOp("Scan", injected)

	_, err := s.ScanConfigs(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, injected) {
		t.Errorf("expected wrapped injected error, got: %v", err)
	}
	if !strings.Contains(err.Error(), "scan configs") {
		t.Errorf("expected context in error message, got: %v", err)
	}
}

func TestScanConfigs_InvalidJSON(t *testing.T) {
	mock := newMockDDB()
	s := newTestStore(mock)

	mock.putRaw("control", map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("bad-pipe")},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: types.ConfigSK},
		"config": &ddbtypes.AttributeValueMemberS{Value: `{not valid`},
	})

	_, err := s.ScanConfigs(context.Background())
	if err == nil {
		t.Fatal("expected unmarshal error, got nil")
	}
	if !strings.Contains(err.Error(), "unmarshal config") {
		t.Errorf("expected 'unmarshal config' in error, got: %v", err)
	}
}

func TestScanConfigs_MissingConfigAttribute(t *testing.T) {
	mock := newMockDDB()
	s := newTestStore(mock)

	// A CONFIG row without the "config" attribute.
	mock.putRaw("control", map[string]ddbtypes.AttributeValue{
		"PK": &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("broken")},
		"SK": &ddbtypes.AttributeValueMemberS{Value: types.ConfigSK},
	})

	_, err := s.ScanConfigs(context.Background())
	if err == nil {
		t.Fatal("expected error for missing config attribute, got nil")
	}
	if !strings.Contains(err.Error(), "config attribute missing") {
		t.Errorf("expected 'config attribute missing' in error, got: %v", err)
	}
}

// --- ConfigCache tests ---

func TestConfigCache_GetAll_Fresh(t *testing.T) {
	mock := newMockDDB()
	s := newTestStore(mock)

	seedConfig(mock, types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "pipe-1", Owner: "team-1"},
	})
	seedConfig(mock, types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "pipe-2", Owner: "team-2"},
	})

	cache := NewConfigCache(s, 5*time.Minute)

	configs, err := cache.GetAll(context.Background())
	if err != nil {
		t.Fatalf("GetAll: %v", err)
	}
	if len(configs) != 2 {
		t.Fatalf("len(configs) = %d, want 2", len(configs))
	}
	if configs["pipe-1"].Pipeline.Owner != "team-1" {
		t.Errorf("pipe-1 owner = %q, want %q", configs["pipe-1"].Pipeline.Owner, "team-1")
	}
	if configs["pipe-2"].Pipeline.Owner != "team-2" {
		t.Errorf("pipe-2 owner = %q, want %q", configs["pipe-2"].Pipeline.Owner, "team-2")
	}
}

func TestConfigCache_GetAll_Cached(t *testing.T) {
	mock := newMockDDB()
	s := newTestStore(mock)

	seedConfig(mock, types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "pipe-1"},
	})

	cache := NewConfigCache(s, 5*time.Minute)

	// First call loads from store.
	configs1, err := cache.GetAll(context.Background())
	if err != nil {
		t.Fatalf("first GetAll: %v", err)
	}
	if len(configs1) != 1 {
		t.Fatalf("first GetAll: len = %d, want 1", len(configs1))
	}

	// Track scan calls by injecting an error on the next Scan.
	var scanCount int32
	origErrFn := mock.errFn
	mock.errFn = func(op string) error {
		if op == "Scan" {
			atomic.AddInt32(&scanCount, 1)
		}
		if origErrFn != nil {
			return origErrFn(op)
		}
		return nil
	}

	// Second call should use cache (no additional Scan).
	configs2, err := cache.GetAll(context.Background())
	if err != nil {
		t.Fatalf("second GetAll: %v", err)
	}
	if len(configs2) != 1 {
		t.Fatalf("second GetAll: len = %d, want 1", len(configs2))
	}

	if atomic.LoadInt32(&scanCount) != 0 {
		t.Errorf("expected 0 additional scans, got %d", scanCount)
	}
}

func TestConfigCache_GetAll_Stale(t *testing.T) {
	mock := newMockDDB()
	s := newTestStore(mock)

	seedConfig(mock, types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "pipe-1"},
	})

	cache := NewConfigCache(s, 1*time.Millisecond)

	// First load.
	configs1, err := cache.GetAll(context.Background())
	if err != nil {
		t.Fatalf("first GetAll: %v", err)
	}
	if len(configs1) != 1 {
		t.Fatalf("first GetAll: len = %d, want 1", len(configs1))
	}

	// Wait for TTL to expire.
	time.Sleep(5 * time.Millisecond)

	// Add another pipeline while cache is stale.
	seedConfig(mock, types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "pipe-2"},
	})

	// Second call should re-scan and find both pipelines.
	configs2, err := cache.GetAll(context.Background())
	if err != nil {
		t.Fatalf("second GetAll: %v", err)
	}
	if len(configs2) != 2 {
		t.Fatalf("second GetAll: len = %d, want 2", len(configs2))
	}
}

func TestConfigCache_Get_SinglePipeline(t *testing.T) {
	mock := newMockDDB()
	s := newTestStore(mock)

	seedConfig(mock, types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "target", Owner: "found-it"},
	})
	seedConfig(mock, types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "other", Owner: "not-this"},
	})

	cache := NewConfigCache(s, 5*time.Minute)

	cfg, err := cache.Get(context.Background(), "target")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil config")
	}
	if cfg.Pipeline.Owner != "found-it" {
		t.Errorf("owner = %q, want %q", cfg.Pipeline.Owner, "found-it")
	}
}

func TestConfigCache_Get_NotFound(t *testing.T) {
	mock := newMockDDB()
	s := newTestStore(mock)

	seedConfig(mock, types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "exists"},
	})

	cache := NewConfigCache(s, 5*time.Minute)

	cfg, err := cache.Get(context.Background(), "nonexistent")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if cfg != nil {
		t.Errorf("expected nil for unknown pipeline, got %+v", cfg)
	}
}

func TestConfigCache_Invalidate(t *testing.T) {
	mock := newMockDDB()
	s := newTestStore(mock)

	seedConfig(mock, types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "pipe-1"},
	})

	cache := NewConfigCache(s, 5*time.Minute)

	// Load initial data.
	configs1, err := cache.GetAll(context.Background())
	if err != nil {
		t.Fatalf("first GetAll: %v", err)
	}
	if len(configs1) != 1 {
		t.Fatalf("first GetAll: len = %d, want 1", len(configs1))
	}

	// Add another pipeline.
	seedConfig(mock, types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "pipe-2"},
	})

	// Without invalidation, cache should still return 1.
	configs2, err := cache.GetAll(context.Background())
	if err != nil {
		t.Fatalf("second GetAll: %v", err)
	}
	if len(configs2) != 1 {
		t.Fatalf("second GetAll (before invalidate): len = %d, want 1", len(configs2))
	}

	// Invalidate and verify re-scan picks up the new pipeline.
	cache.Invalidate()

	configs3, err := cache.GetAll(context.Background())
	if err != nil {
		t.Fatalf("third GetAll: %v", err)
	}
	if len(configs3) != 2 {
		t.Fatalf("third GetAll (after invalidate): len = %d, want 2", len(configs3))
	}
}

func TestConfigCache_ConcurrentAccess(t *testing.T) {
	mock := newMockDDB()
	s := newTestStore(mock)

	seedConfig(mock, types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "concurrent-pipe"},
	})

	cache := NewConfigCache(s, 5*time.Minute)

	const goroutines = 10
	var wg sync.WaitGroup
	errs := make([]error, goroutines)
	results := make([]map[string]*types.PipelineConfig, goroutines)

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			configs, err := cache.GetAll(context.Background())
			errs[idx] = err
			results[idx] = configs
		}(i)
	}
	wg.Wait()

	for i := 0; i < goroutines; i++ {
		if errs[i] != nil {
			t.Errorf("goroutine %d: %v", i, errs[i])
		}
		if len(results[i]) != 1 {
			t.Errorf("goroutine %d: len = %d, want 1", i, len(results[i]))
		}
	}
}

func TestConfigCache_ScanError(t *testing.T) {
	mock := newMockDDB()
	s := newTestStore(mock)

	injected := errors.New("dynamo unavailable")
	mock.errFn = errOnOp("Scan", injected)

	cache := NewConfigCache(s, 5*time.Minute)

	_, err := cache.GetAll(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, injected) {
		t.Errorf("expected wrapped injected error, got: %v", err)
	}
}

func TestConfigCache_GetAll_ReturnsDeepCopy(t *testing.T) {
	mock := newMockDDB()
	s := newTestStore(mock)

	seedConfig(mock, types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "pipe-1", Owner: "original-owner"},
	})

	cache := NewConfigCache(s, 5*time.Minute)

	// First call: get the configs and mutate the returned map.
	configs1, err := cache.GetAll(context.Background())
	if err != nil {
		t.Fatalf("first GetAll: %v", err)
	}

	// Mutate the returned map: change a value and add a new key.
	configs1["pipe-1"].Pipeline.Owner = "mutated"
	configs1["injected"] = &types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "injected"},
	}

	// Second call: the cache must be unaffected by the mutations above.
	configs2, err := cache.GetAll(context.Background())
	if err != nil {
		t.Fatalf("second GetAll: %v", err)
	}

	if _, ok := configs2["injected"]; ok {
		t.Error("injected key should not appear in cached result")
	}
	if configs2["pipe-1"].Pipeline.Owner != "original-owner" {
		t.Errorf("owner = %q, want %q (cache was mutated)", configs2["pipe-1"].Pipeline.Owner, "original-owner")
	}
}
