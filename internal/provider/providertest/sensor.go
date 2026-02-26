package providertest

import (
	"context"
	"testing"
	"time"

	"github.com/dwsmith1983/interlock/internal/provider"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// TestSensorPutGet validates sensor data storage and retrieval.
func TestSensorPutGet(t *testing.T, prov provider.Provider) {
	ctx := context.Background()

	sd := types.SensorData{
		PipelineID: "pipe-sensor-test",
		SensorType: "record-count",
		Value:      map[string]interface{}{"count": float64(42)},
		UpdatedAt:  time.Now(),
	}

	if err := prov.PutSensorData(ctx, sd); err != nil {
		t.Fatalf("PutSensorData: %v", err)
	}

	got, err := prov.GetSensorData(ctx, "pipe-sensor-test", "record-count")
	if err != nil {
		t.Fatalf("GetSensorData: %v", err)
	}
	if got == nil {
		t.Fatal("expected sensor data, got nil")
	}
	if got.PipelineID != "pipe-sensor-test" {
		t.Errorf("PipelineID = %q, want %q", got.PipelineID, "pipe-sensor-test")
	}

	// Non-existent sensor should return nil.
	missing, err := prov.GetSensorData(ctx, "pipe-sensor-test", "nonexistent")
	if err != nil {
		t.Fatalf("GetSensorData nonexistent: %v", err)
	}
	if missing != nil {
		t.Error("expected nil for missing sensor data")
	}
}
