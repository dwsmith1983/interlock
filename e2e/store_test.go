//go:build e2e

package e2e

import (
	"reflect"
	"testing"

	"github.com/dwsmith1983/interlock/internal/store"
)

// TestStoreWriteSensorFieldIfAbsent runs the nested conditional update on the
// e2e DynamoDB emulator (LocalStack), which the in-memory fake only imitates.
// The pipeline has no config, so the stream-router skips its sensor writes.
func TestStoreWriteSensorFieldIfAbsent(t *testing.T) {
	const pipeline, sensor, bare, field = "store-field", "baseline", "bare", "row-count"
	s := &store.Store{Client: ddb, ControlTable: controlTable}
	ctx, cancel := callCtx()
	defer cancel()
	t.Cleanup(func() {
		cctx, ccancel := callCtx()
		defer ccancel()
		for _, key := range []string{sensor, bare} {
			if err := s.DeleteSensor(cctx, pipeline, key); err != nil {
				t.Errorf("delete sensor %s: %v", key, err)
			}
		}
	})
	first := map[string]interface{}{"count": 5000.0}
	other := map[string]interface{}{"count": 1.0}

	write := func(value map[string]interface{}, want bool) {
		t.Helper()
		got, err := s.WriteSensorFieldIfAbsent(ctx, pipeline, sensor, field, value)
		if err != nil || got != want {
			t.Fatalf("WriteSensorFieldIfAbsent(%v) = %v, %v; want %v, nil", value, got, err, want)
		}
	}
	check := func(key string, want map[string]interface{}) {
		t.Helper()
		data, err := s.GetSensorData(ctx, pipeline, key)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(data, want) {
			t.Fatalf("sensor %s data = %v, want %v", key, data, want)
		}
	}

	write(first, false) // no row yet
	check(sensor, nil)

	if err := s.WriteSensor(ctx, pipeline, sensor, map[string]interface{}{"status": "ready"}); err != nil {
		t.Fatal(err)
	}
	write(first, true)
	check(sensor, map[string]interface{}{"status": "ready", field: first})

	write(other, false) // the field is already there
	check(sensor, map[string]interface{}{"status": "ready", field: first})

	// A nil map writes a row without data: the path is rejected.
	if err := s.WriteSensor(ctx, pipeline, bare, nil); err != nil {
		t.Fatal(err)
	}
	got, err := s.WriteSensorFieldIfAbsent(ctx, pipeline, bare, field, first)
	if got || err == nil {
		t.Fatalf("WriteSensorFieldIfAbsent on a row without data = %v, %v; want false and an error", got, err)
	}
	t.Logf("row without data: %v", err)
	check(bare, nil)
}
