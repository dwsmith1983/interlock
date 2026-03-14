package lambda

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExtractFloatOk(t *testing.T) {
	tests := []struct {
		name    string
		data    map[string]interface{}
		key     string
		wantVal float64
		wantOk  bool
	}{
		{"present float", map[string]interface{}{"count": float64(42)}, "count", 42, true},
		{"present zero", map[string]interface{}{"count": float64(0)}, "count", 0, true},
		{"present string", map[string]interface{}{"count": "123.5"}, "count", 123.5, true},
		{"missing key", map[string]interface{}{}, "count", 0, false},
		{"nil map", nil, "count", 0, false},
		{"wrong type", map[string]interface{}{"count": true}, "count", 0, false},
		{"invalid string", map[string]interface{}{"count": "abc"}, "count", 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, ok := ExtractFloatOk(tt.data, tt.key)
			assert.Equal(t, tt.wantOk, ok)
			assert.InDelta(t, tt.wantVal, val, 0.001)
		})
	}
}

func TestDetectDrift(t *testing.T) {
	m := func(k string, v float64) map[string]interface{} {
		return map[string]interface{}{k: v}
	}
	tests := []struct {
		name      string
		baseline  map[string]interface{}
		current   map[string]interface{}
		field     string
		threshold float64
		wantDrift bool
	}{
		{"5000→0 drifts", m("count", 5000.0), m("count", 0.0), "count", 0, true},
		{"0→5000 drifts", m("count", 0.0), m("count", 5000.0), "count", 0, true},
		{"same value no drift", m("count", 100.0), m("count", 100.0), "count", 0, false},
		{"within threshold", m("count", 100.0), m("count", 150.0), "count", 100, false},
		{"exceeds threshold", m("count", 100.0), m("count", 250.0), "count", 100, true},
		{"prev missing no drift", map[string]interface{}{}, m("count", 100.0), "count", 0, false},
		{"curr missing no drift", m("count", 100.0), map[string]interface{}{}, "count", 0, false},
		{"both missing no drift", map[string]interface{}{}, map[string]interface{}{}, "count", 0, false},
		{"negative drift", m("count", 100.0), m("count", 50.0), "count", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := DetectDrift(tt.baseline, tt.current, tt.field, tt.threshold)
			assert.Equal(t, tt.wantDrift, result.Drifted)
		})
	}
}
