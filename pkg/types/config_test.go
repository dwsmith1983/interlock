package types_test

import (
	"testing"

	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/stretchr/testify/assert"
)

func TestTriggerArguments(t *testing.T) {
	tests := []struct {
		name string
		tc   *types.TriggerConfig
		want map[string]string
	}{
		{name: "nil receiver", tc: nil, want: nil},
		{name: "http type (no arguments)", tc: &types.TriggerConfig{Type: types.TriggerHTTP}, want: nil},
		{
			name: "glue",
			tc: &types.TriggerConfig{
				Type: types.TriggerGlue,
				Glue: &types.GlueTriggerConfig{Arguments: map[string]string{"--key": "val"}},
			},
			want: map[string]string{"--key": "val"},
		},
		{
			name: "emr",
			tc: &types.TriggerConfig{
				Type: types.TriggerEMR,
				EMR:  &types.EMRTriggerConfig{Arguments: map[string]string{"a": "b"}},
			},
			want: map[string]string{"a": "b"},
		},
		{
			name: "step-function",
			tc: &types.TriggerConfig{
				Type:         types.TriggerStepFunction,
				StepFunction: &types.StepFunctionTriggerConfig{Arguments: map[string]string{"x": "y"}},
			},
			want: map[string]string{"x": "y"},
		},
		{
			name: "databricks",
			tc: &types.TriggerConfig{
				Type:       types.TriggerDatabricks,
				Databricks: &types.DatabricksTriggerConfig{Arguments: map[string]string{"p": "q"}},
			},
			want: map[string]string{"p": "q"},
		},
		{
			name: "glue with nil config",
			tc:   &types.TriggerConfig{Type: types.TriggerGlue},
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.tc.TriggerArguments())
		})
	}
}

func TestTriggerHeaders(t *testing.T) {
	tests := []struct {
		name string
		tc   *types.TriggerConfig
		want map[string]string
	}{
		{name: "nil receiver", tc: nil, want: nil},
		{name: "glue type (no headers)", tc: &types.TriggerConfig{Type: types.TriggerGlue}, want: nil},
		{
			name: "http",
			tc: &types.TriggerConfig{
				Type: types.TriggerHTTP,
				HTTP: &types.HTTPTriggerConfig{Headers: map[string]string{"Authorization": "Bearer tok"}},
			},
			want: map[string]string{"Authorization": "Bearer tok"},
		},
		{
			name: "airflow",
			tc: &types.TriggerConfig{
				Type:    types.TriggerAirflow,
				Airflow: &types.AirflowTriggerConfig{Headers: map[string]string{"X-Api-Key": "k"}},
			},
			want: map[string]string{"X-Api-Key": "k"},
		},
		{
			name: "databricks",
			tc: &types.TriggerConfig{
				Type:       types.TriggerDatabricks,
				Databricks: &types.DatabricksTriggerConfig{Headers: map[string]string{"Auth": "v"}},
			},
			want: map[string]string{"Auth": "v"},
		},
		{
			name: "http with nil config",
			tc:   &types.TriggerConfig{Type: types.TriggerHTTP},
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.tc.TriggerHeaders())
		})
	}
}

func TestIntOrDefault(t *testing.T) {
	v := 42
	assert.Equal(t, 42, types.IntOrDefault(&v, 10))
	assert.Equal(t, 10, types.IntOrDefault(nil, 10))
}
