package cascade

import (
	"context"
	"fmt"
	"log/slog"
	"testing"

	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockProvider is a minimal in-memory implementation of cascade.Provider.
type mockProvider struct {
	pipelines  map[string]types.PipelineConfig
	dependents map[string][]string // upstream -> downstream IDs
	markers    []markerRecord
	// Error injection
	listDependentsErr error
	listPipelinesErr  error
}

type markerRecord struct {
	PipelineID, ScheduleID, Date, Source string
}

func newMockProvider() *mockProvider {
	return &mockProvider{
		pipelines:  make(map[string]types.PipelineConfig),
		dependents: make(map[string][]string),
	}
}

func (m *mockProvider) ListDependents(_ context.Context, upstreamID string) ([]string, error) {
	if m.listDependentsErr != nil {
		return nil, m.listDependentsErr
	}
	return m.dependents[upstreamID], nil
}

func (m *mockProvider) ListPipelines(_ context.Context) ([]types.PipelineConfig, error) {
	if m.listPipelinesErr != nil {
		return nil, m.listPipelinesErr
	}
	var out []types.PipelineConfig
	for _, p := range m.pipelines {
		out = append(out, p)
	}
	return out, nil
}

func (m *mockProvider) GetPipeline(_ context.Context, id string) (*types.PipelineConfig, error) {
	p, ok := m.pipelines[id]
	if !ok {
		return nil, fmt.Errorf("pipeline %q not found", id)
	}
	return &p, nil
}

func (m *mockProvider) WriteCascadeMarker(_ context.Context, pipelineID, scheduleID, date, source string) error {
	m.markers = append(m.markers, markerRecord{pipelineID, scheduleID, date, source})
	return nil
}

func TestScanDependents_FindsDownstream(t *testing.T) {
	prov := newMockProvider()
	prov.pipelines["pipe-a"] = types.PipelineConfig{Name: "pipe-a"}
	prov.pipelines["pipe-b"] = types.PipelineConfig{
		Name: "pipe-b",
		Traits: map[string]types.TraitConfig{
			"upstream": {Config: map[string]interface{}{"upstreamPipeline": "pipe-a"}},
		},
	}
	prov.pipelines["pipe-c"] = types.PipelineConfig{Name: "pipe-c"}

	deps, err := ScanDependents(context.Background(), prov, "pipe-a")
	require.NoError(t, err)
	assert.Equal(t, []string{"pipe-b"}, deps)
}

func TestScanDependents_NoDownstream(t *testing.T) {
	prov := newMockProvider()
	prov.pipelines["pipe-a"] = types.PipelineConfig{Name: "pipe-a"}
	prov.pipelines["pipe-b"] = types.PipelineConfig{Name: "pipe-b"}

	deps, err := ScanDependents(context.Background(), prov, "pipe-a")
	require.NoError(t, err)
	assert.Empty(t, deps)
}

func TestScanDependents_ListPipelinesError(t *testing.T) {
	prov := newMockProvider()
	prov.listPipelinesErr = fmt.Errorf("storage unavailable")

	_, err := ScanDependents(context.Background(), prov, "pipe-a")
	assert.Error(t, err)
}

func TestNotifyDownstream_UsesIndex(t *testing.T) {
	prov := newMockProvider()
	prov.pipelines["pipe-a"] = types.PipelineConfig{Name: "pipe-a"}
	prov.dependents["pipe-a"] = []string{"pipe-b"}

	notified, err := NotifyDownstream(context.Background(), prov, "pipe-a", "daily", "2026-02-28", slog.Default())
	require.NoError(t, err)
	assert.Equal(t, []string{"pipe-b/daily"}, notified)
	require.Len(t, prov.markers, 1)
	assert.Equal(t, "pipe-b", prov.markers[0].PipelineID)
	assert.Equal(t, "pipe-a", prov.markers[0].Source)
}

func TestNotifyDownstream_FallsBackToScan(t *testing.T) {
	prov := newMockProvider()
	prov.pipelines["pipe-a"] = types.PipelineConfig{Name: "pipe-a"}
	prov.pipelines["pipe-b"] = types.PipelineConfig{
		Name: "pipe-b",
		Traits: map[string]types.TraitConfig{
			"upstream": {Config: map[string]interface{}{"upstreamPipeline": "pipe-a"}},
		},
	}
	// dependents index is empty (bootstrap)

	notified, err := NotifyDownstream(context.Background(), prov, "pipe-a", "daily", "2026-02-28", slog.Default())
	require.NoError(t, err)
	assert.Equal(t, []string{"pipe-b/daily"}, notified)
}

func TestNotifyDownstream_NoDownstream(t *testing.T) {
	prov := newMockProvider()
	prov.pipelines["pipe-a"] = types.PipelineConfig{Name: "pipe-a"}

	notified, err := NotifyDownstream(context.Background(), prov, "pipe-a", "daily", "2026-02-28", slog.Default())
	require.NoError(t, err)
	assert.Nil(t, notified)
}

func TestNotifyDownstream_ScanError(t *testing.T) {
	prov := newMockProvider()
	prov.listPipelinesErr = fmt.Errorf("dynamodb unavailable")

	_, err := NotifyDownstream(context.Background(), prov, "pipe-a", "daily", "2026-02-28", slog.Default())
	assert.Error(t, err)
}
