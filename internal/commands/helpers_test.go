package commands

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/dwsmith1983/interlock/pkg/types"
)

func TestNewProvider_Redis(t *testing.T) {
	cfg := &types.ProjectConfig{
		Provider: "redis",
		Redis:    &types.RedisConfig{Addr: "localhost:6379"},
	}
	p, err := newProvider(cfg)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if p == nil {
		t.Fatal("expected non-nil provider")
	}
}

func TestNewProvider_Unknown(t *testing.T) {
	cfg := &types.ProjectConfig{Provider: "etcd"}
	_, err := newProvider(cfg)
	if err == nil {
		t.Fatal("expected error for unknown provider")
	}
}

func TestLoadPipelineDir_Valid(t *testing.T) {
	dir := t.TempDir()

	data := []byte("name: test-pipeline\narchetype: batch-ingestion\n")
	if err := os.WriteFile(filepath.Join(dir, "test.yaml"), data, 0o644); err != nil {
		t.Fatal(err)
	}

	pipelines, err := loadPipelineDir(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pipelines) != 1 {
		t.Fatalf("expected 1 pipeline, got %d", len(pipelines))
	}
	if pipelines[0].Name != "test-pipeline" {
		t.Errorf("expected name 'test-pipeline', got %q", pipelines[0].Name)
	}
}

func TestLoadPipelineDir_EmptyDir(t *testing.T) {
	dir := t.TempDir()
	pipelines, err := loadPipelineDir(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pipelines) != 0 {
		t.Fatalf("expected 0 pipelines, got %d", len(pipelines))
	}
}

func TestLoadPipelineDir_MissingDir(t *testing.T) {
	pipelines, err := loadPipelineDir("/nonexistent/path/xyzzy")
	if err != nil {
		t.Fatalf("expected nil error for missing dir, got %v", err)
	}
	if pipelines != nil {
		t.Fatalf("expected nil pipelines, got %v", pipelines)
	}
}

func TestLoadPipelineDir_InvalidYAML(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "bad.yaml"), []byte(":\n  :\n  - [invalid"), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := loadPipelineDir(dir)
	if err == nil {
		t.Fatal("expected error for invalid YAML")
	}
}

func TestLoadPipelineDir_EmptyNameSkipped(t *testing.T) {
	dir := t.TempDir()
	// A valid YAML file with no name field should be skipped
	data := []byte("archetype: batch-ingestion\n")
	if err := os.WriteFile(filepath.Join(dir, "noname.yaml"), data, 0o644); err != nil {
		t.Fatal(err)
	}

	pipelines, err := loadPipelineDir(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pipelines) != 0 {
		t.Fatalf("expected 0 pipelines (empty name skipped), got %d", len(pipelines))
	}
}

func TestLoadPipelineDir_NonYAMLFilesSkipped(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "readme.txt"), []byte("not yaml"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "data.json"), []byte(`{"foo":"bar"}`), 0o644); err != nil {
		t.Fatal(err)
	}

	pipelines, err := loadPipelineDir(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pipelines) != 0 {
		t.Fatalf("expected 0 pipelines (non-yaml skipped), got %d", len(pipelines))
	}
}

func TestLoadPipelineDir_YMLExtension(t *testing.T) {
	dir := t.TempDir()
	data := []byte("name: yml-pipeline\n")
	if err := os.WriteFile(filepath.Join(dir, "test.yml"), data, 0o644); err != nil {
		t.Fatal(err)
	}

	pipelines, err := loadPipelineDir(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pipelines) != 1 {
		t.Fatalf("expected 1 pipeline, got %d", len(pipelines))
	}
}

func TestLoadPipelineDir_SubdirectoriesSkipped(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "subdir"), 0o755); err != nil {
		t.Fatal(err)
	}

	pipelines, err := loadPipelineDir(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pipelines) != 0 {
		t.Fatalf("expected 0 pipelines, got %d", len(pipelines))
	}
}
