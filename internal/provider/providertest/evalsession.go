package providertest

import (
	"context"
	"testing"
	"time"

	"github.com/dwsmith1983/interlock/internal/provider"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// TestEvaluationSessionCRUD validates evaluation session storage.
func TestEvaluationSessionCRUD(t *testing.T, prov provider.Provider) {
	ctx := context.Background()

	now := time.Now()
	s := types.EvaluationSession{
		SessionID:     "sess-001",
		PipelineID:    "pipe-session-test",
		TriggerSource: "watcher",
		Status:        types.SessionCompleted,
		Readiness:     types.Ready,
		StartedAt:     now.Add(-10 * time.Second),
		TraitResults: []types.TraitEvaluation{
			{
				PipelineID:  "pipe-session-test",
				TraitType:   "upstream-job-log",
				Status:      types.TraitPass,
				EvaluatedAt: now.Add(-5 * time.Second),
			},
		},
	}
	completed := now
	s.CompletedAt = &completed

	if err := prov.PutEvaluationSession(ctx, s); err != nil {
		t.Fatalf("PutEvaluationSession: %v", err)
	}

	// Get by ID.
	got, err := prov.GetEvaluationSession(ctx, "sess-001")
	if err != nil {
		t.Fatalf("GetEvaluationSession: %v", err)
	}
	if got == nil {
		t.Fatal("expected session, got nil")
	}
	if got.SessionID != "sess-001" {
		t.Errorf("SessionID = %q, want %q", got.SessionID, "sess-001")
	}
	if got.Status != types.SessionCompleted {
		t.Errorf("Status = %q, want %q", got.Status, types.SessionCompleted)
	}

	// List by pipeline.
	sessions, err := prov.ListEvaluationSessions(ctx, "pipe-session-test", 10)
	if err != nil {
		t.Fatalf("ListEvaluationSessions: %v", err)
	}
	if len(sessions) != 1 {
		t.Errorf("expected 1 session, got %d", len(sessions))
	}

	// Get nonexistent.
	missing, err := prov.GetEvaluationSession(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("GetEvaluationSession nonexistent: %v", err)
	}
	if missing != nil {
		t.Error("expected nil for nonexistent session")
	}
}
