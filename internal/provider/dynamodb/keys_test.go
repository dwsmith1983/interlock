package dynamodb

import (
	"strings"
	"testing"
	"time"
)

func TestPipelinePK(t *testing.T) {
	got := pipelinePK("my-pipeline")
	if got != "PIPELINE#my-pipeline" {
		t.Errorf("pipelinePK = %q, want %q", got, "PIPELINE#my-pipeline")
	}
}

func TestRunPK(t *testing.T) {
	got := runPK("run-123")
	if got != "RUN#run-123" {
		t.Errorf("runPK = %q, want %q", got, "RUN#run-123")
	}
}

func TestRerunPK(t *testing.T) {
	got := rerunPK("rerun-abc")
	if got != "RERUN#rerun-abc" {
		t.Errorf("rerunPK = %q, want %q", got, "RERUN#rerun-abc")
	}
}

func TestLockPK(t *testing.T) {
	got := lockPK("eval:pipeline:daily")
	if got != "LOCK#eval:pipeline:daily" {
		t.Errorf("lockPK = %q, want %q", got, "LOCK#eval:pipeline:daily")
	}
}

func TestConfigSK(t *testing.T) {
	got := configSK()
	if got != "CONFIG" {
		t.Errorf("configSK = %q, want %q", got, "CONFIG")
	}
}

func TestTraitSK(t *testing.T) {
	got := traitSK("source-freshness")
	if got != "TRAIT#source-freshness" {
		t.Errorf("traitSK = %q, want %q", got, "TRAIT#source-freshness")
	}
}

func TestReadinessSK(t *testing.T) {
	got := readinessSK()
	if got != "READINESS" {
		t.Errorf("readinessSK = %q, want %q", got, "READINESS")
	}
}

func TestRunLogSK(t *testing.T) {
	got := runLogSK("2025-01-15", "daily")
	want := "RUNLOG#2025-01-15#daily"
	if got != want {
		t.Errorf("runLogSK = %q, want %q", got, want)
	}
}

func TestRunListSK(t *testing.T) {
	ts := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)
	got := runListSK(ts, "run-42")
	if !strings.HasPrefix(got, "RUN#2025-01-15T10:30:00") {
		t.Errorf("runListSK prefix mismatch: %q", got)
	}
	if !strings.HasSuffix(got, "#run-42") {
		t.Errorf("runListSK suffix mismatch: %q", got)
	}
}

func TestRunTruthSK(t *testing.T) {
	got := runTruthSK("run-99")
	if got != "RUN#run-99" {
		t.Errorf("runTruthSK = %q, want %q", got, "RUN#run-99")
	}
}

func TestEventSK_Uniqueness(t *testing.T) {
	ts := time.Now()
	a := eventSK(ts)
	b := eventSK(ts)
	if a == b {
		t.Error("eventSK should produce unique values for same timestamp")
	}
	if !strings.HasPrefix(a, "EVENT#") {
		t.Errorf("eventSK should start with EVENT#, got %q", a)
	}
}

func TestRerunSK(t *testing.T) {
	got := rerunSK("rerun-1")
	if got != "RERUN#rerun-1" {
		t.Errorf("rerunSK = %q, want %q", got, "RERUN#rerun-1")
	}
}

func TestLockSK(t *testing.T) {
	got := lockSK()
	if got != "LOCK" {
		t.Errorf("lockSK = %q, want %q", got, "LOCK")
	}
}

func TestTTLEpoch(t *testing.T) {
	before := time.Now().Add(time.Hour).Unix()
	got := ttlEpoch(time.Hour)
	after := time.Now().Add(time.Hour).Unix()

	if got < before || got > after {
		t.Errorf("ttlEpoch(1h) = %d, expected between %d and %d", got, before, after)
	}
}

func TestIsExpired(t *testing.T) {
	// Zero means no TTL — not expired.
	if isExpired(0) {
		t.Error("isExpired(0) should be false")
	}

	// Epoch in the past — expired.
	past := time.Now().Add(-time.Hour).Unix()
	if !isExpired(past) {
		t.Error("isExpired(past) should be true")
	}

	// Epoch in the future — not expired.
	future := time.Now().Add(time.Hour).Unix()
	if isExpired(future) {
		t.Error("isExpired(future) should be false")
	}
}
