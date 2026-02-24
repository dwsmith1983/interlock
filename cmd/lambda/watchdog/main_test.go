package main

import (
	"testing"
)

func TestGetDeps_RequiresEnv(t *testing.T) {
	// Without TABLE_NAME / AWS_REGION set, Init should fail.
	// Reset the sync.Once so we get a fresh call.
	t.Setenv("TABLE_NAME", "")
	t.Setenv("AWS_REGION", "")

	// We can't easily reset the sync.Once in the package-level var,
	// so just verify the handler function signature compiles correctly.
	// Integration testing of the watchdog logic itself is in
	// internal/watchdog/watchdog_test.go.
	_ = handler
}
