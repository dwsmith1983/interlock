// Package providertest provides shared conformance tests for provider.Provider
// implementations. Call RunAll from a test function to verify a provider
// satisfies the full behavioral contract.
package providertest

import (
	"testing"

	"github.com/dwsmith1983/interlock/internal/provider"
)

// RunAll runs the complete provider conformance suite as subtests.
func RunAll(t *testing.T, prov provider.Provider) {
	t.Helper()

	t.Run("PipelineCRUD", func(t *testing.T) { TestPipelineCRUD(t, prov) })
	t.Run("TraitPutGet", func(t *testing.T) { TestTraitPutGet(t, prov) })
	t.Run("TraitTTLExpiry", func(t *testing.T) { TestTraitTTLExpiry(t, prov) })
	t.Run("RunStatePutGet", func(t *testing.T) { TestRunStatePutGet(t, prov) })
	t.Run("RunStateList", func(t *testing.T) { TestRunStateList(t, prov) })
	t.Run("CompareAndSwap", func(t *testing.T) { TestCompareAndSwap(t, prov) })
	t.Run("CASRaceCondition", func(t *testing.T) { TestCASRaceCondition(t, prov) })
	t.Run("GetRunStateDirectLookup", func(t *testing.T) { TestGetRunStateDirectLookup(t, prov) })
	t.Run("ReadinessPutGet", func(t *testing.T) { TestReadinessPutGet(t, prov) })
	t.Run("RunLogCRUD", func(t *testing.T) { TestRunLogCRUD(t, prov) })
	t.Run("RerunCRUD", func(t *testing.T) { TestRerunCRUD(t, prov) })
	t.Run("Locking", func(t *testing.T) { TestLocking(t, prov) })
	t.Run("LockExpiry", func(t *testing.T) { TestLockExpiry(t, prov) })
	t.Run("EventAppendAndList", func(t *testing.T) { TestEventAppendAndList(t, prov) })
	t.Run("ReadEventsSince", func(t *testing.T) { TestReadEventsSince(t, prov) })
}
