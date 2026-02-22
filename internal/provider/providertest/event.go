package providertest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dwsmith1983/interlock/internal/provider"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// TestEventAppendAndList verifies appending events and listing in chronological order.
func TestEventAppendAndList(t *testing.T, prov provider.Provider) {
	ctx := context.Background()

	now := time.Now()
	for i := 0; i < 5; i++ {
		ev := types.Event{
			Kind:       types.EventTraitEvaluated,
			PipelineID: "ct-event-al",
			TraitType:  fmt.Sprintf("trait-%d", i),
			Status:     "PASS",
			Timestamp:  now.Add(time.Duration(i) * time.Second),
		}
		err := prov.AppendEvent(ctx, ev)
		require.NoError(t, err)
		// Small delay to ensure unique event ordering
		time.Sleep(5 * time.Millisecond)
	}

	events, err := prov.ListEvents(ctx, "ct-event-al", 10)
	require.NoError(t, err)
	assert.Len(t, events, 5)
	// Chronological: oldest first
	assert.Equal(t, "trait-0", events[0].TraitType)
	assert.Equal(t, "trait-4", events[4].TraitType)
}

// TestReadEventsSince verifies cursor-based event reading.
func TestReadEventsSince(t *testing.T, prov provider.Provider) {
	ctx := context.Background()

	now := time.Now()
	for i := 0; i < 5; i++ {
		ev := types.Event{
			Kind:       types.EventRunStateChanged,
			PipelineID: "ct-event-rs",
			RunID:      fmt.Sprintf("ct-ev-run-%d", i),
			Status:     string(types.RunCompleted),
			Timestamp:  now.Add(time.Duration(i) * time.Second),
		}
		err := prov.AppendEvent(ctx, ev)
		require.NoError(t, err)
		time.Sleep(5 * time.Millisecond)
	}

	// Read all from beginning
	records, err := prov.ReadEventsSince(ctx, "ct-event-rs", "", 100)
	require.NoError(t, err)
	assert.Len(t, records, 5)

	// Read from cursor (after 2nd event)
	cursor := records[1].StreamID
	records, err = prov.ReadEventsSince(ctx, "ct-event-rs", cursor, 100)
	require.NoError(t, err)
	assert.Len(t, records, 3)
	assert.Equal(t, "ct-ev-run-2", records[0].Event.RunID)

	// Read with "0-0" cursor
	records, err = prov.ReadEventsSince(ctx, "ct-event-rs", "0-0", 100)
	require.NoError(t, err)
	assert.Len(t, records, 5)
}
