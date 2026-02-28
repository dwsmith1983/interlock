package providertest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dwsmith1983/interlock/internal/provider"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// TestQuarantineCRUD verifies quarantine record put, get, and miss behaviour.
func TestQuarantineCRUD(t *testing.T, prov provider.Provider) {
	ctx := context.Background()

	// Get nonexistent record returns nil, no error.
	got, err := prov.GetQuarantineRecord(ctx, "pipe-quar-test", "2026-02-28", "14")
	require.NoError(t, err)
	assert.Nil(t, got)

	// Put a quarantine record.
	rec := types.QuarantineRecord{
		PipelineID:     "pipe-quar-test",
		Date:           "2026-02-28",
		Hour:           "14",
		Count:          7,
		QuarantinePath: "s3://bucket/quarantine/pipe-quar-test/2026-02-28/14",
		Reasons:        []string{"schema_mismatch", "null_key"},
		Timestamp:      time.Now().Truncate(time.Millisecond),
	}
	err = prov.PutQuarantineRecord(ctx, rec)
	require.NoError(t, err)

	// Get it back and verify fields.
	got, err = prov.GetQuarantineRecord(ctx, "pipe-quar-test", "2026-02-28", "14")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "pipe-quar-test", got.PipelineID)
	assert.Equal(t, "2026-02-28", got.Date)
	assert.Equal(t, "14", got.Hour)
	assert.Equal(t, 7, got.Count)
	assert.Equal(t, "s3://bucket/quarantine/pipe-quar-test/2026-02-28/14", got.QuarantinePath)
	assert.Equal(t, []string{"schema_mismatch", "null_key"}, got.Reasons)

	// Put a second record for a different date/hour.
	rec2 := types.QuarantineRecord{
		PipelineID:     "pipe-quar-test",
		Date:           "2026-03-01",
		Hour:           "09",
		Count:          3,
		QuarantinePath: "s3://bucket/quarantine/pipe-quar-test/2026-03-01/09",
		Reasons:        []string{"duplicate_row"},
		Timestamp:      time.Now().Truncate(time.Millisecond),
	}
	err = prov.PutQuarantineRecord(ctx, rec2)
	require.NoError(t, err)

	// Both records should be independently retrievable.
	got1, err := prov.GetQuarantineRecord(ctx, "pipe-quar-test", "2026-02-28", "14")
	require.NoError(t, err)
	require.NotNil(t, got1)
	assert.Equal(t, 7, got1.Count)

	got2, err := prov.GetQuarantineRecord(ctx, "pipe-quar-test", "2026-03-01", "09")
	require.NoError(t, err)
	require.NotNil(t, got2)
	assert.Equal(t, 3, got2.Count)
}
