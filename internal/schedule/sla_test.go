package schedule

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseSLADeadline_TimeOfDay(t *testing.T) {
	now := time.Date(2026, 2, 21, 10, 0, 0, 0, time.UTC)

	deadline, err := ParseSLADeadline("06:00", "", now)
	require.NoError(t, err)
	assert.Equal(t, 6, deadline.Hour())
	assert.Equal(t, 0, deadline.Minute())
	assert.Equal(t, 21, deadline.Day())
}

func TestParseSLADeadline_Duration(t *testing.T) {
	now := time.Date(2026, 2, 21, 10, 0, 0, 0, time.UTC)

	deadline, err := ParseSLADeadline("2h", "", now)
	require.NoError(t, err)
	assert.Equal(t, 2, deadline.Hour())
	assert.Equal(t, 0, deadline.Minute())
}

func TestParseSLADeadline_Timezone(t *testing.T) {
	now := time.Date(2026, 2, 21, 12, 0, 0, 0, time.UTC)

	deadline, err := ParseSLADeadline("06:00", "America/New_York", now)
	require.NoError(t, err)

	loc, _ := time.LoadLocation("America/New_York")
	assert.Equal(t, loc, deadline.Location())
	assert.Equal(t, 6, deadline.Hour())
}

func TestParseSLADeadline_InvalidFormat(t *testing.T) {
	_, err := ParseSLADeadline("not-a-time", "", time.Now())
	assert.Error(t, err)
}

func TestParseSLADeadline_InvalidTime(t *testing.T) {
	_, err := ParseSLADeadline("25:00", "", time.Now())
	assert.Error(t, err)
}

func TestParseSLADeadline_Empty(t *testing.T) {
	_, err := ParseSLADeadline("", "", time.Now())
	assert.Error(t, err)
}

func TestParseSLADeadline_InvalidTimezone(t *testing.T) {
	_, err := ParseSLADeadline("06:00", "Not/A/Timezone", time.Now())
	assert.Error(t, err)
}

func TestParseSLADeadline_Duration_RelativeToReference(t *testing.T) {
	now := time.Date(2026, 2, 21, 12, 0, 0, 0, time.UTC)
	ref := time.Date(2026, 2, 21, 10, 0, 0, 0, time.UTC)

	deadline, err := ParseSLADeadline("+20m", "", now, ref)
	require.NoError(t, err)
	assert.Equal(t, 10, deadline.Hour())
	assert.Equal(t, 20, deadline.Minute())
}

func TestParseSLADeadline_Duration_ZeroReference_Midnight(t *testing.T) {
	now := time.Date(2026, 2, 21, 12, 0, 0, 0, time.UTC)

	// Zero reference (default time.Time{}) → falls back to midnight.
	deadline, err := ParseSLADeadline("+20m", "", now, time.Time{})
	require.NoError(t, err)
	assert.Equal(t, 0, deadline.Hour())
	assert.Equal(t, 20, deadline.Minute())
}

func TestParseSLADeadline_TimeOfDay_IgnoresReference(t *testing.T) {
	now := time.Date(2026, 2, 21, 12, 0, 0, 0, time.UTC)
	ref := time.Date(2026, 2, 21, 10, 0, 0, 0, time.UTC)

	// HH:MM format should ignore the reference time entirely.
	deadline, err := ParseSLADeadline("14:00", "", now, ref)
	require.NoError(t, err)
	assert.Equal(t, 14, deadline.Hour())
	assert.Equal(t, 0, deadline.Minute())
}

func TestIsBreached(t *testing.T) {
	deadline := time.Date(2026, 2, 21, 6, 0, 0, 0, time.UTC)

	assert.True(t, IsBreached(deadline, time.Date(2026, 2, 21, 7, 0, 0, 0, time.UTC)))
	assert.False(t, IsBreached(deadline, time.Date(2026, 2, 21, 5, 0, 0, 0, time.UTC)))
	assert.False(t, IsBreached(deadline, deadline))
}

func TestIsAtRisk(t *testing.T) {
	deadline := time.Date(2026, 2, 21, 10, 0, 0, 0, time.UTC)
	lead := 5 * time.Minute

	// Within at-risk window (4 minutes before deadline).
	assert.True(t, IsAtRisk(deadline, time.Date(2026, 2, 21, 9, 56, 0, 0, time.UTC), lead))

	// Not yet at-risk (10 minutes before deadline, lead is 5m).
	assert.False(t, IsAtRisk(deadline, time.Date(2026, 2, 21, 9, 50, 0, 0, time.UTC), lead))

	// Already breached (past deadline).
	assert.False(t, IsAtRisk(deadline, time.Date(2026, 2, 21, 10, 1, 0, 0, time.UTC), lead))

	// Exactly at deadline boundary — still at-risk (not yet breached).
	assert.True(t, IsAtRisk(deadline, deadline, lead))

	// Zero lead time — feature disabled.
	assert.False(t, IsAtRisk(deadline, time.Date(2026, 2, 21, 9, 58, 0, 0, time.UTC), 0))

	// Negative lead time — feature disabled.
	assert.False(t, IsAtRisk(deadline, time.Date(2026, 2, 21, 9, 58, 0, 0, time.UTC), -1*time.Minute))
}
