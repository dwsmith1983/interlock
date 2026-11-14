package lambda_test

import (
	"context"
	"log/slog"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/lambda/sla"
)

// schedulerNameRe is the EventBridge Scheduler schedule-name pattern.
var schedulerNameRe = regexp.MustCompile(`^[0-9a-zA-Z_.-]+$`)

const maxSchedulerNameLen = 64

// longPipelineID is a 70-character pipeline ID, long enough that the raw
// schedule name exceeds the Scheduler limit.
var longPipelineID = "team-" + strings.Repeat("x", 65)

func assertValidSchedulerName(t *testing.T, name string) {
	t.Helper()
	assert.LessOrEqual(t, len(name), maxSchedulerNameLen, "name %q is too long", name)
	assert.Regexp(t, schedulerNameRe, name)
}

func TestSLAScheduleName(t *testing.T) {
	require.Len(t, longPipelineID, 70)

	tests := []struct {
		name       string
		pipelineID string
		scheduleID string
		date       string
		suffix     string
		check      func(t *testing.T, got string)
	}{
		{
			name:       "short valid input is unchanged",
			pipelineID: "orders",
			scheduleID: "stream",
			date:       "2026-03-03",
			suffix:     "breach",
			check: func(t *testing.T, got string) {
				assert.Equal(t, "orders-stream-2026-03-03-sla-breach", got)
			},
		},
		{
			name:       "long pipeline ID is capped and keeps the tail",
			pipelineID: longPipelineID,
			scheduleID: "stream",
			date:       "2026-03-03T10",
			suffix:     "breach",
			check: func(t *testing.T, got string) {
				assert.Len(t, got, maxSchedulerNameLen)
				assert.Regexp(t, schedulerNameRe, got)
				assert.True(t, strings.HasSuffix(got, "-2026-03-03T10-sla-breach"), "got %q", got)
				again := lambda.SLAScheduleName(longPipelineID, "stream", "2026-03-03T10", "breach")
				assert.Equal(t, got, again, "name must be deterministic")
				// Golden value: the name persists across deploys, so pin it.
				assert.Equal(t, "95c14ed3-"+strings.Repeat("x", 23)+"-stream-2026-03-03T10-sla-breach", got)
			},
		},
		{
			name:       "invalid characters are sanitized",
			pipelineID: "team/orders:v2",
			scheduleID: "stream",
			date:       "2026-03-03",
			suffix:     "warning",
			check: func(t *testing.T, got string) {
				assertValidSchedulerName(t, got)
				assert.Equal(t, "2f3142e2-team-orders-v2-stream-2026-03-03-sla-warning", got)
			},
		},
		{
			// "-stream-2026-03-03-sla-breach" is 29 characters, so a
			// 35-character pipeline ID makes raw exactly 64.
			name:       "valid raw name of exactly 64 characters is unchanged",
			pipelineID: strings.Repeat("p", 35),
			scheduleID: "stream",
			date:       "2026-03-03",
			suffix:     "breach",
			check: func(t *testing.T, got string) {
				want := strings.Repeat("p", 35) + "-stream-2026-03-03-sla-breach"
				require.Len(t, want, 64)
				assert.Equal(t, want, got)
			},
		},
		{
			name:       "valid raw name of 65 characters is hashed to exactly 64",
			pipelineID: strings.Repeat("p", 36),
			scheduleID: "stream",
			date:       "2026-03-03",
			suffix:     "breach",
			check: func(t *testing.T, got string) {
				raw := strings.Repeat("p", 36) + "-stream-2026-03-03-sla-breach"
				require.Len(t, raw, 65)
				assert.Len(t, got, maxSchedulerNameLen)
				assert.Regexp(t, schedulerNameRe, got)
				assert.NotEqual(t, raw, got)
				assert.Equal(t, "-"+raw[len(raw)-55:], got[8:], "hash prefix then the last 55 characters")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := lambda.SLAScheduleName(tt.pipelineID, tt.scheduleID, tt.date, tt.suffix)
			tt.check(t, got)
		})
	}

	t.Run("sanitized inputs that differ do not collide", func(t *testing.T) {
		slash := lambda.SLAScheduleName("a/b", "stream", "2026-03-03", "breach")
		colon := lambda.SLAScheduleName("a:b", "stream", "2026-03-03", "breach")
		assertValidSchedulerName(t, slash)
		assertValidSchedulerName(t, colon)
		assert.NotEqual(t, slash, colon)
	})
}

func newSchedulerDeps(sched *mockScheduler) *lambda.Deps {
	return &lambda.Deps{
		Scheduler:          sched,
		SLAMonitorARN:      "arn:aws:lambda:us-east-1:123:function:sla-monitor",
		SchedulerRoleARN:   "arn:aws:iam::123:role/scheduler-role",
		SchedulerGroupName: "interlock-sla",
		Logger:             slog.Default(),
		NowFunc:            func() time.Time { return time.Date(2026, 3, 3, 9, 0, 0, 0, time.UTC) },
	}
}

var testSLACalc = lambda.SLAMonitorOutput{
	WarningAt: "2026-03-03T11:45:00Z",
	BreachAt:  "2026-03-03T12:00:00Z",
}

func TestCreateSLASchedules_NameShape(t *testing.T) {
	t.Setenv("SKIP_SCHEDULER", "")
	sched := &mockScheduler{}
	d := newSchedulerDeps(sched)

	err := lambda.CreateSLASchedules(context.Background(), d, longPipelineID, "stream", "2026-03-03T10", testSLACalc, false)
	require.NoError(t, err)
	require.Len(t, sched.created, 2)

	for _, in := range sched.created {
		require.NotNil(t, in.Name)
		assertValidSchedulerName(t, *in.Name)
	}
}

func TestSLACancel_DeletesTheCreatedScheduleNames(t *testing.T) {
	t.Setenv("SKIP_SCHEDULER", "")

	for _, pipelineID := range []string{"orders", longPipelineID, "team/orders:v2"} {
		t.Run(pipelineID, func(t *testing.T) {
			sched := &mockScheduler{}
			d := newSchedulerDeps(sched)

			err := lambda.CreateSLASchedules(context.Background(), d, pipelineID, "stream", "2026-03-03T10", testSLACalc, false)
			require.NoError(t, err)

			_, err = sla.HandleSLAMonitor(context.Background(), d, lambda.SLAMonitorInput{
				Mode:       "cancel",
				PipelineID: pipelineID,
				ScheduleID: "stream",
				Date:       "2026-03-03T10",
				WarningAt:  testSLACalc.WarningAt,
				BreachAt:   testSLACalc.BreachAt,
			})
			require.NoError(t, err)

			require.Len(t, sched.created, 2)
			require.Len(t, sched.deleted, 2)
			for i := range sched.created {
				assert.Equal(t, *sched.created[i].Name, *sched.deleted[i].Name)
			}
		})
	}
}
