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
	"github.com/dwsmith1983/interlock/pkg/validation"
)

// schedulerNameRe is the EventBridge Scheduler schedule-name pattern.
var schedulerNameRe = regexp.MustCompile(`^[0-9a-zA-Z_.-]+$`)

const maxSchedulerNameLen = 64

// maxPipelineID is the longest pipeline ID a config may have.
var maxPipelineID = strings.Repeat("x", validation.MaxPipelineIDLen)

func assertValidSchedulerName(t *testing.T, name string) {
	t.Helper()
	assert.LessOrEqual(t, len(name), maxSchedulerNameLen, "name %q is too long", name)
	assert.Regexp(t, schedulerNameRe, name)
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

	err := lambda.CreateSLASchedules(context.Background(), d, maxPipelineID, "stream", "2026-03-03T10", testSLACalc, false)
	require.NoError(t, err)
	require.Len(t, sched.created, 2)

	for _, in := range sched.created {
		require.NotNil(t, in.Name)
		assertValidSchedulerName(t, *in.Name)
	}
}

func TestSLACancel_DeletesTheCreatedScheduleNames(t *testing.T) {
	t.Setenv("SKIP_SCHEDULER", "")

	for _, pipelineID := range []string{"orders", maxPipelineID} {
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
