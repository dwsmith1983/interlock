package lambda_test

import (
	"context"
	"errors"
	"log/slog"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	schedulerTypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
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

// recordingScheduler records every schedule it is asked to create (then
// fails the create with createErr, if set) and the name of every one it
// deletes.
type recordingScheduler struct {
	created   []*scheduler.CreateScheduleInput
	deleted   []string
	createErr error
}

func (r *recordingScheduler) CreateSchedule(_ context.Context, in *scheduler.CreateScheduleInput, _ ...func(*scheduler.Options)) (*scheduler.CreateScheduleOutput, error) {
	r.created = append(r.created, in)
	if r.createErr != nil {
		return nil, r.createErr
	}
	return &scheduler.CreateScheduleOutput{}, nil
}

func (r *recordingScheduler) DeleteSchedule(_ context.Context, in *scheduler.DeleteScheduleInput, _ ...func(*scheduler.Options)) (*scheduler.DeleteScheduleOutput, error) {
	r.deleted = append(r.deleted, *in.Name)
	return &scheduler.DeleteScheduleOutput{}, nil
}

func newSchedulerDeps(sched *recordingScheduler) *lambda.Deps {
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
	sched := &recordingScheduler{}
	d := newSchedulerDeps(sched)

	err := lambda.CreateSLASchedules(context.Background(), d, maxPipelineID, "stream", "2026-03-03T10", testSLACalc, false)
	require.NoError(t, err)
	require.Len(t, sched.created, 2)

	for _, in := range sched.created {
		require.NotNil(t, in.Name)
		assertValidSchedulerName(t, *in.Name)
	}
}

// TestCreateSLASchedules_Payload pins each one-time schedule the SLA monitor
// creates: its name, its at() time in UTC, the alert it fires, and that it
// deletes itself after firing. Only the warning carries the breach time.
func TestCreateSLASchedules_Payload(t *testing.T) {
	t.Setenv("SKIP_SCHEDULER", "")
	s := &recordingScheduler{}

	err := lambda.CreateSLASchedules(context.Background(), newSchedulerDeps(s), "orders", "stream", "2026-03-03T10", testSLACalc, false)
	require.NoError(t, err)

	require.Len(t, s.created, 2)
	for i, want := range []struct{ name, expr, input string }{
		{"orders-stream-2026-03-03T10-sla-warning", "at(2026-03-03T11:45:00)",
			`{"mode":"fire-alert","pipelineId":"orders","scheduleId":"stream","date":"2026-03-03T10","alertType":"SLA_WARNING","breachAt":"2026-03-03T12:00:00Z"}`},
		{"orders-stream-2026-03-03T10-sla-breach", "at(2026-03-03T12:00:00)",
			`{"mode":"fire-alert","pipelineId":"orders","scheduleId":"stream","date":"2026-03-03T10","alertType":"SLA_BREACH"}`},
	} {
		in := s.created[i]
		require.NotNil(t, in.Name)
		require.NotNil(t, in.GroupName)
		require.NotNil(t, in.ScheduleExpression)
		require.NotNil(t, in.ScheduleExpressionTimezone)
		require.NotNil(t, in.FlexibleTimeWindow)
		require.NotNil(t, in.Target)
		require.NotNil(t, in.Target.Arn)
		require.NotNil(t, in.Target.RoleArn)
		require.NotNil(t, in.Target.Input)
		assert.Equal(t, want.name, *in.Name)
		assert.Equal(t, "interlock-sla", *in.GroupName)
		assert.Equal(t, want.expr, *in.ScheduleExpression)
		assert.Equal(t, "UTC", *in.ScheduleExpressionTimezone)
		assert.Equal(t, schedulerTypes.FlexibleTimeWindowModeOff, in.FlexibleTimeWindow.Mode)
		assert.Equal(t, schedulerTypes.ActionAfterCompletionDelete, in.ActionAfterCompletion)
		assert.Equal(t, "arn:aws:lambda:us-east-1:123:function:sla-monitor", *in.Target.Arn)
		assert.Equal(t, "arn:aws:iam::123:role/scheduler-role", *in.Target.RoleArn)
		assert.Equal(t, want.input, *in.Target.Input)
	}
}

// TestCreateSLASchedules_ErrorsAndSkip pins that the first failed create
// stops the run with the schedule's name in the error, and that
// SKIP_SCHEDULER=true creates nothing.
func TestCreateSLASchedules_ErrorsAndSkip(t *testing.T) {
	t.Run("create error", func(t *testing.T) {
		t.Setenv("SKIP_SCHEDULER", "")
		s := &recordingScheduler{createErr: errors.New("throttling exception")}

		err := lambda.CreateSLASchedules(context.Background(), newSchedulerDeps(s), "orders", "stream", "2026-03-03T10", testSLACalc, false)

		require.EqualError(t, err, `create warning schedule: create one-time schedule "orders-stream-2026-03-03T10-sla-warning": throttling exception`)
		assert.Len(t, s.created, 1, "no breach create after the warning fails")
	})
	t.Run("skip scheduler", func(t *testing.T) {
		t.Setenv("SKIP_SCHEDULER", "true")
		s := &recordingScheduler{}

		err := lambda.CreateSLASchedules(context.Background(), newSchedulerDeps(s), "orders", "stream", "2026-03-03T10", testSLACalc, false)

		require.NoError(t, err)
		assert.Empty(t, s.created)
	})
}

func TestSLACancel_DeletesTheCreatedScheduleNames(t *testing.T) {
	t.Setenv("SKIP_SCHEDULER", "")

	for _, pipelineID := range []string{"orders", maxPipelineID} {
		t.Run(pipelineID, func(t *testing.T) {
			sched := &recordingScheduler{}
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
				assert.Equal(t, *sched.created[i].Name, sched.deleted[i])
			}
		})
	}
}
