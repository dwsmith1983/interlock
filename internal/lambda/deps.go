package lambda

import (
	"log/slog"
	"time"

	"github.com/dwsmith1983/interlock/internal/store"
)

// Deps holds all dependencies for Lambda handlers.
type Deps struct {
	Store              *store.Store
	ConfigCache        *store.ConfigCache
	SFNClient          SFNAPI
	EventBridge        EventBridgeAPI
	Scheduler          SchedulerAPI
	TriggerRunner      TriggerExecutor
	StatusChecker      StatusChecker
	HTTPClient         HTTPDoer
	StateMachineARN    string
	EventBusName       string
	SLAMonitorARN      string // target ARN for Scheduler to invoke
	SchedulerRoleARN   string // execution role for Scheduler
	SchedulerGroupName string // EventBridge Scheduler group name
	SlackBotToken      string
	SlackChannelID     string
	EventsTTLDays      int
	StartedAt          time.Time        // set at Lambda cold start; watchdog skips pre-start schedules
	NowFunc            func() time.Time // returns current time; defaults to time.Now when nil
	Logger             *slog.Logger
}

// Now returns the current time using NowFunc if set, otherwise time.Now.
func (d *Deps) Now() time.Time {
	if d.NowFunc != nil {
		return d.NowFunc()
	}
	return time.Now()
}
