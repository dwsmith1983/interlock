package lambda

import (
	"log/slog"

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
	Logger             *slog.Logger
}
