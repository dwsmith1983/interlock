package lambda

import (
	"log/slog"

	"github.com/dwsmith1983/interlock/internal/store"
)

// Deps holds all dependencies for Lambda handlers.
type Deps struct {
	Store           *store.Store
	ConfigCache     *store.ConfigCache
	SFNClient       SFNAPI
	EventBridge     EventBridgeAPI
	TriggerRunner   TriggerExecutor
	StatusChecker   StatusChecker
	StateMachineARN string
	EventBusName    string
	Logger          *slog.Logger
}
