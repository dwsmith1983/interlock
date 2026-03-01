package v2

import (
	"log/slog"

	store "github.com/dwsmith1983/interlock/internal/store/v2"
	"github.com/dwsmith1983/interlock/internal/trigger"
)

// Deps holds all dependencies for v2 Lambda handlers.
type Deps struct {
	Store           *store.Store
	ConfigCache     *store.ConfigCache
	SFNClient       SFNAPI
	EventBridge     EventBridgeAPI
	TriggerRunner   *trigger.Runner
	StateMachineARN string
	EventBusName    string
	Logger          *slog.Logger
}
