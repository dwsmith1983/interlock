package watchdog

import (
	"context"
	"fmt"
	"time"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/store"
	"github.com/dwsmith1983/interlock/pkg/types"
)

const (
	// configInvalidReportTTL is how long one broken config version stays quiet
	// after it is reported: a config that stays broken is reported once a day.
	configInvalidReportTTL = 24 * time.Hour
	// maxConfigReportsPerPass bounds one pass's reports; rows past it get no
	// marker and are reported on a later pass.
	maxConfigReportsPerPass = 20
	// markerClearTimeout bounds the marker delete after a failed publish.
	markerClearTimeout = 5 * time.Second
)

// reportInvalidConfigs publishes CONFIG_INVALID for each CONFIG row the
// config cache rejected, once per broken version and day: the publish
// happens only when the marker put wins, and a failed publish deletes the
// marker so the next pass reports again. Put, publish and delete errors are
// logged and the other rows are still reported. Only failing to load the
// configs is an error.
func reportInvalidConfigs(ctx context.Context, d *lambda.Deps) error {
	rejected, err := d.ConfigCache.Rejected(ctx)
	if err != nil {
		return fmt.Errorf("load configs: %w", err)
	}
	reported := 0
	for i, r := range rejected {
		if reported == maxConfigReportsPerPass {
			d.Logger.WarnContext(ctx, "invalid config report limit reached, rest left for later passes",
				"limit", maxConfigReportsPerPass, "left", len(rejected)-i)
			break
		}
		marked, err := d.Store.MarkConfigInvalid(ctx, r, d.Now(), configInvalidReportTTL)
		if err != nil {
			d.Logger.ErrorContext(ctx, "failed to mark invalid config", "pk", r.PK, "error", err)
			continue
		}
		if !marked {
			continue
		}
		reported++
		if err := publishConfigInvalid(ctx, d, r); err != nil {
			d.Logger.ErrorContext(ctx, "failed to publish event", "type", types.EventConfigInvalid, "pk", r.PK, "error", err)
			clearCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), markerClearTimeout)
			if err := d.Store.ClearConfigInvalid(clearCtx, r); err != nil {
				d.Logger.ErrorContext(ctx, "failed to delete invalid config marker", "pk", r.PK, "error", err)
			}
			cancel()
		}
	}
	return nil
}

func publishConfigInvalid(ctx context.Context, d *lambda.Deps, r store.RejectedConfig) error {
	id := r.PipelineID
	if id == "" {
		id = r.PK
	}
	detail := map[string]interface{}{"reason": r.Reason}
	if r.Hash != "" {
		detail["hash"] = r.Hash
	}
	return lambda.PublishEvent(ctx, d, string(types.EventConfigInvalid), id, "", "",
		fmt.Sprintf("config for %s rejected, pipeline not running: %s", id, r.Reason), detail)
}
