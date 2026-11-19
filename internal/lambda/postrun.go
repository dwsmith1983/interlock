package lambda

import (
	"strings"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// defaultDriftField is the sensor field used for drift comparison when
// PostRunConfig.DriftField is not set.
const defaultDriftField = "sensor_count"

func ResolveDriftField(cfg *types.PostRunConfig) string {
	if cfg.DriftField != "" {
		return cfg.DriftField
	}
	return defaultDriftField
}

// MatchesPostRunRule returns true if the sensor key matches any post-run rule key
// (prefix match to support per-period sensor keys).
func MatchesPostRunRule(sensorKey string, rules []types.ValidationRule) bool {
	for _, rule := range rules {
		if strings.HasPrefix(sensorKey, rule.Key) {
			return true
		}
	}
	return false
}
