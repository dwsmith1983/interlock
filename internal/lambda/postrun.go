package lambda

import (
	"github.com/dwsmith1983/interlock/pkg/types"
)

// defaultDriftField is the sensor field used for drift comparison when
// PostRunConfig.DriftField is not set.
const defaultDriftField = "sensor_count"

// PostRunSeededField marks a post-run baseline entry written after the run
// completed: the sensor's first value since then, not its value at completion.
const PostRunSeededField = "_seededAfterCompletion"

func ResolveDriftField(cfg *types.PostRunConfig) string {
	if cfg.DriftField != "" {
		return cfg.DriftField
	}
	return defaultDriftField
}

// MatchesPostRunRule returns true if the sensor key matches any post-run rule
// key, as that key or a per-period key of it (see SensorKeyMatches).
func MatchesPostRunRule(sensorKey string, rules []types.ValidationRule) bool {
	for _, rule := range rules {
		if SensorKeyMatches(sensorKey, rule.Key) {
			return true
		}
	}
	return false
}
