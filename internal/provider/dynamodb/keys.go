package dynamodb

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

// PK/SK prefix constants.
const (
	prefixPipeline    = "PIPELINE#"
	prefixRun         = "RUN#"
	prefixRerun       = "RERUN#"
	prefixLock        = "LOCK#"
	prefixTrait       = "TRAIT#"
	prefixRunLog      = "RUNLOG#"
	prefixEvent       = "EVENT#"
	prefixType        = "TYPE#"
	prefixAlert       = "ALERT#"
	prefixTraitHist   = "TRAITHIST#"
	prefixEvalSession = "EVALSESSION#"
	prefixDep         = "DEP#"
	prefixSensor      = "SENSOR#"
	prefixQuarantine  = "QUARANTINE#"

	skConfig    = "CONFIG"
	skReadiness = "READINESS"
	skLock      = "LOCK"
)

func pipelinePK(id string) string   { return prefixPipeline + id }
func runPK(runID string) string     { return prefixRun + runID }
func rerunPK(rerunID string) string { return prefixRerun + rerunID }
func lockPK(key string) string      { return prefixLock + key }

func configSK() string                { return skConfig }
func traitSK(traitType string) string { return prefixTrait + traitType }
func readinessSK() string             { return skReadiness }

func runListSK(createdAt time.Time, runID string) string {
	return prefixRun + createdAt.UTC().Format(time.RFC3339Nano) + "#" + runID
}

func runTruthSK(runID string) string { return prefixRun + runID }

func runLogSK(date, scheduleID string) string {
	return prefixRunLog + date + "#" + scheduleID
}

func eventSK(ts time.Time) string {
	millis := ts.UnixMilli()
	nonce := make([]byte, 4)
	_, _ = rand.Read(nonce)
	return fmt.Sprintf("%s%013d#%s", prefixEvent, millis, hex.EncodeToString(nonce))
}

func rerunSK(rerunID string) string { return prefixRerun + rerunID }
func lockSK() string                { return skLock }

func alertSK(ts time.Time) string {
	millis := ts.UnixMilli()
	nonce := make([]byte, 4)
	_, _ = rand.Read(nonce)
	return fmt.Sprintf("%s%013d#%s", prefixAlert, millis, hex.EncodeToString(nonce))
}

func traitHistSK(traitType string, ts time.Time) string {
	millis := ts.UnixMilli()
	nonce := make([]byte, 4)
	_, _ = rand.Read(nonce)
	return fmt.Sprintf("%s%s#%013d#%s", prefixTraitHist, traitType, millis, hex.EncodeToString(nonce))
}

func evalSessionSK(sessionID string, ts time.Time) string {
	return prefixEvalSession + ts.UTC().Format(time.RFC3339) + "#" + sessionID
}

func depPK(upstreamID string) string        { return prefixDep + upstreamID }
func depSK(downstreamID string) string      { return prefixDep + downstreamID }
func sensorSK(sensorType string) string     { return prefixSensor + sensorType }
func quarantineSK(date, hour string) string { return prefixQuarantine + date + "#" + hour }

func ttlEpoch(d time.Duration) int64 {
	return time.Now().Add(d).Unix()
}

func isExpired(epoch int64) bool {
	return epoch > 0 && time.Now().Unix() > epoch
}
