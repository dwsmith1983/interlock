package firestore

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

// Document ID separator. Firestore does not allow "/" in document IDs,
// so we use "|" to combine PK and SK into a single document ID,
// mirroring the DynamoDB single-table PK|SK convention.
const sep = "|"

// PK/SK prefix constants (matching DynamoDB keys.go).
const (
	prefixPipeline     = "PIPELINE#"
	prefixRun          = "RUN#"
	prefixRerun        = "RERUN#"
	prefixLock         = "LOCK#"
	prefixTrait        = "TRAIT#"
	prefixRunLog       = "RUNLOG#"
	prefixEvent        = "EVENT#"
	prefixType         = "TYPE#"
	prefixMarker       = "MARKER#"
	prefixLateArrival  = "LATEARRIVAL#"
	prefixReplay       = "REPLAY#"

	skConfig    = "CONFIG"
	skReadiness = "READINESS"
	skLock      = "LOCK"
)

// docID constructs a Firestore document ID from PK and SK: "{PK}|{SK}".
func docID(pk, sk string) string { return pk + sep + sk }

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

func lateArrivalSK(date, scheduleID string, ts time.Time) string {
	return fmt.Sprintf("%s%s#%s#%d", prefixLateArrival, date, scheduleID, ts.UnixMilli())
}

func replayPK(pipelineID, date, scheduleID string) string {
	return fmt.Sprintf("%s%s#%s#%s", prefixReplay, pipelineID, date, scheduleID)
}

func replayCreatedSK(ts time.Time) string {
	return fmt.Sprintf("CREATED#%d", ts.UnixMilli())
}

func ttlEpoch(d time.Duration) int64 {
	return time.Now().Add(d).Unix()
}

func isExpired(epoch int64) bool {
	return epoch > 0 && time.Now().Unix() > epoch
}
