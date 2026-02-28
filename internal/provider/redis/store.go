package redis

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/dwsmith1983/interlock/internal/lifecycle"
	"github.com/dwsmith1983/interlock/internal/provider"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// runKeyTTL returns the TTL for a run-related key based on status.
// Terminal runs get the configured retention TTL; active runs get an extra 24h buffer.
func (p *RedisProvider) runKeyTTL(status types.RunStatus) time.Duration {
	if lifecycle.IsTerminal(status) {
		return p.retentionTTL
	}
	return p.retentionTTL + 24*time.Hour
}

// SCAN batch size and sorted-set trim limits for Redis indexes.
const (
	scanBatchSize         = 100
	defaultRunLogIndexMax = 100
	defaultRerunIndexMax  = 100
	defaultRerunGlobalMax = 500
)

func (p *RedisProvider) pipelineKey(id string) string {
	return p.prefix + "pipeline:" + id
}

func (p *RedisProvider) traitKey(pipelineID, traitType string) string {
	return p.prefix + "trait:" + pipelineID + ":" + traitType
}

func (p *RedisProvider) traitPattern(pipelineID string) string {
	return p.prefix + "trait:" + pipelineID + ":*"
}

func (p *RedisProvider) traitHistKey(pipelineID, traitType string) string {
	return p.prefix + "traithist:" + pipelineID + ":" + traitType
}

// ListTraitHistory returns historical trait evaluations, newest first.
func (p *RedisProvider) ListTraitHistory(ctx context.Context, pipelineID, traitType string, limit int) ([]types.TraitEvaluation, error) {
	if limit <= 0 {
		limit = 20
	}
	members, err := p.client.ZRangeArgs(ctx, goredis.ZRangeArgs{
		Key:   p.traitHistKey(pipelineID, traitType),
		Start: 0,
		Stop:  int64(limit - 1),
		Rev:   true,
	}).Result()
	if err != nil {
		return nil, err
	}

	var evals []types.TraitEvaluation
	for _, m := range members {
		var te types.TraitEvaluation
		if err := json.Unmarshal([]byte(m), &te); err != nil {
			p.logger.Warn("skipping corrupt trait history entry", "error", err)
			continue
		}
		evals = append(evals, te)
	}
	return evals, nil
}

func (p *RedisProvider) runKey(runID string) string {
	return p.prefix + "run:" + runID
}

func (p *RedisProvider) runIndexKey(pipelineID string) string {
	return p.prefix + "runs:" + pipelineID
}

func (p *RedisProvider) readinessKey(pipelineID string) string {
	return p.prefix + "readiness:" + pipelineID
}

func (p *RedisProvider) pipelineIndexKey() string {
	return p.prefix + "pipelines"
}

// RegisterPipeline stores a pipeline configuration.
func (p *RedisProvider) RegisterPipeline(ctx context.Context, config types.PipelineConfig) error {
	data, err := json.Marshal(config)
	if err != nil {
		return fmt.Errorf("marshaling pipeline: %w", err)
	}

	pipe := p.client.Pipeline()
	pipe.Set(ctx, p.pipelineKey(config.Name), data, 0)
	pipe.SAdd(ctx, p.pipelineIndexKey(), config.Name)
	_, err = pipe.Exec(ctx)
	if err != nil {
		return err
	}

	// Sync dependency index.
	for _, upstream := range provider.ExtractUpstreams(&config) {
		if err := p.PutDependency(ctx, upstream, config.Name); err != nil {
			p.logger.Warn("failed to sync dependency", "upstream", upstream, "downstream", config.Name, "error", err)
		}
	}
	return nil
}

// GetPipeline retrieves a pipeline configuration.
func (p *RedisProvider) GetPipeline(ctx context.Context, id string) (*types.PipelineConfig, error) {
	data, err := p.client.Get(ctx, p.pipelineKey(id)).Bytes()
	if errors.Is(err, goredis.Nil) {
		return nil, fmt.Errorf("pipeline %q not found", id)
	}
	if err != nil {
		return nil, err
	}

	var config types.PipelineConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, err
	}
	return &config, nil
}

// ListPipelines returns all registered pipelines.
func (p *RedisProvider) ListPipelines(ctx context.Context) ([]types.PipelineConfig, error) {
	names, err := p.client.SMembers(ctx, p.pipelineIndexKey()).Result()
	if err != nil {
		return nil, err
	}

	var pipelines []types.PipelineConfig
	for _, name := range names {
		pc, err := p.GetPipeline(ctx, name)
		if err != nil {
			p.logger.Warn("skipping corrupt pipeline entry", "name", name, "error", err)
			continue
		}
		pipelines = append(pipelines, *pc)
	}
	return pipelines, nil
}

// DeletePipeline removes a pipeline configuration and cleans up its dependency index.
func (p *RedisProvider) DeletePipeline(ctx context.Context, id string) error {
	// Load config first to clean up dependencies.
	config, err := p.GetPipeline(ctx, id)
	if err == nil && config != nil {
		for _, upstream := range provider.ExtractUpstreams(config) {
			if err := p.RemoveDependency(ctx, upstream, id); err != nil {
				p.logger.Warn("failed to clean dependency", "upstream", upstream, "downstream", id, "error", err)
			}
		}
	}

	pipe := p.client.Pipeline()
	pipe.Del(ctx, p.pipelineKey(id))
	pipe.SRem(ctx, p.pipelineIndexKey(), id)
	_, err = pipe.Exec(ctx)
	return err
}

// PutTrait stores a trait evaluation result with TTL, and appends to trait history.
func (p *RedisProvider) PutTrait(ctx context.Context, pipelineID string, trait types.TraitEvaluation, ttl time.Duration) error {
	data, err := json.Marshal(trait)
	if err != nil {
		return err
	}
	if ttl <= 0 {
		err = p.client.Set(ctx, p.traitKey(pipelineID, trait.TraitType), data, 0).Err()
	} else {
		err = p.client.Set(ctx, p.traitKey(pipelineID, trait.TraitType), data, ttl).Err()
	}
	if err != nil {
		return err
	}

	// Dual-write: append history record (best-effort).
	histKey := p.traitHistKey(pipelineID, trait.TraitType)
	score := float64(trait.EvaluatedAt.UnixMilli())
	if err := p.client.ZAdd(ctx, histKey, goredis.Z{Score: score, Member: string(data)}).Err(); err != nil {
		p.logger.Warn("failed to write trait history", "pipeline", pipelineID, "trait", trait.TraitType, "error", err)
	}
	// Trim to keep history bounded.
	p.client.ZRemRangeByRank(ctx, histKey, 0, -101) // keep last 100

	return nil
}

// GetTrait retrieves a single trait evaluation.
func (p *RedisProvider) GetTrait(ctx context.Context, pipelineID, traitType string) (*types.TraitEvaluation, error) {
	data, err := p.client.Get(ctx, p.traitKey(pipelineID, traitType)).Bytes()
	if errors.Is(err, goredis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var te types.TraitEvaluation
	if err := json.Unmarshal(data, &te); err != nil {
		return nil, err
	}
	return &te, nil
}

// GetTraits retrieves all trait evaluations for a pipeline.
func (p *RedisProvider) GetTraits(ctx context.Context, pipelineID string) ([]types.TraitEvaluation, error) {
	var cursor uint64
	var traits []types.TraitEvaluation
	pattern := p.traitPattern(pipelineID)

	for {
		keys, nextCursor, err := p.client.Scan(ctx, cursor, pattern, scanBatchSize).Result()
		if err != nil {
			return nil, err
		}

		for _, key := range keys {
			data, err := p.client.Get(ctx, key).Bytes()
			if err != nil {
				p.logger.Warn("skipping unreadable trait key", "key", key, "error", err)
				continue
			}
			var te types.TraitEvaluation
			if err := json.Unmarshal(data, &te); err != nil {
				p.logger.Warn("skipping corrupt trait data", "key", key, "error", err)
				continue
			}
			traits = append(traits, te)
		}

		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}

	return traits, nil
}

// PutRunState stores a run state.
func (p *RedisProvider) PutRunState(ctx context.Context, run types.RunState) error {
	data, err := json.Marshal(run)
	if err != nil {
		return err
	}

	pipe := p.client.Pipeline()
	pipe.Set(ctx, p.runKey(run.RunID), data, p.runKeyTTL(run.Status))
	pipe.LPush(ctx, p.runIndexKey(run.PipelineID), run.RunID)
	pipe.LTrim(ctx, p.runIndexKey(run.PipelineID), 0, p.runIndexLimit-1)
	_, err = pipe.Exec(ctx)
	return err
}

// GetRunState retrieves a run state.
func (p *RedisProvider) GetRunState(ctx context.Context, runID string) (*types.RunState, error) {
	data, err := p.client.Get(ctx, p.runKey(runID)).Bytes()
	if errors.Is(err, goredis.Nil) {
		return nil, fmt.Errorf("run %q not found", runID)
	}
	if err != nil {
		return nil, err
	}

	var run types.RunState
	if err := json.Unmarshal(data, &run); err != nil {
		return nil, err
	}
	return &run, nil
}

// ListRuns returns recent runs for a pipeline.
func (p *RedisProvider) ListRuns(ctx context.Context, pipelineID string, limit int) ([]types.RunState, error) {
	if limit <= 0 {
		limit = 10
	}
	ids, err := p.client.LRange(ctx, p.runIndexKey(pipelineID), 0, int64(limit-1)).Result()
	if err != nil {
		return nil, err
	}

	var runs []types.RunState
	for _, id := range ids {
		run, err := p.GetRunState(ctx, id)
		if err != nil {
			p.logger.Warn("skipping unreadable run", "runID", id, "error", err)
			continue
		}
		runs = append(runs, *run)
	}
	return runs, nil
}

// CompareAndSwapRunState atomically updates a run state if the version matches.
func (p *RedisProvider) CompareAndSwapRunState(ctx context.Context, runID string, expectedVersion int, newState types.RunState) (bool, error) {
	data, err := json.Marshal(newState)
	if err != nil {
		return false, err
	}

	ttlMs := p.runKeyTTL(newState.Status).Milliseconds()
	result, err := p.casScript.Run(ctx, p.client, []string{p.runKey(runID)}, expectedVersion, string(data), ttlMs).Int()
	if err != nil {
		return false, err
	}
	return result == 1, nil
}

// PutReadiness stores a readiness result.
func (p *RedisProvider) PutReadiness(ctx context.Context, result types.ReadinessResult) error {
	data, err := json.Marshal(result)
	if err != nil {
		return err
	}
	return p.client.Set(ctx, p.readinessKey(result.PipelineID), data, p.readinessTTL).Err()
}

// GetReadiness retrieves a cached readiness result.
func (p *RedisProvider) GetReadiness(ctx context.Context, pipelineID string) (*types.ReadinessResult, error) {
	data, err := p.client.Get(ctx, p.readinessKey(pipelineID)).Bytes()
	if errors.Is(err, goredis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var result types.ReadinessResult
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (p *RedisProvider) runLogKey(pipelineID, date, scheduleID string) string {
	if scheduleID == "" {
		scheduleID = types.DefaultScheduleID
	}
	return p.prefix + "runlog:" + pipelineID + ":" + date + ":" + scheduleID
}

func (p *RedisProvider) runLogIndexKey(pipelineID string) string {
	return p.prefix + "runlogs:" + pipelineID
}

func (p *RedisProvider) lockKey(key string) string {
	return p.prefix + "lock:" + key
}

// PutRunLog stores a run log entry.
func (p *RedisProvider) PutRunLog(ctx context.Context, entry types.RunLogEntry) error {
	if entry.ScheduleID == "" {
		entry.ScheduleID = types.DefaultScheduleID
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	indexMember := entry.Date + ":" + entry.ScheduleID
	pipe := p.client.Pipeline()
	pipe.Set(ctx, p.runLogKey(entry.PipelineID, entry.Date, entry.ScheduleID), data, p.retentionTTL)
	pipe.ZAdd(ctx, p.runLogIndexKey(entry.PipelineID), goredis.Z{
		Score:  float64(entry.StartedAt.Unix()),
		Member: indexMember,
	})
	pipe.ZRemRangeByRank(ctx, p.runLogIndexKey(entry.PipelineID), 0, -(defaultRunLogIndexMax + 1))
	_, err = pipe.Exec(ctx)
	return err
}

// GetRunLog retrieves a run log entry for a pipeline, date, and schedule.
func (p *RedisProvider) GetRunLog(ctx context.Context, pipelineID, date, scheduleID string) (*types.RunLogEntry, error) {
	data, err := p.client.Get(ctx, p.runLogKey(pipelineID, date, scheduleID)).Bytes()
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return nil, nil
		}
		return nil, err
	}
	var entry types.RunLogEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return nil, err
	}
	return &entry, nil
}

// ListRunLogs returns recent run log entries for a pipeline.
func (p *RedisProvider) ListRunLogs(ctx context.Context, pipelineID string, limit int) ([]types.RunLogEntry, error) {
	if limit <= 0 {
		limit = 20
	}
	members, err := p.client.ZRangeArgs(ctx, goredis.ZRangeArgs{
		Key:   p.runLogIndexKey(pipelineID),
		Start: 0,
		Stop:  int64(limit - 1),
		Rev:   true,
	}).Result()
	if err != nil {
		return nil, err
	}

	var entries []types.RunLogEntry
	for _, member := range members {
		// member format: "date:scheduleID"
		date, scheduleID := member, types.DefaultScheduleID
		if idx := lastColon(member); idx > 0 {
			date, scheduleID = member[:idx], member[idx+1:]
		}
		entry, err := p.GetRunLog(ctx, pipelineID, date, scheduleID)
		if err != nil || entry == nil {
			if err != nil {
				p.logger.Warn("skipping unreadable run log", "pipeline", pipelineID, "member", member, "error", err)
			}
			continue
		}
		entries = append(entries, *entry)
	}
	return entries, nil
}

// lastColon returns the index of the last ':' in s, or -1 if not found.
func lastColon(s string) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == ':' {
			return i
		}
	}
	return -1
}

// AcquireLock attempts to acquire a distributed lock with the given key and TTL.
// Returns a non-empty owner token on success, or "" if the lock was not acquired.
func (p *RedisProvider) AcquireLock(ctx context.Context, key string, ttl time.Duration) (string, error) {
	token, err := generateToken()
	if err != nil {
		return "", fmt.Errorf("generating lock token: %w", err)
	}
	ok, err := p.client.SetNX(ctx, p.lockKey(key), token, ttl).Result() //nolint:staticcheck // SetNX is cleaner than SetArgs for lock semantics
	if err != nil {
		return "", err
	}
	if !ok {
		return "", nil
	}
	return token, nil
}

// ReleaseLock releases a distributed lock only if the token matches the current owner.
func (p *RedisProvider) ReleaseLock(ctx context.Context, key string, token string) error {
	_, err := p.releaseLockScript.Run(ctx, p.client, []string{p.lockKey(key)}, token).Int()
	return err
}

// generateToken produces a UUID v4 string using crypto/rand.
func generateToken() (string, error) {
	var uuid [16]byte
	if _, err := rand.Read(uuid[:]); err != nil {
		return "", err
	}
	uuid[6] = (uuid[6] & 0x0f) | 0x40 // version 4
	uuid[8] = (uuid[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:16]), nil
}

func (p *RedisProvider) rerunKey(rerunID string) string {
	return p.prefix + "rerun:" + rerunID
}

func (p *RedisProvider) rerunIndexKey(pipelineID string) string {
	return p.prefix + "reruns:" + pipelineID
}

func (p *RedisProvider) rerunGlobalIndexKey() string {
	return p.prefix + "reruns:all"
}

// PutRerun stores a rerun record.
func (p *RedisProvider) PutRerun(ctx context.Context, record types.RerunRecord) error {
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}

	pipe := p.client.Pipeline()
	pipe.Set(ctx, p.rerunKey(record.RerunID), data, p.runKeyTTL(record.Status))
	pipe.ZAdd(ctx, p.rerunIndexKey(record.PipelineID), goredis.Z{
		Score:  float64(record.RequestedAt.Unix()),
		Member: record.RerunID,
	})
	pipe.ZAdd(ctx, p.rerunGlobalIndexKey(), goredis.Z{
		Score:  float64(record.RequestedAt.Unix()),
		Member: record.RerunID,
	})
	pipe.ZRemRangeByRank(ctx, p.rerunIndexKey(record.PipelineID), 0, -(defaultRerunIndexMax + 1))
	pipe.ZRemRangeByRank(ctx, p.rerunGlobalIndexKey(), 0, -(defaultRerunGlobalMax + 1))
	_, err = pipe.Exec(ctx)
	return err
}

// GetRerun retrieves a rerun record.
func (p *RedisProvider) GetRerun(ctx context.Context, rerunID string) (*types.RerunRecord, error) {
	data, err := p.client.Get(ctx, p.rerunKey(rerunID)).Bytes()
	if errors.Is(err, goredis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var record types.RerunRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return nil, err
	}
	return &record, nil
}

// ListReruns returns recent rerun records for a pipeline.
func (p *RedisProvider) ListReruns(ctx context.Context, pipelineID string, limit int) ([]types.RerunRecord, error) {
	if limit <= 0 {
		limit = 20
	}
	ids, err := p.client.ZRangeArgs(ctx, goredis.ZRangeArgs{
		Key:   p.rerunIndexKey(pipelineID),
		Start: 0,
		Stop:  int64(limit - 1),
		Rev:   true,
	}).Result()
	if err != nil {
		return nil, err
	}
	return p.fetchReruns(ctx, ids)
}

// ListAllReruns returns recent rerun records across all pipelines.
func (p *RedisProvider) ListAllReruns(ctx context.Context, limit int) ([]types.RerunRecord, error) {
	if limit <= 0 {
		limit = 50
	}
	ids, err := p.client.ZRangeArgs(ctx, goredis.ZRangeArgs{
		Key:   p.rerunGlobalIndexKey(),
		Start: 0,
		Stop:  int64(limit - 1),
		Rev:   true,
	}).Result()
	if err != nil {
		return nil, err
	}
	return p.fetchReruns(ctx, ids)
}

func (p *RedisProvider) fetchReruns(ctx context.Context, ids []string) ([]types.RerunRecord, error) {
	var records []types.RerunRecord
	for _, id := range ids {
		rec, err := p.GetRerun(ctx, id)
		if err != nil || rec == nil {
			if err != nil {
				p.logger.Warn("skipping unreadable rerun", "rerunID", id, "error", err)
			}
			continue
		}
		records = append(records, *rec)
	}
	return records, nil
}

func (p *RedisProvider) eventStreamKey(pipelineID string) string {
	return p.prefix + "events:" + pipelineID
}

// AppendEvent writes an event to the pipeline's event stream.
func (p *RedisProvider) AppendEvent(ctx context.Context, event types.Event) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	streamKey := p.eventStreamKey(event.PipelineID)
	minTimestamp := time.Now().Add(-p.retentionTTL).UnixMilli()
	minID := fmt.Sprintf("%d-0", minTimestamp)

	pipe := p.client.Pipeline()
	pipe.XAdd(ctx, &goredis.XAddArgs{
		Stream: streamKey,
		MaxLen: p.eventStreamMax,
		Approx: true,
		Values: map[string]interface{}{
			"kind": string(event.Kind),
			"data": string(data),
		},
	})
	pipe.XTrimMinID(ctx, streamKey, minID)
	_, err = pipe.Exec(ctx)
	return err
}

// ReadEventsSince reads events forward from after sinceID (exclusive).
// Use "0-0" to read from the beginning of the stream.
func (p *RedisProvider) ReadEventsSince(ctx context.Context, pipelineID, sinceID string, count int64) ([]types.EventRecord, error) {
	if sinceID == "" {
		sinceID = "0-0"
	}
	if count <= 0 {
		count = 100
	}
	msgs, err := p.client.XRangeN(ctx, p.eventStreamKey(pipelineID), "("+sinceID, "+", count).Result()
	if err != nil {
		return nil, err
	}

	records := make([]types.EventRecord, 0, len(msgs))
	for _, msg := range msgs {
		data, ok := msg.Values["data"].(string)
		if !ok {
			p.logger.Warn("skipping event with missing data field", "streamID", msg.ID)
			continue
		}
		var ev types.Event
		if err := json.Unmarshal([]byte(data), &ev); err != nil {
			p.logger.Warn("skipping corrupt event data", "streamID", msg.ID, "error", err)
			continue
		}
		records = append(records, types.EventRecord{
			StreamID: msg.ID,
			Event:    ev,
		})
	}
	return records, nil
}

// ListEvents returns recent events for a pipeline from the event stream.
func (p *RedisProvider) ListEvents(ctx context.Context, pipelineID string, limit int) ([]types.Event, error) {
	if limit <= 0 {
		limit = 50
	}
	msgs, err := p.client.XRevRangeN(ctx, p.eventStreamKey(pipelineID), "+", "-", int64(limit)).Result()
	if err != nil {
		return nil, err
	}

	events := make([]types.Event, 0, len(msgs))
	for i := len(msgs) - 1; i >= 0; i-- {
		data, ok := msgs[i].Values["data"].(string)
		if !ok {
			p.logger.Warn("skipping event with missing data field", "streamID", msgs[i].ID)
			continue
		}
		var ev types.Event
		if err := json.Unmarshal([]byte(data), &ev); err != nil {
			p.logger.Warn("skipping corrupt event data", "streamID", msgs[i].ID, "error", err)
			continue
		}
		events = append(events, ev)
	}
	return events, nil
}
