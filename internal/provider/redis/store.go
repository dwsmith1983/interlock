package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/interlock-systems/interlock/pkg/types"
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
	return err
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
			continue
		}
		pipelines = append(pipelines, *pc)
	}
	return pipelines, nil
}

// DeletePipeline removes a pipeline configuration.
func (p *RedisProvider) DeletePipeline(ctx context.Context, id string) error {
	pipe := p.client.Pipeline()
	pipe.Del(ctx, p.pipelineKey(id))
	pipe.SRem(ctx, p.pipelineIndexKey(), id)
	_, err := pipe.Exec(ctx)
	return err
}

// PutTrait stores a trait evaluation result with TTL.
func (p *RedisProvider) PutTrait(ctx context.Context, pipelineID string, trait types.TraitEvaluation, ttl time.Duration) error {
	data, err := json.Marshal(trait)
	if err != nil {
		return err
	}
	if ttl <= 0 {
		return p.client.Set(ctx, p.traitKey(pipelineID, trait.TraitType), data, 0).Err()
	}
	return p.client.Set(ctx, p.traitKey(pipelineID, trait.TraitType), data, ttl).Err()
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
		keys, nextCursor, err := p.client.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return nil, err
		}

		for _, key := range keys {
			data, err := p.client.Get(ctx, key).Bytes()
			if err != nil {
				continue
			}
			var te types.TraitEvaluation
			if err := json.Unmarshal(data, &te); err != nil {
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
	pipe.Set(ctx, p.runKey(run.RunID), data, 0)
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

	result, err := p.casScript.Run(ctx, p.client, []string{p.runKey(runID)}, expectedVersion, string(data)).Int()
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

func (p *RedisProvider) runLogKey(pipelineID, date string) string {
	return p.prefix + "runlog:" + pipelineID + ":" + date
}

func (p *RedisProvider) runLogIndexKey(pipelineID string) string {
	return p.prefix + "runlogs:" + pipelineID
}

func (p *RedisProvider) lockKey(key string) string {
	return p.prefix + "lock:" + key
}

// PutRunLog stores a run log entry.
func (p *RedisProvider) PutRunLog(ctx context.Context, entry types.RunLogEntry) error {
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	pipe := p.client.Pipeline()
	pipe.Set(ctx, p.runLogKey(entry.PipelineID, entry.Date), data, 0)
	pipe.ZAdd(ctx, p.runLogIndexKey(entry.PipelineID), goredis.Z{
		Score:  float64(entry.StartedAt.Unix()),
		Member: entry.Date,
	})
	// Keep only last 100 entries in index
	pipe.ZRemRangeByRank(ctx, p.runLogIndexKey(entry.PipelineID), 0, -101)
	_, err = pipe.Exec(ctx)
	return err
}

// GetRunLog retrieves a run log entry for a pipeline and date.
func (p *RedisProvider) GetRunLog(ctx context.Context, pipelineID, date string) (*types.RunLogEntry, error) {
	data, err := p.client.Get(ctx, p.runLogKey(pipelineID, date)).Bytes()
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
	dates, err := p.client.ZRevRange(ctx, p.runLogIndexKey(pipelineID), 0, int64(limit-1)).Result()
	if err != nil {
		return nil, err
	}

	var entries []types.RunLogEntry
	for _, date := range dates {
		entry, err := p.GetRunLog(ctx, pipelineID, date)
		if err != nil || entry == nil {
			continue
		}
		entries = append(entries, *entry)
	}
	return entries, nil
}

// AcquireLock attempts to acquire a distributed lock with the given key and TTL.
func (p *RedisProvider) AcquireLock(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	ok, err := p.client.SetNX(ctx, p.lockKey(key), "1", ttl).Result()
	return ok, err
}

// ReleaseLock releases a distributed lock.
func (p *RedisProvider) ReleaseLock(ctx context.Context, key string) error {
	return p.client.Del(ctx, p.lockKey(key)).Err()
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
	return p.client.XAdd(ctx, &goredis.XAddArgs{
		Stream: p.eventStreamKey(event.PipelineID),
		MaxLen: p.eventStreamMax,
		Approx: true,
		Values: map[string]interface{}{
			"kind": string(event.Kind),
			"data": string(data),
		},
	}).Err()
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
			continue
		}
		var ev types.Event
		if err := json.Unmarshal([]byte(data), &ev); err != nil {
			continue
		}
		events = append(events, ev)
	}
	return events, nil
}
