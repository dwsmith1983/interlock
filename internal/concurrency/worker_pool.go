// Package concurrency provides bounded concurrency primitives.
package concurrency

import (
	"context"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

// WorkerPool runs submitted tasks with bounded concurrency.
// At most maxConcurrency tasks execute simultaneously.
// A zero-value WorkerPool is not usable; use NewWorkerPool.
type WorkerPool struct {
	g      *errgroup.Group
	ctx    context.Context
	sem    *semaphore.Weighted
	cancel context.CancelFunc
}

// NewWorkerPool creates a WorkerPool that runs at most maxConcurrency
// tasks concurrently. The pool's context is derived from parent so that
// cancelling parent propagates to all running tasks.
func NewWorkerPool(parent context.Context, maxConcurrency int64) *WorkerPool {
	ctx, cancel := context.WithCancel(parent)
	g, gctx := errgroup.WithContext(ctx)
	return &WorkerPool{
		g:      g,
		ctx:    gctx,
		sem:    semaphore.NewWeighted(maxConcurrency),
		cancel: cancel,
	}
}

// Submit enqueues fn for execution in the pool. It blocks until a slot is
// available or the pool's context is cancelled. It returns the context error
// if the context is done before a slot is acquired.
func (p *WorkerPool) Submit(fn func(context.Context) error) error {
	if err := p.sem.Acquire(p.ctx, 1); err != nil {
		return err
	}
	p.g.Go(func() error {
		defer p.sem.Release(1)
		return fn(p.ctx)
	})
	return nil
}

// Wait blocks until all submitted tasks have completed and returns the first
// non-nil error returned by any task, or nil if all tasks succeeded.
func (p *WorkerPool) Wait() error {
	err := p.g.Wait()
	p.cancel()
	return err
}
