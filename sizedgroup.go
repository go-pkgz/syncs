package syncs

import (
	"context"
	"sync"
)

// SizedGroup has the same role as WaitingGroup but adds a limit of the amount of goroutines started concurrently.
// Uses similar Go() scheduling as errgrp.Group, thread safe.
// SizedGroup interface enforces constructor usage and doesn't allow direct creation of sizedGroup
type SizedGroup struct {
	options
	wg    sync.WaitGroup
	sema  Locker
	admit Locker // caps calls in flight with DiscardAfterThreshold above size, nil otherwise
}

// NewSizedGroup makes wait group with limited size alive goroutines
func NewSizedGroup(size int, opts ...GroupOption) *SizedGroup {
	res := SizedGroup{sema: NewSemaphore(size)}
	res.ctx = context.Background()
	for _, opt := range opts {
		opt(&res.options)
	}
	if res.threshold > max(size, 1) { // match NewSemaphore's minimum size of 1
		res.admit = NewSemaphore(res.threshold)
		// left on, Preemptive blocks the caller and Discard drops an admitted call holding its slot
		res.preLock, res.discardIfFull = false, false
	}
	return &res
}

// Go calls the given function in a new goroutine.
// Every call will be unblocked, but some goroutines may wait if semaphore locked.
func (g *SizedGroup) Go(fn func(ctx context.Context)) {
	canceled := func() bool {
		select {
		case <-g.ctx.Done():
			return true
		default:
			return false
		}
	}

	if canceled() {
		return
	}

	if g.admit != nil && !g.admit.TryLock() {
		return
	}

	if g.preLock {
		lockOk := g.sema.TryLock()
		if !lockOk && g.discardIfFull {
			// lock failed and discardIfFull is set, discard this goroutine
			return
		}
		if !lockOk && !g.discardIfFull {
			g.sema.Lock() // make sure we have block until lock is acquired
		}
	}

	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		if g.admit != nil {
			defer g.admit.Unlock()
		}

		isLocked := g.preLock // with preLock the permit is already held by the time goroutine starts
		defer func() {
			if isLocked {
				g.sema.Unlock()
			}
		}()

		if canceled() {
			return
		}

		if !g.preLock {
			g.sema.Lock()
			isLocked = true
		}

		fn(g.ctx)
	}()
}

// Wait blocks until the SizedGroup counter is zero.
// See sync.WaitGroup documentation for more information.
func (g *SizedGroup) Wait() {
	g.wg.Wait()
}
