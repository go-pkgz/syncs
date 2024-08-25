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
	wg            sync.WaitGroup
	workers       chan struct{}
	scheduledJobs chan struct{}
	jobQueue      chan func(ctx context.Context)
	workersMutex  sync.Mutex
}

// NewSizedGroup makes wait group with limited size alive goroutines
func NewSizedGroup(size int, opts ...GroupOption) *SizedGroup {
	if size < 1 {
		size = 1
	}
	res := SizedGroup{workers: make(chan struct{}, size)}
	res.options.ctx = context.Background()
	for _, opt := range opts {
		opt(&res.options)
	}

	// queue size either equal to number of workers or larger, otherwise does not make sense
	queueSize := size
	if res.tresholdSize > size {
		queueSize = res.tresholdSize
	}

	res.jobQueue = make(chan func(ctx context.Context), queueSize)
	res.scheduledJobs = make(chan struct{}, queueSize)
	return &res
}

// Go calls the given function in a new goroutine.
// Every call will be unblocked, but some goroutines may wait if semaphore locked.
func (g *SizedGroup) Go(fn func(ctx context.Context)) {
	if g.canceled() {
		return
	}

	g.wg.Add(1)
	if !g.preLock {
		go func() {
			defer g.wg.Done()
			if g.canceled() {
				return
			}
			g.scheduledJobs <- struct{}{}
			fn(g.ctx)
			<-g.scheduledJobs
		}()
		return
	}

	toRun := func(job func(ctx context.Context)) {
		defer g.wg.Done()
		if g.canceled() {
			return
		}
		job(g.ctx)
		<-g.scheduledJobs
	}

	startWorkerIfNeeded := func() {
		g.workersMutex.Lock()
		select {
		case g.workers <- struct{}{}:
			g.workersMutex.Unlock()
			go func() {
				for {
					select {
					case job := <-g.jobQueue:
						toRun(job)
					default:
						g.workersMutex.Lock()
						select {
						case job := <-g.jobQueue:
							g.workersMutex.Unlock()
							toRun(job)
							continue
						default:
							<-g.workers
							g.workersMutex.Unlock()
						}
						return
					}
				}
			}()
		default:
			g.workersMutex.Unlock()
		}
	}

	if g.discardIfFull {
		select {
		case g.scheduledJobs <- struct{}{}:
			g.jobQueue <- fn
			startWorkerIfNeeded()
		default:
			g.wg.Done()
		}

		return
	}

	g.scheduledJobs <- struct{}{}
	g.jobQueue <- fn
	startWorkerIfNeeded()
}

// Wait blocks until the SizedGroup counter is zero.
// See sync.WaitGroup documentation for more information.
func (g *SizedGroup) Wait() {
	g.wg.Wait()
}

func (g *SizedGroup) canceled() bool {
	select {
	case <-g.ctx.Done():
		return true
	default:
		return false
	}
}
