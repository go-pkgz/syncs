package syncs

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

// ErrSizedGroup is a SizedGroup with error control. Works the same as errgrp.Group, i.e. returns first error.
// Can work as regular errgrp.Group or with early termination. Thread safe.
// ErrSizedGroup interface enforces constructor usage and doesn't allow direct creation of errSizedGroup
type ErrSizedGroup struct {
	options
	wg   sync.WaitGroup
	sema Locker

	termCancel func()
	terminated func() bool
	canceled   func() bool

	err     *MultiError
	errLock sync.RWMutex
	errOnce sync.Once
}

// NewErrSizedGroup makes wait group with limited size alive goroutines.
// By default, all goroutines will be started but will wait inside.
// For limited number of goroutines use Preemptive() options.
// TermOnErr will skip (won't start) all other goroutines if any error returned.
func NewErrSizedGroup(size int, options ...GroupOption) *ErrSizedGroup {
	res := ErrSizedGroup{
		sema:       NewSemaphore(size),
		err:        new(MultiError),
		terminated: func() bool { return false },
	}
	for _, opt := range options {
		opt(&res.options)
	}
	if res.ctx == nil {
		res.ctx = context.Background()
	}
	if res.termOnError {
		res.ctx, res.termCancel = context.WithCancel(res.ctx)
		res.terminated = func() bool { // terminated will be true if any error happened before
			res.errLock.RLock()
			defer res.errLock.RUnlock()
			return res.err.ErrorOrNil() != nil
		}
	}
	res.canceled = func() bool {
		select {
		case <-res.ctx.Done():
			return true
		default:
			return false
		}
	}

	return &res
}

// Go calls the given function in a new goroutine.
// The first call to return a non-nil error cancels the group if termOnError; its error will be
// returned by Wait. If no termOnError all errors will be collected in multierror.
func (g *ErrSizedGroup) Go(f func(ctx context.Context) error) {
	if g.canceled() && (!g.termOnError || len(g.err.Errors()) == 0) {
		g.errOnce.Do(func() {
			// don't repeat this error
			g.err.append(g.ctx.Err())
		})
		return
	}

	g.wg.Add(1)
	var isLocked bool
	if g.preLock {
		lockOk := g.sema.TryLock()
		if lockOk {
			isLocked = true
		}
		if !lockOk && g.discardIfFull {
			// lock failed and discardIfFull is set, discard this goroutine
			g.wg.Done()
			return
		}
		if !lockOk && !g.discardIfFull {
			g.sema.Lock() // make sure we have block until lock is acquired
			isLocked = true
		}
	}

	go func() {
		defer g.wg.Done()
		defer func() {
			if isLocked {
				g.sema.Unlock()
			}
		}()

		if g.terminated() {
			if !g.canceled() {
				g.termCancel()
			}
			return // terminated due prev error, don't run anything in this group anymore
		}

		if !g.preLock {
			g.sema.Lock()
			isLocked = true
		}

		if err := f(g.ctx); err != nil && !g.canceled() {
			g.err.append(err)
		}
	}()
}

// Wait blocks until all function calls from the Go method have returned, then
// returns all errors (if any) wrapped with multierror from them.
func (g *ErrSizedGroup) Wait() error {
	g.wg.Wait()
	return g.err.ErrorOrNil()
}

// MultiError is a thread safe container for multi-error type that implements error interface
type MultiError struct {
	errors []error
	lock   sync.Mutex
}

func (m *MultiError) append(err error) *MultiError {
	m.lock.Lock()
	m.errors = append(m.errors, err)
	m.lock.Unlock()
	return m
}

// ErrorOrNil returns nil if no errors or multierror if errors occurred
func (m *MultiError) ErrorOrNil() error {
	m.lock.Lock()
	defer m.lock.Unlock()
	if len(m.errors) == 0 {
		return nil
	}
	return m
}

// Error returns multi-error string
func (m *MultiError) Error() string {
	m.lock.Lock()
	defer m.lock.Unlock()
	if len(m.errors) == 0 {
		return ""
	}

	errs := []string{}

	for n, e := range m.errors {
		errs = append(errs, fmt.Sprintf("[%d] {%s}", n, e.Error()))
	}
	return fmt.Sprintf("%d error(s) occurred: %s", len(m.errors), strings.Join(errs, ", "))
}

// Errors returns all errors collected
func (m *MultiError) Errors() []error {
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.errors
}
