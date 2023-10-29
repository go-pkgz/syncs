package syncs

import (
	"context"
	"fmt"
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
	if g.canceled() && (!g.termOnError || g.err.len() == 0) {
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
	g.err.makeStr()
	return g.err.ErrorOrNil()
}

// MultiError is a thread safe container for multi-error type that implements error interface
type MultiError struct {
	errors []error
	lock   sync.RWMutex
	str    string
}

// ErrorOrNil returns nil if no errors or multierror if errors occurred
func (m *MultiError) ErrorOrNil() error {
	if m.len() == 0 {
		return nil
	}
	return m
}

// Error returns multi-error string
func (m *MultiError) Error() string {
	return m.str
}

// Errors returns all errors collected
func (m *MultiError) Errors() []error {
	return m.errors
}

func (m *MultiError) append(err error) {
	m.lock.Lock()
	m.errors = append(m.errors, err)
	m.lock.Unlock()
}

func (m *MultiError) len() int {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return len(m.errors)
}

func (m *MultiError) makeStr() {
	lenErrors := m.len()
	if lenErrors == 0 {
		return
	}
	errs := fmt.Sprintf("[0] {%s}", m.Errors()[0].Error())
	if lenErrors > 1 {
		for n, e := range m.Errors()[1:] {
			errs += fmt.Sprintf(", [%d] {%s}", n+1, e.Error())
		}
	}
	m.str = fmt.Sprintf("%d error(s) occurred: %s", lenErrors, errs)
}
