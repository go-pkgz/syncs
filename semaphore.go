package syncs

import "sync"

// Locker is a superset of sync.Locker interface with TryLock method.
type Locker interface {
	sync.Locker
	TryLock() bool
}

// Semaphore implementation, counted lock only. Implements Locker interface, thread safe.
type semaphore struct {
	ch chan struct{}
}

// NewSemaphore makes Semaphore with given capacity
func NewSemaphore(capacity int) Locker {
	if capacity <= 0 {
		capacity = 1
	}
	return &semaphore{ch: make(chan struct{}, capacity)}
}

// Lock acquires semaphore, can block if out of capacity.
func (s *semaphore) Lock() {
	s.ch <- struct{}{}
}

// Unlock releases semaphore. It panics if called without a matching Lock/TryLock,
// matching sync.Mutex rather than blocking forever on an empty channel.
func (s *semaphore) Unlock() {
	select {
	case <-s.ch:
	default:
		panic("syncs: unlock of unlocked semaphore")
	}
}

// TryLock acquires semaphore if possible, returns true if acquired, false otherwise.
func (s *semaphore) TryLock() bool {
	select {
	case s.ch <- struct{}{}:
		return true
	default:
		return false
	}
}
