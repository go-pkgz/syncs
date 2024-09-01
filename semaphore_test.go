package syncs

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSemaphore(t *testing.T) {
	tbl := []struct {
		name        string
		capacity    int
		lockTimes   int
		expectedErr bool
	}{
		{"ZeroCapacity", 0, 0, false},
		{"CapacityOne", 1, 1, false},
		{"CapacityTwo", 2, 2, false},
		{"ExceedCapacity", 2, 3, true},
	}

	for _, tt := range tbl {
		t.Run(tt.name, func(t *testing.T) {
			var locks int32
			sema := NewSemaphore(tt.capacity)

			wg := sync.WaitGroup{}
			wg.Add(tt.lockTimes)
			for i := 0; i < tt.lockTimes; i++ {
				go func() {
					sema.Lock()
					atomic.AddInt32(&locks, 1)
					wg.Done()
				}()
			}

			time.Sleep(10 * time.Millisecond) // wait a little for locks to acquire

			// if number of locks are less than capacity, all should be acquired
			if tt.lockTimes <= tt.capacity {
				assert.Equal(t, tt.lockTimes, int(atomic.LoadInt32(&locks)))
				wg.Wait()
				return
			}
			// if number of locks exceed capacity, it should hang after reaching the capacity
			assert.Equal(t, tt.capacity, int(atomic.LoadInt32(&locks)))
			sema.Unlock()
			time.Sleep(10 * time.Millisecond)
			// after unlock, it should be able to acquire another lock
			assert.Equal(t, tt.capacity+1, int(atomic.LoadInt32(&locks)))
			wg.Wait()
		})
	}
}

func TestSemaphore_TryLock(t *testing.T) {
	tbl := []struct {
		name          string
		capacity      int
		lockTimes     int
		expectedLocks int
	}{
		{"ZeroCapacity", 0, 1, 1},
		{"CapacityOne", 1, 1, 1},
		{"CapacityTwo", 2, 2, 2},
		{"ExceedCapacity", 2, 3, 2},
	}

	for _, tt := range tbl {
		t.Run(tt.name, func(t *testing.T) {
			var locks int32
			sema := NewSemaphore(tt.capacity)

			for i := 0; i < tt.lockTimes; i++ {
				if sema.TryLock() {
					atomic.AddInt32(&locks, 1)
				}
			}

			// Check the acquired locks, it should not exceed capacity.
			assert.Equal(t, tt.expectedLocks, int(atomic.LoadInt32(&locks)))
		})
	}
}
