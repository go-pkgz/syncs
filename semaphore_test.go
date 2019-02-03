package syncs

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSemaphore(t *testing.T) {
	var locks int32
	var sema sync.Locker
	go func() {
		sema = NewSemaphore(3)
		sema.Lock()
		atomic.AddInt32(&locks, 1)
		sema.Lock()
		atomic.AddInt32(&locks, 1)
		sema.Lock()
		atomic.AddInt32(&locks, 1)
		sema.Lock()
		atomic.AddInt32(&locks, 1)
	}()

	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int32(3), atomic.LoadInt32(&locks), "3 locks ok, hangs on 4th")

	sema.Unlock()
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int32(4), atomic.LoadInt32(&locks), "4 locks should happen")
}
