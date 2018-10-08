package syncs

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSemaphore(t *testing.T) {
	var after3Locks, after4Locks bool
	var sema sync.Locker
	go func() {
		sema = NewSemaphore(3)
		sema.Lock()
		sema.Lock()
		sema.Lock()
		after3Locks = true
		sema.Lock()
		after4Locks = true
	}()

	time.Sleep(100 * time.Millisecond)
	assert.True(t, after3Locks, "3 locks ok")
	assert.False(t, after4Locks, "4 locks should not be able to pass")

	sema.Unlock()
	time.Sleep(100 * time.Millisecond)
	assert.True(t, after4Locks, "4 locks ok")
}
