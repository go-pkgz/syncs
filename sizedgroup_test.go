package syncs

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSizedGroup(t *testing.T) {
	swg := NewSizedGroup(10)
	var c uint32

	for range 1000 {
		swg.Go(func(ctx context.Context) {
			time.Sleep(5 * time.Millisecond)
			atomic.AddUint32(&c, 1)
		})
	}
	assert.True(t, runtime.NumGoroutine() > 500, "goroutines %d", runtime.NumGoroutine())
	swg.Wait()
	assert.Equal(t, uint32(1000), c, fmt.Sprintf("%d, not all routines have been executed", c))
}

func TestSizedGroup_Discard(t *testing.T) {
	swg := NewSizedGroup(10, Preemptive, Discard)
	var c uint32
	base := runtime.NumGoroutine() // count of goroutines not related to the group

	for range 100 {
		swg.Go(func(ctx context.Context) {
			time.Sleep(5 * time.Millisecond)
			atomic.AddUint32(&c, 1)
		})
	}
	assert.LessOrEqual(t, runtime.NumGoroutine(), base+50, "no goroutine spawned per submitted function")
	swg.Wait()
	assert.Equal(t, uint32(10), c, fmt.Sprintf("%d, not all routines have been executed", c))
}

func TestSizedGroup_Preemptive(t *testing.T) {
	swg := NewSizedGroup(10, Preemptive)
	var c uint32
	base := runtime.NumGoroutine() // count of goroutines not related to the group

	for range 100 {
		swg.Go(func(ctx context.Context) {
			time.Sleep(5 * time.Millisecond)
			atomic.AddUint32(&c, 1)
		})
	}
	assert.LessOrEqual(t, runtime.NumGoroutine(), base+50, "no goroutine spawned per submitted function")
	swg.Wait()
	assert.Equal(t, uint32(100), c, fmt.Sprintf("%d, not all routines have been executed", c))
}

func TestSizedGroup_Canceled(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	swg := NewSizedGroup(10, Preemptive, Context(ctx))
	var c uint32

	for range 100 {
		swg.Go(func(ctx context.Context) {
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Millisecond):
			}
			atomic.AddUint32(&c, 1)
		})
	}
	swg.Wait()
	assert.True(t, c < 100)
}

func TestSizedGroup_CanceledPreemptiveReleasesPermit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	swg := NewSizedGroup(1, Preemptive, Context(ctx))
	locker := &signalingLocker{Locker: swg.sema, locking: make(chan struct{}, 1)}
	swg.sema = locker

	wait := func(ch <-chan struct{}) {
		t.Helper()
		select {
		case <-ch:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for the group")
		}
	}
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	started := make(chan struct{})
	swg.Go(func(context.Context) {
		close(started)
		<-release
	})
	wait(started)

	var called atomic.Bool
	submitted := make(chan struct{})
	go func() {
		swg.Go(func(context.Context) { called.Store(true) })
		close(submitted)
	}()
	wait(locker.locking)
	cancel()
	release <- struct{}{}
	wait(submitted)
	swg.Wait()

	assert.False(t, called.Load(), "callback submitted before cancel must not run after it")
	require.True(t, swg.sema.TryLock(), "skipped work must release its permit")
	swg.sema.Unlock()
}

// illustrates the use of a SizedGroup for concurrent, limited execution of goroutines.
func ExampleSizedGroup_go() {

	grp := NewSizedGroup(10) // create sized waiting group allowing maximum 10 goroutines

	var c uint32
	for range 1000 {
		grp.Go(func(ctx context.Context) { // Go call is non-blocking, like regular go statement
			// do some work in 10 goroutines in parallel
			atomic.AddUint32(&c, 1)
			time.Sleep(10 * time.Millisecond)
		})
	}
	// Note: grp.Go acts like go command - never blocks. This code will be executed right away
	log.Print("all 1000 jobs submitted")

	grp.Wait() // wait for completion
}
