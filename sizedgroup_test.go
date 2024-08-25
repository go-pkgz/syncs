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
)

func TestSizedGroup(t *testing.T) {
	swg := NewSizedGroup(10)
	var c uint32

	for i := 0; i < 1000; i++ {
		swg.Go(func(context.Context) {
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

	for i := 0; i < 100; i++ {
		swg.Go(func(context.Context) {
			time.Sleep(5 * time.Millisecond)
			atomic.AddUint32(&c, 1)
		})
	}
	assert.True(t, runtime.NumGoroutine() < 15, "goroutines %d", runtime.NumGoroutine())
	swg.Wait()
	assert.Equal(t, uint32(10), c, fmt.Sprintf("%d, not all routines have been executed", c))
}

func TestSizedGroup_WithWrongSizeValuePassed(t *testing.T) {
	swg := NewSizedGroup(0, Discard)
	var c uint32

	for i := 0; i < 100; i++ {
		swg.Go(func(context.Context) {
			time.Sleep(5 * time.Millisecond)
			atomic.AddUint32(&c, 1)
		})
	}
	assert.True(t, runtime.NumGoroutine() < 6, "goroutines %d", runtime.NumGoroutine())
	swg.Wait()
	assert.Equal(t, uint32(1), c, fmt.Sprintf("%d, wrong number of routines has been executed", c))
}

func TestSizedGroup_Preemptive(t *testing.T) {
	swg := NewSizedGroup(10, Preemptive)
	var c uint32

	for i := 0; i < 100; i++ {
		swg.Go(func(context.Context) {
			time.Sleep(5 * time.Millisecond)
			atomic.AddUint32(&c, 1)
		})
	}
	assert.True(t, runtime.NumGoroutine() < 15, "goroutines %d", runtime.NumGoroutine())
	swg.Wait()
	assert.Equal(t, uint32(100), c, fmt.Sprintf("%d, not all routines have been executed", c))
}

func TestSizedGroup_CanceledPreemtiveMode(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	swg := NewSizedGroup(10, Preemptive, Context(ctx))
	var c uint32

	for i := 0; i < 100; i++ {
		swg.Go(func(context.Context) {
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

func TestSizedGroup_CanceledNonPreemptiveMode(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	swg := NewSizedGroup(10, Context(ctx))
	var c uint32

	for i := 0; i < 2000; i++ {
		swg.Go(func(context.Context) {
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Millisecond):
			}
			atomic.AddUint32(&c, 1)
		})
	}
	swg.Wait()
	assert.True(t, c < 25)
}

func TestSizedGroup_DiscardAfterTreshold(t *testing.T) {
	swg := NewSizedGroup(10, DiscardAfterTreshold(20))
	var c uint32

	for i := 0; i < 100; i++ {
		swg.Go(func(context.Context) {
			time.Sleep(5 * time.Millisecond)
			atomic.AddUint32(&c, 1)
		})
	}
	assert.True(t, runtime.NumGoroutine() < 15, "goroutines %d", runtime.NumGoroutine())
	swg.Wait()
	assert.Equal(t, uint32(20), c, fmt.Sprintf("%d, wrong number of routines have been executed", c))
}

func TestSizedGroup_DiscardAfterTreshold_WithNegativeTreshold(t *testing.T) {
	swg := NewSizedGroup(10, DiscardAfterTreshold(-1))
	var c uint32

	for i := 0; i < 100; i++ {
		swg.Go(func(context.Context) {
			time.Sleep(5 * time.Millisecond)
			atomic.AddUint32(&c, 1)
		})
	}
	assert.True(t, runtime.NumGoroutine() < 15, "goroutines %d", runtime.NumGoroutine())
	swg.Wait()
	assert.Equal(t, uint32(10), c, fmt.Sprintf("%d, wrong number of routines have been executed", c))
}

func TestSizedGroup_DiscardAfterTreshold_WithTresholdNotAboveSize(t *testing.T) {
	swg := NewSizedGroup(10, DiscardAfterTreshold(10))
	var c uint32

	for i := 0; i < 100; i++ {
		swg.Go(func(context.Context) {
			time.Sleep(5 * time.Millisecond)
			atomic.AddUint32(&c, 1)
		})
	}
	assert.True(t, runtime.NumGoroutine() < 15, "goroutines %d", runtime.NumGoroutine())
	swg.Wait()
	assert.Equal(t, uint32(10), c, fmt.Sprintf("%d, wrong number of routines have been executed", c))
}

// illustrates the use of a SizedGroup for concurrent, limited execution of goroutines.
func ExampleSizedGroup_go() {

	grp := NewSizedGroup(10) // create sized waiting group allowing maximum 10 goroutines

	var c uint32
	for i := 0; i < 1000; i++ {
		grp.Go(func(context.Context) { // Go call is non-blocking, like regular go statement
			// do some work in 10 goroutines in parallel
			atomic.AddUint32(&c, 1)
			time.Sleep(10 * time.Millisecond)
		})
	}
	// Note: grp.Go acts like go command - never blocks. This code will be executed right away
	log.Print("all 1000 jobs submitted")

	grp.Wait() // wait for completion
}
