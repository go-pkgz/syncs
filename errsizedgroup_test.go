package syncs

import (
	"context"
	"errors"
	"fmt"
	"log"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestErrorSizedGroup(t *testing.T) {
	ewg := NewErrSizedGroup(10)
	var c uint32

	for i := range 1000 {
		ewg.Go(func() error {
			time.Sleep(time.Millisecond * 10)
			atomic.AddUint32(&c, 1)
			if i == 100 {
				return errors.New("err1")
			}
			if i == 200 {
				return errors.New("err2")
			}
			return nil
		})
	}
	assert.True(t, runtime.NumGoroutine() > 500, "goroutines %d", runtime.NumGoroutine())

	err := ewg.Wait()
	require.NotNil(t, err)
	assert.True(t, strings.HasPrefix(err.Error(), "2 error(s) occurred:"))
	assert.Equal(t, uint32(1000), c, fmt.Sprintf("%d, not all routines have been executed.", c))
}

// recordMax sets m to v if v is greater than the value m holds already
func recordMax(m *atomic.Int32, v int32) {
	for {
		cur := m.Load()
		if v <= cur || m.CompareAndSwap(cur, v) {
			return
		}
	}
}

func TestErrorSizedGroup_Preemptive(t *testing.T) {
	ewg := NewErrSizedGroup(10, Preemptive)
	var c uint32
	base := runtime.NumGoroutine() // count of goroutines not related to the group
	var running, maxRunning atomic.Int32

	for i := range 100 {
		ewg.Go(func() error {
			defer running.Add(-1)
			recordMax(&maxRunning, running.Add(1))
			atomic.AddUint32(&c, 1)
			if i == 10 {
				return errors.New("err1")
			}
			if i == 20 {
				return errors.New("err2")
			}
			time.Sleep(time.Millisecond)
			return nil
		})
	}

	assert.LessOrEqual(t, runtime.NumGoroutine(), base+50, "no goroutine spawned per submitted function")
	err := ewg.Wait()
	require.NotNil(t, err)
	assert.LessOrEqual(t, maxRunning.Load(), int32(10), "no more than the group size running at once")
	assert.True(t, strings.HasPrefix(err.Error(), "2 error(s) occurred:"))
	assert.Equal(t, uint32(100), c, fmt.Sprintf("%d, not all routines have been executed.", c))
}

func TestErrorSizedGroup_Discard(t *testing.T) {
	ewg := NewErrSizedGroup(10, Discard)
	var c uint32
	base := runtime.NumGoroutine() // count of goroutines not related to the group
	var running, maxRunning atomic.Int32

	for range 1000 {
		ewg.Go(func() error {
			defer running.Add(-1)
			recordMax(&maxRunning, running.Add(1))
			atomic.AddUint32(&c, 1)
			time.Sleep(10 * time.Millisecond)
			return nil
		})
	}

	assert.LessOrEqual(t, runtime.NumGoroutine(), base+50, "no goroutine spawned per submitted function")
	err := ewg.Wait()
	assert.NoError(t, err)
	assert.LessOrEqual(t, maxRunning.Load(), int32(10), "no more than the group size running at once")
	assert.Equal(t, uint32(10), c)
}

func TestErrorSizedGroup_NoError(t *testing.T) {
	ewg := NewErrSizedGroup(10)
	var c uint32

	for range 1000 {
		ewg.Go(func() error {
			atomic.AddUint32(&c, 1)
			return nil
		})
	}

	err := ewg.Wait()
	assert.Nil(t, err)
	assert.Equal(t, uint32(1000), c, fmt.Sprintf("%d, not all routines have been executed.", c))
}

func TestErrorSizedGroup_Term(t *testing.T) {
	ewg := NewErrSizedGroup(10, TermOnErr)
	var c uint32

	for i := range 1000 {
		ewg.Go(func() error {
			atomic.AddUint32(&c, 1)
			if i == 100 {
				return errors.New("err")
			}
			return nil
		})
	}

	err := ewg.Wait()
	assert.NotNil(t, err)
	assert.Equal(t, "1 error(s) occurred: [0] {err}", err.Error())
	assert.True(t, c < uint32(1000), fmt.Sprintf("%d, some of routines has to be terminated early", c))
}

func TestErrorSizedGroup_TermOnErr(t *testing.T) {
	ewg := NewErrSizedGroup(10, TermOnErr)
	var c uint32

	const N = 1000
	const errIndex = 100 // index of a function that will return an error

	for i := range N {
		ewg.Go(func() error {
			val := atomic.AddUint32(&c, 1)
			if i == errIndex || val > uint32(errIndex+1) {
				return fmt.Errorf("err from function %d", i)
			}
			return nil
		})
	}

	err := ewg.Wait()
	t.Logf("error: %v", err)

	require.NotNil(t, err)
	assert.Contains(t, err.Error(), "err from function ")
	// we don't know how many routines will be executed before the error, but it should be less than 10
	require.LessOrEqual(t, c, uint32(errIndex+100), fmt.Sprintf("%d, routines have to be terminated early", c))
}

func TestErrorSizedGroup_WaitWithoutGo(t *testing.T) {
	ewg := NewErrSizedGroup(10)
	assert.NoError(t, ewg.Wait())
}

func TestErrorSizedGroup_TermAndPreemptive(t *testing.T) {
	ewg := NewErrSizedGroup(10, TermOnErr, Preemptive)
	var c uint32

	done := make(chan struct{})
	go func() {
		for i := range 1000 {
			ewg.Go(func() error {
				time.Sleep(10 * time.Millisecond)
				atomic.AddUint32(&c, 1)
				if i == 100 {
					return errors.New("err")
				}
				return nil
			})
		}

		err := ewg.Wait()
		assert.NotNil(t, err)
		assert.Equal(t, "1 error(s) occurred: [0] {err}", err.Error())
		assert.True(t, c < uint32(1000), fmt.Sprintf("%d, some of routines has to be terminated early", c))

		done <- struct{}{}
	}()

	select {
	case <-time.After(5 * time.Second):
		t.Fatal("timeout deadlock may happen")
	case <-done:
	}
}

func TestErrorSizedGroup_ConcurrencyLimit(t *testing.T) {
	concurrentGoroutines := int32(0)
	maxConcurrentGoroutines := int32(0)
	ewg := NewErrSizedGroup(5) // Limit of concurrent goroutines set to 5

	for range 100 {
		ewg.Go(func() error {
			atomic.AddInt32(&concurrentGoroutines, 1)
			defer atomic.AddInt32(&concurrentGoroutines, -1)

			if v := atomic.LoadInt32(&concurrentGoroutines); v > atomic.LoadInt32(&maxConcurrentGoroutines) {
				atomic.StoreInt32(&maxConcurrentGoroutines, v)
			}

			time.Sleep(time.Millisecond * 50)
			return nil
		})
	}

	err := ewg.Wait()
	assert.Nil(t, err)
	assert.Equal(t, int32(5), maxConcurrentGoroutines)
}

func TestErrorSizedGroup_MultiError(t *testing.T) {
	ewg := NewErrSizedGroup(10)

	for i := range 10 {
		ewg.Go(func() error {
			return fmt.Errorf("error from goroutine %d", i)
		})
	}

	err := ewg.Wait()
	assert.NotNil(t, err)

	for i := range 10 {
		assert.Contains(t, err.Error(), fmt.Sprintf("error from goroutine %d", i))
	}

	var merr *MultiError
	assert.True(t, errors.As(err, &merr))
	assert.Len(t, merr.Errors(), 10)
}

func TestErrorSizedGroup_MultiErrorUnwrap(t *testing.T) {
	errFirst := errors.New("first")
	errSecond := errors.New("second")

	ewg := NewErrSizedGroup(2)
	ewg.Go(func() error { return fmt.Errorf("wrapped: %w", errFirst) })
	ewg.Go(func() error { return errSecond })

	err := ewg.Wait()
	require.Error(t, err)
	assert.ErrorIs(t, err, errFirst, "matches the wrapped error of one of the goroutines")
	assert.ErrorIs(t, err, errSecond)
	assert.NotErrorIs(t, err, errors.New("something else"))

	var merr *MultiError
	require.ErrorAs(t, err, &merr)
	merr.Errors()[0] = nil // the caller can't affect the collected errors
	assert.Len(t, merr.Errors(), 2)
	assert.NotNil(t, merr.Errors()[0])
}

func TestErrorSizedGroup_Cancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ewg := NewErrSizedGroup(10, Context(ctx))

	var c uint32
	const N = 1000

	for i := range N {
		time.Sleep(1 * time.Millisecond) // prevent all the goroutines to be started at once
		ewg.Go(func() error {
			atomic.AddUint32(&c, 1)
			if i == 100 {
				cancel()
			}
			time.Sleep(1 * time.Millisecond) // simulate some work
			return nil
		})
	}

	err := ewg.Wait()
	require.EqualError(t, err, "1 error(s) occurred: [0] {context canceled}")
	assert.ErrorIs(t, ctx.Err(), context.Canceled, ctx.Err())
	t.Logf("completed: %d", c)
	// 100 submitted before the cancellation, up to 10 more running and one waiting for the semaphore
	require.LessOrEqual(t, c, uint32(120), "some of goroutines has to be terminated early")
}

func TestErrorSizedGroup_CancelWithPreemptive(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ewg := NewErrSizedGroup(10, Context(ctx), Preemptive)

	var c uint32
	const N = 1000

	for i := range N {
		ewg.Go(func() error {
			atomic.AddUint32(&c, 1)
			if i == 100 {
				cancel()
			}
			time.Sleep(1 * time.Millisecond) // simulate some work
			return nil
		})
	}

	err := ewg.Wait()
	require.EqualError(t, err, "1 error(s) occurred: [0] {context canceled}")
	assert.ErrorIs(t, ctx.Err(), context.Canceled, ctx.Err())
	t.Logf("completed: %d", c)
	// 100 submitted before the cancellation, up to 10 more running and one waiting for the semaphore
	require.LessOrEqual(t, c, uint32(120), "some of goroutines has to be terminated early")
}

func TestErrorSizedGroup_CancelWithActiveErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ewg := NewErrSizedGroup(4, Context(ctx))

	release := make(chan struct{})
	var returned atomic.Int32
	const N = 100
	for range N {
		ewg.Go(func() error {
			<-release
			returned.Add(1)
			return errors.New("failed")
		})
	}

	cancel()
	close(release)
	for returned.Load() < N/2 { // make sure workers record their errors while the canceled call records ctx.Err()
		runtime.Gosched()
	}
	ewg.Go(func() error { return nil })

	err := ewg.Wait()
	require.Error(t, err)
	var merr *MultiError
	require.True(t, errors.As(err, &merr))
	assert.Len(t, merr.Errors(), N+1)
}

// illustrates the use of a SizedGroup for concurrent, limited execution of goroutines.
func ExampleErrSizedGroup_go() {

	// create sized waiting group allowing maximum 10 goroutines
	grp := NewErrSizedGroup(10)

	var c uint32
	for range 1000 {
		// Go call is non-blocking, like regular go statement
		grp.Go(func() error {
			// do some work in 10 goroutines in parallel
			atomic.AddUint32(&c, 1)
			time.Sleep(10 * time.Millisecond)
			return nil
		})
	}
	// Note: grp.Go acts like go command - never blocks. This code will be executed right away
	log.Print("all 1000 jobs submitted")

	// wait for completion
	if err := grp.Wait(); err != nil {
		panic(err)
	}
}
