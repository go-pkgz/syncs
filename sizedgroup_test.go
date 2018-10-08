package syncs

import (
	"fmt"
	"log"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSizedGroup(t *testing.T) {
	swg := NewSizedGroup(10)
	var c uint32

	for i := 0; i < 1000; i++ {
		swg.Go(func() {
			atomic.AddUint32(&c, 1)
		})
	}

	swg.Wait()
	assert.Equal(t, uint32(1000), c, fmt.Sprintf("%d, not all routines have been executed.", c))
}

// illustrates the use of a SizedGroup for concurrent, limited execution of goroutines.
func ExampleSizedGroup_go() {

	grp := NewSizedGroup(10) // create sized waiting group allowing maximum 10 goroutines

	var c uint32
	for i := 0; i < 1000; i++ {
		grp.Go(func() { // Go call is non-blocking, like regular go statement
			// do some work in 10 goroutines in parallel
			atomic.AddUint32(&c, 1)
			time.Sleep(10 * time.Millisecond)
		})
	}
	// Note: grp.Go acts like go command - never blocks. This code will be executed right away
	log.Print("all 1000 jobs submitted")

	grp.Wait() // wait for completion
}
