package syncs

import "context"

type options struct {
	ctx           context.Context
	preLock       bool
	termOnError   bool
	discardIfFull bool
	threshold     int
}

// GroupOption functional option type
type GroupOption func(o *options)

// Context sets the group's context. Canceling it does not interrupt calls already running.
func Context(ctx context.Context) GroupOption {
	return func(o *options) {
		o.ctx = ctx
	}
}

// Preemptive sets locking mode preventing spawning waiting goroutine. May cause Go call to block!
func Preemptive(o *options) {
	o.preLock = true
}

// TermOnErr prevents new goroutines to start after first error
func TermOnErr(o *options) {
	o.termOnError = true
}

// Discard will discard new goroutines if semaphore is full, i.e. no more goroutines allowed
func Discard(o *options) {
	o.discardIfFull = true
	o.preLock = true // discard implies preemptive
}

// DiscardAfterThreshold accepts up to threshold calls in flight, running or waiting to run, and
// discards the rest. The Go call never blocks: accepted calls past the group size wait in their
// goroutines. With threshold above the group size it overrides Preemptive and Discard in any order,
// otherwise it works as Discard. SizedGroup only, ErrSizedGroup treats it as Discard.
func DiscardAfterThreshold(threshold int) GroupOption {
	return func(o *options) {
		o.threshold = threshold
		o.discardIfFull = true
		o.preLock = true
	}
}
