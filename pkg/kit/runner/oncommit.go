package runner

import (
	"context"
	"sync"
)

type commitKey struct{}

// commitHooks collects side effects of one batch attempt.
type commitHooks struct {
	mu  sync.Mutex
	fns []func()
}

func (c *commitHooks) add(fn func()) {
	c.mu.Lock()
	c.fns = append(c.fns, fn)
	c.mu.Unlock()
}

func (c *commitHooks) run() {
	c.mu.Lock()
	fns := c.fns
	c.fns = nil
	c.mu.Unlock()
	for _, fn := range fns {
		fn()
	}
}

// OnCommit registers fn to run once the current batch is committed and all its outputs are
// acknowledged. A batch that is rolled back and retried discards its registrations, so
// in-memory side effects such as business counters happen exactly once per input.
// Outside a runner batch (e.g. in handler unit tests) fn runs immediately.
func OnCommit(ctx context.Context, fn func()) {
	if c, ok := ctx.Value(commitKey{}).(*commitHooks); ok {
		c.add(fn)
		return
	}
	fn()
}
