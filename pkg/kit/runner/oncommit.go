package runner

import (
	"context"
	"sync"

	"github.com/jackc/pgx/v5"
)

type batchKey struct{}

// Batch is the scope of one partition batch attempt (one database transaction). Handlers use it
// to share state across the records of a batch — e.g. a stock ledger that is locked once and
// written once — and to register work that must run inside the transaction just before it
// commits, or only after the batch has been committed and acknowledged.
type Batch struct {
	mu     sync.Mutex
	values map[any]any
	before []func(context.Context, pgx.Tx) error
	after  []func()
}

// NewBatch returns ctx carrying a fresh batch scope. The runner creates one per batch attempt;
// tests use it to drive handlers and adapters the same way.
func NewBatch(ctx context.Context) (context.Context, *Batch) {
	b := &Batch{values: map[any]any{}}
	return context.WithValue(ctx, batchKey{}, b), b
}

// BatchOf returns the batch scope of ctx, or nil outside a runner batch.
func BatchOf(ctx context.Context) *Batch {
	b, _ := ctx.Value(batchKey{}).(*Batch)
	return b
}

// Value returns the batch value stored under key, creating it with create on first use.
// create runs without the batch lock held, so it may register BeforeCommit work itself.
// A batch is processed by one goroutine, so create runs at most once per key.
func (b *Batch) Value(key any, create func() any) any {
	b.mu.Lock()
	v, ok := b.values[key]
	b.mu.Unlock()
	if ok {
		return v
	}
	v = create()
	b.mu.Lock()
	b.values[key] = v
	b.mu.Unlock()
	return v
}

// BeforeCommit registers fn to run inside the batch transaction after the last record.
func (b *Batch) BeforeCommit(fn func(context.Context, pgx.Tx) error) {
	b.mu.Lock()
	b.before = append(b.before, fn)
	b.mu.Unlock()
}

// RunBeforeCommit runs the registered before-commit functions in registration order.
func (b *Batch) RunBeforeCommit(ctx context.Context, tx pgx.Tx) error {
	b.mu.Lock()
	fns := b.before
	b.before = nil
	b.mu.Unlock()
	for _, fn := range fns {
		if err := fn(ctx, tx); err != nil {
			return err
		}
	}
	return nil
}

func (b *Batch) addAfter(fn func()) {
	b.mu.Lock()
	b.after = append(b.after, fn)
	b.mu.Unlock()
}

func (b *Batch) runAfter() {
	b.mu.Lock()
	fns := b.after
	b.after = nil
	b.mu.Unlock()
	for _, fn := range fns {
		fn()
	}
}

// OnCommit registers fn to run once the current batch is committed and all its outputs are
// acknowledged. A batch that is rolled back and retried discards its registrations, so
// in-memory side effects such as business counters happen exactly once per input.
// Outside a runner batch (e.g. in handler unit tests) fn runs immediately.
func OnCommit(ctx context.Context, fn func()) {
	if b := BatchOf(ctx); b != nil {
		b.addAfter(fn)
		return
	}
	fn()
}
