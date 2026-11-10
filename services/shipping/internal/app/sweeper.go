package app

import (
	"context"
	"log/slog"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/jackc/pgx/v5"

	"kafkalab/pkg/kit/runner"
	"kafkalab/services/shipping/internal/domain"
)

// SweepRepository is what the sweeper needs from the join store.
type SweepRepository interface {
	Save(ctx context.Context, tx pgx.Tx, o Order) error
	// Overdue returns up to limit open orders first seen before the given instant, locked
	// with FOR UPDATE SKIP LOCKED: rows a handler or another pod's sweeper holds are skipped.
	Overdue(ctx context.Context, tx pgx.Tx, before time.Time, limit int) ([]Order, error)
	// Ready returns up to limit orders held back by a warehouse outage, locked the same way.
	Ready(ctx context.Context, tx pgx.Tx, limit int) ([]Order, error)
	// Waiting counts orders that are not terminal yet (open or ready), across all pods.
	Waiting(ctx context.Context, tx pgx.Tx) (int64, error)
	// Purge deletes up to limit terminal orders last changed before the given instant.
	Purge(ctx context.Context, tx pgx.Tx, before time.Time, limit int) (int64, error)
}

// Transactor runs fn in a transaction (implemented by *pg.DB).
type Transactor interface {
	InTx(ctx context.Context, fn func(pgx.Tx) error) error
}

// Publisher produces events synchronously: it returns once every record is acknowledged.
// The sweeper runs outside the kit runner, so it publishes on its own.
type Publisher interface {
	Publish(ctx context.Context, outs []runner.Output) error
}

// Leader tells whether this pod currently exports the cluster-wide join gauge.
type Leader interface {
	IsLeader(ctx context.Context) bool
}

// Sweeper periodically enforces the join deadline and releases held-back shipments.
type Sweeper struct {
	DB        Transactor
	Repo      SweepRepository
	Publisher Publisher
	Leader    Leader
	Control   Knobs
	Metrics   *metrics.Set
	Log       *slog.Logger
	Now       func() time.Time
	// Deadline is how long an order may wait for its second half (JOIN_DEADLINE).
	Deadline time.Duration
	// Every is the pause between sweeps (SWEEP_EVERY).
	Every time.Duration
	// Retain is how long terminal rows are kept to absorb late inputs.
	Retain time.Duration
	// Batch bounds the rows of one sweep transaction.
	Batch int
}

// Run sweeps every s.Every until ctx ends.
func (s *Sweeper) Run(ctx context.Context) error {
	t := time.NewTicker(s.Every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			s.Tick(ctx)
		}
	}
}

// Tick runs sweep transactions until nothing is left to do, then refreshes the join gauge and
// purges old terminal rows. Errors are logged; the next tick retries.
func (s *Sweeper) Tick(ctx context.Context) {
	for {
		more, err := s.Sweep(ctx)
		if err != nil {
			s.Log.WarnContext(ctx, "sweep failed, retrying on the next tick", "error", err)
			break
		}
		if !more {
			break
		}
	}
	s.exportWaiting(ctx)
	if err := s.purge(ctx); err != nil {
		s.Log.WarnContext(ctx, "purging terminal orders failed", "error", err)
	}
}

// Sweep handles one batch of overdue and held-back orders in a single transaction and reports
// whether a batch was full (more work may be waiting).
//
// Ordering matters: the rows are locked, the events are produced synchronously, and only
// then is the state change committed.
//   - Produce fails: the transaction rolls back, the rows stay open/ready and the next sweep
//     tries again. Nothing was published, so nothing is lost.
//   - Produce succeeds but the commit fails (crash, connection loss): the next sweep produces
//     the same decision again. Its event id is DeriveEventID(orderId, "shipped"|"cancelled"),
//     so consumers deduplicate the second copy and the duplicate is harmless.
//
// The reverse order (commit, then produce) could lose a decision forever: a crash between
// the two would leave a terminal row whose event was never published.
func (s *Sweeper) Sweep(ctx context.Context) (bool, error) {
	var cancelled int
	var more bool
	err := s.DB.InTx(ctx, func(tx pgx.Tx) error {
		now := s.Now()
		p := Policy(s.Control)
		overdue, err := s.Repo.Overdue(ctx, tx, now.Add(-s.Deadline), s.Batch)
		if err != nil {
			return err
		}
		var ready []Order
		if !p.WarehouseOutage {
			if ready, err = s.Repo.Ready(ctx, tx, s.Batch); err != nil {
				return err
			}
		}
		more = len(overdue) == s.Batch || len(ready) == s.Batch
		type step struct {
			o     Order
			d     domain.Decision
			cause string
		}
		steps := make([]step, 0, len(overdue)+len(ready))
		for _, o := range overdue {
			steps = append(steps, step{o, domain.OnDeadline(o.Join), "deadline:" + o.OrderID})
		}
		for _, o := range ready {
			steps = append(steps, step{o, domain.OnSweep(o.Join, p), o.CauseEventID})
		}
		var outs []runner.Output
		for _, st := range steps {
			st.o.Join = st.d.Next
			if err := s.Repo.Save(ctx, tx, st.o); err != nil {
				return err
			}
			outs = append(outs, Outputs(st.o, st.d, st.cause, now)...)
		}
		cancelled = len(overdue)
		if len(outs) == 0 {
			return nil
		}
		return s.Publisher.Publish(ctx, outs)
	})
	if err != nil {
		return false, err
	}
	if cancelled > 0 {
		s.Metrics.GetOrCreateCounter("lab_join_deadline_exceeded_total").Add(cancelled)
		s.Log.InfoContext(ctx, "orders cancelled after the join deadline", "count", cancelled)
	}
	return more, nil
}

// exportWaiting sets lab_join_open_orders. Join rows are not owned by partitions, so every pod
// would count the same rows; only the leader exports the count and all others export 0,
// which keeps sum(lab_join_open_orders) correct.
func (s *Sweeper) exportWaiting(ctx context.Context) {
	g := s.Metrics.GetOrCreateGauge("lab_join_open_orders", nil)
	if !s.Leader.IsLeader(ctx) {
		g.Set(0)
		return
	}
	var n int64
	err := s.DB.InTx(ctx, func(tx pgx.Tx) error {
		var err error
		n, err = s.Repo.Waiting(ctx, tx)
		return err
	})
	if err != nil {
		s.Log.WarnContext(ctx, "counting open orders failed", "error", err)
		return
	}
	g.Set(float64(n))
}

func (s *Sweeper) purge(ctx context.Context) error {
	for {
		var n int64
		err := s.DB.InTx(ctx, func(tx pgx.Tx) error {
			var err error
			n, err = s.Repo.Purge(ctx, tx, s.Now().Add(-s.Retain), s.Batch)
			return err
		})
		if err != nil || n < int64(s.Batch) {
			return err
		}
	}
}
