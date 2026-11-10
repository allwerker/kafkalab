package app

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/jackc/pgx/v5"

	"kafkalab/pkg/events"
	"kafkalab/pkg/kit/runner"
	"kafkalab/services/shipping/internal/domain"
)

var errDown = errors.New("connection refused")

// memRepo is an in-memory join store. Together with memDB it behaves transactionally: a
// failed transaction leaves the rows as they were.
type memRepo struct {
	rows    map[string]Order
	updated map[string]time.Time
	now     *time.Time

	failLock, failSave, failOverdue, failReady, failWaiting, failPurge error
}

func newRepo(now *time.Time) *memRepo {
	return &memRepo{rows: map[string]Order{}, updated: map[string]time.Time{}, now: now}
}

func (m *memRepo) Lock(_ context.Context, _ pgx.Tx, id string, now time.Time) (Order, error) {
	if m.failLock != nil {
		return Order{}, m.failLock
	}
	o, ok := m.rows[id]
	if !ok {
		o = Order{OrderID: id, Join: domain.Join{Status: domain.StatusOpen}, FirstSeenAt: now}
		m.rows[id] = o
	}
	return o, nil
}

func (m *memRepo) Save(_ context.Context, _ pgx.Tx, o Order) error {
	if m.failSave != nil {
		return m.failSave
	}
	m.rows[o.OrderID] = o
	m.updated[o.OrderID] = *m.now
	return nil
}

func (m *memRepo) with(status domain.Status, limit int, keep func(Order) bool) []Order {
	var out []Order
	for _, o := range m.rows {
		if o.Join.Status == status && keep(o) {
			out = append(out, o)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].FirstSeenAt.Before(out[j].FirstSeenAt) })
	return out[:min(limit, len(out))]
}

func (m *memRepo) Overdue(_ context.Context, _ pgx.Tx, before time.Time, limit int) ([]Order, error) {
	return m.with(domain.StatusOpen, limit, func(o Order) bool { return o.FirstSeenAt.Before(before) }), m.failOverdue
}

func (m *memRepo) Ready(_ context.Context, _ pgx.Tx, limit int) ([]Order, error) {
	return m.with(domain.StatusReady, limit, func(Order) bool { return true }), m.failReady
}

func (m *memRepo) Waiting(context.Context, pgx.Tx) (int64, error) {
	n := 0
	for _, o := range m.rows {
		if !o.Join.Status.Terminal() {
			n++
		}
	}
	return int64(n), m.failWaiting
}

func (m *memRepo) Purge(_ context.Context, _ pgx.Tx, before time.Time, limit int) (int64, error) {
	if m.failPurge != nil {
		return 0, m.failPurge
	}
	ids := slices.Sorted(maps.Keys(m.rows))
	var n int64
	for _, id := range ids {
		if m.rows[id].Join.Status.Terminal() && m.updated[id].Before(before) && n < int64(limit) {
			delete(m.rows, id)
			n++
		}
	}
	return n, nil
}

// memDB runs transactions against memRepo and rolls the rows back when fn fails.
type memDB struct{ repo *memRepo }

func (d memDB) InTx(_ context.Context, fn func(pgx.Tx) error) error {
	rows, updated := maps.Clone(d.repo.rows), maps.Clone(d.repo.updated)
	if err := fn(nil); err != nil {
		d.repo.rows, d.repo.updated = rows, updated
		return err
	}
	return nil
}

// memPublisher records what was published, or fails like an unreachable cluster.
type memPublisher struct {
	published []runner.Output
	failing   error
}

func (p *memPublisher) Publish(_ context.Context, outs []runner.Output) error {
	if p.failing != nil {
		return p.failing
	}
	p.published = append(p.published, outs...)
	return nil
}

type knobs events.Knobs

func (k *knobs) Knobs() events.Knobs { return events.Knobs(*k) }

type leader bool

func (l leader) IsLeader(context.Context) bool { return bool(l) }

var t0 = time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)

// world is one shipping pod with its in-memory store, clock and knobs.
type world struct {
	now     time.Time
	knobs   knobs
	repo    *memRepo
	pub     *memPublisher
	metrics *metrics.Set
	handler *Handler
	sweeper *Sweeper
}

func newWorld() *world {
	w := &world{now: t0, pub: &memPublisher{}, metrics: metrics.NewSet()}
	w.repo = newRepo(&w.now)
	clock := func() time.Time { return w.now }
	w.handler = &Handler{Repo: w.repo, Control: &w.knobs, Now: clock}
	w.sweeper = &Sweeper{DB: memDB{w.repo}, Repo: w.repo, Publisher: w.pub, Leader: leader(true), Control: &w.knobs,
		Metrics: w.metrics, Log: slog.New(slog.DiscardHandler), Now: clock,
		Deadline: 300 * time.Second, Every: time.Millisecond, Retain: 2 * time.Hour, Batch: 100}
	return w
}

func (w *world) handle(t *testing.T, v any) []runner.Output {
	t.Helper()
	outs, err := w.handler.Handle(context.Background(), nil, runner.Input{Value: v})
	if err != nil {
		t.Fatal(err)
	}
	return outs
}

func (w *world) counter(name string) uint64 { return w.metrics.GetOrCreateCounter(name).Get() }
func (w *world) gauge(name string) float64  { return w.metrics.GetOrCreateGauge(name, nil).Get() }

func inputMeta(id, order string) events.EventMeta {
	return events.EventMeta{EventID: id, OrderID: order, OrderCreatedAt: t0.Add(-2 * time.Second)}
}

func paid(order string) *events.PaymentCompleted {
	return &events.PaymentCompleted{Meta: inputMeta("pay-evt-"+order, order), PaymentID: "pay-" + order, AmountCents: 4990}
}

func declined(order string) *events.PaymentFailed {
	return &events.PaymentFailed{Meta: inputMeta("payfail-evt-"+order, order), Reason: events.PaymentFailureReasonDeclined}
}

func reserved(order string) *events.InventoryReserved {
	return &events.InventoryReserved{Meta: inputMeta("res-evt-"+order, order), ReservationID: "res-" + order, Warehouse: "WH-NORTH"}
}

func rejected(order string) *events.InventoryRejected {
	return &events.InventoryRejected{Meta: inputMeta("rej-evt-"+order, order), Reason: events.InventoryRejectionReasonOutOfStock}
}
