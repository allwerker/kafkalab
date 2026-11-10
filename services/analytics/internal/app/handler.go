// Package app turns business events into the business metrics of docs/contracts/metrics.md.
// Analytics is a pure counter service: most events only bump counters, only order lifecycle
// events touch the database (one small row per order), so batch transactions stay tiny.
//
// Counters are exported by every replica for the partitions it owns; sums over pods are
// correct. No gauges: "orders in flight" is created − shipped − cancelled in PromQL.
package app

import (
	"context"
	"fmt"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/jackc/pgx/v5"

	"kafkalab/pkg/contracts"
	"kafkalab/pkg/events"
	"kafkalab/pkg/kit/kafka"
	"kafkalab/pkg/kit/runner"
	"kafkalab/services/analytics/internal/domain"
)

// Repository stores the order rows inside the batch transaction.
type Repository interface {
	// Upsert inserts seed if the order is unknown (fresh=true); otherwise it returns the stored
	// row, locked until commit. forgetAfter is the retention of an inserted row.
	Upsert(ctx context.Context, tx pgx.Tx, seed domain.Order, forgetAfter time.Time) (stored domain.Order, fresh bool, err error)
	Save(ctx context.Context, tx pgx.Tx, o domain.Order, forgetAfter time.Time) error
}

// Handler counts every business event.
type Handler struct {
	Repo    Repository
	Metrics *metrics.Set
	Now     func() time.Time
}

// Handle implements runner.Handler.
func (h *Handler) Handle(ctx context.Context, tx pgx.Tx, in runner.Input) ([]runner.Output, error) {
	switch e := in.Value.(type) {
	case *events.OrderCreated:
		return nil, h.onCreated(ctx, tx, e)
	case *events.OrderShipped:
		return nil, h.onOutcome(ctx, tx, e.Meta, contracts.TopicOrdersShipped, domain.OutcomeShipped, func() {
			h.inc("lab_orders_shipped_total", "carrier", e.Carrier, "warehouse", e.Warehouse)
		})
	case *events.OrderCancelled:
		return nil, h.onOutcome(ctx, tx, e.Meta, contracts.TopicOrdersCancelled, domain.OutcomeCancelled, func() {
			h.inc("lab_orders_cancelled_total", "reason", string(e.Reason))
		})
	case *events.PaymentCompleted:
		h.inc("lab_payments_total", "result", "completed", "reason", "")
	case *events.PaymentFailed:
		h.inc("lab_payments_total", "result", "failed", "reason", string(e.Reason))
	case *events.PaymentRefunded:
		h.inc("lab_refunds_total")
		h.add("lab_refunded_cents_total", e.AmountCents)
	case *events.InventoryReserved:
		h.inc("lab_reservations_total", "result", "reserved", "reason", "")
	case *events.InventoryRejected:
		h.inc("lab_reservations_total", "result", "rejected", "reason", string(e.Reason))
	case *events.InventoryReleased:
		h.inc("lab_releases_total")
	case *events.NotificationSent:
		h.inc("lab_notifications_total", "channel", string(e.Channel))
	default:
		return nil, runner.Permanent(fmt.Errorf("analytics does not handle %T", in.Value))
	}
	return nil, nil
}

func (h *Handler) onCreated(ctx context.Context, tx pgx.Tx, e *events.OrderCreated) error {
	now := h.Now()
	seed := domain.Order{OrderID: e.Meta.OrderID, Created: true, TotalCents: e.TotalCents, Region: string(e.Region)}
	stored, fresh, err := h.Repo.Upsert(ctx, tx, seed, domain.ForgetAfter(seed, now))
	if err != nil {
		return err
	}
	eff := domain.OnCreated(stored, fresh, seed)
	if err := h.apply(ctx, tx, eff, now); err != nil {
		return err
	}
	if eff.Duplicate {
		h.inc("lab_duplicates_total", "topic", contracts.TopicOrdersCreated)
		return nil
	}
	h.inc("lab_orders_created_total", "region", seed.Region, "payment_method", string(e.PaymentMethod))
	h.add(kafka.Label("lab_order_value_cents_total", "region", seed.Region), e.TotalCents)
	return nil
}

func (h *Handler) onOutcome(ctx context.Context, tx pgx.Tx, meta events.EventMeta, topic string, outcome domain.Outcome, count func()) error {
	now := h.Now()
	seed := domain.Order{OrderID: meta.OrderID, Outcome: outcome}
	stored, fresh, err := h.Repo.Upsert(ctx, tx, seed, domain.ForgetAfter(seed, now))
	if err != nil {
		return err
	}
	eff := domain.OnOutcome(stored, fresh, outcome)
	if err := h.apply(ctx, tx, eff, now); err != nil {
		return err
	}
	if eff.Duplicate {
		h.inc("lab_duplicates_total", "topic", topic)
		return nil
	}
	count()
	h.Metrics.GetOrCreateHistogram(kafka.Label("lab_order_completion_seconds", "outcome", string(outcome))).
		Update(domain.CompletionSeconds(meta.OrderCreatedAt, now))
	return nil
}

// apply stores a changed row and counts revenue that became known.
func (h *Handler) apply(ctx context.Context, tx pgx.Tx, eff domain.Effects, now time.Time) error {
	if eff.Changed {
		if err := h.Repo.Save(ctx, tx, eff.Next, domain.ForgetAfter(eff.Next, now)); err != nil {
			return err
		}
	}
	if eff.RevenueCents > 0 {
		h.add(kafka.Label("lab_revenue_cents_total", "region", eff.Next.Region), eff.RevenueCents)
	}
	return nil
}

func (h *Handler) inc(name string, labels ...string) {
	h.Metrics.GetOrCreateCounter(kafka.Label(name, labels...)).Inc()
}

func (h *Handler) add(series string, cents int64) {
	h.Metrics.GetOrCreateCounter(series).AddInt64(cents)
}
