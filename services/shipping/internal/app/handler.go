// Package app connects the shipping join to events: the handler applies payment and stock
// outcomes as they arrive (in any order, possibly on different pods), the sweeper cancels
// orders whose join deadline passed and ships orders held back by a warehouse outage.
package app

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"kafkalab/pkg/contracts"
	"kafkalab/pkg/events"
	"kafkalab/pkg/kit/runner"
	"kafkalab/services/shipping/internal/domain"
)

// Order is the stored join state of one order.
type Order struct {
	OrderID       string
	Join          domain.Join
	PaymentID     string
	ReservationID string
	Warehouse     string
	// CauseEventID is the input that last changed the join; a shipment released later by the
	// sweeper names it as its cause.
	CauseEventID   string
	OrderCreatedAt time.Time
	FirstSeenAt    time.Time
}

// Repository loads and stores join rows inside a transaction.
type Repository interface {
	// Lock returns the order's join (a new open row first seen at now if unknown) and holds a
	// row lock until commit, so the two halves of an order are applied one after the other
	// even when two pods receive them at the same time.
	Lock(ctx context.Context, tx pgx.Tx, orderID string, now time.Time) (Order, error)
	Save(ctx context.Context, tx pgx.Tx, o Order) error
}

// Knobs is the control-plane view.
type Knobs interface{ Knobs() events.Knobs }

// Handler handles payments.completed|failed and inventory.reserved|rejected.
type Handler struct {
	Repo    Repository
	Control Knobs
	Now     func() time.Time
}

// input is one half of the join, normalised from its event.
type input struct {
	meta    events.EventMeta
	payment bool // payment half (else stock half)
	ok      bool
	fill    func(*Order)
}

// Handle implements runner.Handler.
func (h *Handler) Handle(ctx context.Context, tx pgx.Tx, in runner.Input) ([]runner.Output, error) {
	var i input
	switch e := in.Value.(type) {
	case *events.PaymentCompleted:
		i = input{meta: e.Meta, payment: true, ok: true, fill: func(o *Order) { o.PaymentID = e.PaymentID }}
	case *events.PaymentFailed:
		i = input{meta: e.Meta, payment: true}
	case *events.InventoryReserved:
		i = input{meta: e.Meta, ok: true, fill: func(o *Order) { o.ReservationID, o.Warehouse = e.ReservationID, e.Warehouse }}
	case *events.InventoryRejected:
		i = input{meta: e.Meta}
	default:
		return nil, runner.Permanent(fmt.Errorf("shipping does not handle %T", in.Value))
	}
	now := h.Now()
	o, err := h.Repo.Lock(ctx, tx, i.meta.OrderID, now)
	if err != nil {
		return nil, err
	}
	p := Policy(h.Control)
	var d domain.Decision
	if i.payment {
		d = domain.OnPayment(o.Join, i.ok, p)
	} else {
		d = domain.OnStock(o.Join, i.ok, p)
	}
	if d.Next == o.Join {
		// Terminal order or a half we already know: a no-op, the first outcome stands.
		return nil, nil
	}
	if i.fill != nil {
		i.fill(&o)
	}
	if o.OrderCreatedAt.IsZero() {
		o.OrderCreatedAt = i.meta.OrderCreatedAt
	}
	o.Join, o.CauseEventID = d.Next, i.meta.EventID
	if err := h.Repo.Save(ctx, tx, o); err != nil {
		return nil, err
	}
	return Outputs(o, d, i.meta.EventID, now), nil
}

// Policy reads the decision-relevant knobs.
func Policy(k Knobs) domain.Policy {
	return domain.Policy{WarehouseOutage: k.Knobs().WarehouseOutage}
}

// Outputs turns a decision about o into the event to publish (none for ActionNone). The event
// id depends only on the order, so two pods racing on the same order emit the same event.
func Outputs(o Order, d domain.Decision, causationID string, now time.Time) []runner.Output {
	switch d.Action {
	case domain.ActionShip:
		meta := meta(o, "shipped", causationID, now)
		s := domain.NewShipment(o.OrderID, o.Warehouse)
		return []runner.Output{{Topic: contracts.TopicOrdersShipped, Key: o.OrderID, Value: &events.OrderShipped{
			Meta: meta, ShipmentID: "shp-" + meta.EventID[:13], Warehouse: o.Warehouse, Carrier: s.Carrier, TrackingCode: s.TrackingCode,
		}}}
	case domain.ActionCancel:
		return []runner.Output{{Topic: contracts.TopicOrdersCancelled, Key: o.OrderID, Value: &events.OrderCancelled{
			Meta: meta(o, "cancelled", causationID, now), Reason: events.CancellationReason(d.Reason),
			RefundRequired: d.RefundRequired, ReleaseRequired: d.ReleaseRequired,
		}}}
	default:
		return nil
	}
}

func meta(o Order, outcome, causationID string, now time.Time) events.EventMeta {
	return events.EventMeta{
		EventID:        contracts.DeriveEventID(o.OrderID, outcome),
		CausationID:    causationID,
		OrderID:        o.OrderID,
		OccurredAt:     now.UTC(),
		OrderCreatedAt: o.OrderCreatedAt,
		Producer:       contracts.ServiceShipping,
	}
}
