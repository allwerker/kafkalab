// Package app connects the payment rules to events: it loads the order's payment state,
// applies the domain decision and returns the events to publish.
package app

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/jackc/pgx/v5"

	"kafkalab/pkg/contracts"
	"kafkalab/pkg/events"
	"kafkalab/pkg/kit/runner"
	"kafkalab/services/payment/internal/domain"
)

// Payment is the stored payment state of one order.
type Payment struct {
	OrderID     string
	State       domain.State
	PaymentID   string
	AmountCents int64
	Method      events.PaymentMethod
}

// Repository loads and stores payments inside the batch transaction.
type Repository interface {
	// Lock returns the order's payment (State "" if unknown) and holds a row lock until commit,
	// so concurrent inputs for the same order are applied one after the other.
	Lock(ctx context.Context, tx pgx.Tx, orderID string) (Payment, error)
	Save(ctx context.Context, tx pgx.Tx, p Payment) error
}

// Knobs is the control-plane view of the handler.
type Knobs interface{ Knobs() events.Knobs }

// Handler handles orders.created and orders.cancelled.
type Handler struct {
	Repo    Repository
	Control Knobs
	Now     func() time.Time
}

// Handle implements runner.Handler.
func (h *Handler) Handle(ctx context.Context, tx pgx.Tx, in runner.Input) ([]runner.Output, error) {
	switch e := in.Value.(type) {
	case *events.OrderCreated:
		return h.onOrderCreated(ctx, tx, e)
	case *events.OrderCancelled:
		return h.onOrderCancelled(ctx, tx, e)
	default:
		return nil, runner.Permanent(fmt.Errorf("payment does not handle %T", in.Value))
	}
}

func (h *Handler) onOrderCreated(ctx context.Context, tx pgx.Tx, o *events.OrderCreated) ([]runner.Output, error) {
	if o.TotalCents <= 0 {
		return nil, runner.Permanent(errors.New("order total must be positive"))
	}
	p, err := h.Repo.Lock(ctx, tx, o.Meta.OrderID)
	if err != nil {
		return nil, err
	}
	k := h.Control.Knobs()
	d := domain.OnOrderCreated(p.State,
		domain.Order{AmountCents: o.TotalCents, Method: domain.Method(o.PaymentMethod)},
		domain.Policy{DeclineProbability: k.BusinessFailureProbability, ProviderDown: k.ProviderDown},
		Roll(o.Meta.EventID))
	if d.Action == domain.ActionNone {
		return nil, h.save(ctx, tx, p, d.Next)
	}
	p.AmountCents, p.Method = o.TotalCents, o.PaymentMethod
	meta := h.meta(o.Meta, "payment")
	if d.Action == domain.ActionCharged {
		p.PaymentID = "pay-" + meta.EventID[:13]
		return []runner.Output{{Topic: contracts.TopicPaymentsCompleted, Key: o.Meta.OrderID, Value: &events.PaymentCompleted{
			Meta: meta, PaymentID: p.PaymentID, AmountCents: o.TotalCents, PaymentMethod: o.PaymentMethod,
		}}}, h.save(ctx, tx, p, d.Next)
	}
	return []runner.Output{{Topic: contracts.TopicPaymentsFailed, Key: o.Meta.OrderID, Value: &events.PaymentFailed{
		Meta: meta, AmountCents: o.TotalCents, Reason: events.PaymentFailureReason(d.Reason),
	}}}, h.save(ctx, tx, p, d.Next)
}

func (h *Handler) onOrderCancelled(ctx context.Context, tx pgx.Tx, c *events.OrderCancelled) ([]runner.Output, error) {
	p, err := h.Repo.Lock(ctx, tx, c.Meta.OrderID)
	if err != nil {
		return nil, err
	}
	d := domain.OnOrderCancelled(p.State)
	if err := h.save(ctx, tx, p, d.Next); err != nil {
		return nil, err
	}
	if d.Action != domain.ActionRefund {
		return nil, nil
	}
	return []runner.Output{{Topic: contracts.TopicPaymentsRefunded, Key: c.Meta.OrderID, Value: &events.PaymentRefunded{
		Meta: h.meta(c.Meta, "refund"), PaymentID: p.PaymentID, AmountCents: p.AmountCents,
	}}}, nil
}

func (h *Handler) save(ctx context.Context, tx pgx.Tx, p Payment, next domain.State) error {
	if p.State == next {
		return nil
	}
	p.State = next
	return h.Repo.Save(ctx, tx, p)
}

func (h *Handler) meta(cause events.EventMeta, outcome string) events.EventMeta {
	return events.EventMeta{
		EventID:        contracts.DeriveEventID(cause.EventID, outcome),
		CausationID:    cause.EventID,
		OrderID:        cause.OrderID,
		OccurredAt:     h.Now().UTC(),
		OrderCreatedAt: cause.OrderCreatedAt,
		Producer:       contracts.ServicePayment,
	}
}

// Roll maps an event id to a stable number in [0,1): the same order always gets the same
// authorisation result, even when it is redelivered after a crash.
func Roll(eventID string) float64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(eventID))
	return float64(h.Sum64()>>11) / float64(1<<53)
}
