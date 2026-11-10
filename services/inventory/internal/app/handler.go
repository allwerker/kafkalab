// Package app connects the inventory rules to events: it loads the order's reservation and the
// stock it needs, applies the domain decision and returns the events to publish.
package app

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"kafkalab/pkg/contracts"
	"kafkalab/pkg/events"
	"kafkalab/pkg/kit/runner"
	"kafkalab/services/inventory/internal/domain"
)

// Reservation is the stored inventory state of one order.
type Reservation struct {
	OrderID       string
	State         domain.State
	ReservationID string
	Warehouse     string
	Lines         []domain.Line // reserved quantities, needed for a later release
}

// Repository loads and stores reservations and stock inside the batch transaction.
type Repository interface {
	// Lock returns the order's reservation (State "" if unknown) and holds a row lock until
	// commit, so concurrent inputs for the same order are applied one after the other.
	Lock(ctx context.Context, tx pgx.Tx, orderID string) (Reservation, error)
	Save(ctx context.Context, tx pgx.Tx, r Reservation) error
	// LockStock locks the stock rows of skus in SKU order and returns the units on hand.
	// Unknown products are missing from the result.
	LockStock(ctx context.Context, tx pgx.Tx, skus []string) (map[string]int, error)
	// Take removes and Return adds back the quantities of rows locked by LockStock.
	Take(ctx context.Context, tx pgx.Tx, lines []domain.Line) error
	Return(ctx context.Context, tx pgx.Tx, lines []domain.Line) error
}

// Handler handles orders.created and orders.cancelled.
type Handler struct {
	Repo Repository
	Now  func() time.Time
}

// Handle implements runner.Handler.
func (h *Handler) Handle(ctx context.Context, tx pgx.Tx, in runner.Input) ([]runner.Output, error) {
	switch e := in.Value.(type) {
	case *events.OrderCreated:
		return h.onOrderCreated(ctx, tx, e)
	case *events.OrderCancelled:
		return h.onOrderCancelled(ctx, tx, e)
	default:
		return nil, runner.Permanent(fmt.Errorf("inventory does not handle %T", in.Value))
	}
}

func (h *Handler) onOrderCreated(ctx context.Context, tx pgx.Tx, o *events.OrderCreated) ([]runner.Output, error) {
	lines := make([]domain.Line, len(o.Lines))
	for i, l := range o.Lines {
		lines[i] = domain.Line{SKU: l.Sku, Quantity: int(l.Quantity)}
	}
	demand, err := domain.Demand(lines)
	if err != nil {
		return nil, runner.Permanent(err)
	}
	r, err := h.Repo.Lock(ctx, tx, o.Meta.OrderID)
	if err != nil {
		return nil, err
	}
	var available map[string]int
	if domain.NeedsStock(r.State) {
		if available, err = h.Repo.LockStock(ctx, tx, domain.SKUs(demand)); err != nil {
			return nil, err
		}
	}
	d := domain.OnOrderCreated(r.State, demand, available)
	meta := h.meta(o.Meta, "inventory")
	switch d.Action {
	case domain.ActionReserve:
		if err := h.Repo.Take(ctx, tx, demand); err != nil {
			return nil, err
		}
		r.ReservationID, r.Warehouse, r.Lines = "res-"+meta.EventID[:13], domain.Warehouse(string(o.Region)), demand
		return []runner.Output{{Topic: contracts.TopicInventoryReserved, Key: o.Meta.OrderID, Value: &events.InventoryReserved{
			Meta: meta, ReservationID: r.ReservationID, Warehouse: r.Warehouse, Lines: reservedLines(demand),
		}}}, h.save(ctx, tx, r, d.Next)
	case domain.ActionReject:
		return []runner.Output{{Topic: contracts.TopicInventoryRejected, Key: o.Meta.OrderID, Value: &events.InventoryRejected{
			Meta: meta, Reason: events.InventoryRejectionReason(d.Reason), Sku: d.SKU,
		}}}, h.save(ctx, tx, r, d.Next)
	default:
		return nil, h.save(ctx, tx, r, d.Next)
	}
}

func (h *Handler) onOrderCancelled(ctx context.Context, tx pgx.Tx, c *events.OrderCancelled) ([]runner.Output, error) {
	r, err := h.Repo.Lock(ctx, tx, c.Meta.OrderID)
	if err != nil {
		return nil, err
	}
	d := domain.OnOrderCancelled(r.State)
	if d.Action != domain.ActionRelease {
		return nil, h.save(ctx, tx, r, d.Next)
	}
	if _, err := h.Repo.LockStock(ctx, tx, domain.SKUs(r.Lines)); err != nil {
		return nil, err
	}
	if err := h.Repo.Return(ctx, tx, r.Lines); err != nil {
		return nil, err
	}
	return []runner.Output{{Topic: contracts.TopicInventoryReleased, Key: c.Meta.OrderID, Value: &events.InventoryReleased{
		Meta: h.meta(c.Meta, "release"), ReservationID: r.ReservationID, Lines: reservedLines(r.Lines),
	}}}, h.save(ctx, tx, r, d.Next)
}

func (h *Handler) save(ctx context.Context, tx pgx.Tx, r Reservation, next domain.State) error {
	if r.State == next {
		return nil
	}
	r.State = next
	return h.Repo.Save(ctx, tx, r)
}

func (h *Handler) meta(cause events.EventMeta, outcome string) events.EventMeta {
	return events.EventMeta{
		EventID:        contracts.DeriveEventID(cause.EventID, outcome),
		CausationID:    cause.EventID,
		OrderID:        cause.OrderID,
		OccurredAt:     h.Now().UTC(),
		OrderCreatedAt: cause.OrderCreatedAt,
		Producer:       contracts.ServiceInventory,
	}
}

func reservedLines(lines []domain.Line) []events.ReservedLine {
	out := make([]events.ReservedLine, len(lines))
	for i, l := range lines {
		out[i] = events.ReservedLine{Sku: l.SKU, Quantity: int32(l.Quantity)}
	}
	return out
}
