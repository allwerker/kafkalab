package app

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"kafkalab/pkg/contracts"
	"kafkalab/pkg/events"
	"kafkalab/pkg/kit/runner"
	"kafkalab/services/inventory/internal/domain"
)

// memRepo is an in-memory reservation and stock store. Each failure field breaks one operation.
type memRepo struct {
	rows  map[string]Reservation
	stock map[string]int

	lockErr, saveErr, stockErr, takeErr, returnErr error
	lockedSKUs                                     [][]string
}

func newRepo(stock map[string]int) *memRepo {
	return &memRepo{rows: map[string]Reservation{}, stock: stock}
}

func (m *memRepo) Lock(_ context.Context, _ pgx.Tx, id string) (Reservation, error) {
	if m.lockErr != nil {
		return Reservation{}, m.lockErr
	}
	r, ok := m.rows[id]
	if !ok {
		r = Reservation{OrderID: id}
	}
	return r, nil
}

func (m *memRepo) Save(_ context.Context, _ pgx.Tx, r Reservation) error {
	if m.saveErr != nil {
		return m.saveErr
	}
	m.rows[r.OrderID] = r
	return nil
}

func (m *memRepo) LockStock(_ context.Context, _ pgx.Tx, skus []string) (map[string]int, error) {
	if m.stockErr != nil {
		return nil, m.stockErr
	}
	m.lockedSKUs = append(m.lockedSKUs, skus)
	out := map[string]int{}
	for _, s := range skus {
		if v, ok := m.stock[s]; ok {
			out[s] = v
		}
	}
	return out, nil
}

func (m *memRepo) Take(_ context.Context, _ pgx.Tx, lines []domain.Line) error {
	if m.takeErr != nil {
		return m.takeErr
	}
	for _, l := range lines {
		m.stock[l.SKU] -= l.Quantity
	}
	return nil
}

func (m *memRepo) Return(_ context.Context, _ pgx.Tx, lines []domain.Line) error {
	if m.returnErr != nil {
		return m.returnErr
	}
	for _, l := range lines {
		m.stock[l.SKU] += l.Quantity
	}
	return nil
}

var now = time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)

func handler(repo *memRepo) *Handler {
	return &Handler{Repo: repo, Now: func() time.Time { return now }}
}

func order(id string, region events.Region, lines ...events.OrderLine) *events.OrderCreated {
	return &events.OrderCreated{Meta: events.EventMeta{EventID: "e-" + id, OrderID: id, OrderCreatedAt: now.Add(-time.Second)},
		Region: region, Lines: lines, TotalCents: 100}
}

func line(sku string, q int32) events.OrderLine {
	return events.OrderLine{Sku: sku, Quantity: q, UnitPriceCents: 100}
}

func cancel(id string) *events.OrderCancelled {
	return &events.OrderCancelled{Meta: events.EventMeta{EventID: "c-" + id, OrderID: id}, Reason: events.CancellationReasonPaymentFailed, ReleaseRequired: true}
}

func handle(t *testing.T, h *Handler, v any) []runner.Output {
	t.Helper()
	outs, err := h.Handle(context.Background(), nil, runner.Input{Value: v})
	if err != nil {
		t.Fatal(err)
	}
	return outs
}

func Test_shouldReserveAllLinesFromTheRegionalWarehouseWhenStockSuffices(t *testing.T) {
	// GIVEN: 10 units of sku-001 and 5 of sku-002, and a southern order for 2+1 sku-001 and 1 sku-002
	repo := newRepo(map[string]int{"sku-001": 10, "sku-002": 5})
	o := order("o-1", events.RegionSouth, line("sku-002", 1), line("sku-001", 2), line("sku-001", 1))

	// WHEN: inventory handles the order
	outs := handle(t, handler(repo), o)

	// THEN: inventory.reserved is published from munich for the merged lines
	ir := outs[0].Value.(*events.InventoryReserved)
	want := []events.ReservedLine{{Sku: "sku-001", Quantity: 3}, {Sku: "sku-002", Quantity: 1}}
	if outs[0].Topic != contracts.TopicInventoryReserved || outs[0].Key != "o-1" || ir.Warehouse != "munich" ||
		!reflect.DeepEqual(ir.Lines, want) || ir.Meta.EventID != contracts.DeriveEventID("e-o-1", "inventory") ||
		ir.Meta.CausationID != "e-o-1" || ir.Meta.Producer != "inventory" || !ir.Meta.OccurredAt.Equal(now) ||
		!ir.Meta.OrderCreatedAt.Equal(o.Meta.OrderCreatedAt) || ir.ReservationID == "" {
		t.Fatalf("unexpected %+v", ir)
	}
	// AND: the stock was taken and the stock rows were locked in SKU order
	if repo.stock["sku-001"] != 7 || repo.stock["sku-002"] != 4 || !reflect.DeepEqual(repo.lockedSKUs[0], []string{"sku-001", "sku-002"}) {
		t.Fatalf("stock %v locked %v", repo.stock, repo.lockedSKUs)
	}
	if r := repo.rows["o-1"]; r.State != domain.StateReserved || r.ReservationID != ir.ReservationID || r.Warehouse != "munich" {
		t.Fatalf("stored %+v", r)
	}
}

func Test_shouldRejectWithoutTakingAnyStockWhenOneProductIsSoldOut(t *testing.T) {
	// GIVEN: plenty of sku-001 but only one unit of sku-003
	repo := newRepo(map[string]int{"sku-001": 50, "sku-003": 1})

	// WHEN: an order for 1x sku-001 and 2x sku-003 arrives
	outs := handle(t, handler(repo), order("o-2", events.RegionNorth, line("sku-001", 1), line("sku-003", 2)))

	// THEN: inventory.rejected names sku-003 as out of stock and no unit was taken
	ir := outs[0].Value.(*events.InventoryRejected)
	if outs[0].Topic != contracts.TopicInventoryRejected || ir.Reason != events.InventoryRejectionReasonOutOfStock ||
		ir.Sku != "sku-003" || ir.Meta.EventID != contracts.DeriveEventID("e-o-2", "inventory") {
		t.Fatalf("unexpected %+v", ir)
	}
	if repo.stock["sku-001"] != 50 || repo.stock["sku-003"] != 1 || repo.rows["o-2"].State != domain.StateRejected {
		t.Fatalf("stock %v state %s", repo.stock, repo.rows["o-2"].State)
	}
}

func Test_shouldRejectAsUnknownWhenTheOrderedProductIsNotStocked(t *testing.T) {
	// GIVEN: a flash sale on a product the warehouse does not carry
	repo := newRepo(map[string]int{"sku-001": 50})

	// WHEN: the order arrives
	outs := handle(t, handler(repo), order("o-3", events.RegionEast, line("sku-999", 1)))

	// THEN: it is rejected with UNKNOWN_SKU
	if ir := outs[0].Value.(*events.InventoryRejected); ir.Reason != events.InventoryRejectionReasonUnknownSku || ir.Sku != "sku-999" {
		t.Fatalf("unexpected %+v", ir)
	}
}

func Test_shouldReturnTheStockWhenShippingCancelsAReservedOrder(t *testing.T) {
	// GIVEN: a reserved order for 4x sku-010 from leipzig
	repo := newRepo(map[string]int{"sku-010": 20})
	h := handler(repo)
	reserved := handle(t, h, order("o-4", events.RegionEast, line("sku-010", 4)))[0].Value.(*events.InventoryReserved)

	// WHEN: shipping cancels it because the payment failed
	outs := handle(t, h, cancel("o-4"))

	// THEN: inventory.released returns the same 4 units of the same reservation
	rel := outs[0].Value.(*events.InventoryReleased)
	if outs[0].Topic != contracts.TopicInventoryReleased || rel.ReservationID != reserved.ReservationID ||
		!reflect.DeepEqual(rel.Lines, reserved.Lines) || rel.Meta.EventID != contracts.DeriveEventID("c-o-4", "release") {
		t.Fatalf("unexpected %+v", rel)
	}
	if repo.stock["sku-010"] != 20 || repo.rows["o-4"].State != domain.StateReleased {
		t.Fatalf("stock %v state %s", repo.stock, repo.rows["o-4"].State)
	}
}

func Test_shouldNeverReserveAnOrderWhenItsCancellationArrivedFirst(t *testing.T) {
	// GIVEN: a deadline cancellation that reached inventory before the order
	repo := newRepo(map[string]int{"sku-001": 10})
	h := handler(repo)
	if outs := handle(t, h, cancel("o-5")); outs != nil {
		t.Fatalf("early cancellation must publish nothing, got %+v", outs)
	}

	// WHEN: the late order arrives
	outs := handle(t, h, order("o-5", events.RegionWest, line("sku-001", 1)))

	// THEN: no stock is touched and nothing is published
	if outs != nil || repo.stock["sku-001"] != 10 || len(repo.lockedSKUs) != 0 || repo.rows["o-5"].State != domain.StateCancelledEarly {
		t.Fatalf("outs=%+v stock=%v state=%s", outs, repo.stock, repo.rows["o-5"].State)
	}
}

func Test_shouldReleaseNothingWhenTheCancelledOrderWasRejected(t *testing.T) {
	// GIVEN: an order that was rejected for lack of stock
	repo := newRepo(map[string]int{"sku-001": 0})
	h := handler(repo)
	handle(t, h, order("o-6", events.RegionNorth, line("sku-001", 1)))

	// WHEN: shipping cancels it
	outs := handle(t, h, cancel("o-6"))

	// THEN: nothing is released or published
	if outs != nil || repo.stock["sku-001"] != 0 || repo.rows["o-6"].State != domain.StateRejected {
		t.Fatalf("outs=%+v state=%s", outs, repo.rows["o-6"].State)
	}
}

func Test_shouldReserveOnlyOnceWhenTheSameOrderArrivesWithANewEventID(t *testing.T) {
	// GIVEN: a reserved order
	repo := newRepo(map[string]int{"sku-001": 10})
	h := handler(repo)
	handle(t, h, order("o-7", events.RegionNorth, line("sku-001", 2)))

	// WHEN: a copy with a different event id arrives (not caught by event-id deduplication)
	copied := order("o-7", events.RegionNorth, line("sku-001", 2))
	copied.Meta.EventID = "e-o-7-copy"
	outs := handle(t, h, copied)

	// THEN: the stock is not taken twice
	if outs != nil || repo.stock["sku-001"] != 8 {
		t.Fatalf("outs=%+v stock=%v", outs, repo.stock)
	}
}

func Test_shouldDeadLetterTheOrderWhenALineHasNoQuantity(t *testing.T) {
	// GIVEN: a corrupt order with a zero quantity
	o := order("o-8", events.RegionNorth, line("sku-001", 0))

	// WHEN: inventory handles it
	_, err := handler(newRepo(nil)).Handle(context.Background(), nil, runner.Input{Value: o})

	// THEN: it is rejected permanently, so it goes to the dead letter topic
	if !runner.IsPermanent(err) {
		t.Fatalf("expected permanent error, got %v", err)
	}
}

func Test_shouldRejectEventsInventoryDoesNotHandleWhenTheyArrive(t *testing.T) {
	// GIVEN: a payment event delivered to inventory by mistake
	// WHEN: inventory handles it
	_, err := handler(newRepo(nil)).Handle(context.Background(), nil, runner.Input{Value: &events.PaymentCompleted{}})

	// THEN: it is rejected permanently
	if !runner.IsPermanent(err) {
		t.Fatalf("expected permanent error, got %v", err)
	}
}

func Test_shouldRetryLaterWhenTheInventoryDatabaseFails(t *testing.T) {
	down := errors.New("connection refused")
	cases := map[string]struct {
		fail  func(*memRepo)
		input func(h *Handler) any
	}{
		"order lock":        {func(m *memRepo) { m.lockErr = down }, func(*Handler) any { return order("o-9", events.RegionNorth, line("sku-001", 1)) }},
		"stock lock":        {func(m *memRepo) { m.stockErr = down }, func(*Handler) any { return order("o-9", events.RegionNorth, line("sku-001", 1)) }},
		"taking stock":      {func(m *memRepo) { m.takeErr = down }, func(*Handler) any { return order("o-9", events.RegionNorth, line("sku-001", 1)) }},
		"saving":            {func(m *memRepo) { m.saveErr = down }, func(*Handler) any { return order("o-9", events.RegionNorth, line("sku-001", 1)) }},
		"cancel lock":       {func(m *memRepo) { m.lockErr = down }, func(*Handler) any { return cancel("o-9") }},
		"release stock":     {func(m *memRepo) { m.stockErr = down }, reservedThenCancel},
		"returning stock":   {func(m *memRepo) { m.returnErr = down }, reservedThenCancel},
		"saving the cancel": {func(m *memRepo) { m.saveErr = down }, reservedThenCancel},
	}
	for name, c := range cases {
		// GIVEN: an input whose processing hits a database failure while <name>
		repo := newRepo(map[string]int{"sku-001": 10})
		h := handler(repo)
		in := c.input(h)
		c.fail(repo)

		// WHEN: inventory handles it
		_, err := h.Handle(context.Background(), nil, runner.Input{Value: in})

		// THEN: the error is transient, so the runner retries the batch
		if err == nil || runner.IsPermanent(err) {
			t.Fatalf("%s: got %v", name, err)
		}
	}
}

// reservedThenCancel reserves an order with a healthy store and returns its cancellation.
func reservedThenCancel(h *Handler) any {
	if _, err := h.Handle(context.Background(), nil, runner.Input{Value: order("o-9", events.RegionNorth, line("sku-001", 1))}); err != nil {
		panic(err)
	}
	return cancel("o-9")
}
