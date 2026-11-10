package app

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/jackc/pgx/v5"

	"kafkalab/pkg/events"
	"kafkalab/pkg/kit/runner"
	"kafkalab/services/analytics/internal/domain"
)

var errDown = errors.New("connection refused")

var t0 = time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)

// memRepo is an in-memory orders table.
type memRepo struct {
	rows       map[string]domain.Order
	forget     map[string]time.Time
	failUpsert error
	failSave   error
	failPurge  error
}

func newRepo() *memRepo {
	return &memRepo{rows: map[string]domain.Order{}, forget: map[string]time.Time{}}
}

func (m *memRepo) Upsert(_ context.Context, _ pgx.Tx, seed domain.Order, forget time.Time) (domain.Order, bool, error) {
	if m.failUpsert != nil {
		return domain.Order{}, false, m.failUpsert
	}
	if o, ok := m.rows[seed.OrderID]; ok {
		return o, false, nil
	}
	m.rows[seed.OrderID], m.forget[seed.OrderID] = seed, forget
	return seed, true, nil
}

func (m *memRepo) Save(_ context.Context, _ pgx.Tx, o domain.Order, forget time.Time) error {
	if m.failSave != nil {
		return m.failSave
	}
	m.rows[o.OrderID], m.forget[o.OrderID] = o, forget
	return nil
}

func (m *memRepo) Purge(_ context.Context, _ pgx.Tx, now time.Time, limit int) (int64, error) {
	if m.failPurge != nil {
		return 0, m.failPurge
	}
	var n int64
	for id, at := range m.forget {
		if at.Before(now) && n < int64(limit) {
			delete(m.rows, id)
			delete(m.forget, id)
			n++
		}
	}
	return n, nil
}

type world struct {
	now  time.Time
	repo *memRepo
	set  *metrics.Set
	h    *Handler
}

func newWorld() *world {
	w := &world{now: t0, repo: newRepo(), set: metrics.NewSet()}
	w.h = &Handler{Repo: w.repo, Metrics: w.set, Now: func() time.Time { return w.now }}
	return w
}

func (w *world) handle(t *testing.T, v any) {
	t.Helper()
	if _, err := w.h.Handle(context.Background(), nil, runner.Input{Value: v}); err != nil {
		t.Fatal(err)
	}
}

// metric returns the exported value of one series, as Prometheus would scrape it.
func (w *world) metric(series string) string {
	var sb strings.Builder
	w.set.WritePrometheus(&sb)
	for _, line := range strings.Split(sb.String(), "\n") {
		if rest, ok := strings.CutPrefix(line, series+" "); ok {
			return rest
		}
	}
	return "absent"
}

func order(id, region string, cents int64) *events.OrderCreated {
	return &events.OrderCreated{Meta: events.EventMeta{EventID: "evt-" + id, OrderID: id, OrderCreatedAt: t0.Add(-4 * time.Second)},
		Region: events.Region(region), PaymentMethod: events.PaymentMethodPaypal, TotalCents: cents}
}

func shipped(id string) *events.OrderShipped {
	return &events.OrderShipped{Meta: events.EventMeta{EventID: "shp-" + id, OrderID: id, OrderCreatedAt: t0.Add(-4 * time.Second)},
		Carrier: "DHL", Warehouse: "WH-NORTH"}
}

func cancelled(id string) *events.OrderCancelled {
	return &events.OrderCancelled{Meta: events.EventMeta{EventID: "cnc-" + id, OrderID: id, OrderCreatedAt: t0.Add(-2 * time.Second)},
		Reason: events.CancellationReasonOutOfStock}
}

func Test_shouldCountTheOrderAndItsValueWhenAnOrderIsCreated(t *testing.T) {
	// GIVEN: analytics with no history
	w := newWorld()

	// WHEN: a 49.90 EUR PayPal order from NORTH arrives
	w.handle(t, order("o-1", "NORTH", 4990))

	// THEN: one order and 4990 cents of order value are counted for NORTH, no revenue yet
	if w.metric(`lab_orders_created_total{region="NORTH",payment_method="PAYPAL"}`) != "1" ||
		w.metric(`lab_order_value_cents_total{region="NORTH"}`) != "4990" ||
		w.metric(`lab_revenue_cents_total{region="NORTH"}`) != "absent" {
		t.Fatal(w.dump())
	}
}

func Test_shouldCountRevenueAndCompletionTimeWhenTheOrderShips(t *testing.T) {
	// GIVEN: a counted 49.90 EUR order from NORTH, created 4 s ago
	w := newWorld()
	w.handle(t, order("o-1", "NORTH", 4990))

	// WHEN: it ships with DHL from WH-NORTH
	w.handle(t, shipped("o-1"))

	// THEN: the shipment, its revenue and a 4 s completion time are recorded
	if w.metric(`lab_orders_shipped_total{carrier="DHL",warehouse="WH-NORTH"}`) != "1" ||
		w.metric(`lab_revenue_cents_total{region="NORTH"}`) != "4990" ||
		w.metric(`lab_order_completion_seconds_sum{outcome="shipped"}`) != "4" ||
		w.metric(`lab_order_completion_seconds_count{outcome="shipped"}`) != "1" {
		t.Fatal(w.dump())
	}
	if w.repo.forget["o-1"] != t0.Add(domain.KeepAfterOutcome) {
		t.Fatalf("finished order kept until %v", w.repo.forget["o-1"])
	}
}

func Test_shouldStillCountRevenueWhenTheShipmentIsProcessedBeforeTheOrder(t *testing.T) {
	// GIVEN: analytics lags on orders.created and sees the shipment first
	w := newWorld()
	w.handle(t, shipped("o-2"))
	before := w.metric(`lab_revenue_cents_total{region="WEST"}`)

	// WHEN: the order arrives afterwards
	w.handle(t, order("o-2", "WEST", 1200))

	// THEN: the shipment counted at once, the revenue once the value is known
	if before != "absent" || w.metric(`lab_revenue_cents_total{region="WEST"}`) != "1200" ||
		w.metric(`lab_orders_shipped_total{carrier="DHL",warehouse="WH-NORTH"}`) != "1" ||
		w.metric(`lab_orders_created_total{region="WEST",payment_method="PAYPAL"}`) != "1" {
		t.Fatal(w.dump())
	}
}

func Test_shouldCountTheCancellationReasonWhenAnOrderIsCancelled(t *testing.T) {
	// GIVEN: a counted order
	w := newWorld()
	w.handle(t, order("o-3", "EAST", 900))

	// WHEN: it is cancelled for lack of stock
	w.handle(t, cancelled("o-3"))

	// THEN: the cancellation is counted by reason, with its completion time, and no revenue
	if w.metric(`lab_orders_cancelled_total{reason="OUT_OF_STOCK"}`) != "1" ||
		w.metric(`lab_order_completion_seconds_sum{outcome="cancelled"}`) != "2" ||
		w.metric(`lab_revenue_cents_total{region="EAST"}`) != "absent" {
		t.Fatal(w.dump())
	}
}

func Test_shouldCountABusinessDuplicateWhenTheSameOrderIsCreatedTwice(t *testing.T) {
	// GIVEN: a counted order
	w := newWorld()
	w.handle(t, order("o-4", "SOUTH", 500))

	// WHEN: the same order arrives again under a different event id
	again := order("o-4", "SOUTH", 500)
	again.Meta.EventID = "evt-o-4-copy"
	w.handle(t, again)

	// THEN: it is counted as a duplicate, not as a second order
	if w.metric(`lab_orders_created_total{region="SOUTH",payment_method="PAYPAL"}`) != "1" ||
		w.metric(`lab_order_value_cents_total{region="SOUTH"}`) != "500" ||
		w.metric(`lab_duplicates_total{topic="orders.created"}`) != "1" {
		t.Fatal(w.dump())
	}
}

func Test_shouldCountADuplicateWhenAnOrderReachesASecondOutcome(t *testing.T) {
	// GIVEN: a shipped order
	w := newWorld()
	w.handle(t, order("o-5", "NORTH", 100))
	w.handle(t, shipped("o-5"))

	// WHEN: a second shipment and a cancellation arrive for it
	w.handle(t, shipped("o-5"))
	w.handle(t, cancelled("o-5"))

	// THEN: both are duplicates; shipments, cancellations and revenue are unchanged
	if w.metric(`lab_duplicates_total{topic="orders.shipped"}`) != "1" || w.metric(`lab_duplicates_total{topic="orders.cancelled"}`) != "1" ||
		w.metric(`lab_orders_shipped_total{carrier="DHL",warehouse="WH-NORTH"}`) != "1" ||
		w.metric(`lab_orders_cancelled_total{reason="OUT_OF_STOCK"}`) != "absent" ||
		w.metric(`lab_revenue_cents_total{region="NORTH"}`) != "100" {
		t.Fatal(w.dump())
	}
}

func Test_shouldCountEveryOtherBusinessEventWhenItArrives(t *testing.T) {
	// GIVEN: analytics with no history
	w := newWorld()

	// WHEN: one of each payment, inventory and notification event arrives
	for _, v := range []any{
		&events.PaymentCompleted{},
		&events.PaymentFailed{Reason: events.PaymentFailureReasonFraudSuspected},
		&events.PaymentRefunded{AmountCents: 2500},
		&events.InventoryReserved{},
		&events.InventoryRejected{Reason: events.InventoryRejectionReasonUnknownSku},
		&events.InventoryReleased{},
		&events.NotificationSent{Channel: events.NotificationChannelSms},
	} {
		w.handle(t, v)
	}

	// THEN: each lands in its business counter with the contract labels
	for series, want := range map[string]string{
		`lab_payments_total{result="completed",reason=""}`:             "1",
		`lab_payments_total{result="failed",reason="FRAUD_SUSPECTED"}`: "1",
		`lab_refunds_total`:                                              "1",
		`lab_refunded_cents_total`:                                       "2500",
		`lab_reservations_total{result="reserved",reason=""}`:            "1",
		`lab_reservations_total{result="rejected",reason="UNKNOWN_SKU"}`: "1",
		`lab_releases_total`:                                             "1",
		`lab_notifications_total{channel="SMS"}`:                         "1",
	} {
		if got := w.metric(series); got != want {
			t.Errorf("%s = %s, want %s\n%s", series, got, want, w.dump())
		}
	}
}

func Test_shouldRejectEventsAnalyticsDoesNotKnowWhenTheyArrive(t *testing.T) {
	// GIVEN: a platform event delivered as a business event
	// WHEN: analytics handles it
	_, err := newWorld().h.Handle(context.Background(), nil, runner.Input{Value: &events.ControlSnapshot{}})

	// THEN: it is rejected permanently
	if !runner.IsPermanent(err) {
		t.Fatalf("got %v", err)
	}
}

func Test_shouldRetryWithoutCountingWhenTheOrdersTableIsUnavailable(t *testing.T) {
	// GIVEN: a shipment whose order is still unknown, a counted order, and a database that
	// cannot be read (first round) or written (second round)
	for _, fail := range []func(*memRepo){
		func(m *memRepo) { m.failUpsert = errDown },
		func(m *memRepo) { m.failSave = errDown },
	} {
		w := newWorld()
		w.handle(t, shipped("o-6")) // the late creation must be written
		w.handle(t, order("o-7", "NORTH", 100))
		fail(w.repo)

		// WHEN: an order and a cancellation arrive
		_, errCreated := w.h.Handle(context.Background(), nil, runner.Input{Value: order("o-6", "NORTH", 100)})
		_, errCancelled := w.h.Handle(context.Background(), nil, runner.Input{Value: cancelled("o-7")})

		// THEN: both are retried and neither the late order nor the cancellation is counted
		if errCreated == nil || runner.IsPermanent(errCreated) || errCancelled == nil || runner.IsPermanent(errCancelled) ||
			w.metric(`lab_orders_created_total{region="NORTH",payment_method="PAYPAL"}`) != "1" ||
			w.metric(`lab_orders_cancelled_total{reason="OUT_OF_STOCK"}`) != "absent" {
			t.Fatalf("created: %v cancelled: %v\n%s", errCreated, errCancelled, w.dump())
		}
	}
}

func (w *world) dump() string {
	var sb strings.Builder
	w.set.WritePrometheus(&sb)
	return sb.String()
}
