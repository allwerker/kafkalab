package app

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"kafkalab/pkg/contracts"
	"kafkalab/pkg/events"
	"kafkalab/pkg/kit/runner"
	"kafkalab/services/payment/internal/domain"
)

// memRepo is an in-memory payment store.
type memRepo struct {
	rows        map[string]Payment
	failing     error
	saveFailing error
}

func newRepo() *memRepo { return &memRepo{rows: map[string]Payment{}} }

func (m *memRepo) Lock(_ context.Context, _ pgx.Tx, id string) (Payment, error) {
	if m.failing != nil {
		return Payment{}, m.failing
	}
	p, ok := m.rows[id]
	if !ok {
		p = Payment{OrderID: id}
	}
	return p, nil
}

func (m *memRepo) Save(_ context.Context, _ pgx.Tx, p Payment) error {
	if m.saveFailing != nil {
		return m.saveFailing
	}
	m.rows[p.OrderID] = p
	return nil
}

type knobs events.Knobs

func (k knobs) Knobs() events.Knobs { return events.Knobs(k) }

var now = time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)

func handler(repo *memRepo, k events.Knobs) *Handler {
	return &Handler{Repo: repo, Control: knobs(k), Now: func() time.Time { return now }}
}

// orderWithRoll finds an event id whose stable roll satisfies pred, so tests control the outcome.
func orderWithRoll(id string, cents int64, m events.PaymentMethod, pred func(float64) bool) *events.OrderCreated {
	for i := 0; ; i++ {
		eid := id + "-" + strconv.Itoa(i)
		if pred(Roll(eid)) {
			return &events.OrderCreated{Meta: events.EventMeta{EventID: eid, OrderID: id, OrderCreatedAt: now.Add(-time.Second)},
				TotalCents: cents, PaymentMethod: m}
		}
	}
}

func lucky(r float64) bool   { return r > 0.2 }
func unlucky(r float64) bool { return r < 0.01 }

func handle(t *testing.T, h *Handler, v any) []runner.Output {
	t.Helper()
	outs, err := h.Handle(context.Background(), nil, runner.Input{Value: v})
	if err != nil {
		t.Fatal(err)
	}
	return outs
}

func Test_shouldPublishACompletedPaymentWhenANewOrderIsAuthorised(t *testing.T) {
	// GIVEN: a new 89.00 EUR PayPal order
	repo := newRepo()
	h := handler(repo, events.Knobs{})
	o := orderWithRoll("o-1", 8900, events.PaymentMethodPaypal, lucky)

	// WHEN: payment handles it
	outs := handle(t, h, o)

	// THEN: payments.completed is published for the full amount and the charge is stored
	pc := outs[0].Value.(*events.PaymentCompleted)
	if outs[0].Topic != contracts.TopicPaymentsCompleted || outs[0].Key != "o-1" || pc.AmountCents != 8900 ||
		pc.Meta.CausationID != o.Meta.EventID || pc.Meta.EventID != contracts.DeriveEventID(o.Meta.EventID, "payment") ||
		pc.Meta.Producer != "payment" || !pc.Meta.OrderCreatedAt.Equal(o.Meta.OrderCreatedAt) || !pc.Meta.OccurredAt.Equal(now) {
		t.Fatalf("unexpected %+v", pc)
	}
	if repo.rows["o-1"].State != domain.StateCharged || repo.rows["o-1"].PaymentID != pc.PaymentID {
		t.Fatalf("stored %+v", repo.rows["o-1"])
	}
}

func Test_shouldPublishAFailedPaymentWhenTheProviderDeclines(t *testing.T) {
	// GIVEN: a card order that the provider declines
	repo := newRepo()
	o := orderWithRoll("o-2", 1500, events.PaymentMethodCard, unlucky)

	// WHEN: payment handles it
	outs := handle(t, handler(repo, events.Knobs{}), o)

	// THEN: payments.failed is published with the reason
	pf := outs[0].Value.(*events.PaymentFailed)
	if outs[0].Topic != contracts.TopicPaymentsFailed || pf.Reason != events.PaymentFailureReasonDeclined || pf.AmountCents != 1500 {
		t.Fatalf("unexpected %+v", pf)
	}
}

func Test_shouldFailPaymentsAsUnavailableWhenTheProviderDownScenarioRuns(t *testing.T) {
	// GIVEN: the Payment Provider Down scenario
	o := orderWithRoll("o-3", 1500, events.PaymentMethodCard, lucky)

	// WHEN: a normally successful order arrives
	outs := handle(t, handler(newRepo(), events.Knobs{ProviderDown: true}), o)

	// THEN: it fails with PROVIDER_UNAVAILABLE
	if pf := outs[0].Value.(*events.PaymentFailed); pf.Reason != events.PaymentFailureReasonProviderUnavailable {
		t.Fatalf("unexpected %+v", pf)
	}
}

func Test_shouldDeclineMoreOrdersWhenTheFailureProbabilityKnobIsRaised(t *testing.T) {
	// GIVEN: a scenario forcing a 100% decline probability
	all := 1.0
	o := orderWithRoll("o-4", 1500, events.PaymentMethodWallet, lucky)

	// WHEN: the order arrives
	outs := handle(t, handler(newRepo(), events.Knobs{BusinessFailureProbability: &all}), o)

	// THEN: it is declined
	if outs[0].Topic != contracts.TopicPaymentsFailed {
		t.Fatalf("unexpected %+v", outs)
	}
}

func Test_shouldRefundTheChargeWhenShippingCancelsAPaidOrder(t *testing.T) {
	// GIVEN: a charged order
	repo := newRepo()
	h := handler(repo, events.Knobs{})
	o := orderWithRoll("o-5", 2500, events.PaymentMethodCard, lucky)
	charged := handle(t, h, o)[0].Value.(*events.PaymentCompleted)
	cancel := &events.OrderCancelled{Meta: events.EventMeta{EventID: "c-5", OrderID: "o-5"}, Reason: events.CancellationReasonOutOfStock, RefundRequired: true}

	// WHEN: shipping cancels it because the item is out of stock
	outs := handle(t, h, cancel)

	// THEN: the customer gets the same payment refunded
	pr := outs[0].Value.(*events.PaymentRefunded)
	if outs[0].Topic != contracts.TopicPaymentsRefunded || pr.PaymentID != charged.PaymentID || pr.AmountCents != 2500 ||
		pr.Meta.EventID != contracts.DeriveEventID("c-5", "refund") || repo.rows["o-5"].State != domain.StateRefunded {
		t.Fatalf("unexpected %+v / %+v", pr, repo.rows["o-5"])
	}
}

func Test_shouldNeverChargeAnOrderWhenItsCancellationArrivedFirst(t *testing.T) {
	// GIVEN: a deadline cancellation that reached payment before the order itself
	repo := newRepo()
	h := handler(repo, events.Knobs{})
	if outs := handle(t, h, &events.OrderCancelled{Meta: events.EventMeta{EventID: "c-6", OrderID: "o-6"}, Reason: events.CancellationReasonDeadlineExceeded}); outs != nil {
		t.Fatalf("early cancellation must publish nothing, got %+v", outs)
	}

	// WHEN: the order finally arrives
	outs := handle(t, h, orderWithRoll("o-6", 999, events.PaymentMethodCard, lucky))

	// THEN: nothing is charged or published
	if outs != nil || repo.rows["o-6"].State != domain.StateCancelledEarly {
		t.Fatalf("outs=%+v state=%s", outs, repo.rows["o-6"].State)
	}
}

func Test_shouldNotRefundWhenTheCancelledOrderWasNeverCharged(t *testing.T) {
	// GIVEN: an order whose payment was declined
	repo := newRepo()
	h := handler(repo, events.Knobs{})
	handle(t, h, orderWithRoll("o-7", 1500, events.PaymentMethodCard, unlucky))

	// WHEN: shipping cancels it
	outs := handle(t, h, &events.OrderCancelled{Meta: events.EventMeta{EventID: "c-7", OrderID: "o-7"}, Reason: events.CancellationReasonPaymentFailed})

	// THEN: no refund is published
	if outs != nil || repo.rows["o-7"].State != domain.StateDeclined {
		t.Fatalf("outs=%+v state=%s", outs, repo.rows["o-7"].State)
	}
}

func Test_shouldRejectAnOrderPermanentlyWhenItsTotalIsNotPositive(t *testing.T) {
	// GIVEN: a corrupt order with a total of zero
	o := &events.OrderCreated{Meta: events.EventMeta{EventID: "e-8", OrderID: "o-8"}, TotalCents: 0}

	// WHEN: payment handles it
	_, err := handler(newRepo(), events.Knobs{}).Handle(context.Background(), nil, runner.Input{Value: o})

	// THEN: it is rejected permanently, so it goes to the dead letter topic
	if !runner.IsPermanent(err) {
		t.Fatalf("expected permanent error, got %v", err)
	}
}

func Test_shouldRejectEventsPaymentDoesNotHandleWhenTheyArrive(t *testing.T) {
	// GIVEN: a shipment event delivered to payment by mistake
	// WHEN: payment handles it
	_, err := handler(newRepo(), events.Knobs{}).Handle(context.Background(), nil, runner.Input{Value: &events.OrderShipped{}})

	// THEN: it is rejected permanently
	if !runner.IsPermanent(err) {
		t.Fatalf("expected permanent error, got %v", err)
	}
}

func Test_shouldRetryLaterWhenThePaymentStoreIsUnavailable(t *testing.T) {
	// GIVEN: the database is failing
	repo := newRepo()
	repo.failing = errors.New("connection refused")
	h := handler(repo, events.Knobs{})

	// WHEN: an order or a cancellation arrives
	_, errOrder := h.Handle(context.Background(), nil, runner.Input{Value: orderWithRoll("o-9", 1, events.PaymentMethodCard, lucky)})
	_, errCancel := h.Handle(context.Background(), nil, runner.Input{Value: &events.OrderCancelled{Meta: events.EventMeta{OrderID: "o-9"}}})

	// THEN: both fail transiently (not permanently), so the runner retries them
	if errOrder == nil || errCancel == nil || runner.IsPermanent(errOrder) || runner.IsPermanent(errCancel) {
		t.Fatalf("got %v / %v", errOrder, errCancel)
	}
}

func Test_shouldPublishNothingWhenADuplicateOrderWithANewEventIDArrives(t *testing.T) {
	// GIVEN: an order that is already charged
	repo := newRepo()
	h := handler(repo, events.Knobs{})
	handle(t, h, orderWithRoll("o-10", 700, events.PaymentMethodCard, lucky))

	// WHEN: a copy with a different event id arrives (not caught by event-id deduplication)
	outs := handle(t, h, orderWithRoll("o-10", 700, events.PaymentMethodCard, func(r float64) bool { return r > 0.5 }))

	// THEN: the order is not charged twice
	if outs != nil {
		t.Fatalf("expected no output, got %+v", outs)
	}
}

func Test_shouldDecideTheSameWayWhenTheSameEventIsRolledTwice(t *testing.T) {
	// GIVEN: an event id
	// WHEN: its roll is computed twice
	a, b := Roll("evt-123"), Roll("evt-123")

	// THEN: the result is identical and within [0,1)
	if a != b || a < 0 || a >= 1 {
		t.Fatalf("got %v and %v", a, b)
	}
}

func Test_shouldRetryTheRefundLaterWhenItsStateCannotBeStored(t *testing.T) {
	// GIVEN: a charged order and a database that starts failing on writes
	repo := newRepo()
	h := handler(repo, events.Knobs{})
	handle(t, h, orderWithRoll("o-11", 1200, events.PaymentMethodCard, lucky))
	repo.saveFailing = errors.New("connection refused")

	// WHEN: the cancellation arrives
	outs, err := h.Handle(context.Background(), nil, runner.Input{Value: &events.OrderCancelled{Meta: events.EventMeta{EventID: "c-11", OrderID: "o-11"}}})

	// THEN: no refund is published yet and the error is retried
	if err == nil || outs != nil {
		t.Fatalf("outs=%+v err=%v", outs, err)
	}
}
