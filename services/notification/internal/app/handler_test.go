package app

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"kafkalab/pkg/contracts"
	"kafkalab/pkg/events"
	"kafkalab/pkg/kit/runner"
	"kafkalab/services/notification/internal/domain"
)

// memLog is an in-memory notification log.
type memLog struct {
	sent    []Notification
	failing error
}

func (m *memLog) Record(_ context.Context, _ pgx.Tx, n Notification) error {
	if m.failing != nil {
		return m.failing
	}
	m.sent = append(m.sent, n)
	return nil
}

// fakeProvider accepts every message unless it is down.
type fakeProvider struct {
	delivered []Notification
	down      error
}

func (p *fakeProvider) Send(_ context.Context, n Notification) error {
	if p.down != nil {
		return p.down
	}
	p.delivered = append(p.delivered, n)
	return nil
}

var now = time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)

func newHandler() (*Handler, *memLog, *fakeProvider) {
	log, prov := &memLog{}, &fakeProvider{}
	return &Handler{Repo: log, Provider: prov, Now: func() time.Time { return now }}, log, prov
}

func input(v any, eventID, orderID string) runner.Input {
	return runner.Input{Value: v, Meta: events.EventMeta{EventID: eventID, OrderID: orderID, OrderCreatedAt: now.Add(-time.Minute)}}
}

func Test_shouldSendTheMatchingTemplateWhenACustomerRelevantEventArrives(t *testing.T) {
	cases := map[string]struct {
		event    any
		template string
	}{
		"new order":    {&events.OrderCreated{}, "order-confirmed"},
		"shipment":     {&events.OrderShipped{}, "order-shipped"},
		"cancellation": {&events.OrderCancelled{}, "order-cancelled"},
		"refund":       {&events.PaymentRefunded{}, "payment-refunded"},
	}
	for name, c := range cases {
		// GIVEN: a <name> event for order o-1
		h, log, prov := newHandler()

		// WHEN: notification handles it
		outs, err := h.Handle(context.Background(), nil, input(c.event, "e-"+name, "o-1"))

		// THEN: exactly one notifications.sent with that template is published on the customer's channel
		if err != nil || len(outs) != 1 || outs[0].Topic != contracts.TopicNotificationsSent || outs[0].Key != "o-1" {
			t.Fatalf("%s: outs=%+v err=%v", name, outs, err)
		}
		ns := outs[0].Value.(*events.NotificationSent)
		if ns.Template != c.template || string(ns.Channel) != string(domain.ChannelFor("o-1")) ||
			ns.Meta.EventID != contracts.DeriveEventID("e-"+name, "notification") || ns.Meta.CausationID != "e-"+name ||
			ns.Meta.Producer != "notification" || !ns.Meta.OccurredAt.Equal(now) || !ns.Meta.OrderCreatedAt.Equal(now.Add(-time.Minute)) {
			t.Fatalf("%s: unexpected %+v", name, ns)
		}
		// AND: the message went through the provider and is in the notification log
		if len(prov.delivered) != 1 || len(log.sent) != 1 || log.sent[0].Template != c.template || !log.sent[0].SentAt.Equal(now) {
			t.Fatalf("%s: delivered=%+v log=%+v", name, prov.delivered, log.sent)
		}
	}
}

func Test_shouldRetryLaterWhenTheProviderIsDown(t *testing.T) {
	// GIVEN: the SMS/e-mail gateway is unavailable
	h, log, prov := newHandler()
	prov.down = errors.New("gateway timeout")

	// WHEN: an order is shipped
	outs, err := h.Handle(context.Background(), nil, input(&events.OrderShipped{}, "e-1", "o-1"))

	// THEN: nothing is logged or published and the error is retried
	if err == nil || runner.IsPermanent(err) || outs != nil || len(log.sent) != 0 {
		t.Fatalf("outs=%+v err=%v", outs, err)
	}
}

func Test_shouldRetryLaterWhenTheNotificationLogCannotBeWritten(t *testing.T) {
	// GIVEN: the database is failing
	h, log, _ := newHandler()
	log.failing = errors.New("connection refused")

	// WHEN: an order is confirmed
	outs, err := h.Handle(context.Background(), nil, input(&events.OrderCreated{}, "e-1", "o-1"))

	// THEN: nothing is published and the error is retried
	if err == nil || runner.IsPermanent(err) || outs != nil {
		t.Fatalf("outs=%+v err=%v", outs, err)
	}
}

func Test_shouldRejectEventsNotificationDoesNotHandleWhenTheyArrive(t *testing.T) {
	// GIVEN: an inventory event delivered to notification by mistake
	h, _, prov := newHandler()

	// WHEN: notification handles it
	_, err := h.Handle(context.Background(), nil, input(&events.InventoryReserved{}, "e-1", "o-1"))

	// THEN: it is rejected permanently and the customer is not bothered
	if !runner.IsPermanent(err) || len(prov.delivered) != 0 {
		t.Fatalf("expected permanent error, got %v", err)
	}
}
