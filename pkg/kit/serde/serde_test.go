package serde

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"kafkalab/pkg/contracts"
	"kafkalab/pkg/events"
	"kafkalab/pkg/kit/testkit"
)

func newSerde(t *testing.T, topics ...string) *Serde {
	t.Helper()
	reg := testkit.NewRegistry(t, true)
	s, err := New(context.Background(), Options{Registry: reg.Client(t), Topics: topics})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func Test_shouldReadBackTheSameOrderWhenAnOrderIsEncodedAndDecoded(t *testing.T) {
	// GIVEN: a freshly created order with two lines
	s := newSerde(t, contracts.TopicOrdersCreated)
	at := time.UnixMilli(1_700_000_000_000).UTC()
	order := &events.OrderCreated{
		Meta:       events.EventMeta{EventID: "e-1", OrderID: "o-1", OccurredAt: at, OrderCreatedAt: at, Producer: "order-generator"},
		CustomerID: "c-9", Region: "NORTH", PaymentMethod: "CARD", Currency: "EUR", TotalCents: 2598,
		Lines: []events.OrderLine{{Sku: "sku-1", Quantity: 2, UnitPriceCents: 1299}},
	}

	// WHEN: it is encoded for Kafka and decoded by a consumer
	b, err := s.Encode(order)
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.Decode(b)

	// THEN: the consumer sees the identical order, framed in the Confluent wire format
	if err != nil {
		t.Fatal(err)
	}
	back := got.(*events.OrderCreated)
	if b[0] != 0 || back.Meta.OrderID != "o-1" || back.TotalCents != 2598 || back.Lines[0].Sku != "sku-1" || !back.Meta.OccurredAt.Equal(at) {
		t.Fatalf("round trip mismatch: %+v", back)
	}
}

func Test_shouldRejectRecordsAsUndecodableWhenAPoisonMessageArrives(t *testing.T) {
	// GIVEN: a consumer of orders and a record of random garbage
	s := newSerde(t, contracts.TopicOrdersCreated)

	// WHEN: the garbage is decoded
	for _, poison := range [][]byte{[]byte("{not avro}"), {0, 0, 0, 0, 99, 1, 2}, nil} {
		_, err := s.Decode(poison)

		// THEN: it is reported as undecodable so the runner dead-letters it
		if !errors.Is(err, ErrUndecodable) {
			t.Fatalf("expected ErrUndecodable for %q, got %v", poison, err)
		}
	}
}

func Test_shouldRefuseToEncodeWhenTheEventTypeIsNotOneOfTheServiceTopics(t *testing.T) {
	// GIVEN: a payment service that only knows its own topics
	s := newSerde(t, contracts.TopicPaymentsCompleted)

	// WHEN: it tries to encode a shipment
	_, err := s.Encode(&events.OrderShipped{})

	// THEN: encoding fails instead of producing a record no one can read
	if err == nil {
		t.Fatal("expected an error for an unregistered type")
	}
}

func Test_shouldWaitForTheSchemaWhenTheRegistrationJobHasNotRunYet(t *testing.T) {
	// GIVEN: a registry that is empty at startup
	reg := testkit.NewRegistry(t, false)
	go func() {
		time.Sleep(100 * time.Millisecond)
		reg.Add(contracts.Subject(contracts.TopicOrdersShipped), mustRead(t, "OrderShipped"))
	}()

	// WHEN: shipping starts and resolves its schemas
	s, err := New(context.Background(), Options{Registry: reg.Client(t), Topics: []string{contracts.TopicOrdersShipped},
		RetryEvery: 20 * time.Millisecond, Log: slog.New(slog.NewTextHandler(io.Discard, nil))})

	// THEN: it waits until the job registered the schema and then works
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Encode(&events.OrderShipped{Meta: events.EventMeta{OrderID: "o"}}); err != nil {
		t.Fatal(err)
	}
}

func Test_shouldGiveUpWhenTheRegistryStaysDownUntilStartupIsCancelled(t *testing.T) {
	// GIVEN: a registry outage during startup
	reg := testkit.NewRegistry(t, true)
	reg.SetDown(true)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// WHEN: a service resolves its schemas
	_, err := New(ctx, Options{Registry: reg.Client(t), Topics: []string{contracts.TopicOrdersCreated}, RetryEvery: 10 * time.Millisecond})

	// THEN: it fails with a message naming the topic
	if err == nil || !strings.Contains(err.Error(), contracts.TopicOrdersCreated) {
		t.Fatalf("expected error naming the topic, got %v", err)
	}
}

func Test_shouldFailFastWhenATopicHasNoAvroType(t *testing.T) {
	// GIVEN: the dead letter topic, which carries raw bytes
	reg := testkit.NewRegistry(t, true)

	// WHEN: a service asks for its schema
	_, err := New(context.Background(), Options{Registry: reg.Client(t), Topics: []string{contracts.TopicOrdersDLQ}})

	// THEN: it is told that the topic has no Avro type
	if err == nil {
		t.Fatal("expected error for orders.dlq")
	}
}

func Test_shouldKeepRetryingWhenTheRegisteredSchemaIsBroken(t *testing.T) {
	// GIVEN: a subject whose schema text is not valid Avro
	reg := testkit.NewRegistry(t, false)
	reg.Add(contracts.Subject(contracts.TopicOrdersCreated), `{"type":"nonsense"}`)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()

	// WHEN: the service resolves it
	_, err := New(ctx, Options{Registry: reg.Client(t), Topics: []string{contracts.TopicOrdersCreated}, RetryEvery: 10 * time.Millisecond})

	// THEN: startup fails with the parse problem
	if err == nil || !strings.Contains(err.Error(), "parse") {
		t.Fatalf("expected parse error, got %v", err)
	}
}

func Test_shouldExposeTheEnvelopeWhenAnyBusinessEventIsGiven(t *testing.T) {
	// GIVEN: one event of every business type, each for order o-1
	m := events.EventMeta{OrderID: "o-1"}
	all := []any{
		&events.OrderCreated{Meta: m}, &events.PaymentCompleted{Meta: m}, &events.PaymentFailed{Meta: m},
		&events.PaymentRefunded{Meta: m}, &events.InventoryReserved{Meta: m}, &events.InventoryRejected{Meta: m},
		&events.InventoryReleased{Meta: m}, &events.OrderShipped{Meta: m}, &events.OrderCancelled{Meta: m},
		&events.NotificationSent{Meta: m},
	}
	for _, e := range all {
		// WHEN: the runner asks for its envelope
		got, ok := Meta(e)

		// THEN: it gets the order id for idempotency and latency metrics
		if !ok || got.OrderID != "o-1" {
			t.Fatalf("no meta for %T", e)
		}
	}
	// AND: platform events have no business envelope
	if _, ok := Meta(&events.ControlSnapshot{}); ok {
		t.Fatal("control snapshots must not have a business envelope")
	}
}

func Test_shouldProvideAPrototypeWhenATopicHasAnAvroType(t *testing.T) {
	// GIVEN: every Avro topic of the contract
	for topic, name := range contracts.ValueTypes {
		// WHEN: its prototype is created
		proto, ok := Prototypes[name]

		// THEN: it exists and yields a fresh value
		if !ok || proto() == nil {
			t.Fatalf("no prototype for %s (%s)", topic, name)
		}
	}
}

func mustRead(t *testing.T, name string) string {
	t.Helper()
	b, err := readSchema(name)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
