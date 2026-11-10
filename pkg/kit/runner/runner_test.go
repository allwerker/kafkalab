package runner

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"kafkalab/pkg/contracts"
	"kafkalab/pkg/events"
)

func Test_shouldPublishTheOutcomeAndCommitWhenAnOrderIsProcessed(t *testing.T) {
	// GIVEN: a payment service and one new order of 42.00 EUR
	h := newHarness(t)
	var calls atomic.Int64
	h.start(h.runner(paymentHandler(&calls)))

	// WHEN: the order arrives
	h.produce(order("o-1", 4200))

	// THEN: the customer is charged once, payments.completed is published and the offset is committed
	out := h.read(contracts.TopicPaymentsCompleted, 1)
	v, err := h.serde.Decode(out[0].Value)
	if err != nil {
		t.Fatal(err)
	}
	if p := v.(*events.PaymentCompleted); p.AmountCents != 4200 || string(out[0].Key) != "o-1" || header(out[0], contracts.HeaderProducer) != "payment" {
		t.Fatalf("unexpected outcome %+v", p)
	}
	h.eventually("offset commit", func() bool {
		offs, err := kadm.NewClient(h.writer).FetchOffsets(context.Background(), "payment")
		if err != nil {
			return false
		}
		o, ok := offs.Lookup(contracts.TopicOrdersCreated, out[0].Partition)
		return ok && o.At == 1
	})
	if h.charges() != 1 || !h.hasMetric(`lab_records_processed_total{topic="orders.created",result="ok"} 1`) {
		t.Fatalf("charges=%d\n%s", h.charges(), h.metrics())
	}
}

func Test_shouldRepublishTheStoredOutcomeWithoutChargingTwiceWhenAnOrderIsRedelivered(t *testing.T) {
	// GIVEN: a payment service that already processed an order
	h := newHarness(t)
	var calls atomic.Int64
	h.start(h.runner(paymentHandler(&calls)))
	o := order("o-dup", 1999)
	h.produce(o)
	h.read(contracts.TopicPaymentsCompleted, 1)

	// WHEN: the same order event is delivered again (producer retry, Duplicate Delivery scenario)
	h.produce(o)

	// THEN: the customer is not charged again, but the identical outcome is re-published
	out := h.read(contracts.TopicPaymentsCompleted, 2)
	if calls.Load() != 1 || h.charges() != 1 {
		t.Fatalf("handler ran %d times, %d charges", calls.Load(), h.charges())
	}
	a, _ := h.serde.Decode(out[0].Value)
	b, _ := h.serde.Decode(out[1].Value)
	if a.(*events.PaymentCompleted).Meta.EventID != b.(*events.PaymentCompleted).Meta.EventID {
		t.Fatal("re-published outcome must carry the same event id")
	}
	h.eventually("duplicate metric", func() bool {
		return h.hasMetric(`lab_records_processed_total{topic="orders.created",result="duplicate"} 1`)
	})
}

func Test_shouldDeadLetterTheRecordWithItsOriginWhenAPoisonMessageArrives(t *testing.T) {
	// GIVEN: a payment service
	h := newHarness(t)
	var calls atomic.Int64
	h.start(h.runner(paymentHandler(&calls)))

	// WHEN: a malformed record is followed by a valid order (Poison Message scenario)
	h.produceRaw("o-poison", []byte("this is not avro"))
	h.produce(order("o-ok", 500))

	// THEN: the garbage lands in orders.dlq with its origin, and the valid order is still processed
	dlq := h.read(contracts.TopicOrdersDLQ, 1)
	if string(dlq[0].Value) != "this is not avro" || header(dlq[0], contracts.HeaderDLQSourceTopic) != contracts.TopicOrdersCreated ||
		header(dlq[0], contracts.HeaderDLQReason) != "undecodable" || header(dlq[0], contracts.HeaderDLQConsumer) != "payment" ||
		header(dlq[0], contracts.HeaderDLQSourceOffset) != "0" || header(dlq[0], contracts.HeaderDLQSourcePartition) == "" ||
		header(dlq[0], contracts.HeaderDLQError) == "" {
		t.Fatalf("unexpected DLQ record %+v", dlq[0].Headers)
	}
	h.read(contracts.TopicPaymentsCompleted, 1)
	h.eventually("dlq metric", func() bool {
		return h.hasMetric(`lab_dlq_records_total{topic="orders.created",reason="undecodable"} 1`)
	})
}

func Test_shouldDeadLetterWithoutSideEffectsWhenTheHandlerRejectsAnOrderPermanently(t *testing.T) {
	// GIVEN: a handler that writes a charge and then finds the order invalid
	h := newHarness(t)
	h.start(h.runner(HandlerFunc(func(ctx context.Context, tx pgx.Tx, in Input) ([]Output, error) {
		_, _ = tx.Exec(ctx, `INSERT INTO charges VALUES ($1, 1)`, in.Meta.OrderID)
		return nil, Permanent(errors.New("order has no lines"))
	})))

	// WHEN: such an order arrives
	h.produce(order("o-invalid", 0))

	// THEN: it is dead-lettered as rejected and the half-written charge is rolled back
	dlq := h.read(contracts.TopicOrdersDLQ, 1)
	if header(dlq[0], contracts.HeaderDLQReason) != "rejected" || header(dlq[0], contracts.HeaderDLQError) != "order has no lines" {
		t.Fatalf("unexpected DLQ headers %+v", dlq[0].Headers)
	}
	if h.charges() != 0 {
		t.Fatal("rejected order must not leave a charge behind")
	}
}

func Test_shouldDeadLetterTheRecordWhenAnEventOfTheWrongTypeArrivesOnATopic(t *testing.T) {
	// GIVEN: a misconfigured producer that writes a payment event onto orders.created
	h := newHarness(t)
	var calls atomic.Int64
	h.start(h.runner(paymentHandler(&calls)))
	b, err := h.serde.Encode(&events.PaymentCompleted{Meta: events.EventMeta{EventID: "x", OrderID: "o-x"}, PaymentMethod: events.PaymentMethodCard})
	if err != nil {
		t.Fatal(err)
	}

	// WHEN: the payment service reads it
	h.produceRaw("o-x", b)

	// THEN: it is dead-lettered as unexpected instead of crashing the handler
	dlq := h.read(contracts.TopicOrdersDLQ, 1)
	if header(dlq[0], contracts.HeaderDLQReason) != "unexpected-type" || calls.Load() != 0 {
		t.Fatalf("unexpected %+v", dlq[0].Headers)
	}
}

func Test_shouldRetryWithoutDeadLetteringWhenTheDatabaseIsTemporarilyFailing(t *testing.T) {
	// GIVEN: a handler whose database calls fail twice (e.g. during a failover)
	h := newHarness(t)
	var calls atomic.Int64
	ok := paymentHandler(&atomic.Int64{})
	h.start(h.runner(HandlerFunc(func(ctx context.Context, tx pgx.Tx, in Input) ([]Output, error) {
		if calls.Add(1) <= 2 {
			return nil, errors.New("connection refused")
		}
		return ok(ctx, tx, in)
	})))

	// WHEN: an order arrives
	h.produce(order("o-retry", 100))

	// THEN: the order is processed after the retries and nothing is dead-lettered
	h.read(contracts.TopicPaymentsCompleted, 1)
	if calls.Load() != 3 || !h.hasMetric(`lab_processing_retries_total{topic="orders.created",reason="db"} 2`) || h.hasMetric(`lab_dlq_records_total`) {
		t.Fatalf("calls=%d\n%s", calls.Load(), h.metrics())
	}
}

func Test_shouldRetryInjectedErrorsWhenTheProcessingErrorKnobIsOn(t *testing.T) {
	// GIVEN: a service with a 100% transient error rate (chaos) that is lifted after a moment
	h := newHarness(t)
	h.control.Set(events.Knobs{ProcessingErrorProbability: 1})
	var calls atomic.Int64
	h.start(h.runner(paymentHandler(&calls), func(c *Config) { c.Random = func() float64 { return 0.5 } }))
	h.produce(order("o-chaos", 100))
	h.eventually("injected retries", func() bool {
		return h.hasMetric(`lab_processing_retries_total{topic="orders.created",reason="transient"}`)
	})

	// WHEN: the experiment ends
	h.control.Set(events.Knobs{})

	// THEN: the order is processed exactly once
	h.read(contracts.TopicPaymentsCompleted, 1)
	if calls.Load() != 1 {
		t.Fatalf("handler ran %d times", calls.Load())
	}
}

func Test_shouldSpendTheInjectedLatencyWhenTheProcessingLatencyKnobIsSet(t *testing.T) {
	// GIVEN: Warehouse Overload making each record take 200 ms plus up to 100 ms jitter
	h := newHarness(t)
	h.control.Set(events.Knobs{ProcessingLatencyMs: 200, ProcessingJitterMs: 100})
	var calls atomic.Int64
	h.start(h.runner(paymentHandler(&calls), func(c *Config) { c.Random = func() float64 { return 0.5 } }))

	// WHEN: two orders arrive
	h.produce(order("o-a", 1))
	h.produce(order("o-b", 1))

	// THEN: each one waited 250 ms
	h.read(contracts.TopicPaymentsCompleted, 2)
	if got := h.sleeps.Load(); got != 500 {
		t.Fatalf("expected 500 ms of injected latency, got %d", got)
	}
}

func Test_shouldStopConsumingButStayHealthyWhenSilentFailureIsActive(t *testing.T) {
	// GIVEN: a paused payment service (Silent Failure)
	h := newHarness(t)
	h.control.Set(events.Knobs{Paused: true})
	var calls atomic.Int64
	r := h.runner(paymentHandler(&calls))
	h.start(r)
	h.eventually("paused gauge", func() bool { return h.hasMetric("lab_consumer_paused 1") })

	// WHEN: orders arrive while it is paused, and later the pause is lifted
	h.produce(order("o-wait", 100))
	time.Sleep(300 * time.Millisecond)
	if calls.Load() != 0 || !r.Ready() {
		t.Fatalf("paused service processed %d records (ready=%v)", calls.Load(), r.Ready())
	}
	h.control.Set(events.Knobs{})

	// THEN: nothing was processed during the pause, the backlog is processed afterwards
	h.read(contracts.TopicPaymentsCompleted, 1)
	if !h.hasMetric("lab_consumer_paused 0") {
		t.Fatal("expected paused gauge back at 0")
	}
}

func Test_shouldRebuildTheClientAndKeepProcessingWhenTheTuningLabChangesSettings(t *testing.T) {
	// GIVEN: a running payment service that processed one order
	h := newHarness(t)
	var calls atomic.Int64
	r := h.runner(paymentHandler(&calls))
	h.start(r)
	h.produce(order("o-before", 1))
	h.read(contracts.TopicPaymentsCompleted, 1)
	before := r.Client()

	// WHEN: a player sets linger to 50 ms and zstd compression
	linger, codec := int32(50), "zstd"
	h.control.Set(events.Knobs{Client: events.ClientTuning{LingerMs: &linger, Compression: &codec}})
	h.eventually("client rebuild", func() bool { return h.hasMetric("lab_client_rebuilds_total 1") })
	h.produce(order("o-after", 2))

	// THEN: a new client is in use and orders keep flowing without reprocessing old ones
	h.read(contracts.TopicPaymentsCompleted, 2)
	h.eventually("new client", func() bool { c := r.Client(); return c != nil && c != before })
	if calls.Load() != 2 {
		t.Fatalf("handler ran %d times", calls.Load())
	}
}

func Test_shouldRetryUntilTheSchemaIsKnownWhenAnOutputCannotBeEncoded(t *testing.T) {
	// GIVEN: a handler that first emits an event type this service has no schema for
	h := newHarness(t)
	var calls atomic.Int64
	ok := paymentHandler(&atomic.Int64{})
	h.start(h.runner(HandlerFunc(func(ctx context.Context, tx pgx.Tx, in Input) ([]Output, error) {
		if calls.Add(1) == 1 {
			return []Output{{Topic: contracts.TopicOrdersShipped, Value: &events.OrderShipped{}}}, nil
		}
		return ok(ctx, tx, in)
	})))

	// WHEN: an order arrives
	h.produce(order("o-enc", 1))

	// THEN: the encoding problem is retried as a registry problem and nothing half-done is published
	h.read(contracts.TopicPaymentsCompleted, 1)
	if !h.hasMetric(`lab_processing_retries_total{topic="orders.created",reason="registry"} 1`) {
		t.Fatal(h.metrics())
	}
}

func Test_shouldRetryProducingUntilTheTopicAcceptsWritesWhenTheBrokerRejectsThem(t *testing.T) {
	// GIVEN: a handler writing to a topic that does not exist yet, with short delivery timeouts
	h := newHarness(t)
	var calls atomic.Int64
	h.start(h.runner(HandlerFunc(func(ctx context.Context, tx pgx.Tx, in Input) ([]Output, error) {
		calls.Add(1)
		return []Output{{Topic: "late.topic", Key: in.Key, Value: &events.PaymentFailed{Meta: in.Meta, Reason: events.PaymentFailureReasonDeclined}}}, nil
	}), func(c *Config) { c.ExtraOpts = append(c.ExtraOpts, kgo.RecordDeliveryTimeout(time.Second)) }))
	h.produce(order("o-late", 1))
	h.eventually("produce retries", func() bool {
		return h.hasMetric(`lab_processing_retries_total{topic="orders.created",reason="produce"}`)
	})

	// WHEN: the topic is created
	if _, err := kadm.NewClient(h.writer).CreateTopic(context.Background(), 1, 1, nil, "late.topic"); err != nil {
		t.Fatal(err)
	}

	// THEN: the stored outcome is published and the handler never ran twice
	h.read("late.topic", 1)
	if calls.Load() != 1 {
		t.Fatalf("handler ran %d times", calls.Load())
	}
}

func Test_shouldForgetOldEventIDsWhenTheyAreOutsideTheDedupWindow(t *testing.T) {
	// GIVEN: two processed events from two hours ago and one from now
	h := newHarness(t)
	ctx := context.Background()
	for i, age := range []time.Duration{2 * time.Hour, 2 * time.Hour, 0} {
		if _, err := h.db.Pool.Exec(ctx, `INSERT INTO processed_events(event_id, topic, processed_at) VALUES ($1, 't', now() - $2::interval)`,
			fmt.Sprint("e", i), fmt.Sprintf("%d seconds", int(age.Seconds()))); err != nil {
			t.Fatal(err)
		}
	}
	r := h.runner(nil)

	// WHEN: the janitor runs
	purged := r.Purge(ctx)

	// THEN: only the old ids are forgotten
	var left int
	_ = h.db.Pool.QueryRow(ctx, `SELECT count(*) FROM processed_events`).Scan(&left)
	if purged != 2 || left != 1 {
		t.Fatalf("purged %d, %d left", purged, left)
	}
}

func Test_shouldReportPurgeProblemsWhenTheDatabaseIsGone(t *testing.T) {
	// GIVEN: a closed database
	h := newHarness(t)
	r := h.runner(nil)
	h.db.Close()

	// WHEN: the janitor runs
	purged := r.Purge(context.Background())

	// THEN: it purges nothing and does not crash the service
	if purged != 0 {
		t.Fatalf("purged %d", purged)
	}
}

func Test_shouldClassifyRejectionsAsPermanentWhenWrapped(t *testing.T) {
	// GIVEN: a validation error wrapped by a handler
	cause := errors.New("negative amount")
	err := fmt.Errorf("handle: %w", Permanent(cause))

	// WHEN / THEN: the runner recognises it as permanent, the cause stays inspectable; plain errors are not permanent
	if !IsPermanent(err) || !errors.Is(err, cause) || IsPermanent(errors.New("timeout")) || err.Error() != "handle: negative amount" {
		t.Fatal("permanent classification broken")
	}
}

func Test_shouldStopPromptlyWhenTheServiceShutsDownDuringLongProcessing(t *testing.T) {
	// GIVEN: a real sleep function and a context that is already cancelled
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// WHEN: the runner would wait
	err := sleepCtx(ctx, time.Hour)
	errZero := sleepCtx(context.Background(), 0)

	// THEN: it returns immediately
	if !errors.Is(err, context.Canceled) || errZero != nil {
		t.Fatalf("got %v / %v", err, errZero)
	}
}
