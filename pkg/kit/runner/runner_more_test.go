package runner

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"kafkalab/pkg/contracts"
	"kafkalab/pkg/events"
	"kafkalab/pkg/kit/kafka"
)

func Test_shouldContinueTheOrdersTraceWhenTheIncomingRecordCarriesTraceContext(t *testing.T) {
	// GIVEN: a traced order producer and a payment service with tracing enabled
	h := newHarness(t)
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	prop := propagation.TraceContext{}
	tracer := kotel.NewTracer(kotel.TracerProvider(tp), kotel.TracerPropagator(prop))
	hooks := kotel.NewKotel(kotel.WithTracer(tracer)).Hooks()
	producer, err := kgo.NewClient(kgo.SeedBrokers(h.cluster.Brokers...), kgo.WithHooks(hooks...))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	var calls atomic.Int64
	h.start(h.runner(paymentHandler(&calls), func(c *Config) { c.Tracer = tracer; c.Hooks = hooks }))
	ctx, root := tp.Tracer("generator").Start(context.Background(), "create order")
	b, _ := h.serde.Encode(order("o-traced", 700))

	// WHEN: the order is produced inside the generator's trace
	if err := producer.ProduceSync(ctx, &kgo.Record{Topic: contracts.TopicOrdersCreated, Key: []byte("o-traced"), Value: b, Context: ctx}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	root.End()
	h.read(contracts.TopicPaymentsCompleted, 1)

	// THEN: processing and the published payment belong to the same trace as the order
	h.eventually("process and publish spans", func() bool {
		var process, publish bool
		for _, s := range rec.Ended() {
			if s.SpanContext().TraceID() != root.SpanContext().TraceID() {
				continue
			}
			process = process || s.Name() == contracts.TopicOrdersCreated+" process"
			publish = publish || s.Name() == contracts.TopicPaymentsCompleted+" publish"
		}
		return process && publish
	})
}

func Test_shouldUseSafeDefaultsWhenOnlyTheEssentialsAreConfigured(t *testing.T) {
	// GIVEN: a service that configures only name and topics
	r := New(Config{Service: "inventory", Topics: []string{contracts.TopicOrdersCreated}})

	// WHEN: the defaults are inspected
	cfg := r.cfg

	// THEN: the group is the service name and retries back off up to five seconds
	if cfg.Group != "inventory" || cfg.Backoff(1) != 250*time.Millisecond || cfg.Backoff(100) != 5*time.Second ||
		cfg.DedupWindow != time.Hour || cfg.JanitorEvery != time.Minute || cfg.Metrics == nil || cfg.Sleep == nil {
		t.Fatalf("unexpected defaults %+v", cfg)
	}
}

func Test_shouldStillDeliverWhenAcksAreLoweredAndNetworkChaosAddsLatency(t *testing.T) {
	// GIVEN: a player lowered acks to leader and Network Chaos adds 20 ms per request
	h := newHarness(t)
	leader := "leader"
	h.control.Set(events.Knobs{Client: events.ClientTuning{Acks: &leader}})
	fd := kafka.NewFaultDialer(func() kafka.Faults { return kafka.Faults{Latency: 20 * time.Millisecond} })
	var calls atomic.Int64
	h.start(h.runner(paymentHandler(&calls), func(c *Config) { c.Dialer = fd.DialContext }))

	// WHEN: an order arrives
	h.produce(order("o-slow-net", 300))

	// THEN: it is still processed (just slower)
	h.read(contracts.TopicPaymentsCompleted, 1)
	if calls.Load() != 1 {
		t.Fatalf("handler ran %d times", calls.Load())
	}
}

func Test_shouldDeadLetterTheRecordWhenAPlatformEventLeaksOntoABusinessTopic(t *testing.T) {
	// GIVEN: a control snapshot accidentally written to orders.created
	h := newHarness(t)
	var calls atomic.Int64
	h.start(h.runner(paymentHandler(&calls)))
	b, err := h.serde.Encode(&events.ControlSnapshot{Service: "payment", Generation: 1, IssuedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}

	// WHEN: the payment service reads it
	h.produceRaw("payment", b)

	// THEN: it goes to the DLQ as undecodable for business processing
	dlq := h.read(contracts.TopicOrdersDLQ, 1)
	if header(dlq[0], contracts.HeaderDLQReason) != "undecodable" || calls.Load() != 0 {
		t.Fatalf("unexpected %+v", dlq[0].Headers)
	}
}

func Test_shouldDropTheBatchWhenThePartitionWasHandedToAnotherPodDuringRetries(t *testing.T) {
	// GIVEN: a runner that no longer owns partition 0 of orders.created
	h := newHarness(t)
	var calls atomic.Int64
	r := h.runner(paymentHandler(&calls))
	b, _ := h.serde.Encode(order("o-moved", 1))

	// WHEN: it tries to process a batch of that partition
	r.processPartition(context.Background(), nil, contracts.TopicOrdersCreated, 0, []*kgo.Record{{Topic: contracts.TopicOrdersCreated, Value: b}})

	// THEN: it leaves the batch to the new owner instead of processing it twice
	if calls.Load() != 0 {
		t.Fatal("batch of a revoked partition must not be processed")
	}
}

func Test_shouldGiveUpTheBatchWhenTheServiceShutsDownDuringInjectedLatency(t *testing.T) {
	// GIVEN: a service that is slowed down by an hour per record
	h := newHarness(t)
	h.control.Set(events.Knobs{ProcessingLatencyMs: 3_600_000})
	var calls atomic.Int64
	r := h.runner(paymentHandler(&calls), func(c *Config) { c.Sleep = sleepCtx })
	ctx, cancel := context.WithCancel(context.Background())
	r.owned[contracts.TopicOrdersCreated] = map[int32]bool{0: true}
	b, _ := h.serde.Encode(order("o-stop", 1))
	go func() { time.Sleep(100 * time.Millisecond); cancel() }()

	// WHEN: the pod is stopped while the record is being "processed"
	start := time.Now()
	r.processPartition(ctx, nil, contracts.TopicOrdersCreated, 0, []*kgo.Record{{Topic: contracts.TopicOrdersCreated, Value: b}})

	// THEN: the batch is abandoned quickly (it will be redelivered to the next owner)
	if time.Since(start) > 5*time.Second || calls.Load() != 0 {
		t.Fatalf("took %v, calls %d", time.Since(start), calls.Load())
	}
}

func Test_shouldShutDownCleanlyWhenStoppedWhilePaused(t *testing.T) {
	// GIVEN: a paused service
	h := newHarness(t)
	h.control.Set(events.Knobs{Paused: true})
	var calls atomic.Int64
	r := h.runner(paymentHandler(&calls))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	h.eventually("paused", func() bool { return h.hasMetric("lab_consumer_paused 1") })

	// WHEN: the pod receives SIGTERM
	cancel()

	// THEN: the runner returns without error
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("runner did not stop")
	}
}

func Test_shouldPurgeOldEventIDsPeriodicallyWhenRunning(t *testing.T) {
	// GIVEN: an old processed event and a janitor running every 20 ms
	h := newHarness(t)
	if _, err := h.db.Pool.Exec(context.Background(), `INSERT INTO processed_events(event_id, topic, processed_at) VALUES ('old', 't', now() - interval '3 hours')`); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int64
	h.start(h.runner(paymentHandler(&calls), func(c *Config) { c.JanitorEvery = 20 * time.Millisecond }))

	// WHEN: the service runs for a moment
	// THEN: the old id disappears
	h.eventually("old id purged", func() bool {
		var n int
		_ = h.db.Pool.QueryRow(context.Background(), `SELECT count(*) FROM processed_events WHERE event_id = 'old'`).Scan(&n)
		return n == 0
	})
}

func Test_shouldAcceptAnyBusinessEventWhenTheTopicIsNotPartOfTheContract(t *testing.T) {
	// GIVEN: an experimental topic outside the contract
	// WHEN: the type check runs
	ok := expectedType("experiments.sandbox", &events.OrderShipped{})

	// THEN: any business event is accepted there, while contract topics stay strict
	if !ok || expectedType(contracts.TopicOrdersCreated, &events.OrderShipped{}) {
		t.Fatal("type check broken")
	}
}

func Test_shouldCommitWithoutPublishingWhenTheHandlerHasNothingToPublish(t *testing.T) {
	// GIVEN: an analytics-style handler that only records the order and publishes nothing
	h := newHarness(t)
	var calls atomic.Int64
	h.start(h.runner(HandlerFunc(func(context.Context, pgx.Tx, Input) ([]Output, error) {
		calls.Add(1)
		return nil, nil
	})))

	// WHEN: an order arrives
	h.produce(order("o-silent", 1))

	// THEN: it is counted as processed without any output
	h.eventually("processed", func() bool {
		return h.hasMetric(`lab_records_processed_total{topic="orders.created",result="ok"} 1`)
	})
	if calls.Load() != 1 || h.hasMetric("lab_records_produced_total") {
		t.Fatalf("calls=%d\n%s", calls.Load(), h.metrics())
	}
}

func Test_shouldCountEachOrderOnceWhenTheBatchIsRetriedAfterAFailure(t *testing.T) {
	// GIVEN: an analytics-style handler that counts in memory and fails on its first attempt
	h := newHarness(t)
	var attempts, counted atomic.Int64
	h.start(h.runner(HandlerFunc(func(ctx context.Context, _ pgx.Tx, _ Input) ([]Output, error) {
		OnCommit(ctx, func() { counted.Add(1) })
		if attempts.Add(1) == 1 {
			return nil, errors.New("connection refused")
		}
		return nil, nil
	})))

	// WHEN: one order arrives and its batch is retried
	h.produce(order("o-count", 1))
	h.eventually("processed", func() bool {
		return h.hasMetric(`lab_records_processed_total{topic="orders.created",result="ok"} 1`)
	})

	// THEN: the counter moved exactly once although the handler ran twice
	if attempts.Load() != 2 || counted.Load() != 1 {
		t.Fatalf("attempts=%d counted=%d", attempts.Load(), counted.Load())
	}
}

func Test_shouldRunTheSideEffectImmediatelyWhenNoBatchIsActive(t *testing.T) {
	// GIVEN: a handler unit test calling the handler directly
	ran := false

	// WHEN: it registers a post-commit side effect
	OnCommit(context.Background(), func() { ran = true })

	// THEN: the side effect runs right away
	if !ran {
		t.Fatal("expected immediate execution")
	}
}
