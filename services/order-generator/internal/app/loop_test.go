package app

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/twmb/franz-go/pkg/kgo"

	"kafkalab/pkg/contracts"
	"kafkalab/pkg/events"
	"kafkalab/services/order-generator/internal/domain"
)

// fakeProducer records produced records and answers every promise with deliveryErr.
type fakeProducer struct {
	mu          sync.Mutex
	recs        []*kgo.Record
	deliveryErr error
}

func (p *fakeProducer) Produce(_ context.Context, r *kgo.Record, promise func(*kgo.Record, error)) {
	p.mu.Lock()
	p.recs = append(p.recs, r)
	err := p.deliveryErr
	p.mu.Unlock()
	promise(r, err)
}

func (p *fakeProducer) Refresh(context.Context) error { return nil }

func (p *fakeProducer) records() []*kgo.Record {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]*kgo.Record(nil), p.recs...)
}

// fakeEncoder keeps every encoded order and returns its event id as the "encoded" bytes.
type fakeEncoder struct {
	mu      sync.Mutex
	orders  map[string]*events.OrderCreated
	failing error
}

func (e *fakeEncoder) Encode(v any) ([]byte, error) {
	if e.failing != nil {
		return nil, e.failing
	}
	o := v.(*events.OrderCreated)
	e.mu.Lock()
	defer e.mu.Unlock()
	e.orders[o.Meta.EventID] = o
	return []byte(o.Meta.EventID), nil
}

func (e *fakeEncoder) order(rec *kgo.Record) *events.OrderCreated {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.orders[string(rec.Value)]
}

type knobs struct {
	mu sync.Mutex
	k  events.Knobs
}

func (k *knobs) Knobs() events.Knobs { k.mu.Lock(); defer k.mu.Unlock(); return k.k }

var start = time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)

type fixture struct {
	loop  *Loop
	prod  *fakeProducer
	enc   *fakeEncoder
	knobs *knobs
	set   *metrics.Set
}

func newFixture(k events.Knobs) *fixture {
	f := &fixture{prod: &fakeProducer{}, enc: &fakeEncoder{orders: map[string]*events.OrderCreated{}}, knobs: &knobs{k: k}, set: metrics.NewSet()}
	n := 0
	var mu sync.Mutex
	f.loop = &Loop{Producer: f.prod, Encoder: f.enc, Control: f.knobs, Gen: domain.NewGenerator(5), BaseRate: 50,
		Metrics: f.set, Now: func() time.Time { return start },
		NewID: func() string { mu.Lock(); defer mu.Unlock(); n++; return "id-" + strconv.Itoa(n) }}
	return f
}

func (f *fixture) metric(name string) float64 {
	var buf bytes.Buffer
	f.set.WritePrometheus(&buf)
	for _, line := range strings.Split(buf.String(), "\n") {
		if v, ok := strings.CutPrefix(line, name+" "); ok {
			x, _ := strconv.ParseFloat(v, 64)
			return x
		}
	}
	return -1
}

func ptr[T any](v T) *T { return &v }

func (f *fixture) emit(t *testing.T, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if err := f.loop.Emit(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}

func Test_shouldPublishAConsistentOrderWhenTheGeneratorEmits(t *testing.T) {
	// GIVEN: the generator in normal operation
	f := newFixture(events.Knobs{})

	// WHEN: one order is emitted
	f.emit(t, 1)

	// THEN: one orders.created record keyed by the order id is produced, tagged with its producer
	recs := f.prod.records()
	o := f.enc.order(recs[0])
	if len(recs) != 1 || recs[0].Topic != contracts.TopicOrdersCreated || string(recs[0].Key) != o.Meta.OrderID ||
		string(recs[0].Headers[0].Value) != contracts.ServiceOrderGenerator {
		t.Fatalf("unexpected record %+v", recs[0])
	}
	// AND: the order has its own event id, a creation time, lines and no padding
	if o.Meta.EventID == o.Meta.OrderID || !o.Meta.OrderCreatedAt.Equal(start) || len(o.Lines) == 0 || o.Currency != "EUR" ||
		o.Padding != nil || o.Meta.Producer != contracts.ServiceOrderGenerator {
		t.Fatalf("unexpected order %+v", o)
	}
	if f.metric(`lab_generator_orders_total{kind="normal"}`) != 1 {
		t.Fatal("normal order not counted")
	}
}

func Test_shouldAccumulateFractionalOrdersWhenTheRateIsBelowTheTickRate(t *testing.T) {
	// GIVEN: a rate of 10 orders per second, stepped every 50 ms (half an order per step)
	f := newFixture(events.Knobs{OrdersPerSecond: ptr(10.0)})
	ctx := context.Background()

	// WHEN: the loop steps through one second
	for i := 0; i <= 20; i++ {
		if err := f.loop.Step(ctx, start.Add(time.Duration(i)*50*time.Millisecond)); err != nil {
			t.Fatal(err)
		}
	}

	// THEN: exactly ten orders were produced
	if n := len(f.prod.records()); n != 10 {
		t.Fatalf("expected 10 orders, got %d", n)
	}
}

func Test_shouldFollowTheRateKnobWhenTheControlPlaneChangesIt(t *testing.T) {
	// GIVEN: a base rate of 50/s and a running experiment raising it to 400/s
	f := newFixture(events.Knobs{OrdersPerSecond: ptr(400.0)})
	ctx := context.Background()

	// WHEN: two seconds pass in 100 ms steps, and the experiment ends after the first second
	for i := 0; i <= 20; i++ {
		if i == 11 {
			f.knobs.mu.Lock()
			f.knobs.k = events.Knobs{}
			f.knobs.mu.Unlock()
		}
		_ = f.loop.Step(ctx, start.Add(time.Duration(i)*100*time.Millisecond))
	}

	// THEN: 400 orders came in the first second and 50 in the second one
	if n := len(f.prod.records()); n != 450 {
		t.Fatalf("expected 450 orders, got %d", n)
	}
}

func Test_shouldNotBurstWhenTheLoopWasStalledForAWhile(t *testing.T) {
	// GIVEN: a generator at 20 orders per second
	f := newFixture(events.Knobs{OrdersPerSecond: ptr(20.0)})
	ctx := context.Background()
	_ = f.loop.Step(ctx, start)

	// WHEN: the next step happens a whole minute later (producer was blocked)
	_ = f.loop.Step(ctx, start.Add(time.Minute))

	// THEN: at most one second's worth of orders is released, not 1,200
	if n := len(f.prod.records()); n != 20 {
		t.Fatalf("expected 20 orders, got %d", n)
	}
}

func Test_shouldEmitNothingWhenTheRateKnobIsZeroOrNegative(t *testing.T) {
	// GIVEN: an experiment that stopped all traffic with a negative rate
	f := newFixture(events.Knobs{OrdersPerSecond: ptr(-5.0)})
	ctx := context.Background()

	// WHEN: ten seconds pass
	for i := 0; i <= 10; i++ {
		_ = f.loop.Step(ctx, start.Add(time.Duration(i)*time.Second))
	}

	// THEN: no order is produced and the target rate is zero
	if n := len(f.prod.records()); n != 0 || f.loop.Rate(start) != 0 {
		t.Fatalf("expected silence, got %d orders", n)
	}
}

func Test_shouldBreatheAroundTheBaseRateWhenTheTrafficWaveIsOn(t *testing.T) {
	// GIVEN: a base rate of 100/s with the traffic wave on, and the same without
	f := newFixture(events.Knobs{})
	f.loop.BaseRate, f.loop.Wave = 100, true
	flat := newFixture(events.Knobs{})
	flat.loop.BaseRate = 100

	// WHEN: the target rate is read a quarter of a compressed day apart
	a, b := f.loop.Rate(start), f.loop.Rate(start.Add(15*time.Minute))

	// THEN: the wave moves the rate within ±20% while the flat profile stays at 100
	if a == b || a < 80 || a > 120 || b < 80 || b > 120 || flat.loop.Rate(start) != 100 {
		t.Fatalf("wave %v / %v, flat %v", a, b, flat.loop.Rate(start))
	}
}

func Test_shouldPublishTheIdenticalRecordTwiceWhenDuplicateDeliveryRuns(t *testing.T) {
	// GIVEN: the Duplicate Delivery scenario at 100%
	f := newFixture(events.Knobs{DuplicateProbability: 1})

	// WHEN: one order is emitted
	f.emit(t, 1)

	// THEN: the same record (same key, same event id) is produced twice and counted as a duplicate
	recs := f.prod.records()
	if len(recs) != 2 || !bytes.Equal(recs[0].Value, recs[1].Value) || !bytes.Equal(recs[0].Key, recs[1].Key) {
		t.Fatalf("unexpected %+v", recs)
	}
	if f.metric(`lab_generator_orders_total{kind="normal"}`) != 1 || f.metric(`lab_generator_orders_total{kind="duplicate"}`) != 1 {
		t.Fatal("duplicate not counted")
	}
}

func Test_shouldPublishUndecodableBytesWhenThePoisonMessageScenarioRuns(t *testing.T) {
	// GIVEN: the Poison Message scenario at 100%
	f := newFixture(events.Knobs{PoisonProbability: 1})

	// WHEN: a record is emitted
	f.emit(t, 1)

	// THEN: a non-Avro record (no Confluent magic byte) lands on orders.created and no order was encoded
	recs := f.prod.records()
	if len(recs) != 1 || recs[0].Topic != contracts.TopicOrdersCreated || recs[0].Value[0] == 0 || len(f.enc.orders) != 0 {
		t.Fatalf("unexpected %+v", recs)
	}
	if f.metric(`lab_generator_orders_total{kind="poison"}`) != 1 {
		t.Fatal("poison not counted")
	}
}

func Test_shouldSendTheConfiguredShareToOnePartitionWhenHotPartitionRuns(t *testing.T) {
	// GIVEN: the Hot Partition scenario sending 60% of orders to partition 2
	f := newFixture(events.Knobs{HotPartition: ptr(int32(2)), HotPartitionRatio: 0.6})

	// WHEN: 1,000 orders are emitted
	f.emit(t, 1000)

	// THEN: partition 2 receives about 60% + 40%/6 ≈ 67% of all orders
	perPartition := map[int]int{}
	for _, r := range f.prod.records() {
		perPartition[domain.Partition(string(r.Key), 6)]++
	}
	if perPartition[2] < 620 || perPartition[2] > 720 {
		t.Fatalf("partition spread %v", perPartition)
	}
	if hot := f.metric(`lab_generator_orders_total{kind="hot"}`); hot < 550 || hot > 650 {
		t.Fatalf("hot orders counted: %v", hot)
	}
}

func Test_shouldIgnoreTheHotPartitionWhenItDoesNotExist(t *testing.T) {
	// GIVEN: a misconfigured experiment naming partition 9 and one naming -1 of 6
	for _, p := range []int32{9, -1} {
		f := newFixture(events.Knobs{HotPartition: ptr(p), HotPartitionRatio: 1})

		// WHEN: orders are emitted
		f.emit(t, 20)

		// THEN: they are normal orders spread as usual instead of hanging the generator
		if f.metric(`lab_generator_orders_total{kind="normal"}`) != 20 {
			t.Fatalf("partition %d: orders not emitted normally", p)
		}
	}
}

func Test_shouldFillThePayloadWhenFatMessagesRuns(t *testing.T) {
	// GIVEN: the Fat Messages scenario with 50 KB padding
	f := newFixture(events.Knobs{PaddingBytes: 50_000})

	// WHEN: an order is emitted
	f.emit(t, 1)

	// THEN: the order carries 50 KB of filler
	if o := f.enc.order(f.prod.records()[0]); len(o.Padding) != 50_000 {
		t.Fatalf("padding %d bytes", len(o.Padding))
	}
}

func Test_shouldPutTheFlashSaleProductIntoOrdersWhenAPopularSkuIsSet(t *testing.T) {
	// GIVEN: a flash sale on sku-033 for every order
	f := newFixture(events.Knobs{PopularSku: ptr("sku-033"), PopularSkuProbability: 1})

	// WHEN: 20 orders are emitted
	f.emit(t, 20)

	// THEN: every order starts with the flash sale product
	for _, r := range f.prod.records() {
		if sku := f.enc.order(r).Lines[0].Sku; sku != "sku-033" {
			t.Fatalf("got %s", sku)
		}
	}
}

func Test_shouldReportLaggingWhenDeliveriesFail(t *testing.T) {
	// GIVEN: brokers rejecting every record
	f := newFixture(events.Knobs{})
	f.prod.deliveryErr = errors.New("NOT_ENOUGH_REPLICAS")
	now := start
	f.loop.Now = func() time.Time { return now }

	// WHEN: three orders are emitted
	f.emit(t, 3)

	// THEN: the failures are counted and the generator reports lagging
	if f.metric("lab_generator_produce_errors_total") != 3 || !f.loop.Lagging() {
		t.Fatal("failures not reported")
	}
	// AND: once no failure happened for a while, it recovers
	now = now.Add(time.Minute)
	if f.loop.Lagging() {
		t.Fatal("still lagging a minute after the last failure")
	}
}

func Test_shouldNotCountFailuresWhenRecordsAreAbortedByShutdown(t *testing.T) {
	// GIVEN: a generator that is shutting down, so buffered records are aborted
	f := newFixture(events.Knobs{})
	f.prod.deliveryErr = fmt.Errorf("aborted: %w", context.Canceled)

	// WHEN: an order is emitted
	f.emit(t, 1)

	// THEN: it is not reported as a broker problem
	if f.metric("lab_generator_produce_errors_total") > 0 || f.loop.Lagging() {
		t.Fatal("shutdown counted as failure")
	}
}

func Test_shouldReportLaggingWhenTheProducerBufferFillsUp(t *testing.T) {
	// GIVEN: a producer whose buffer holds 6,000 and later 100 records
	f := newFixture(events.Knobs{})
	buffered := int64(6_000)
	f.loop.Buffered = func() int64 { return buffered }
	f.emit(t, 1)

	// WHEN / THEN: it lags with a full buffer and recovers once the buffer drains
	if !f.loop.Lagging() {
		t.Fatal("full buffer not reported")
	}
	buffered = 100
	if f.loop.Lagging() {
		t.Fatal("drained buffer still reported")
	}
}

func Test_shouldStopTheGeneratorWhenOrdersCannotBeEncoded(t *testing.T) {
	// GIVEN: an encoder that lost its schema
	f := newFixture(events.Knobs{OrdersPerSecond: ptr(1000.0)})
	f.enc.failing = errors.New("schema not found")
	f.loop.Now, f.loop.Tick = time.Now, time.Millisecond

	// WHEN: the loop runs
	err := f.loop.Run(context.Background())

	// THEN: it stops with the error instead of silently producing nothing
	if err == nil {
		t.Fatal("expected an error")
	}
}

func Test_shouldEmitUntilStoppedWhenTheLoopRuns(t *testing.T) {
	// GIVEN: a running generator at 1,000 orders per second with real time and random ids
	f := newFixture(events.Knobs{OrdersPerSecond: ptr(1000.0)})
	f.loop.Now, f.loop.Tick, f.loop.NewID = time.Now, time.Millisecond, nil
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- f.loop.Run(ctx) }()

	// WHEN: some orders went out and the service is stopped
	deadline := time.Now().Add(5 * time.Second)
	for len(f.prod.records()) < 20 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	err := <-done

	// THEN: it stops cleanly and exported the generator metrics on the way
	if err != nil || len(f.prod.records()) < 20 || f.metric("lab_generator_target_rate") != 1000 || f.metric("lab_generator_lagging") != 0 {
		t.Fatalf("err=%v orders=%d rate=%v", err, len(f.prod.records()), f.metric("lab_generator_target_rate"))
	}
}

func Test_shouldExportLaggingAsOneWhenTheProducerFallsBehind(t *testing.T) {
	// GIVEN: a running generator whose deliveries fail
	f := newFixture(events.Knobs{OrdersPerSecond: ptr(1000.0)})
	f.prod.deliveryErr = errors.New("broker down")
	f.loop.Now, f.loop.Tick = time.Now, time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- f.loop.Run(ctx) }()

	// WHEN: a few deliveries failed
	deadline := time.Now().Add(5 * time.Second)
	for f.metric("lab_generator_produce_errors_total") < 5 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	lagging := f.metric("lab_generator_lagging")
	cancel()
	<-done

	// THEN: the lagging gauge shows 1
	if lagging != 1 {
		t.Fatalf("lagging=%v", lagging)
	}
}
