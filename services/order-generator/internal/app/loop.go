// Package app turns generated orders into orders.created events at the configured rate and
// applies the generator scenarios (Flash Sale, Hot Partition, Fat Messages, Duplicate Delivery,
// Poison Message).
package app

import (
	"context"
	"errors"
	"math"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"kafkalab/pkg/contracts"
	"kafkalab/pkg/events"
	"kafkalab/services/order-generator/internal/domain"
)

// Producer sends records asynchronously (implemented by *producer.Producer).
type Producer interface {
	Produce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error))
	Refresh(ctx context.Context) error
}

// Encoder encodes events (implemented by *serde.Serde).
type Encoder interface{ Encode(any) ([]byte, error) }

// Knobs is the control-plane view.
type Knobs interface{ Knobs() events.Knobs }

// Defaults of the optional Loop fields.
const (
	DefaultTick         = 50 * time.Millisecond
	DefaultLagThreshold = 5_000 // buffered records (the client buffers at most 20,000)
	DefaultPartitions   = 6     // partitions of orders.created (deploy/topics.yaml)
	// lagMemory is how long a failed delivery keeps lab_generator_lagging at 1.
	lagMemory = 15 * time.Second
)

// Loop emits orders at BaseRate per second (optionally modulated by the traffic wave), or at the
// ordersPerSecond knob when set.
type Loop struct {
	Producer Producer
	Encoder  Encoder
	Control  Knobs
	Gen      *domain.Generator
	BaseRate float64
	// Wave modulates BaseRate with a compressed day curve (±20%) while no rate knob is set.
	Wave    bool
	Metrics *metrics.Set
	Now     func() time.Time
	// Tick is the emit interval (default 50 ms).
	Tick time.Duration
	// NewID returns a fresh order or event id (default random UUID v4).
	NewID func() string
	// Buffered returns the records waiting in the producer (nil: unknown).
	Buffered func() int64
	// LagThreshold: more buffered records than this means the producer cannot keep up.
	LagThreshold int64
	// Partitions of orders.created, needed to aim orders at the hot partition.
	Partitions int

	lastFailure atomic.Int64 // unix nanos of the last failed delivery
	last        time.Time    // time of the previous step
	credit      float64      // orders due but not yet emitted
}

func (l *Loop) defaults() {
	if l.Tick == 0 {
		l.Tick = DefaultTick
	}
	if l.NewID == nil {
		l.NewID = uuid.NewString
	}
	if l.LagThreshold == 0 {
		l.LagThreshold = DefaultLagThreshold
	}
	if l.Partitions == 0 {
		l.Partitions = DefaultPartitions
	}
}

// Rate returns the target orders per second at now.
func (l *Loop) Rate(now time.Time) float64 {
	if r := l.Control.Knobs().OrdersPerSecond; r != nil {
		return max(*r, 0)
	}
	if l.Wave {
		return domain.Wave(l.BaseRate, now)
	}
	return l.BaseRate
}

// Lagging reports whether the producer cannot keep up: deliveries failed recently or the
// client buffer is filling up.
func (l *Loop) Lagging() bool {
	if l.Buffered != nil && l.Buffered() > l.LagThreshold {
		return true
	}
	last := l.lastFailure.Load()
	return last != 0 && l.Now().UnixNano()-last < int64(lagMemory)
}

// Run emits orders until ctx ends.
func (l *Loop) Run(ctx context.Context) error {
	l.defaults()
	l.Metrics.GetOrCreateGauge("lab_generator_target_rate", func() float64 { return l.Rate(l.Now()) })
	l.Metrics.GetOrCreateGauge("lab_generator_lagging", func() float64 {
		if l.Lagging() {
			return 1
		}
		return 0
	})
	l.Metrics.GetOrCreateCounter("lab_generator_produce_errors_total")
	t := time.NewTicker(l.Tick)
	defer t.Stop()
	l.last = l.Now()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
		if err := l.Step(ctx, l.Now()); err != nil {
			return err
		}
	}
}

// Step emits the orders due at now. Credit accumulates fractional orders between steps; it is
// capped at one second's worth, so a producer that stalled does not release a burst when it
// recovers.
func (l *Loop) Step(ctx context.Context, now time.Time) error {
	if err := l.Producer.Refresh(ctx); err != nil { // coverage-ignore: only invalid static options fail here
		return err
	}
	if l.last.IsZero() {
		l.last = now
	}
	rate := l.Rate(now)
	l.credit = math.Min(l.credit+rate*now.Sub(l.last).Seconds(), math.Max(rate, 1))
	l.last = now
	for ; l.credit >= 1 && ctx.Err() == nil; l.credit-- {
		if err := l.Emit(ctx); err != nil {
			return err
		}
	}
	return nil
}

// Emit produces one record: normally an order, sometimes (by scenario) a duplicated order, an
// order aimed at the hot partition or an undecodable poison record.
func (l *Loop) Emit(ctx context.Context) error {
	l.defaults()
	k := l.Control.Knobs()
	plan := l.Gen.Plan(domain.Scenario{
		DuplicateProbability: k.DuplicateProbability,
		PoisonProbability:    k.PoisonProbability,
		HotPartition:         l.hotPartition(k),
		HotPartitionRatio:    k.HotPartitionRatio,
	})
	id, kind := l.NewID(), "normal"
	if plan.Poison {
		l.produce(ctx, id, []byte("POISON "+id+": this record is not Confluent Avro"), "poison")
		return nil
	}
	if plan.Hot {
		id, kind = domain.KeyOnPartition(l.NewID, int(*k.HotPartition), l.Partitions), "hot"
	}
	b, err := l.Encoder.Encode(l.order(id, k))
	if err != nil {
		return err
	}
	l.produce(ctx, id, b, kind)
	if plan.Duplicate {
		// Same bytes, same event id: consumers must detect it as a duplicate delivery.
		l.produce(ctx, id, b, "duplicate")
	}
	return nil
}

// hotPartition returns the hot partition knob if it names an existing partition.
func (l *Loop) hotPartition(k events.Knobs) *int {
	if k.HotPartition == nil || *k.HotPartition < 0 || int(*k.HotPartition) >= l.Partitions {
		return nil
	}
	p := int(*k.HotPartition)
	return &p
}

func (l *Loop) order(id string, k events.Knobs) *events.OrderCreated {
	popular := ""
	if k.PopularSku != nil {
		popular = *k.PopularSku
	}
	o := l.Gen.Next(popular, k.PopularSkuProbability)
	now := l.Now().UTC()
	ev := &events.OrderCreated{
		Meta:          events.EventMeta{EventID: l.NewID(), OrderID: id, OccurredAt: now, OrderCreatedAt: now, Producer: contracts.ServiceOrderGenerator},
		CustomerID:    o.CustomerID,
		Region:        events.Region(o.Region),
		PaymentMethod: events.PaymentMethod(o.PaymentMethod),
		TotalCents:    o.TotalCents,
		Currency:      "EUR",
		Padding:       l.Gen.Padding(int(k.PaddingBytes)),
	}
	for _, line := range o.Lines {
		ev.Lines = append(ev.Lines, events.OrderLine{Sku: line.SKU, Quantity: int32(line.Quantity), UnitPriceCents: line.PriceCents})
	}
	return ev
}

func (l *Loop) produce(ctx context.Context, key string, value []byte, kind string) {
	rec := &kgo.Record{
		Topic: contracts.TopicOrdersCreated, Key: []byte(key), Value: value,
		Headers: []kgo.RecordHeader{{Key: contracts.HeaderProducer, Value: []byte(contracts.ServiceOrderGenerator)}},
	}
	l.Producer.Produce(ctx, rec, l.delivered)
	l.Metrics.GetOrCreateCounter(`lab_generator_orders_total{kind="` + kind + `"}`).Inc()
}

// delivered is the produce promise: failed deliveries are counted and flag the generator as lagging.
func (l *Loop) delivered(_ *kgo.Record, err error) {
	if err == nil || errors.Is(err, context.Canceled) { // cancelled = shutting down, not a broker problem
		return
	}
	l.Metrics.GetOrCreateCounter("lab_generator_produce_errors_total").Inc()
	l.lastFailure.Store(l.Now().UnixNano())
}
