// Package app turns generated orders into orders.created events at the configured rate.
package app

import (
	"context"
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

// Loop emits orders at BaseRate per second, or at the ordersPerSecond knob when set.
type Loop struct {
	Producer Producer
	Encoder  Encoder
	Control  Knobs
	Gen      *domain.Generator
	BaseRate float64
	Metrics  *metrics.Set
	Now      func() time.Time
	Tick     time.Duration // default 50 ms
}

// Rate returns the target orders per second.
func (l *Loop) Rate() float64 {
	if r := l.Control.Knobs().OrdersPerSecond; r != nil {
		return *r
	}
	return l.BaseRate
}

// Run emits orders until ctx ends.
func (l *Loop) Run(ctx context.Context) error {
	if l.Tick == 0 {
		l.Tick = 50 * time.Millisecond
	}
	l.Metrics.GetOrCreateGauge("lab_generator_target_rate", l.Rate)
	t := time.NewTicker(l.Tick)
	defer t.Stop()
	last := l.Now()
	credit := 0.0
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
		if err := l.Producer.Refresh(ctx); err != nil { // coverage-ignore: only invalid static options fail here
			return err
		}
		now := l.Now()
		credit += l.Rate() * now.Sub(last).Seconds()
		last = now
		for ; credit >= 1; credit-- {
			if err := l.Emit(ctx); err != nil {
				return err
			}
		}
	}
}

// Emit produces one order.
func (l *Loop) Emit(ctx context.Context) error {
	k := l.Control.Knobs()
	popular := ""
	if k.PopularSku != nil {
		popular = *k.PopularSku
	}
	o := l.Gen.Next(popular, k.PopularSkuProbability)
	now := l.Now().UTC()
	id := uuid.NewString()
	ev := &events.OrderCreated{
		Meta:          events.EventMeta{EventID: uuid.NewString(), OrderID: id, OccurredAt: now, OrderCreatedAt: now, Producer: contracts.ServiceOrderGenerator},
		CustomerID:    o.CustomerID,
		Region:        events.Region(o.Region),
		PaymentMethod: events.PaymentMethod(o.PaymentMethod),
		TotalCents:    o.TotalCents,
		Currency:      "EUR",
	}
	for _, line := range o.Lines {
		ev.Lines = append(ev.Lines, events.OrderLine{Sku: line.SKU, Quantity: int32(line.Quantity), UnitPriceCents: line.PriceCents})
	}
	b, err := l.Encoder.Encode(ev)
	if err != nil {
		return err
	}
	l.Producer.Produce(ctx, &kgo.Record{Topic: contracts.TopicOrdersCreated, Key: []byte(id), Value: b}, nil)
	l.Metrics.GetOrCreateCounter(`lab_generator_orders_total{kind="normal"}`).Inc()
	return nil
}
