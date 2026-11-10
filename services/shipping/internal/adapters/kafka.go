package adapters

import (
	"context"
	"fmt"

	"github.com/VictoriaMetrics/metrics"
	"github.com/twmb/franz-go/pkg/kgo"

	"kafkalab/pkg/contracts"
	"kafkalab/pkg/kit/kafka"
	"kafkalab/pkg/kit/runner"
)

// Encoder encodes events (implemented by *serde.Serde).
type Encoder interface{ Encode(any) ([]byte, error) }

// Client is the producing client (implemented by *producer.Producer, which follows the Tuning Lab).
type Client interface {
	Client() *kgo.Client
	Refresh(ctx context.Context) error
}

// KafkaPublisher implements app.Publisher with synchronous, acknowledged produces.
type KafkaPublisher struct {
	Producer Client
	Codec    Encoder
	Metrics  *metrics.Set
}

// Publish encodes outs and waits until the brokers acknowledged all of them.
func (p KafkaPublisher) Publish(ctx context.Context, outs []runner.Output) error {
	if err := p.Producer.Refresh(ctx); err != nil { // coverage-ignore: only invalid static options fail here
		return err
	}
	recs := make([]*kgo.Record, len(outs))
	for i, o := range outs {
		b, err := p.Codec.Encode(o.Value)
		if err != nil {
			return fmt.Errorf("encode %s: %w", o.Topic, err)
		}
		recs[i] = &kgo.Record{Topic: o.Topic, Key: []byte(o.Key), Value: b,
			Headers: []kgo.RecordHeader{{Key: contracts.HeaderProducer, Value: []byte(contracts.ServiceShipping)}}}
	}
	if err := p.Producer.Client().ProduceSync(ctx, recs...).FirstErr(); err != nil {
		return err
	}
	for _, o := range outs {
		p.Metrics.GetOrCreateCounter(kafka.Label("lab_records_produced_total", "topic", o.Topic)).Inc()
	}
	return nil
}
