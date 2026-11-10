package adapters

import (
	"context"
	"log/slog"
	"sync/atomic"

	"github.com/VictoriaMetrics/metrics"
	"github.com/twmb/franz-go/pkg/kgo"

	"kafkalab/pkg/contracts"
	"kafkalab/pkg/kit/kafka"
)

// DLQGroup is the consumer group that counts dead letters.
const DLQGroup = contracts.ServiceAnalytics + "-dlq"

// DLQCounter counts orders.dlq records per source topic (lab_dlq_total{source_topic}).
//
// Documented exception to "every consumer uses the kit runner": orders.dlq carries the
// original, often undecodable bytes, not Avro. The runner would dead-letter them again (into
// the topic it is reading). Counting needs no database and no exactly-once processing, so a
// plain franz-go group client with the kit's base options (and hooks) is enough. Offsets are
// auto-committed: after a crash a few records may be counted twice, which a counter tolerates.
type DLQCounter struct {
	Brokers   []string
	Hooks     []kgo.Hook
	Metrics   *metrics.Set
	Log       *slog.Logger
	ExtraOpts []kgo.Opt // appended to the client options

	ready atomic.Bool
}

// Run counts until ctx ends.
func (d *DLQCounter) Run(ctx context.Context) error {
	opts := append(kafka.BaseOptions(d.Brokers, DLQGroup, d.Hooks...),
		kgo.ConsumerGroup(DLQGroup),
		kgo.ConsumeTopics(contracts.TopicOrdersDLQ),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	cl, err := kgo.NewClient(append(opts, d.ExtraOpts...)...)
	if err != nil { // coverage-ignore: only invalid static options fail here
		return err
	}
	defer cl.Close()
	d.ready.Store(true)
	defer d.ready.Store(false)
	for {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			return nil
		}
		fs.EachError(func(topic string, p int32, err error) { // coverage-ignore: log only; the client retries fetches itself
			d.Log.WarnContext(ctx, "dlq fetch error", "topic", topic, "partition", p, "error", err)
		})
		fs.EachRecord(d.count)
	}
}

// Ready reports whether the counter is consuming.
func (d *DLQCounter) Ready() bool { return d.ready.Load() }

func (d *DLQCounter) count(r *kgo.Record) {
	source := "unknown"
	for _, h := range r.Headers {
		if h.Key == contracts.HeaderDLQSourceTopic {
			source = string(h.Value)
		}
	}
	d.Metrics.GetOrCreateCounter(kafka.Label("lab_dlq_total", "source_topic", source)).Inc()
}
