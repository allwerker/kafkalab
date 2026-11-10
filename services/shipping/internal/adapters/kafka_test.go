package adapters

import (
	"context"
	"testing"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/twmb/franz-go/pkg/kgo"

	"kafkalab/pkg/contracts"
	"kafkalab/pkg/events"
	"kafkalab/pkg/kit/producer"
	"kafkalab/pkg/kit/runner"
	"kafkalab/pkg/kit/serde"
	"kafkalab/pkg/kit/testkit"
)

type publisherWorld struct {
	cluster *testkit.Cluster
	codec   *serde.Serde
	pub     KafkaPublisher
	set     *metrics.Set
}

func newPublisher(t *testing.T) publisherWorld {
	t.Helper()
	ctx := context.Background()
	cluster := testkit.NewCluster(t)
	reg := testkit.NewRegistry(t, true)
	codec, err := serde.New(ctx, serde.Options{Registry: reg.Client(t), Topics: []string{contracts.TopicOrdersShipped, contracts.TopicOrdersCancelled}})
	if err != nil {
		t.Fatal(err)
	}
	prod, err := producer.New(producer.Config{Service: contracts.ServiceShipping, Brokers: cluster.Brokers, Control: noKnobs{}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { prod.Close(context.Background()) })
	set := metrics.NewSet()
	return publisherWorld{cluster, codec, KafkaPublisher{Producer: prod, Codec: codec, Metrics: set}, set}
}

func Test_shouldDeliverTheSweepersDecisionWhenItIsPublished(t *testing.T) {
	// GIVEN: a deadline cancellation decided by the sweeper
	w := newPublisher(t)
	ctx := context.Background()
	cancel := &events.OrderCancelled{Meta: events.EventMeta{EventID: "e-1", OrderID: "o-1", CausationID: "deadline:o-1",
		OccurredAt: time.UnixMilli(1000).UTC(), OrderCreatedAt: time.UnixMilli(500).UTC(), Producer: contracts.ServiceShipping},
		Reason: events.CancellationReasonDeadlineExceeded, RefundRequired: true}

	// WHEN: it is published
	err := w.pub.Publish(ctx, []runner.Output{{Topic: contracts.TopicOrdersCancelled, Key: "o-1", Value: cancel}})

	// THEN: it is on orders.cancelled, keyed by the order, decodable, and counted as produced
	if err != nil {
		t.Fatal(err)
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(w.cluster.Brokers...), kgo.ConsumeTopics(contracts.TopicOrdersCancelled),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	pctx, stop := context.WithTimeout(ctx, 10*time.Second)
	defer stop()
	recs := cl.PollFetches(pctx).Records()
	if len(recs) != 1 || string(recs[0].Key) != "o-1" || string(recs[0].Headers[0].Value) != contracts.ServiceShipping {
		t.Fatalf("records %+v", recs)
	}
	got, err := w.codec.Decode(recs[0].Value)
	if err != nil || got.(*events.OrderCancelled).Meta.CausationID != "deadline:o-1" {
		t.Fatalf("decoded %+v, %v", got, err)
	}
	if n := w.set.GetOrCreateCounter(`lab_records_produced_total{topic="orders.cancelled"}`).Get(); n != 1 {
		t.Fatalf("produced counter %d", n)
	}
}

func Test_shouldReportAFailureWhenAnEventCannotBeEncoded(t *testing.T) {
	// GIVEN: an event of a type without a registered schema
	w := newPublisher(t)

	// WHEN: it is published
	err := w.pub.Publish(context.Background(), []runner.Output{{Topic: contracts.TopicOrdersShipped, Key: "o-1", Value: &events.OrderCreated{}}})

	// THEN: the sweep fails (and is rolled back) instead of publishing garbage
	if err == nil {
		t.Fatal("expected an encoding error")
	}
}

func Test_shouldReportAFailureWhenTheBrokersDoNotAcknowledge(t *testing.T) {
	// GIVEN: a shipment and a produce that gives up before Kafka acknowledged it
	w := newPublisher(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// WHEN: it is published
	err := w.pub.Publish(ctx, []runner.Output{{Topic: contracts.TopicOrdersShipped, Key: "o-1", Value: &events.OrderShipped{Meta: events.EventMeta{EventID: "e", OrderID: "o-1"}}}})

	// THEN: the error reaches the sweeper, which keeps the order for the next sweep
	if err == nil || w.set.GetOrCreateCounter(`lab_records_produced_total{topic="orders.shipped"}`).Get() != 0 {
		t.Fatalf("got %v", err)
	}
}
