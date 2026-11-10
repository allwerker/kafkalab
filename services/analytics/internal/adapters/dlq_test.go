package adapters

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/twmb/franz-go/pkg/kgo"

	"kafkalab/pkg/contracts"
	"kafkalab/pkg/kit/testkit"
)

func Test_shouldCountDeadLettersPerSourceTopicWhenTheyReachTheDLQ(t *testing.T) {
	// GIVEN: two poison orders and one rejected payment in orders.dlq, plus a record without
	// a source header
	cluster := testkit.NewCluster(t)
	ctx := context.Background()
	cl, err := kgo.NewClient(kgo.SeedBrokers(cluster.Brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	dead := func(source string) *kgo.Record {
		r := &kgo.Record{Topic: contracts.TopicOrdersDLQ, Key: []byte("o"), Value: []byte{0xde, 0xad}}
		if source != "" {
			r.Headers = []kgo.RecordHeader{{Key: contracts.HeaderDLQReason, Value: []byte("undecodable")},
				{Key: contracts.HeaderDLQSourceTopic, Value: []byte(source)}}
		}
		return r
	}
	if err := cl.ProduceSync(ctx, dead("orders.created"), dead("orders.created"), dead("payments.completed"), dead("")).FirstErr(); err != nil {
		t.Fatal(err)
	}
	set := metrics.NewSet()
	d := &DLQCounter{Brokers: cluster.Brokers, Metrics: set, Log: slog.New(slog.DiscardHandler)}
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error)

	// WHEN: the DLQ counter consumes them
	go func() { done <- d.Run(runCtx) }()
	counter := func(src string) uint64 {
		return set.GetOrCreateCounter(`lab_dlq_total{source_topic="` + src + `"}`).Get()
	}
	deadline := time.Now().Add(15 * time.Second)
	for counter("orders.created")+counter("payments.completed")+counter("unknown") < 4 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	ready := d.Ready()
	stop()

	// THEN: every dead letter is counted under the topic it came from, and the counter
	// stops cleanly when the service shuts down
	if counter("orders.created") != 2 || counter("payments.completed") != 1 || counter("unknown") != 1 || !ready {
		t.Fatalf("created=%d payments=%d unknown=%d ready=%v", counter("orders.created"), counter("payments.completed"), counter("unknown"), ready)
	}
	if err := <-done; err != nil || d.Ready() {
		t.Fatalf("run ended with %v, ready=%v", err, d.Ready())
	}
}
