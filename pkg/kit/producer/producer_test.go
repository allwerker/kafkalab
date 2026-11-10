package producer

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/twmb/franz-go/pkg/kgo"

	"kafkalab/pkg/events"
	"kafkalab/pkg/kit/kafka"
	"kafkalab/pkg/kit/testkit"
)

type knobs struct {
	mu sync.Mutex
	k  events.Knobs
}

func (k *knobs) Knobs() events.Knobs { k.mu.Lock(); defer k.mu.Unlock(); return k.k }
func (k *knobs) set(v events.Knobs)  { k.mu.Lock(); k.k = v; k.mu.Unlock() }
func ptr[T any](v T) *T              { return &v }

func count(t *testing.T, brokers []string, topic string) int {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	n := 0
	for {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			return n
		}
		n += fs.NumRecords()
	}
}

func Test_shouldDeliverOrdersWhenTheGeneratorProduces(t *testing.T) {
	// GIVEN: a generator producer on a cluster
	c := testkit.NewCluster(t)
	p, err := New(Config{Service: "order-generator", Brokers: c.Brokers, Control: &knobs{}})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(3)

	// WHEN: three orders are produced
	for i := 0; i < 3; i++ {
		p.Produce(context.Background(), &kgo.Record{Topic: "orders.created", Value: []byte("o")}, func(_ *kgo.Record, err error) {
			if err != nil {
				t.Error(err)
			}
			wg.Done()
		})
	}
	wg.Wait()
	p.Close(context.Background())

	// THEN: all three are on the topic
	if n := count(t, c.Brokers, "orders.created"); n != 3 {
		t.Fatalf("expected 3 orders, got %d", n)
	}
}

func Test_shouldRebuildWithoutLosingBufferedOrdersWhenTheTuningLabChangesSettings(t *testing.T) {
	// GIVEN: a generator with buffered orders and a player switching to acks=leader and zstd
	c := testkit.NewCluster(t)
	k := &knobs{}
	var logs bytes.Buffer
	set := metrics.NewSet()
	p, err := New(Config{Service: "order-generator", Brokers: c.Brokers, Control: k, Metrics: set,
		Log: slogTo(&logs), Hooks: []kgo.Hook{kafka.NewHooks(set)}})
	if err != nil {
		t.Fatal(err)
	}
	before := p.Client()
	for i := 0; i < 50; i++ {
		p.Produce(context.Background(), &kgo.Record{Topic: "orders.created", Value: []byte("o")}, nil)
	}
	k.set(events.Knobs{Client: events.ClientTuning{Acks: ptr("leader"), Compression: ptr("zstd")}})

	// WHEN: the producer refreshes its settings (twice, the second time nothing changed)
	if err := p.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := p.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	p.Produce(context.Background(), &kgo.Record{Topic: "orders.created", Value: []byte("after")}, nil)
	p.Close(context.Background())

	// THEN: all orders arrived, exactly one rebuild happened and the lossy setting was logged
	if n := count(t, c.Brokers, "orders.created"); n != 51 {
		t.Fatalf("expected 51 orders, got %d", n)
	}
	var b bytes.Buffer
	set.WritePrometheus(&b)
	if p.Client() == before || !strings.Contains(b.String(), "lab_client_rebuilds_total 1") || !strings.Contains(logs.String(), "lowered acks") {
		t.Fatalf("rebuild not visible:\n%s\n%s", b.String(), logs.String())
	}
}

func Test_shouldUseTheNetworkChaosDialerWhenOneIsConfigured(t *testing.T) {
	// GIVEN: a generator whose connections go through the fault dialer
	c := testkit.NewCluster(t)
	fd := kafka.NewFaultDialer(func() kafka.Faults { return kafka.Faults{} })
	p, err := New(Config{Service: "order-generator", Brokers: c.Brokers, Control: &knobs{}, Dialer: fd.DialContext})
	if err != nil {
		t.Fatal(err)
	}

	// WHEN: an order is produced synchronously
	err = p.Client().ProduceSync(context.Background(), &kgo.Record{Topic: "orders.created", Value: []byte("o")}).FirstErr()
	p.Close(context.Background())

	// THEN: it is delivered through the wrapped connection
	if err != nil {
		t.Fatal(err)
	}
}

func Test_shouldLogAndContinueWhenTheOldClientCannotFlushBeforeARebuild(t *testing.T) {
	// GIVEN: a generator whose broker is gone, with an order stuck in the buffer
	c := testkit.NewCluster(t)
	k := &knobs{}
	var logs bytes.Buffer
	p, err := New(Config{Service: "order-generator", Brokers: c.Brokers, Control: k, Log: slogTo(&logs)})
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
	p.Produce(context.Background(), &kgo.Record{Topic: "orders.created", Value: []byte("stuck")}, nil)
	k.set(events.Knobs{Client: events.ClientTuning{LingerMs: ptr(int32(10))}})
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// WHEN: the tuning changes
	err = p.Refresh(ctx)

	// THEN: the rebuild still happens and the failed flush is logged
	if err != nil || !strings.Contains(logs.String(), "flush before client rebuild failed") {
		t.Fatalf("err=%v logs=%s", err, logs.String())
	}
	p.Client().Close()
}
