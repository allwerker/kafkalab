package runner

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kgo"

	"kafkalab/pkg/contracts"
	"kafkalab/pkg/events"
	"kafkalab/pkg/kit/pg"
	"kafkalab/pkg/kit/serde"
	"kafkalab/pkg/kit/testkit"
)

// fakeControl is a control plane the test can flip at will.
type fakeControl struct {
	mu sync.Mutex
	k  events.Knobs
	ch chan struct{}
}

func newFakeControl() *fakeControl { return &fakeControl{ch: make(chan struct{})} }

func (f *fakeControl) Knobs() events.Knobs { f.mu.Lock(); defer f.mu.Unlock(); return f.k }
func (f *fakeControl) Changed() <-chan struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ch
}
func (f *fakeControl) Set(k events.Knobs) {
	f.mu.Lock()
	f.k = k
	close(f.ch)
	f.ch = make(chan struct{})
	f.mu.Unlock()
}

type harness struct {
	t       *testing.T
	cluster *testkit.Cluster
	serde   *serde.Serde
	db      *pg.DB
	control *fakeControl
	set     *metrics.Set
	writer  *kgo.Client
	sleeps  atomic.Int64 // total injected sleep in ms (latency knobs)
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	cluster := testkit.NewCluster(t)
	reg := testkit.NewRegistry(t, true)
	s, err := serde.New(context.Background(), serde.Options{Registry: reg.Client(t),
		Topics: []string{contracts.TopicOrdersCreated, contracts.TopicPaymentsCompleted, contracts.TopicPaymentsFailed, contracts.TopicPlatformControl}})
	if err != nil {
		t.Fatal(err)
	}
	db, err := pg.Open(context.Background(), pg.Options{URL: testkit.PostgresDSN(t)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if err := db.Migrate(context.Background(), mustSub(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Pool.Exec(context.Background(), `CREATE TABLE charges (order_id text PRIMARY KEY, cents bigint NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	w, err := kgo.NewClient(kgo.SeedBrokers(cluster.Brokers...))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Close)
	return &harness{t: t, cluster: cluster, serde: s, db: db, control: newFakeControl(), set: metrics.NewSet(), writer: w}
}

// paymentHandler charges every order once and answers with payments.completed.
func paymentHandler(calls *atomic.Int64) HandlerFunc {
	return func(ctx context.Context, tx pgx.Tx, in Input) ([]Output, error) {
		calls.Add(1)
		o := in.Value.(*events.OrderCreated)
		if _, err := tx.Exec(ctx, `INSERT INTO charges VALUES ($1, $2)`, o.Meta.OrderID, o.TotalCents); err != nil {
			return nil, err
		}
		return []Output{{Topic: contracts.TopicPaymentsCompleted, Key: o.Meta.OrderID, Value: &events.PaymentCompleted{
			Meta:      events.EventMeta{EventID: contracts.DeriveEventID(o.Meta.EventID, "payment"), CausationID: o.Meta.EventID, OrderID: o.Meta.OrderID, OccurredAt: time.Now(), OrderCreatedAt: o.Meta.OrderCreatedAt, Producer: "payment"},
			PaymentID: "pay-" + o.Meta.OrderID, AmountCents: o.TotalCents, PaymentMethod: o.PaymentMethod,
		}}}, nil
	}
}

func (h *harness) runner(handler Handler, mutate ...func(*Config)) *Runner {
	cfg := Config{
		Service: "payment", Topics: []string{contracts.TopicOrdersCreated}, Brokers: h.cluster.Brokers,
		DB: h.db, Codec: h.serde, Control: h.control, Handler: handler, Metrics: h.set,
		Backoff: func(int) time.Duration { return 5 * time.Millisecond },
		Sleep: func(ctx context.Context, d time.Duration) error {
			if d >= 50*time.Millisecond { // latency knobs; backoffs are tiny
				h.sleeps.Add(d.Milliseconds())
				return ctx.Err()
			}
			return sleepCtx(ctx, d)
		},
		ExtraOpts: []kgo.Opt{kgo.FetchMaxWait(100 * time.Millisecond)},
	}
	for _, m := range mutate {
		m(&cfg)
	}
	return New(cfg)
}

func (h *harness) start(r *Runner) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	h.t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				h.t.Errorf("runner returned %v", err)
			}
		case <-time.After(15 * time.Second):
			h.t.Error("runner did not stop")
		}
	})
}

func order(id string, cents int64) *events.OrderCreated {
	now := time.Now().UTC().Truncate(time.Millisecond)
	return &events.OrderCreated{
		Meta:       events.EventMeta{EventID: "evt-" + id, OrderID: id, OccurredAt: now, OrderCreatedAt: now, Producer: "order-generator"},
		CustomerID: "c-1", Region: events.RegionNorth, PaymentMethod: events.PaymentMethodCard,
		Lines: []events.OrderLine{{Sku: "sku-1", Quantity: 1, UnitPriceCents: cents}}, TotalCents: cents, Currency: "EUR",
	}
}

func (h *harness) produce(o *events.OrderCreated) {
	h.t.Helper()
	b, err := h.serde.Encode(o)
	if err != nil {
		h.t.Fatal(err)
	}
	h.produceRaw(o.Meta.OrderID, b)
}

func (h *harness) produceRaw(key string, value []byte) {
	h.t.Helper()
	if err := h.writer.ProduceSync(context.Background(), &kgo.Record{Topic: contracts.TopicOrdersCreated, Key: []byte(key), Value: value}).FirstErr(); err != nil {
		h.t.Fatal(err)
	}
}

// read returns at least n records of topic (waits up to 15 s).
func (h *harness) read(topic string, n int) []*kgo.Record {
	h.t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(h.cluster.Brokers...), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		h.t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var out []*kgo.Record
	for len(out) < n {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			h.t.Fatalf("expected %d records on %s, got %d", n, topic, len(out))
		}
		out = append(out, fs.Records()...)
	}
	return out
}

func (h *harness) metrics() string {
	var b bytes.Buffer
	h.set.WritePrometheus(&b)
	return b.String()
}

func (h *harness) eventually(what string, cond func() bool) {
	h.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			h.t.Fatalf("timed out waiting for: %s\nmetrics:\n%s", what, h.metrics())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (h *harness) hasMetric(line string) bool { return strings.Contains(h.metrics(), line) }

func (h *harness) charges() int {
	var n int
	_ = h.db.Pool.QueryRow(context.Background(), `SELECT count(*) FROM charges`).Scan(&n)
	return n
}

func header(r *kgo.Record, key string) string {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
