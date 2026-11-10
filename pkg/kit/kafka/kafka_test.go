package kafka

import (
	"bytes"
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/twmb/franz-go/pkg/kgo"

	"kafkalab/pkg/events"
	"kafkalab/pkg/kit/testkit"
)

func ptr[T any](v T) *T { return &v }

func exposition(set *metrics.Set) string {
	var b bytes.Buffer
	set.WritePrometheus(&b)
	return b.String()
}

func Test_shouldExportClientTrafficMetricsWhenAServiceProducesAndConsumes(t *testing.T) {
	// GIVEN: a service client with lab hooks connected to a cluster
	cluster := testkit.NewCluster(t)
	set := metrics.NewSet()
	hooks := NewHooks(set)
	cl, err := kgo.NewClient(append(BaseOptions(cluster.Brokers, "payment", hooks),
		kgo.ConsumeTopics("orders.created"), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))...)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	hooks.WatchBuffers(func() *kgo.Client { return cl })

	// WHEN: it produces an order and reads it back
	if err := cl.ProduceSync(context.Background(), &kgo.Record{Topic: "orders.created", Value: []byte("order")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if fs := cl.PollRecords(ctx, 10); fs.NumRecords() != 1 {
		t.Fatalf("expected 1 record, got %d (%v)", fs.NumRecords(), fs.Errors())
	}

	// THEN: connections, bytes, produce batches and fetched records are visible as metrics
	out := exposition(set)
	for _, want := range []string{
		"kafka_client_connects_total", "kafka_client_write_bytes_total", "kafka_client_read_bytes_total",
		"kafka_client_request_duration_seconds", `kafka_client_produce_records_total{topic="orders.created"} 1`,
		`kafka_client_produce_batches_total{topic="orders.created"} 1`, `kafka_client_fetch_records_total{topic="orders.created"} 1`,
		"kafka_client_buffered_produce_records", "kafka_client_buffered_fetch_records",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in\n%s", want, out)
		}
	}
}

func Test_shouldCountConnectionProblemsWhenABrokerIsUnreachableOrThrottles(t *testing.T) {
	// GIVEN: hooks observing a broker that refuses connections, disconnects and throttles
	set := metrics.NewSet()
	h := NewHooks(set)
	broker := kgo.BrokerMetadata{NodeID: 2}

	// WHEN: the client reports these events
	h.OnBrokerConnect(broker, time.Millisecond, nil, errors.New("refused"))
	h.OnBrokerDisconnect(broker, nil)
	h.OnBrokerThrottle(broker, 250*time.Millisecond, true)

	// THEN: each is counted per broker node
	out := exposition(set)
	for _, want := range []string{
		`kafka_client_connect_errors_total{node="2"} 1`,
		`kafka_client_disconnects_total{node="2"} 1`,
		`kafka_client_throttle_seconds_total{node="2"} 0.25`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in\n%s", want, out)
		}
	}
}

func Test_shouldReportEmptyBuffersWhenTheClientIsBeingRebuilt(t *testing.T) {
	// GIVEN: buffer gauges watching a client slot that is empty during a Tuning Lab rebuild
	set := metrics.NewSet()
	NewHooks(set).WatchBuffers(func() *kgo.Client { return nil })

	// WHEN: metrics are scraped
	out := exposition(set)

	// THEN: both gauges report zero instead of failing the scrape
	if !strings.Contains(out, "kafka_client_buffered_produce_records 0") || !strings.Contains(out, "kafka_client_buffered_fetch_records 0") {
		t.Fatalf("unexpected exposition\n%s", out)
	}
}

func Test_shouldKeepLabDefaultsWhenNoTuningIsRequested(t *testing.T) {
	// GIVEN: no Tuning Lab settings
	// WHEN: options are derived
	opts, maxPoll, lossy := TuningOptions(events.ClientTuning{})

	// THEN: only the default linger applies, polls stay bounded and delivery stays safe
	if len(opts) != 1 || maxPoll != DefaultMaxPollRecords || lossy {
		t.Fatalf("got %d opts, maxPoll %d, lossy %v", len(opts), maxPoll, lossy)
	}
}

func Test_shouldFlagTheSettingsAsLossyWhenAcksAreLoweredBelowAll(t *testing.T) {
	// GIVEN: a player lowering acks in the Tuning Lab
	for acks, wantLossy := range map[string]bool{"leader": true, "none": true, "all": false} {
		// WHEN: options are derived
		_, _, lossy := TuningOptions(events.ClientTuning{Acks: ptr(acks)})

		// THEN: anything but acks=all is flagged because idempotent writes are switched off
		if lossy != wantLossy {
			t.Fatalf("acks=%s: lossy=%v", acks, lossy)
		}
	}
}

func Test_shouldApplyEveryTuningKnobWhenAllSettingsAreGiven(t *testing.T) {
	// GIVEN: a fully tuned throughput profile with every compression codec
	for _, codec := range []string{"gzip", "snappy", "lz4", "zstd", "none"} {
		tuning := events.ClientTuning{
			Acks: ptr("all"), LingerMs: ptr(int32(20)), BatchMaxBytes: ptr(int32(1 << 20)), Compression: ptr(codec),
			FetchMaxBytes: ptr(int32(4 << 20)), FetchMaxWaitMs: ptr(int32(200)), FetchMinBytes: ptr(int32(1024)),
			MaxPollRecords: ptr(int32(50)),
		}

		// WHEN: options are derived and a client is built from them
		opts, maxPoll, _ := TuningOptions(tuning)
		cl, err := kgo.NewClient(append(BaseOptions([]string{"127.0.0.1:1"}, "tuned"), opts...)...)

		// THEN: the client accepts every option and polls are capped as requested
		if err != nil || maxPoll != 50 || len(opts) != 8 {
			t.Fatalf("codec %s: err=%v maxPoll=%d opts=%d", codec, err, maxPoll, len(opts))
		}
		cl.Close()
	}
}

func Test_shouldDetectATuningChangeWhenAnySettingDiffers(t *testing.T) {
	// GIVEN: the current settings of a service
	current := events.ClientTuning{LingerMs: ptr(int32(5)), Compression: ptr("lz4")}

	// WHEN / THEN: equal settings need no rebuild, any difference does
	if !SameTuning(current, events.ClientTuning{LingerMs: ptr(int32(5)), Compression: ptr("lz4")}) {
		t.Fatal("equal settings must not trigger a rebuild")
	}
	for _, other := range []events.ClientTuning{
		{LingerMs: ptr(int32(6)), Compression: ptr("lz4")},
		{Compression: ptr("lz4")},
		{LingerMs: ptr(int32(5)), Compression: ptr("zstd")},
		{LingerMs: ptr(int32(5))},
	} {
		if SameTuning(current, other) {
			t.Fatalf("expected a difference for %+v", other)
		}
	}
}

func Test_shouldFormatMetricNamesWhenLabelsAreGiven(t *testing.T) {
	// GIVEN / WHEN: metric names with and without labels
	plain := Label("lab_client_rebuilds_total")
	labelled := Label("lab_records_processed_total", "topic", "orders.created", "result", "ok")

	// THEN: they follow the Prometheus exposition syntax
	if plain != "lab_client_rebuilds_total" || labelled != `lab_records_processed_total{topic="orders.created",result="ok"}` {
		t.Fatalf("got %s / %s", plain, labelled)
	}
}

// --- fault injection ---------------------------------------------------------------------

type pipeConn struct {
	net.Conn
	written bytes.Buffer
	closed  bool
}

func (p *pipeConn) Write(b []byte) (int, error) { return p.written.Write(b) }
func (p *pipeConn) Close() error                { p.closed = true; return nil }

func faultDialer(f Faults, random float64) (*FaultDialer, *pipeConn, *[]time.Duration) {
	conn := &pipeConn{}
	var slept []time.Duration
	d := NewFaultDialer(func() Faults { return f })
	d.dial = func(context.Context, string, string) (net.Conn, error) { return conn, nil }
	d.random = func() float64 { return random }
	d.sleep = func(x time.Duration) { slept = append(slept, x) }
	return d, conn, &slept
}

func Test_shouldPassTrafficUntouchedWhenNoNetworkChaosIsActive(t *testing.T) {
	// GIVEN: a healthy network
	d, conn, slept := faultDialer(Faults{}, 0)

	// WHEN: a request is written
	c, err := d.DialContext(context.Background(), "tcp", "broker:9092")
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Write([]byte("produce"))

	// THEN: it arrives immediately and nothing is dropped
	if err != nil || conn.written.String() != "produce" || len(*slept) != 0 || d.Drops() != 0 {
		t.Fatalf("err=%v written=%q slept=%v drops=%d", err, conn.written.String(), *slept, d.Drops())
	}
}

func Test_shouldDelayEveryRequestWhenNetworkLatencyIsInjected(t *testing.T) {
	// GIVEN: Network Chaos adding 300 ms to the payment service's Kafka traffic
	d, conn, slept := faultDialer(Faults{Latency: 300 * time.Millisecond}, 0.9)

	// WHEN: two requests are written
	c, _ := d.DialContext(context.Background(), "tcp", "broker:9092")
	_, _ = c.Write([]byte("a"))
	_, _ = c.Write([]byte("b"))

	// THEN: each one waited for the injected latency before being sent
	if len(*slept) != 2 || (*slept)[0] != 300*time.Millisecond || conn.written.String() != "ab" {
		t.Fatalf("slept=%v written=%q", *slept, conn.written.String())
	}
}

func Test_shouldResetTheConnectionWhenPacketLossHits(t *testing.T) {
	// GIVEN: Network Chaos dropping every connection
	d, conn, _ := faultDialer(Faults{DropProbability: 0.5}, 0.9) // the dial itself gets through

	// WHEN: a request is written on an established connection
	c, err := d.DialContext(context.Background(), "tcp", "broker:9092")
	if err != nil {
		t.Fatal(err)
	}
	d.random = func() float64 { return 0.1 }
	_, err = c.Write([]byte("fetch"))

	// THEN: the connection is closed like a reset and the client has to reconnect
	if !errors.Is(err, ErrInjectedReset) || !conn.closed || d.Drops() != 1 {
		t.Fatalf("err=%v closed=%v drops=%d", err, conn.closed, d.Drops())
	}
}

func Test_shouldRefuseNewConnectionsWhenPacketLossHitsTheHandshake(t *testing.T) {
	// GIVEN: heavy packet loss
	d, _, _ := faultDialer(Faults{DropProbability: 0.8}, 0.2)

	// WHEN: the client connects to a broker
	_, err := d.DialContext(context.Background(), "tcp", "broker:9092")

	// THEN: the connection attempt fails as injected
	if !errors.Is(err, ErrInjectedReset) || d.Drops() != 1 {
		t.Fatalf("err=%v drops=%d", err, d.Drops())
	}
}

func Test_shouldReportRealDialErrorsWhenTheBrokerIsGenuinelyDown(t *testing.T) {
	// GIVEN: no chaos, but nothing listens on the broker address
	d := NewFaultDialer(func() Faults { return Faults{} })

	// WHEN: the client connects
	_, err := d.DialContext(context.Background(), "tcp", "127.0.0.1:1")

	// THEN: the real network error is passed through, not an injected one
	if err == nil || errors.Is(err, ErrInjectedReset) {
		t.Fatalf("expected a real dial error, got %v", err)
	}
}

func Test_shouldBackOffLongerButAtMostTwoSecondsWhenABrokerKeepsFailing(t *testing.T) {
	// GIVEN: a broker that keeps rejecting requests
	// WHEN: the client computes its retry pauses
	first, fifth, hundredth := retryBackoff(1), retryBackoff(5), retryBackoff(100)

	// THEN: pauses grow with every try but never exceed two seconds
	if first != 100*time.Millisecond || fifth != 500*time.Millisecond || hundredth != 2*time.Second {
		t.Fatalf("got %v %v %v", first, fifth, hundredth)
	}
}
