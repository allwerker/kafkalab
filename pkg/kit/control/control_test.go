package control

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"kafkalab/pkg/contracts"
	"kafkalab/pkg/events"
	"kafkalab/pkg/kit/serde"
	"kafkalab/pkg/kit/testkit"
)

type env struct {
	cluster *testkit.Cluster
	serde   *serde.Serde
	writer  *kgo.Client
	clock   *clock
}

type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time  { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *clock) Set(t time.Time) { c.mu.Lock(); c.now = t; c.mu.Unlock() }

func newEnv(t *testing.T) *env {
	t.Helper()
	cluster := testkit.NewCluster(t)
	reg := testkit.NewRegistry(t, true)
	s, err := serde.New(context.Background(), serde.Options{Registry: reg.Client(t), Topics: []string{contracts.TopicPlatformControl}})
	if err != nil {
		t.Fatal(err)
	}
	w, err := kgo.NewClient(kgo.SeedBrokers(cluster.Brokers...), kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Close)
	return &env{cluster: cluster, serde: s, writer: w, clock: &clock{now: time.Unix(1_800_000_000, 0)}}
}

func (e *env) publish(t *testing.T, snap events.ControlSnapshot) {
	t.Helper()
	if err := Publish(context.Background(), e.writer, e.serde, &snap); err != nil {
		t.Fatal(err)
	}
}

func (e *env) start(t *testing.T, service string, set *metrics.Set) *Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := Start(ctx, Options{Brokers: e.cluster.Brokers, Service: service, Decoder: e.serde, Now: e.clock.Now, Metrics: set})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

func waitFor(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a control change")
	}
}

func Test_shouldStartWithBaselineKnobsWhenTheControlboardNeverPublished(t *testing.T) {
	// GIVEN: a fresh cluster without any control snapshots
	e := newEnv(t)

	// WHEN: the payment service starts its control client
	c := e.start(t, "payment", nil)

	// THEN: it runs with baseline knobs and reports generation 0
	if k := c.Knobs(); k.Paused || k.ProcessingLatencyMs != 0 {
		t.Fatalf("expected baseline knobs, got %+v", k)
	}
	if _, ok := c.Snapshot(); ok {
		t.Fatal("expected no snapshot")
	}
}

func Test_shouldApplyTheLatestSnapshotBeforeProcessingWhenAServiceRestartsDuringAnExperiment(t *testing.T) {
	// GIVEN: a running "Warehouse Overload" experiment that slowed inventory down twice
	e := newEnv(t)
	e.publish(t, events.ControlSnapshot{Service: "inventory", Generation: 1, IssuedAt: e.clock.Now(), Knobs: events.Knobs{ProcessingLatencyMs: 50}})
	e.publish(t, events.ControlSnapshot{Service: "inventory", Generation: 2, IssuedAt: e.clock.Now(), Knobs: events.Knobs{ProcessingLatencyMs: 500},
		Experiments: []string{"exp-1"}})
	e.publish(t, events.ControlSnapshot{Service: "payment", Generation: 7, IssuedAt: e.clock.Now(), Knobs: events.Knobs{ProviderDown: true}})

	// WHEN: an inventory pod (re)starts
	c := e.start(t, "inventory", nil)

	// THEN: before it processes anything it already runs with the newest inventory knobs only
	snap, ok := c.Snapshot()
	if !ok || snap.Generation != 2 || c.Knobs().ProcessingLatencyMs != 500 || c.Knobs().ProviderDown {
		t.Fatalf("unexpected state %+v / %+v", snap, c.Knobs())
	}
}

func Test_shouldNotifyAndApplyWhenTheControlboardChangesKnobsWhileRunning(t *testing.T) {
	// GIVEN: a running notification service
	e := newEnv(t)
	set := metrics.NewSet()
	c := e.start(t, "notification", set)
	changed := c.Changed()

	// WHEN: "Notification Meltdown" starts
	e.publish(t, events.ControlSnapshot{Service: "notification", Generation: 1, IssuedAt: e.clock.Now(),
		Knobs: events.Knobs{ProcessingLatencyMs: 1000, Paused: true}})
	waitFor(t, changed)

	// THEN: the service sees the new knobs and exports the applied generation and knob values
	if c.Knobs().ProcessingLatencyMs != 1000 {
		t.Fatalf("knobs not applied: %+v", c.Knobs())
	}
	var b bytes.Buffer
	set.WritePrometheus(&b)
	for _, want := range []string{"lab_control_generation 1", `lab_control_knob{knob="processingLatencyMs"} 1000`, `lab_control_knob{knob="paused"} 1`} {
		if !strings.Contains(b.String(), want) {
			t.Fatalf("missing %q in\n%s", want, b.String())
		}
	}
}

func Test_shouldFallBackToBaselineWhenTheSnapshotExpiresEvenWithoutARevert(t *testing.T) {
	// GIVEN: a "Payment Provider Down" snapshot that expires in 200 ms and a controlboard that crashed
	e := newEnv(t)
	e.clock.Set(time.Now())
	expires := e.clock.Now().Add(200 * time.Millisecond)
	e.publish(t, events.ControlSnapshot{Service: "payment", Generation: 3, IssuedAt: e.clock.Now(), ExpiresAt: &expires,
		Knobs: events.Knobs{ProviderDown: true}})
	c := e.start(t, "payment", nil)
	if !c.Knobs().ProviderDown {
		t.Fatal("expected the outage to be active")
	}
	changed := c.Changed()

	// WHEN: the expiry passes
	e.clock.Set(expires.Add(time.Millisecond))
	waitFor(t, changed)

	// THEN: the service heals itself and runs with baseline knobs again
	if c.Knobs().ProviderDown {
		t.Fatal("expected baseline knobs after expiry")
	}
}

func Test_shouldIgnoreOlderGenerationsWhenRecordsArriveOutOfOrder(t *testing.T) {
	// GIVEN: a shipping service already at generation 5
	e := newEnv(t)
	e.publish(t, events.ControlSnapshot{Service: "shipping", Generation: 5, IssuedAt: e.clock.Now(), Knobs: events.Knobs{WarehouseOutage: true}})
	c := e.start(t, "shipping", nil)

	// WHEN: a stale generation 4 shows up, followed by generation 6
	e.publish(t, events.ControlSnapshot{Service: "shipping", Generation: 4, IssuedAt: e.clock.Now()})
	changed := c.Changed()
	e.publish(t, events.ControlSnapshot{Service: "shipping", Generation: 6, IssuedAt: e.clock.Now(), Knobs: events.Knobs{DbLatencyMs: 10}})
	waitFor(t, changed)

	// THEN: the stale record never overrode the newer state
	snap, _ := c.Snapshot()
	if snap.Generation != 6 || c.Knobs().WarehouseOutage || c.Knobs().DbLatencyMs != 10 {
		t.Fatalf("unexpected %+v", snap)
	}
}

func Test_shouldSkipGarbageWhenTheControlTopicContainsAnUndecodableRecord(t *testing.T) {
	// GIVEN: a corrupt record followed by a valid snapshot for analytics
	e := newEnv(t)
	if err := e.writer.ProduceSync(context.Background(), &kgo.Record{Topic: contracts.TopicPlatformControl, Key: []byte("analytics"), Value: []byte("junk")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	e.publish(t, events.ControlSnapshot{Service: "analytics", Generation: 1, IssuedAt: e.clock.Now(), Knobs: events.Knobs{Paused: true}})

	// WHEN: analytics starts
	c := e.start(t, "analytics", nil)

	// THEN: the garbage is ignored and the valid snapshot applies
	if !c.Knobs().Paused {
		t.Fatal("expected the valid snapshot to apply")
	}
}

func Test_shouldTrackEveryServiceWhenTheControlboardReadsTheTopic(t *testing.T) {
	// GIVEN: snapshots for two services
	e := newEnv(t)
	e.publish(t, events.ControlSnapshot{Service: "payment", Generation: 2, IssuedAt: e.clock.Now(), Knobs: events.Knobs{ProviderDown: true}})
	e.publish(t, events.ControlSnapshot{Service: "inventory", Generation: 9, IssuedAt: e.clock.Now()})

	// WHEN: the controlboard restarts and reads the topic without a service filter
	c := e.start(t, "", nil)

	// THEN: it recovers the desired state of every service, e.g. to continue generation numbers
	p, okP := c.SnapshotOf("payment")
	i, okI := c.SnapshotOf("inventory")
	if !okP || !okI || p.Generation != 2 || i.Generation != 9 || !c.KnobsOf("payment").ProviderDown {
		t.Fatalf("unexpected %+v %+v", p, i)
	}
}

func Test_shouldFailStartupWhenTheControlTopicCannotBeReached(t *testing.T) {
	// GIVEN: no Kafka at all and a short startup deadline
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	// WHEN: a service starts its control client
	_, err := Start(ctx, Options{Brokers: []string{"127.0.0.1:1"}, Service: "payment", Decoder: nil})

	// THEN: startup fails instead of running with unknown knobs
	if err == nil {
		t.Fatal("expected startup to fail")
	}
}

func Test_shouldFailStartupWhenCatchUpIsInterrupted(t *testing.T) {
	// GIVEN: a control topic with data and a deadline that expires during catch-up
	e := newEnv(t)
	e.publish(t, events.ControlSnapshot{Service: "payment", Generation: 1, IssuedAt: e.clock.Now()})
	ctx, cancel := context.WithCancel(context.Background())
	// Fetches are answered only after the deadline, so catch-up cannot finish.
	e.cluster.ControlKey(int16(kmsg.Fetch), func(kmsg.Request) (kmsg.Response, error, bool) {
		e.cluster.KeepControl()
		cancel()
		return nil, errors.New("broker unreachable"), true // no record ever arrives
	})

	// WHEN: the service starts
	_, err := Start(ctx, Options{Brokers: e.cluster.Brokers, Service: "payment", Decoder: e.serde})

	// THEN: it reports the interrupted catch-up
	if err == nil {
		t.Fatal("expected an error")
	}
}

func Test_shouldRefuseToPublishWhenTheSnapshotHasNoService(t *testing.T) {
	// GIVEN: a snapshot without a target service
	e := newEnv(t)

	// WHEN: the controlboard publishes it
	err := Publish(context.Background(), e.writer, e.serde, &events.ControlSnapshot{Generation: 1})

	// THEN: it is rejected, because nobody would ever apply it
	if err == nil {
		t.Fatal("expected an error")
	}
}

func Test_shouldReportEncodingProblemsWhenPublishingFails(t *testing.T) {
	// GIVEN: an encoder that cannot reach the schema registry
	e := newEnv(t)

	// WHEN: a snapshot is published
	err := Publish(context.Background(), e.writer, failingEncoder{}, &events.ControlSnapshot{Service: "payment"})

	// THEN: the error reaches the caller
	if err == nil || !errors.Is(err, errRegistry) {
		t.Fatalf("expected registry error, got %v", err)
	}
}

var errRegistry = errors.New("registry down")

type failingEncoder struct{}

func (failingEncoder) Encode(any) ([]byte, error) { return nil, errRegistry }

func Test_shouldKeepTheExtendedExperimentActiveWhenTheControlboardPublishesALaterExpiry(t *testing.T) {
	// GIVEN: a Silent Failure that would expire soon
	e := newEnv(t)
	e.clock.Set(time.Now())
	soon := e.clock.Now().Add(50 * time.Millisecond)
	e.publish(t, events.ControlSnapshot{Service: "payment", Generation: 1, IssuedAt: e.clock.Now(), ExpiresAt: &soon, Knobs: events.Knobs{Paused: true}})
	c := e.start(t, "payment", nil)

	// WHEN: the experiment is extended by one hour before it expires
	later := e.clock.Now().Add(time.Hour)
	changed := c.Changed()
	e.publish(t, events.ControlSnapshot{Service: "payment", Generation: 2, IssuedAt: e.clock.Now(), ExpiresAt: &later, Knobs: events.Knobs{Paused: true}})
	waitFor(t, changed)
	time.Sleep(100 * time.Millisecond)
	e.clock.Set(soon.Add(time.Second))

	// THEN: the service stays paused past the original expiry
	if !c.Knobs().Paused {
		t.Fatal("expected the extended experiment to stay active")
	}
}
