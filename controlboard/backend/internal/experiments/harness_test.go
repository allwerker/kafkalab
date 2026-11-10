package experiments

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/metrics"

	"kafkalab/controlboard/backend/internal/api"
	"kafkalab/controlboard/backend/internal/k8s/k8sfake"
	"kafkalab/pkg/contracts"
	"kafkalab/pkg/events"
	"kafkalab/pkg/kit/pg"
	"kafkalab/pkg/kit/testkit"
)

var ctx = context.Background()

type ctxT = context.Context

type fakePublisher struct {
	mu        sync.Mutex
	snaps     []events.ControlSnapshot
	audits    []events.AuditEvent
	failSnap  error
	failAudit error
}

func (p *fakePublisher) PublishControl(_ context.Context, s *events.ControlSnapshot) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failSnap != nil {
		return p.failSnap
	}
	p.snaps = append(p.snaps, *s)
	return nil
}

func (p *fakePublisher) PublishAudit(_ context.Context, a *events.AuditEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failAudit != nil {
		return p.failAudit
	}
	p.audits = append(p.audits, *a)
	return nil
}

type fakeObserver struct {
	mu       sync.Mutex
	kpis     api.Kpis
	brokers  []api.BrokerState
	services []api.ServiceState
	alerts   []api.Alert
	lag      int64
}

func (o *fakeObserver) Kpis() api.Kpis { o.mu.Lock(); defer o.mu.Unlock(); return o.kpis }
func (o *fakeObserver) Brokers() []api.BrokerState {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]api.BrokerState(nil), o.brokers...)
}
func (o *fakeObserver) Services() []api.ServiceState {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]api.ServiceState(nil), o.services...)
}
func (o *fakeObserver) Alerts() []api.Alert {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]api.Alert(nil), o.alerts...)
}
func (o *fakeObserver) TotalLag() int64 { o.mu.Lock(); defer o.mu.Unlock(); return o.lag }

func (o *fakeObserver) set(f func(o *fakeObserver)) { o.mu.Lock(); defer o.mu.Unlock(); f(o) }

// healthy: three brokers up, every service with one ready pod that applied generation gen.
func (o *fakeObserver) healthy(gen int64) {
	o.set(func(o *fakeObserver) {
		o.brokers = []api.BrokerState{{ID: 0, Up: true}, {ID: 1, Up: true}, {ID: 2, Up: true}}
		o.services = nil
		for _, s := range contracts.Services {
			o.services = append(o.services, api.ServiceState{Name: s, Replicas: 1, ReadyReplicas: 1, AppliedGeneration: gen})
		}
	})
}

type fakeAnnotator struct {
	mu      sync.Mutex
	starts  []string
	updates []string
	ends    []time.Time
	fail    error
}

func (a *fakeAnnotator) Start(_ context.Context, _ time.Time, text string, tags []string) (int64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.fail != nil {
		return 0, a.fail
	}
	a.starts = append(a.starts, text+" ["+strings.Join(tags, ",")+"]")
	return int64(len(a.starts)), nil
}

func (a *fakeAnnotator) Update(_ context.Context, _ int64, end time.Time, text string, tags []string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.fail != nil {
		return a.fail
	}
	a.updates = append(a.updates, text+" ["+strings.Join(tags, ",")+"]")
	a.ends = append(a.ends, end)
	return nil
}

type fakeAdmin struct {
	mu        sync.Mutex
	deleted   []string
	committed []string
	failDel   error
	failCom   error
}

func (a *fakeAdmin) DeleteRecordsToEnd(_ context.Context, topics []string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.deleted = append(a.deleted, topics...)
	return a.failDel
}

func (a *fakeAdmin) CommitGroupsToEnd(_ context.Context, groups []string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.committed = append(a.committed, groups...)
	return a.failCom
}

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) add(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

type harness struct {
	t      *testing.T
	e      *Engine
	cfg    Config
	db     *pg.DB
	pub    *fakePublisher
	obs    *fakeObserver
	ann    *fakeAnnotator
	admin  *fakeAdmin
	kube   *k8sfake.Server
	clock  *clock
	set    *metrics.Set
	mu     sync.Mutex
	events []api.Event
	cancel func()
}

func openDB(t *testing.T) *pg.DB {
	t.Helper()
	db, err := pg.Open(ctx, pg.Options{URL: testkit.PostgresDSN(t)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	return db
}

// newHarness starts an engine with fakes around it: an in-memory Kubernetes API with the lab's
// deployments and broker pods, a recording Kafka publisher and a scriptable observer.
func newHarness(t *testing.T, opts ...func(*Config)) *harness {
	t.Helper()
	h := &harness{t: t, db: openDB(t), pub: &fakePublisher{}, obs: &fakeObserver{}, ann: &fakeAnnotator{}, admin: &fakeAdmin{},
		kube: k8sfake.New(t), clock: &clock{t: time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)}, set: metrics.NewSet()}
	h.obs.healthy(0)
	for _, s := range contracts.Services {
		h.kube.AddDeployment("commerce", s, 1, map[string]string{"app.kubernetes.io/part-of": "kafka-lab"})
		h.kube.AddPod("commerce", s+"-pod", map[string]string{"app.kubernetes.io/name": s})
	}
	h.kube.AddDeployment("kafka", "strimzi-cluster-operator", 1, nil)
	h.kube.AddKafka("kafka", "kafka-lab")
	for i := range 3 {
		name := "kafka-lab-dual-role-" + string(rune('0'+i))
		h.kube.AddPod("kafka", name, map[string]string{"strimzi.io/cluster": "kafka-lab", "strimzi.io/pod-name": name})
	}
	h.cfg = Config{DB: h.db, Publisher: h.pub, Observer: h.obs, Kube: h.kube.Client(), Admin: h.admin, Annotator: h.ann,
		Metrics: h.set, Now: h.clock.Now, Rand: func() float64 { return 0.5 }, ChaosNetImage: "chaos-net:test",
		ResetPodTimeout: 3 * time.Second, PollEvery: 10 * time.Millisecond}
	for _, o := range opts {
		o(&h.cfg)
	}
	h.restart()
	return h
}

// restart starts a (new) engine on the same database, as after a controlboard restart.
func (h *harness) restart() {
	h.t.Helper()
	if h.e != nil {
		h.cancel()
		h.e.Close()
	}
	h.cfg.Metrics = metrics.NewSet()
	h.set = h.cfg.Metrics
	e, err := New(h.cfg)
	if err != nil {
		h.t.Fatal(err)
	}
	h.e = e
	ch, cancel := e.Subscribe()
	h.cancel = cancel
	go func() {
		for ev := range ch {
			h.mu.Lock()
			h.events = append(h.events, ev)
			h.mu.Unlock()
		}
	}()
	h.t.Cleanup(func() { cancel(); e.Close() })
}

func (h *harness) start(req api.StartRequest) api.Experiment {
	h.t.Helper()
	x, err := h.e.Start(ctx, req)
	if err != nil {
		h.t.Fatalf("start %s: %v", req.Scenario, err)
	}
	return x
}

// advance moves the clock and runs one engine tick.
func (h *harness) advance(d time.Duration) {
	h.clock.add(d)
	h.e.Tick(ctx)
}

// last returns the newest snapshot published for a service.
func (h *harness) last(svc string) events.ControlSnapshot {
	h.t.Helper()
	h.pub.mu.Lock()
	defer h.pub.mu.Unlock()
	for i := len(h.pub.snaps) - 1; i >= 0; i-- {
		if h.pub.snaps[i].Service == svc {
			return h.pub.snaps[i]
		}
	}
	h.t.Fatalf("no snapshot for %s", svc)
	return events.ControlSnapshot{}
}

func (h *harness) snapCount() int {
	h.pub.mu.Lock()
	defer h.pub.mu.Unlock()
	return len(h.pub.snaps)
}

func (h *harness) auditActions() []string {
	h.pub.mu.Lock()
	defer h.pub.mu.Unlock()
	var out []string
	for _, a := range h.pub.audits {
		out = append(out, a.Action)
	}
	return out
}

func (h *harness) experiment(id string) api.Experiment {
	h.t.Helper()
	for _, x := range h.e.List() {
		if x.ID == id {
			return x
		}
	}
	h.t.Fatalf("experiment %s not listed", id)
	return api.Experiment{}
}

// timeline returns the timeline entries emitted so far (waits briefly for the subscriber).
func (h *harness) timeline() []api.TimelineEntry {
	time.Sleep(20 * time.Millisecond)
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []api.TimelineEntry
	for _, ev := range h.events {
		if ev.Timeline != nil {
			out = append(out, *ev.Timeline)
		}
	}
	return out
}

func (h *harness) exposition() string {
	var b bytes.Buffer
	h.set.WritePrometheus(&b)
	return b.String()
}

// appliedAll lets every service report the newest generation it was sent.
func (h *harness) appliedAll() {
	h.obs.set(func(o *fakeObserver) {
		for i := range o.services {
			o.services[i].AppliedGeneration = 1 << 40
		}
	})
}

func contains(xs []string, x string) bool {
	for _, y := range xs {
		if y == x {
			return true
		}
	}
	return false
}

var errBoom = errors.New("boom")
