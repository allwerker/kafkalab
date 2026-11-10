// Package experiments is the controlboard's experiment engine: scenario catalogue, experiment
// lifecycle (scheduled → running → recovering → completed | aborted | failed), the effective
// desired state per service (baseline ⊕ manual overrides ⊕ active experiments ⊕ tuning),
// safety (arming, confirmation, broker-fault lock, timeouts, panic), persistence and
// reconciliation in PostgreSQL, audit events, metrics, observations and reports.
// It implements api.Experiments (see docs/contracts/controlboard-ports.md).
package experiments

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	mrand "math/rand/v2"
	"sync"
	"time"

	"github.com/VictoriaMetrics/metrics"

	"kafkalab/controlboard/backend/internal/api"
	"kafkalab/controlboard/backend/internal/drills"
	"kafkalab/controlboard/backend/internal/k8s"
	"kafkalab/pkg/events"
	"kafkalab/pkg/kit/pg"
)

// Publisher writes to platform.control and platform.audit (KafkaPublisher in production).
type Publisher interface {
	PublishControl(ctx context.Context, snap *events.ControlSnapshot) error
	PublishAudit(ctx context.Context, ev *events.AuditEvent) error
}

// Annotator writes Grafana annotations (GrafanaAnnotator in production).
type Annotator interface {
	// Start creates an annotation at t and returns its id.
	Start(ctx context.Context, t time.Time, text string, tags []string) (int64, error)
	// Update changes an annotation: end time (zero = keep), text and tags.
	Update(ctx context.Context, id int64, end time.Time, text string, tags []string) error
}

// Admin is the Kafka admin part of the simulation reset (KafkaAdmin in production).
type Admin interface {
	// DeleteRecordsToEnd moves the log start offset of every partition to its end offset.
	DeleteRecordsToEnd(ctx context.Context, topics []string) error
	// CommitGroupsToEnd sets the committed offsets of the (empty) groups to the end offsets of
	// every partition they had offsets for.
	CommitGroupsToEnd(ctx context.Context, groups []string) error
}

// Sample is one instant-vector result of a PromQL query.
type Sample struct {
	Labels map[string]string
	Value  float64
}

// Query runs an instant PromQL query (VMQuery in production).
type Query func(ctx context.Context, promql string) ([]Sample, error)

// Config configures New. Only DB, Publisher and Observer are required.
type Config struct {
	// DB is connected with search_path=controlboard; New applies the engine's migrations.
	DB        *pg.DB
	Publisher Publisher
	Observer  api.Observer
	// Generations returns the highest generation on platform.control (KafkaGenerations);
	// nil starts at 0 (tests).
	Generations func(ctx context.Context) (int64, error)
	// Kube is nil when the controlboard runs outside the cluster: Kubernetes and broker
	// scenarios, scaling and reset then fail with ErrUnavailable.
	Kube      *k8s.Client
	Admin     Admin
	Query     Query
	Annotator Annotator
	Metrics   *metrics.Set
	Log       *slog.Logger
	Now       func() time.Time
	// Rand returns values in [0,1) (parameter randomisation, drill pick).
	Rand func() float64

	// GrafanaURL is the public base for dashboard links (default http://grafana.localhost:8080).
	GrafanaURL string
	// ChaosNetImage is the network fault helper (images/chaos-net); empty disables the broker
	// network scenarios.
	ChaosNetImage string
	// HealthURL is the dead-man switch the chaos-net helper polls.
	HealthURL string

	CommerceNamespace  string // default commerce
	KafkaNamespace     string // default kafka
	KafkaCluster       string // default kafka-lab
	BrokerPodPrefix    string // default kafka-lab-dual-role-
	OperatorDeployment string // default strimzi-cluster-operator

	// TickEvery drives the background loop; 0 disables it (tests call Tick).
	TickEvery     time.Duration
	ObserveEvery  time.Duration // default 5 s
	ExpiryMargin  time.Duration // default 30 s, added to the snapshot expiry
	SafetyTimeout time.Duration // default 30 min, hard end for experiments without auto-recover
	// RecoveryTimeout bounds the recovering phase (default 5 min).
	RecoveryTimeout time.Duration
	DiskGuardEvery  time.Duration // default 60 s
	// DiskWarnRatio is the PVC usage that triggers a timeline warning (default 0.7).
	DiskWarnRatio float64
	// ResetPodTimeout bounds waiting for consumer pods to stop during a reset (default 90 s).
	ResetPodTimeout time.Duration
	PollEvery       time.Duration // default 1 s
}

func (c *Config) defaults() {
	def := func(p *string, v string) {
		if *p == "" {
			*p = v
		}
	}
	defD := func(p *time.Duration, v time.Duration) {
		if *p == 0 {
			*p = v
		}
	}
	def(&c.GrafanaURL, "http://grafana.localhost:8080")
	def(&c.HealthURL, "http://controlboard.control:8080/healthz")
	def(&c.CommerceNamespace, "commerce")
	def(&c.KafkaNamespace, "kafka")
	def(&c.KafkaCluster, "kafka-lab")
	def(&c.BrokerPodPrefix, "kafka-lab-dual-role-")
	def(&c.OperatorDeployment, "strimzi-cluster-operator")
	defD(&c.ObserveEvery, 5*time.Second)
	defD(&c.ExpiryMargin, 30*time.Second)
	defD(&c.SafetyTimeout, 30*time.Minute)
	defD(&c.RecoveryTimeout, 5*time.Minute)
	defD(&c.DiskGuardEvery, time.Minute)
	defD(&c.ResetPodTimeout, 90*time.Second)
	defD(&c.PollEvery, time.Second)
	if c.DiskWarnRatio == 0 {
		c.DiskWarnRatio = 0.7
	}
	if c.Metrics == nil {
		c.Metrics = metrics.NewSet()
	}
	if c.Log == nil {
		c.Log = slog.New(slog.DiscardHandler)
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	if c.Rand == nil {
		c.Rand = mrand.Float64
	}
	if c.Generations == nil {
		c.Generations = func(context.Context) (int64, error) { return 0, nil }
	}
}

// Engine implements api.Experiments.
type Engine struct {
	cfg     Config
	store   *store
	catalog []*Scenario
	byID    map[string]*Scenario

	// op serialises every state-changing operation (they do I/O: Kafka, Kubernetes);
	// mu guards the fields below and is never held during I/O.
	op sync.Mutex
	mu sync.Mutex

	settings  settings
	runs      map[string]*run // active and recently ended experiments
	order     []string        // run ids, oldest first
	gen       int64
	published map[string]published
	subs      map[int]chan api.Event
	nextSub   int
	resetting bool
	diskWarn  map[string]bool
	lastDisk  time.Time

	drills *drills.Service
	met    *engineMetrics

	stop chan struct{}
	done chan struct{}
}

type published struct {
	gen  int64
	key  string
	snap events.ControlSnapshot
}

var _ api.Experiments = (*Engine)(nil)

// New migrates the schema, restores the persisted state, reverts every experiment a previous
// controlboard left unfinished, republishes all services and starts the background loop
// (when cfg.TickEvery > 0).
func New(cfg Config) (*Engine, error) {
	if cfg.DB == nil || cfg.Publisher == nil || cfg.Observer == nil {
		return nil, errors.New("experiments: DB, Publisher and Observer are required")
	}
	cfg.defaults()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	st := &store{db: cfg.DB}
	if err := st.migrate(ctx); err != nil {
		return nil, err
	}
	e := &Engine{cfg: cfg, store: st, runs: map[string]*run{}, published: map[string]published{},
		subs: map[int]chan api.Event{}, diskWarn: map[string]bool{}, stop: make(chan struct{}), done: make(chan struct{})}
	e.catalog = catalog(&e.cfg)
	e.byID = map[string]*Scenario{}
	for _, s := range e.catalog {
		e.byID[s.Def.ID] = s
	}
	e.met = newEngineMetrics(cfg.Metrics, e)
	gen, err := cfg.Generations(ctx)
	if err != nil {
		return nil, fmt.Errorf("experiments: read generations from platform.control: %w", err)
	}
	e.gen = gen
	if e.settings, err = st.loadSettings(ctx); err != nil {
		return nil, err
	}
	e.drills, err = drills.New(ctx, drills.Config{DB: cfg.DB, Host: (*drillHost)(e), Now: cfg.Now, Rand: cfg.Rand, Metrics: cfg.Metrics})
	if err != nil {
		return nil, err
	}
	if err := e.reconcile(ctx); err != nil {
		return nil, err
	}
	if cfg.TickEvery > 0 {
		go e.loop()
	} else {
		close(e.done)
	}
	return e, nil
}

// Close stops the background loop. Active experiments keep their (expiring) snapshots and are
// reverted by the next controlboard on startup.
func (e *Engine) Close() {
	select {
	case <-e.stop:
	default:
		close(e.stop)
	}
	<-e.done
}

func (e *Engine) loop() {
	defer close(e.done)
	t := time.NewTicker(e.cfg.TickEvery)
	defer t.Stop()
	for {
		select {
		case <-e.stop:
			return
		case <-t.C:
			e.Tick(context.Background())
		}
	}
}

// Subscribe implements api.Experiments.
func (e *Engine) Subscribe() (<-chan api.Event, func()) {
	e.mu.Lock()
	defer e.mu.Unlock()
	ch := make(chan api.Event, 256)
	id := e.nextSub
	e.nextSub++
	e.subs[id] = ch
	var once sync.Once
	return ch, func() {
		once.Do(func() {
			e.mu.Lock()
			defer e.mu.Unlock()
			delete(e.subs, id)
			close(ch)
		})
	}
}

func (e *Engine) emit(ev api.Event) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.emitLocked(ev)
}

func (e *Engine) emitLocked(ev api.Event) {
	for _, ch := range e.subs {
		select {
		case ch <- ev:
		default: // slow subscriber: drop, never block the engine
		}
	}
}

func (e *Engine) timeline(kind, severity, title, detail, expID string) {
	e.emit(api.Event{Timeline: &api.TimelineEntry{ID: newID("tl"), At: e.cfg.Now().UTC(), Kind: kind,
		Severity: severity, Title: title, Detail: detail, ExperimentID: expID}})
}

func (e *Engine) audit(ctx context.Context, actor, action, expID, scenario string, details map[string]string) {
	ev := &events.AuditEvent{EventID: newID("audit"), At: e.cfg.Now().UTC(), Actor: actor, Action: action, Details: details}
	if ev.Details == nil {
		ev.Details = map[string]string{}
	}
	if expID != "" {
		ev.ExperimentID = &expID
	}
	if scenario != "" {
		ev.Scenario = &scenario
	}
	if err := e.cfg.Publisher.PublishAudit(context.WithoutCancel(ctx), ev); err != nil {
		e.cfg.Log.Warn("audit event not published", "action", action, "error", err)
	}
}

// Scenarios implements api.Experiments.
func (e *Engine) Scenarios() []api.ScenarioDef {
	out := make([]api.ScenarioDef, len(e.catalog))
	for i, s := range e.catalog {
		out[i] = s.Def
	}
	return out
}

// Drills implements api.Experiments.
func (e *Engine) Drills() api.DrillAPI { return e.drills }

func newID(prefix string) string {
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	return prefix + "-" + hex.EncodeToString(b)
}
