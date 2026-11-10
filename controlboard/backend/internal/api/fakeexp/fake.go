// Package fakeexp is an in-memory api.Experiments: deterministic, no Kafka, no Kubernetes.
// The board's tests use it, and main.go uses it until the real experiment engine is merged.
package fakeexp

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"sync"
	"time"

	"kafkalab/controlboard/backend/internal/api"
	"kafkalab/pkg/contracts"
)

// Scenario ids of the fake catalog.
const (
	ScenarioTraffic    = "fake-scenario"
	ScenarioBrokerStop = "fake-broker-stop"
)

// Fake implements api.Experiments and api.DrillAPI in memory.
type Fake struct {
	mu       sync.Mutex
	now      func() time.Time
	armed    bool
	sim      string
	seq      int
	exps     []*api.Experiment // newest first
	controls map[string]*api.ServiceControl
	tuning   map[string]api.Values
	drill    *api.Drill
	board    []api.LeaderboardEntry
	player   string
	subs     map[int]chan api.Event
}

// New returns a fake with the simulation running and chaos disarmed. now may be nil.
func New(now func() time.Time) *Fake {
	if now == nil {
		now = time.Now
	}
	f := &Fake{now: func() time.Time { return now().UTC() }, sim: "running",
		controls: map[string]*api.ServiceControl{}, tuning: map[string]api.Values{}, subs: map[int]chan api.Event{}}
	for _, s := range contracts.Services {
		f.controls[s] = &api.ServiceControl{Service: s, Knobs: api.Values{}}
		f.tuning[s] = api.Values{}
	}
	return f
}

var catalog = []api.ScenarioDef{
	{ID: ScenarioTraffic, Name: "Fake Traffic Spike", Description: "Doubles the order rate (fake engine).",
		Category: "traffic", Severity: "low", DefaultDurationSec: 60,
		Params:           []api.ScenarioParam{{Key: "factor", Label: "Factor", Type: "number", Min: ptr(1.0), Max: ptr(10.0), Step: ptr(1.0), Default: 2.0}},
		ExpectedSymptoms: []string{"orders/s rises"}, Dashboards: []api.DashboardLink{}},
	{ID: ScenarioBrokerStop, Name: "Fake Broker Stop", Description: "Pretends to stop a broker (fake engine).",
		Category: "broker", Severity: "high", Destructive: true, DefaultDurationSec: 120,
		Params:           []api.ScenarioParam{{Key: "broker", Label: "Broker", Type: "broker", Options: []string{"0", "1", "2"}, Default: 1.0}},
		ExpectedSymptoms: []string{"under-replicated partitions"}, Dashboards: []api.DashboardLink{}},
}

func ptr[T any](v T) *T { return &v }

// Scenarios implements api.Experiments.
func (f *Fake) Scenarios() []api.ScenarioDef { return slices.Clone(catalog) }

// Start implements api.Experiments.
func (f *Fake) Start(_ context.Context, req api.StartRequest) (api.Experiment, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	i := slices.IndexFunc(catalog, func(s api.ScenarioDef) bool { return s.ID == req.Scenario })
	if i < 0 {
		return api.Experiment{}, fmt.Errorf("%w: %q", api.ErrUnknownScenario, req.Scenario)
	}
	def := catalog[i]
	if def.Destructive && !f.armed {
		return api.Experiment{}, fmt.Errorf("%w: arm chaos first", api.ErrChaosDisarmed)
	}
	if def.Destructive && req.Confirm != def.ID {
		return api.Experiment{}, fmt.Errorf("%w: type %q to confirm", api.ErrConfirmationRequired, def.ID)
	}
	f.refresh()
	if def.Category == "broker" && slices.ContainsFunc(f.exps, func(e *api.Experiment) bool { return e.Scenario == def.ID && active(e) }) {
		return api.Experiment{}, fmt.Errorf("%w: one broker fault at a time", api.ErrBrokerFaultActive)
	}
	now := f.now()
	dur := req.DurationSec
	if dur <= 0 {
		dur = def.DefaultDurationSec
	}
	f.seq++
	e := &api.Experiment{ID: "exp-" + strconv.Itoa(f.seq), Scenario: def.ID, Name: def.Name, Category: def.Category, Severity: def.Severity, Status: api.StatusRunning,
		Params: maps.Clone(req.Params), CreatedAt: now, StartsAt: now.Add(time.Duration(req.StartDelaySec) * time.Second),
		AutoRecover: req.AutoRecover, Affected: []string{}, Observations: []api.Observation{}}
	if e.Params == nil {
		e.Params = api.Values{}
	}
	e.EndsAt = e.StartsAt.Add(time.Duration(dur) * time.Second)
	if req.StartDelaySec > 0 {
		e.Status = api.StatusScheduled
	}
	f.exps = append([]*api.Experiment{e}, f.exps...)
	f.publishExperiment(e)
	f.timeline(api.KindExperiment, api.SeverityWarning, "Experiment started: "+def.Name, e.ID)
	return clone(e), nil
}

// Stop implements api.Experiments.
func (f *Fake) Stop(_ context.Context, id string) (api.Experiment, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.refresh()
	for _, e := range f.exps {
		if e.ID != id {
			continue
		}
		if active(e) {
			f.end(e, api.StatusAborted)
		}
		return clone(e), nil
	}
	return api.Experiment{}, fmt.Errorf("%w: experiment %q", api.ErrNotFound, id)
}

// List implements api.Experiments.
func (f *Fake) List() []api.Experiment {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.refresh()
	out := []api.Experiment{}
	ended := 0
	for _, e := range f.exps {
		if !active(e) {
			if ended == 20 {
				continue
			}
			ended++
		}
		out = append(out, clone(e))
	}
	return out
}

// Arm implements api.Experiments.
func (f *Fake) Arm(_ context.Context, armed bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.armed = armed
	f.timeline(api.KindSystem, api.SeverityInfo, "Chaos armed: "+strconv.FormatBool(armed), "")
	return nil
}

// Armed implements api.Experiments.
func (f *Fake) Armed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.armed
}

// Panic implements api.Experiments.
func (f *Fake) Panic(_ context.Context) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.refresh()
	n := 0
	for _, e := range f.exps {
		if active(e) {
			f.end(e, api.StatusAborted)
			n++
		}
	}
	for _, c := range f.controls {
		if len(c.Knobs) > 0 || c.Replicas != nil {
			c.Knobs, c.Paused, c.Replicas = api.Values{}, false, nil
			c.DesiredGeneration++
			n++
		}
	}
	f.armed = false
	f.timeline(api.KindRecovery, api.SeveritySuccess, "Panic: everything reverted", "")
	return n, nil
}

// Simulation implements api.Experiments.
func (f *Fake) Simulation(_ context.Context, action string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch action {
	case "start", "reset":
		f.sim = "running"
	case "stop":
		f.sim = "stopped"
	default:
		return "", fmt.Errorf("%w: unknown simulation action %q", api.ErrInvalid, action)
	}
	f.timeline(api.KindSimulation, api.SeverityInfo, "Simulation "+action, "")
	return f.sim, nil
}

// SimulationState implements api.Experiments.
func (f *Fake) SimulationState() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sim
}

// Controls implements api.Experiments.
func (f *Fake) Controls() []api.ServiceControl {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]api.ServiceControl, 0, len(contracts.Services))
	for _, s := range contracts.Services {
		c := *f.controls[s]
		c.Knobs = maps.Clone(c.Knobs)
		if c.Replicas != nil {
			c.Replicas = ptr(*c.Replicas)
		}
		out = append(out, c)
	}
	return out
}

// SetKnobs implements api.Experiments.
func (f *Fake) SetKnobs(_ context.Context, service string, knobs api.Values) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.controls[service]
	if !ok {
		return fmt.Errorf("%w: %q", api.ErrUnknownService, service)
	}
	for k, v := range knobs {
		if v == nil {
			delete(c.Knobs, k)
		} else {
			c.Knobs[k] = v
		}
	}
	c.Paused = c.Knobs["paused"] == true
	c.DesiredGeneration++
	f.timeline(api.KindConfig, api.SeverityInfo, "Knobs changed: "+service, "")
	return nil
}

// ResetKnobs implements api.Experiments.
func (f *Fake) ResetKnobs(ctx context.Context, service string) error {
	f.mu.Lock()
	c, ok := f.controls[service]
	var keys api.Values
	if ok {
		keys = api.Values{}
		for k := range c.Knobs {
			keys[k] = nil
		}
	}
	f.mu.Unlock()
	return f.SetKnobs(ctx, service, keys)
}

// Scale implements api.Experiments.
func (f *Fake) Scale(_ context.Context, service string, replicas int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.controls[service]
	if !ok {
		return fmt.Errorf("%w: %q", api.ErrUnknownService, service)
	}
	c.Replicas = ptr(replicas)
	f.timeline(api.KindConfig, api.SeverityInfo, fmt.Sprintf("Scaled %s to %d", service, replicas), "")
	return nil
}

var tuningSettings = []api.TuningSetting{
	{Key: "acks", Label: "acks", Type: "select", Options: []string{"all", "leader", "none"}, Default: "all", Lossy: []string{"leader", "none"}, Help: "Producer acknowledgements."},
	{Key: "lingerMs", Label: "linger.ms", Type: "number", Min: ptr(0.0), Max: ptr(1000.0), Step: ptr(1.0), Default: 5.0, Help: "Producer batching delay."},
	{Key: "batchMaxBytes", Label: "batch.max.bytes", Type: "number", Min: ptr(1024.0), Max: ptr(8388608.0), Step: ptr(1024.0), Default: 1048576.0, Help: "Maximum batch size."},
	{Key: "compression", Label: "compression", Type: "select", Options: []string{"none", "gzip", "snappy", "lz4", "zstd"}, Default: "none", Help: "Batch compression codec."},
	{Key: "fetchMaxBytes", Label: "fetch.max.bytes", Type: "number", Min: ptr(1024.0), Max: ptr(52428800.0), Step: ptr(1024.0), Default: 8388608.0, Help: "Maximum bytes per fetch."},
	{Key: "fetchMaxWaitMs", Label: "fetch.max.wait.ms", Type: "number", Min: ptr(0.0), Max: ptr(5000.0), Step: ptr(10.0), Default: 5000.0, Help: "Maximum broker wait per fetch."},
	{Key: "fetchMinBytes", Label: "fetch.min.bytes", Type: "number", Min: ptr(1.0), Max: ptr(1048576.0), Step: ptr(1.0), Default: 1.0, Help: "Minimum bytes per fetch."},
	{Key: "maxPollRecords", Label: "max.poll.records", Type: "number", Min: ptr(1.0), Max: ptr(10000.0), Step: ptr(1.0), Default: 500.0, Help: "Records per poll."},
}

// Tuning implements api.Experiments.
func (f *Fake) Tuning() api.TuningState {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.tuningState()
}

func (f *Fake) tuningState() api.TuningState {
	t := api.TuningState{Settings: slices.Clone(tuningSettings), Services: map[string]api.Values{}}
	for s, v := range f.tuning {
		t.Services[s] = maps.Clone(v)
	}
	return t
}

// SetTuning implements api.Experiments.
func (f *Fake) SetTuning(_ context.Context, service string, values api.Values) (api.TuningState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	t, ok := f.tuning[service]
	if !ok {
		return api.TuningState{}, fmt.Errorf("%w: %q", api.ErrUnknownService, service)
	}
	for k := range values {
		if !slices.ContainsFunc(tuningSettings, func(s api.TuningSetting) bool { return s.Key == k }) {
			return api.TuningState{}, fmt.Errorf("%w: unknown tuning setting %q", api.ErrInvalid, k)
		}
	}
	for k, v := range values {
		if v == nil {
			delete(t, k)
		} else {
			t[k] = v
		}
	}
	f.timeline(api.KindConfig, api.SeverityInfo, "Tuning changed: "+service, "")
	return f.tuningState(), nil
}

// Drills implements api.Experiments.
func (f *Fake) Drills() api.DrillAPI { return drills{f} }

// Subscribe implements api.Experiments.
func (f *Fake) Subscribe() (<-chan api.Event, func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seq++
	id := f.seq
	ch := make(chan api.Event, 64)
	f.subs[id] = ch
	return ch, func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		if _, ok := f.subs[id]; ok {
			delete(f.subs, id)
			close(ch)
		}
	}
}

func (f *Fake) publish(ev api.Event) {
	for _, ch := range f.subs {
		select {
		case ch <- ev:
		default: // slow subscriber loses the event
		}
	}
}

func (f *Fake) publishExperiment(e *api.Experiment) {
	c := clone(e)
	f.publish(api.Event{Experiment: &c})
}

func (f *Fake) timeline(kind, severity, title, experimentID string) {
	f.publish(api.Event{Timeline: &api.TimelineEntry{At: f.now(), Kind: kind, Severity: severity, Title: title, ExperimentID: experimentID}})
}

// refresh advances scheduled and running experiments whose time has come.
func (f *Fake) refresh() {
	now := f.now()
	for _, e := range f.exps {
		if e.Status == api.StatusScheduled && !now.Before(e.StartsAt) {
			e.Status = api.StatusRunning
			f.publishExperiment(e)
		}
		if e.Status == api.StatusRunning && e.AutoRecover && !now.Before(e.EndsAt) {
			f.end(e, api.StatusCompleted)
		}
	}
}

func (f *Fake) end(e *api.Experiment, status string) {
	at := f.now()
	e.Status, e.EndedAt = status, &at
	e.Report = &api.ExperimentReport{Alerts: []string{}}
	f.publishExperiment(e)
	f.timeline(api.KindRecovery, api.SeveritySuccess, "Experiment "+status+": "+e.Name, e.ID)
}

func active(e *api.Experiment) bool {
	return e.Status == api.StatusScheduled || e.Status == api.StatusRunning || e.Status == api.StatusRecovering
}

func clone(e *api.Experiment) api.Experiment {
	c := *e
	c.Params = maps.Clone(e.Params)
	c.Affected = slices.Clone(e.Affected)
	c.Observations = slices.Clone(e.Observations)
	if e.Report != nil {
		r := *e.Report
		r.Alerts = slices.Clone(r.Alerts)
		c.Report = &r
	}
	return c
}

// drills implements api.DrillAPI on top of the fake.
type drills struct{ f *Fake }

func (d drills) Start(_ context.Context, difficulty, player string) (api.Drill, error) {
	f := d.f
	f.mu.Lock()
	defer f.mu.Unlock()
	if !slices.Contains([]string{"easy", "medium", "hard"}, difficulty) {
		return api.Drill{}, fmt.Errorf("%w: difficulty %q", api.ErrInvalid, difficulty)
	}
	if f.drill != nil && f.drill.Status == "running" {
		return api.Drill{}, fmt.Errorf("%w: finish it first", api.ErrDrillActive)
	}
	f.seq++
	f.drill = &api.Drill{ID: "drill-" + strconv.Itoa(f.seq), Status: "running", Difficulty: difficulty, StartedAt: f.now(),
		Hints: []string{}, Guesses: []api.DrillGuess{},
		Candidates: []api.DrillCandidate{{ID: ScenarioTraffic, Name: catalog[0].Name}, {ID: ScenarioBrokerStop, Name: catalog[1].Name}}}
	f.player = player
	return d.changed(), nil
}

func (d drills) Hint(_ context.Context, id string) (api.Drill, error) {
	return d.update(id, func(dr *api.Drill) {
		dr.HintsUsed++
		dr.Hints = append(dr.Hints, "Hint "+strconv.Itoa(dr.HintsUsed)+": watch the orders/s KPI")
	})
}

func (d drills) Guess(_ context.Context, id, scenario string) (api.Drill, error) {
	return d.update(id, func(dr *api.Drill) {
		correct := scenario == ScenarioTraffic
		dr.Guesses = append(dr.Guesses, api.DrillGuess{Scenario: scenario, Correct: correct, At: d.f.now()})
		if !correct {
			return
		}
		at := d.f.now()
		score := max(100-10*float64(dr.HintsUsed)-5*float64(len(dr.Guesses)-1), 0)
		dr.Status, dr.SolvedAt, dr.Score, dr.Scenario = "solved", &at, &score, ScenarioTraffic
		d.f.board = append(d.f.board, api.LeaderboardEntry{Player: d.f.player, Score: score, Scenario: ScenarioTraffic,
			DurationSec: at.Sub(dr.StartedAt).Seconds(), At: at})
		slices.SortStableFunc(d.f.board, func(a, b api.LeaderboardEntry) int { return cmp.Compare(b.Score, a.Score) })
	})
}

func (d drills) Reveal(_ context.Context, id string) (api.Drill, error) {
	return d.update(id, func(dr *api.Drill) { dr.Status, dr.Scenario = "revealed", ScenarioTraffic })
}

func (d drills) Leaderboard() []api.LeaderboardEntry {
	d.f.mu.Lock()
	defer d.f.mu.Unlock()
	return append([]api.LeaderboardEntry{}, d.f.board...)
}

func (d drills) Current() *api.Drill {
	d.f.mu.Lock()
	defer d.f.mu.Unlock()
	if d.f.drill == nil {
		return nil
	}
	c := cloneDrill(d.f.drill)
	return &c
}

// update applies fn to the running drill with the given id.
func (d drills) update(id string, fn func(*api.Drill)) (api.Drill, error) {
	f := d.f
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.drill == nil || f.drill.ID != id {
		return api.Drill{}, fmt.Errorf("%w: drill %q", api.ErrNotFound, id)
	}
	if f.drill.Status != "running" {
		return api.Drill{}, fmt.Errorf("%w: %s", api.ErrDrillFinished, f.drill.Status)
	}
	fn(f.drill)
	return d.changed(), nil
}

func (d drills) changed() api.Drill {
	c := cloneDrill(d.f.drill)
	d.f.publish(api.Event{Drill: &c})
	return cloneDrill(d.f.drill)
}

func cloneDrill(dr *api.Drill) api.Drill {
	c := *dr
	c.Hints = slices.Clone(dr.Hints)
	c.Guesses = slices.Clone(dr.Guesses)
	c.Candidates = slices.Clone(dr.Candidates)
	return c
}
