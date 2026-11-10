// Package board is the controlboard's core: it composes the experiment engine, the live feed,
// observed metrics, alerts and links into State/Tick, serves the REST API and drives the
// WebSocket hub.
package board

import (
	"context"
	"log/slog"
	"maps"
	"strconv"
	"sync"
	"time"

	"github.com/VictoriaMetrics/metrics"

	"kafkalab/controlboard/backend/internal/api"
	"kafkalab/controlboard/backend/internal/hub"
	"kafkalab/controlboard/backend/internal/observe"
)

// TimelineSize is the number of timeline entries kept in memory.
const TimelineSize = 500

// Feed is the live business feed (implemented by *feed.Feed).
type Feed interface {
	Rates() map[string]float64
	Drain() api.Events
	Order(id string) (api.OrderTimeline, bool)
}

// Observed is the latest observation of metrics and lag (implemented by *observe.Collector).
type Observed interface {
	Snapshot() observe.Snapshot
}

// Alerts is the alert list (implemented by *alerts.Manager).
type Alerts interface {
	Active() []api.Alert
	All() []api.Alert
	Receive(ctx context.Context, list []api.Alert) error
}

// Links lists the tool links (implemented by *links.Checker).
type Links interface {
	Links() []api.Link
}

// Options configures New.
type Options struct {
	Exp      api.Experiments
	Feed     Feed
	Observed Observed
	Alerts   Alerts
	Links    Links
	// ParseWebhook decodes an Alertmanager notification (alerts.ParseWebhook with Grafana links).
	ParseWebhook func(body []byte) ([]api.Alert, error)
	// FireTest fires LabTestAlert through Alertmanager.
	FireTest func(ctx context.Context) error
	// Origins are extra WebSocket origins (e.g. the Vite dev server).
	Origins []string
	Metrics *metrics.Set
	Log     *slog.Logger
	Now     func() time.Time
	// Intervals of the WebSocket messages (contract: 1 s, 250 ms, 10 s).
	TickEvery, EventsEvery, StateEvery time.Duration
}

// Board is the composed controlboard.
type Board struct {
	o   Options
	hub *hub.Hub

	mu       sync.Mutex
	timeline []api.TimelineEntry // newest first
	seq      int
}

// New returns a board; call Run to start the WebSocket loops.
func New(o Options) *Board {
	if o.Metrics == nil {
		o.Metrics = metrics.NewSet()
	}
	if o.Log == nil {
		o.Log = slog.New(slog.DiscardHandler)
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.TickEvery == 0 {
		o.TickEvery = time.Second
	}
	if o.EventsEvery == 0 {
		o.EventsEvery = 250 * time.Millisecond
	}
	if o.StateEvery == 0 {
		o.StateEvery = 10 * time.Second
	}
	b := &Board{o: o}
	b.hub = hub.New(hub.Options{OnConnect: b.greeting, Origins: o.Origins, Metrics: o.Metrics, Log: o.Log, Now: o.Now})
	return b
}

func (b *Board) greeting() []api.Message {
	return []api.Message{
		b.hub.Message(api.MsgHello, api.Hello{Version: 1, ServerTime: b.o.Now().UTC()}),
		b.hub.Message(api.MsgState, b.State()),
	}
}

// Run sends tick, events and state messages and forwards engine changes until ctx ends.
func (b *Board) Run(ctx context.Context) {
	events, cancel := b.o.Exp.Subscribe()
	defer cancel()
	defer b.hub.Close()
	tick, evs, state := time.NewTicker(b.o.TickEvery), time.NewTicker(b.o.EventsEvery), time.NewTicker(b.o.StateEvery)
	defer tick.Stop()
	defer evs.Stop()
	defer state.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			b.hub.Broadcast(api.MsgTick, b.Tick())
		case <-evs.C:
			b.hub.Broadcast(api.MsgEvents, b.o.Feed.Drain())
		case <-state.C:
			b.hub.Broadcast(api.MsgState, b.State())
		case ev, ok := <-events:
			if !ok {
				events = nil // engine closed the subscription; keep serving the rest
				continue
			}
			b.forward(ev)
		}
	}
}

func (b *Board) forward(ev api.Event) {
	switch {
	case ev.Experiment != nil:
		b.hub.Broadcast(api.MsgExperiment, ev.Experiment)
	case ev.Timeline != nil:
		b.AddTimeline(*ev.Timeline)
	case ev.Drill != nil:
		b.hub.Broadcast(api.MsgDrill, ev.Drill)
	}
}

// AlertChanged announces an alert status change to every client.
func (b *Board) AlertChanged(a api.Alert) { b.hub.Broadcast(api.MsgAlert, a) }

// AddTimeline stores an entry (filling id and time when missing) and announces it.
func (b *Board) AddTimeline(e api.TimelineEntry) {
	b.mu.Lock()
	b.seq++
	if e.ID == "" {
		e.ID = "tl-" + strconv.Itoa(b.seq)
	}
	if e.At.IsZero() {
		e.At = b.o.Now().UTC()
	}
	b.timeline = append([]api.TimelineEntry{e}, b.timeline[:min(len(b.timeline), TimelineSize-1)]...)
	b.mu.Unlock()
	b.hub.Broadcast(api.MsgTimeline, e)
}

// Timeline returns up to limit entries, newest first.
func (b *Board) Timeline(limit int) []api.TimelineEntry {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]api.TimelineEntry, min(limit, len(b.timeline)))
	copy(out, b.timeline)
	return out
}

// State is the full board state.
func (b *Board) State() api.State {
	snap := b.o.Observed.Snapshot()
	return api.State{
		Simulation:  b.o.Exp.SimulationState(),
		ChaosArmed:  b.o.Exp.Armed(),
		Services:    b.services(snap),
		Brokers:     snap.Brokers,
		Kpis:        snap.Kpis,
		Experiments: b.o.Exp.List(),
		Alerts:      b.o.Alerts.Active(),
		Links:       b.o.Links.Links(),
		Drill:       b.o.Exp.Drills().Current(),
	}
}

// Tick is the data of the 1 s "tick" message.
func (b *Board) Tick() api.Tick {
	snap := b.o.Observed.Snapshot()
	return api.Tick{Kpis: snap.Kpis, Services: b.services(snap), Brokers: snap.Brokers, Rates: b.o.Feed.Rates(),
		Lag: snap.Lag.ByGroup, Partitions: snap.Lag.Partitions}
}

// services merges the engine's desired control state with what Kubernetes and the pods report.
func (b *Board) services(snap observe.Snapshot) []api.ServiceState {
	controls := b.o.Exp.Controls()
	out := make([]api.ServiceState, 0, len(controls))
	for _, c := range controls {
		o := snap.Services[c.Service]
		s := api.ServiceState{Name: c.Service, Replicas: o.Replicas, ReadyReplicas: o.ReadyReplicas, Paused: c.Paused,
			DesiredGeneration: c.DesiredGeneration, AppliedGeneration: o.AppliedGeneration, Knobs: api.Values{}}
		maps.Copy(s.Knobs, c.Knobs)
		if c.Replicas != nil {
			s.Replicas = *c.Replicas
		}
		switch {
		case !o.Known:
			s.Health = api.HealthUnknown
		case s.Replicas == 0 || s.ReadyReplicas == 0:
			s.Health = api.HealthDown
		case s.ReadyReplicas < s.Replicas || s.Paused:
			s.Health = api.HealthDegraded
		default:
			s.Health = api.HealthUp
		}
		out = append(out, s)
	}
	return out
}

// SetExperiments plugs in the engine. The engine needs Observer() at construction and the
// board needs the engine, so main creates the board first, then the engine, then calls this
// before serving requests.
func (b *Board) SetExperiments(exp api.Experiments) { b.o.Exp = exp }

// Observer returns the board's view for the experiment engine.
func (b *Board) Observer() api.Observer { return observer{b} }

type observer struct{ b *Board }

func (o observer) Kpis() api.Kpis               { return o.b.o.Observed.Snapshot().Kpis }
func (o observer) Brokers() []api.BrokerState   { return o.b.o.Observed.Snapshot().Brokers }
func (o observer) Services() []api.ServiceState { return o.b.services(o.b.o.Observed.Snapshot()) }
func (o observer) Alerts() []api.Alert          { return o.b.o.Alerts.Active() }
func (o observer) TotalLag() int64              { return o.b.o.Observed.Snapshot().Lag.Total }
