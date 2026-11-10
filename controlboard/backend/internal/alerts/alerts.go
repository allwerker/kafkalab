// Package alerts keeps the board's alert list: firing and resolved alerts from the
// Alertmanager webhook (persisted in PostgreSQL for 24 h) merged with pending alerts polled
// from vmalert. Firing via the webhook wins over a pending alert of the same rule.
package alerts

import (
	"cmp"
	"context"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/VictoriaMetrics/metrics"

	"kafkalab/controlboard/backend/internal/api"
	"kafkalab/controlboard/backend/internal/grafana"
)

// Retention of resolved alerts in the history and in State.alerts.
const (
	History        = 24 * time.Hour
	RecentResolved = 15 * time.Minute
)

// Store persists alerts keyed by (fingerprint, startsAt).
type Store interface {
	Upsert(ctx context.Context, a api.Alert, updated time.Time) error
	// Load returns the alerts updated at or after since.
	Load(ctx context.Context, since time.Time) ([]api.Alert, error)
	// Prune deletes alerts last updated before before.
	Prune(ctx context.Context, before time.Time) error
}

// Options configures New.
type Options struct {
	Store   Store
	Grafana grafana.Links
	// OnChange is called (outside the lock) for every alert whose status changed.
	OnChange func(api.Alert)
	// OnTimeline is called for every timeline entry an alert change produces.
	OnTimeline func(api.TimelineEntry)
	Metrics    *metrics.Set
	Log        *slog.Logger
	Now        func() time.Time
}

// Manager holds the current alerts. All methods are safe for concurrent use.
type Manager struct {
	o       Options
	mu      sync.Mutex
	alerts  map[string]api.Alert // webhook alerts by key(fingerprint, startsAt)
	pending map[string]api.Alert // vmalert pending alerts by fingerprint
}

// New returns an empty manager; call Load to restore the history.
func New(o Options) *Manager {
	if o.OnChange == nil {
		o.OnChange = func(api.Alert) {}
	}
	if o.OnTimeline == nil {
		o.OnTimeline = func(api.TimelineEntry) {}
	}
	if o.Metrics == nil {
		o.Metrics = metrics.NewSet()
	}
	if o.Log == nil {
		o.Log = slog.New(slog.DiscardHandler)
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	return &Manager{o: o, alerts: map[string]api.Alert{}, pending: map[string]api.Alert{}}
}

func key(a api.Alert) string { return a.Fingerprint + "|" + a.StartsAt.UTC().Format(time.RFC3339Nano) }

// Load restores the last 24 h from the store.
func (m *Manager) Load(ctx context.Context) error {
	list, err := m.o.Store.Load(ctx, m.o.Now().Add(-History))
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, a := range list {
		m.alerts[key(a)] = a
	}
	return nil
}

// Receive stores alerts from an Alertmanager notification and announces status changes.
func (m *Manager) Receive(ctx context.Context, in []api.Alert) error {
	now := m.o.Now().UTC()
	var changed []api.Alert
	for _, a := range in {
		m.o.Metrics.GetOrCreateCounter(`lab_alerts_received_total{status="` + a.Status + `"}`).Inc()
		if err := m.o.Store.Upsert(ctx, a, now); err != nil {
			return err
		}
		m.mu.Lock()
		prev, seen := m.alerts[key(a)]
		m.alerts[key(a)] = a
		m.mu.Unlock()
		if !seen || prev.Status != a.Status {
			changed = append(changed, a)
		}
	}
	for _, a := range changed {
		m.announce(a)
	}
	return nil
}

// SetPending replaces the pending alerts with the latest vmalert view. A pending alert that
// disappeared without firing is announced as resolved; one that turned into a firing
// webhook alert is dropped silently (the firing alert was announced already).
func (m *Manager) SetPending(list []api.Alert) {
	now := m.o.Now().UTC()
	m.mu.Lock()
	next := map[string]api.Alert{}
	var changed []api.Alert
	for _, p := range list {
		if m.firingLocked(p) {
			continue
		}
		next[p.Fingerprint] = p
		if _, ok := m.pending[p.Fingerprint]; !ok {
			changed = append(changed, p)
		}
	}
	for fp, p := range m.pending {
		if _, ok := next[fp]; ok || m.firingLocked(p) {
			continue
		}
		p.Status, p.EndsAt = api.AlertResolved, &now
		changed = append(changed, p)
	}
	m.pending = next
	m.mu.Unlock()
	for _, a := range changed {
		m.announce(a)
	}
}

// firingLocked reports whether a webhook alert of the same rule fires (labels of p ⊆ labels of it).
func (m *Manager) firingLocked(p api.Alert) bool {
	for _, a := range m.alerts {
		if a.Status != api.AlertFiring || a.Name != p.Name {
			continue
		}
		match := true
		for k, v := range p.Labels {
			if k != "alertgroup" && a.Labels[k] != v {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

func (m *Manager) announce(a api.Alert) {
	m.o.OnChange(a)
	e := api.TimelineEntry{At: m.o.Now().UTC(), Kind: api.KindAlert, Detail: a.Summary}
	switch a.Status {
	case api.AlertFiring:
		e.Title, e.Severity = "Alert firing: "+a.Name, a.Severity
	case api.AlertPending:
		e.Title, e.Severity = "Alert pending: "+a.Name, api.SeverityInfo
	default:
		e.Title, e.Severity = "Alert resolved: "+a.Name, api.SeveritySuccess
	}
	m.o.OnTimeline(e)
}

// Active returns pending and firing alerts plus those resolved in the last 15 minutes.
func (m *Manager) Active() []api.Alert {
	since := m.o.Now().Add(-RecentResolved)
	return m.list(func(a api.Alert) bool {
		return a.Status != api.AlertResolved || a.EndsAt == nil || !a.EndsAt.Before(since)
	})
}

// All returns the 24 h history plus the pending alerts.
func (m *Manager) All() []api.Alert {
	return m.list(func(api.Alert) bool { return true })
}

var rank = map[string]int{api.AlertFiring: 0, api.AlertPending: 1, api.AlertResolved: 2}

func (m *Manager) list(keep func(api.Alert) bool) []api.Alert {
	m.mu.Lock()
	out := []api.Alert{}
	for _, src := range []map[string]api.Alert{m.alerts, m.pending} {
		for _, a := range src {
			if keep(a) {
				a.Labels = maps.Clone(a.Labels)
				out = append(out, a)
			}
		}
	}
	m.mu.Unlock()
	slices.SortFunc(out, func(a, b api.Alert) int {
		return cmp.Or(cmp.Compare(rank[a.Status], rank[b.Status]), b.StartsAt.Compare(a.StartsAt), strings.Compare(a.Fingerprint, b.Fingerprint))
	})
	return out
}

// Prune drops alerts older than the history from memory and the store.
func (m *Manager) Prune(ctx context.Context) error {
	before := m.o.Now().Add(-History)
	m.mu.Lock()
	for k, a := range m.alerts {
		end := a.StartsAt
		if a.EndsAt != nil {
			end = *a.EndsAt
		}
		if a.Status == api.AlertResolved && end.Before(before) {
			delete(m.alerts, k)
		}
	}
	m.mu.Unlock()
	return m.o.Store.Prune(ctx, before)
}

// Run prunes the history every interval until ctx ends.
func (m *Manager) Run(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := m.Prune(ctx); err != nil {
				m.o.Log.WarnContext(ctx, "pruning alert history failed", "error", err)
			}
		}
	}
}

// fromLabels fills the fields every alert source derives from labels and annotations.
func fromLabels(g grafana.Links, labels, annotations map[string]string) api.Alert {
	if labels == nil {
		labels = map[string]string{}
	}
	sev := labels["severity"]
	if sev != api.SeverityCritical && sev != api.SeverityInfo {
		sev = api.SeverityWarning
	}
	return api.Alert{
		Name: labels["alertname"], Severity: sev, Component: labels["component"],
		Summary: annotations["summary"], Description: annotations["description"],
		Labels: labels, DashboardURL: g.Dashboard(labels["dashboard"]), RunbookURL: annotations["runbook_url"],
	}
}
