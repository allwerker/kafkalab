package experiments

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"time"

	"kafkalab/controlboard/backend/internal/api"
	"kafkalab/controlboard/backend/internal/tuning"
	"kafkalab/pkg/contracts"
	"kafkalab/pkg/events"
)

// The desired state of a service is always computed from scratch, never restored:
//
//	knobs = baseline (neutral defaults)
//	      ← manual overrides (SetKnobs, replace)
//	      ⊕ overlays of every running experiment (merge: max / OR, see mergeKnobs)
//	      ← simulation stopped (order-generator ordersPerSecond = 0)
//	knobs.client = Tuning Lab settings
//
// expiresAt = latest deadline of the contributing experiments + ExpiryMargin, or none when no
// experiment contributes (manual overrides and tuning never expire).

// affectedServices returns the services the plan's overlay touches at a moment of the run.
func (p Plan) affectedServices(elapsed time.Duration) []string {
	if p.Overlay == nil {
		return nil
	}
	return sortedKeys(p.Overlay(elapsed))
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// desiredLocked computes the full snapshot content of one service (mu held).
func (e *Engine) desiredLocked(svc string, now time.Time) (events.Knobs, []string, *time.Time) {
	var k events.Knobs
	assignKnobs(&k, e.settings.Manual[svc])
	var exps []string
	var expires *time.Time
	for _, id := range e.order {
		r := e.runs[id]
		if !r.injecting() || r.plan.Overlay == nil {
			continue
		}
		o, ok := r.plan.Overlay(now.Sub(*r.rec.StartedAt))[svc]
		if !ok {
			continue
		}
		mergeKnobs(&k, o)
		exps = append(exps, r.rec.Exp.ID)
		if end := r.rec.Deadline.Add(e.cfg.ExpiryMargin); expires == nil || end.After(*expires) {
			expires = &end
		}
	}
	if svc == contracts.ServiceOrderGenerator && e.settings.Simulation == "stopped" {
		zero := 0.0
		k.OrdersPerSecond = &zero
	}
	k.Client = tuning.ToClient(e.settings.Tuning[svc])
	return k, exps, expires
}

// publishFor publishes a full snapshot for each service whose desired state changed (or all
// given services with force). Generations increase monotonically across all services.
// Called with op held.
func (e *Engine) publishFor(ctx context.Context, services []string, force bool) error {
	now := e.cfg.Now().UTC()
	for _, svc := range services {
		e.mu.Lock()
		k, exps, expires := e.desiredLocked(svc, now)
		snap := events.ControlSnapshot{Service: svc, IssuedAt: now, ExpiresAt: expires, Experiments: exps, Knobs: k}
		if snap.Experiments == nil {
			snap.Experiments = []string{}
		}
		key := snapshotKey(snap)
		prev, seen := e.published[svc]
		if seen && prev.key == key && !force {
			e.mu.Unlock()
			continue
		}
		e.gen++
		snap.Generation = e.gen
		e.mu.Unlock()
		if err := e.cfg.Publisher.PublishControl(ctx, &snap); err != nil {
			return fmt.Errorf("%w: publish control snapshot for %s: %v", api.ErrUnavailable, svc, err)
		}
		e.mu.Lock()
		e.published[svc] = published{gen: snap.Generation, key: key, snap: snap}
		e.mu.Unlock()
	}
	return nil
}

func snapshotKey(s events.ControlSnapshot) string {
	b, _ := json.Marshal(struct {
		E []string
		X *time.Time
		K events.Knobs
	}{s.Experiments, s.ExpiresAt, s.Knobs})
	return string(b)
}

// Controls implements api.Experiments.
func (e *Engine) Controls() []api.ServiceControl {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := e.cfg.Now()
	out := make([]api.ServiceControl, 0, len(contracts.Services))
	for _, svc := range contracts.Services {
		k, _, _ := e.desiredLocked(svc, now)
		c := api.ServiceControl{Service: svc, Paused: k.Paused, DesiredGeneration: e.published[svc].gen, Knobs: nonDefaultKnobs(k)}
		if n, ok := e.settings.Desired[svc]; ok {
			c.Replicas = &n
		}
		out = append(out, c)
	}
	return out
}

func validService(svc string) error {
	if !slices.Contains(contracts.Services, svc) {
		return fmt.Errorf("%w: %q", api.ErrUnknownService, svc)
	}
	return nil
}
