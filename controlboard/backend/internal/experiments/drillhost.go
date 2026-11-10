package experiments

import (
	"context"
	"slices"
	"time"

	"kafkalab/controlboard/backend/internal/api"
	"kafkalab/controlboard/backend/internal/drills"
)

// drillHost is the engine as seen by the drills package.
type drillHost Engine

var _ drills.Host = (*drillHost)(nil)

// drillDuration is how long an Incident Drill's fault lasts at most.
const drillDuration = 5 * time.Minute

func (h *drillHost) e() *Engine { return (*Engine)(h) }

func (h *drillHost) available(s *Scenario) bool {
	return s.Drill != "" && !s.Def.Destructive && (!s.Kube || h.cfg.Kube != nil)
}

func (h *drillHost) Pool(difficulty string) []api.ScenarioDef {
	var out []api.ScenarioDef
	for _, s := range h.catalog {
		if s.Drill == difficulty && h.available(s) {
			out = append(out, s.Def)
		}
	}
	return out
}

func (h *drillHost) Scenario(id string) api.ScenarioDef { return h.byID[id].Def }

func (h *drillHost) Candidates() []api.DrillCandidate {
	var out []api.DrillCandidate
	for _, s := range h.catalog {
		if h.available(s) {
			out = append(out, api.DrillCandidate{ID: s.Def.ID, Name: s.Def.Name})
		}
	}
	return out
}

func (h *drillHost) StartHidden(ctx context.Context, scenario, drillID string, no int) (string, error) {
	r, err := h.e().startRun(ctx, h.byID[scenario], api.StartRequest{Scenario: scenario,
		DurationSec: int(drillDuration.Seconds()), AutoRecover: true}, "drill", drillID, no)
	if err != nil {
		return "", err
	}
	return r.rec.Exp.ID, nil
}

func (h *drillHost) Affected(id string) []string {
	r, err := h.e().find(id)
	if err != nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(r.rec.Exp.Affected)
}

// Reveal makes a drill experiment visible. It never takes op: the engine may call it while
// finishing the experiment.
func (h *drillHost) Reveal(ctx context.Context, id string) {
	e := h.e()
	r, err := e.find(id)
	if err != nil {
		return
	}
	e.mu.Lock()
	was := r.rec.Hidden
	r.rec.Hidden = false
	end := time.Time{}
	if r.rec.Exp.EndedAt != nil {
		end = *r.rec.Exp.EndedAt
	}
	e.mu.Unlock()
	if !was {
		return
	}
	if err := e.store.saveRecord(ctx, &r.rec); err != nil {
		e.cfg.Log.Warn("experiment not persisted", "id", id, "error", err)
	}
	e.annotateUpdate(ctx, r, end)
	e.audit(ctx, "drill", "drill.scenario_revealed", id, r.rec.Exp.Scenario, map[string]string{"drill": r.rec.DrillID})
	e.timeline(api.KindDrill, api.SeverityInfo, drillName(r.rec.DrillNo)+" was "+r.rec.Exp.Name, e.timelineDetail(r), id)
	e.changed(r)
}

func (h *drillHost) Stop(ctx context.Context, id string) { _, _ = h.e().Stop(ctx, id) }

func (h *drillHost) Notify(d api.Drill, severity, title, detail string) {
	e := h.e()
	e.emit(api.Event{Drill: &d})
	e.timeline(api.KindDrill, severity, title, detail, "")
}

func (h *drillHost) Audit(ctx context.Context, action string, details map[string]string) {
	h.e().audit(ctx, "drill", action, "", hidden, details)
}
