package experiments

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"kafkalab/controlboard/backend/internal/api"
)

// record is the persisted form of an experiment (doc column).
type record struct {
	// Exp is the real, unredacted experiment.
	Exp api.Experiment `json:"exp"`
	// Revert holds each applied action's revert data by action index.
	Revert map[string]json.RawMessage `json:"revert,omitempty"`
	// Drill fields: an unsolved drill experiment is shown redacted everywhere.
	Hidden  bool   `json:"hidden,omitempty"`
	DrillID string `json:"drillId,omitempty"`
	DrillNo int    `json:"drillNo,omitempty"`
	// Annotation is the Grafana annotation id.
	Annotation int64 `json:"annotation,omitempty"`
	// Deadline is when the engine reverts at the latest (EndsAt with auto-recover, else the safety timeout).
	Deadline   time.Time  `json:"deadline"`
	StartedAt  *time.Time `json:"startedAt,omitempty"`
	RevertedAt *time.Time `json:"revertedAt,omitempty"`
	// Outcome is the final status decided when the experiment was reverted.
	Outcome string `json:"outcome,omitempty"`
	Reason  string `json:"reason,omitempty"`
	// RevertGen is the generation published per affected service when reverting.
	RevertGen map[string]int64 `json:"revertGen,omitempty"`
	Report    reportState      `json:"report"`
}

// run is an experiment in memory.
type run struct {
	rec      record
	scenario *Scenario
	plan     Plan
	env      *Env
	step     int64
	observed time.Time
	// metricActive is the registered lab_experiment_active series (labels fixed at start).
	metricActive string
}

func (r *run) active() bool {
	switch r.rec.Exp.Status {
	case api.StatusScheduled, api.StatusRunning, api.StatusRecovering:
		return true
	}
	return false
}

// injecting reports whether the experiment's faults are currently in force.
func (r *run) injecting() bool { return r.rec.Exp.Status == api.StatusRunning }

const (
	maxDuration   = 30 * time.Minute
	maxStartDelay = time.Hour
	keepEnded     = 20
)

// Start implements api.Experiments.
func (e *Engine) Start(ctx context.Context, req api.StartRequest) (api.Experiment, error) {
	sc, ok := e.byID[req.Scenario]
	if !ok {
		return api.Experiment{}, fmt.Errorf("%w: %q", api.ErrUnknownScenario, req.Scenario)
	}
	e.mu.Lock()
	armed := e.settings.Armed
	e.mu.Unlock()
	if sc.Def.Destructive {
		if !armed {
			return api.Experiment{}, fmt.Errorf("%w: arm chaos before starting %s", api.ErrChaosDisarmed, sc.Def.Name)
		}
		if req.Confirm != sc.Def.ID {
			return api.Experiment{}, fmt.Errorf("%w: type %q to confirm %s", api.ErrConfirmationRequired, sc.Def.ID, sc.Def.Name)
		}
	}
	r, err := e.startRun(ctx, sc, req, "user", "", 0)
	if err != nil {
		return api.Experiment{}, err
	}
	return e.view(r), nil
}

// startRun validates, plans and schedules or activates an experiment. drillID/drillNo mark
// a hidden Incident Drill experiment.
func (e *Engine) startRun(ctx context.Context, sc *Scenario, req api.StartRequest, actor, drillID string, drillNo int) (*run, error) {
	dur := time.Duration(req.DurationSec) * time.Second
	if req.DurationSec == 0 {
		dur = time.Duration(sc.Def.DefaultDurationSec) * time.Second
	}
	delay := time.Duration(req.StartDelaySec) * time.Second
	if dur < 10*time.Second || dur > maxDuration {
		return nil, fmt.Errorf("%w: duration must be 10..%d s", api.ErrInvalid, int(maxDuration.Seconds()))
	}
	if delay < 0 || delay > maxStartDelay {
		return nil, fmt.Errorf("%w: start delay must be 0..%d s", api.ErrInvalid, int(maxStartDelay.Seconds()))
	}
	params, err := resolveParams(sc.Def, req.Params, e.cfg.Rand)
	if err != nil {
		return nil, err
	}
	if sc.Kube && e.cfg.Kube == nil {
		return nil, fmt.Errorf("%w: %s needs the Kubernetes API (controlboard runs outside the cluster)", api.ErrUnavailable, sc.Def.Name)
	}
	plan, err := sc.Plan(PlanInput{Params: params, Duration: dur, Cfg: &e.cfg})
	if err != nil {
		return nil, err
	}
	var applied []Check
	for _, svc := range plan.affectedServices(0) {
		applied = append(applied, appliedCheck(e, svc))
	}
	plan.Checks = append(applied, plan.Checks...)
	e.op.Lock()
	defer e.op.Unlock()
	if plan.Broker {
		e.mu.Lock()
		for _, other := range e.runs {
			if other.active() && other.plan.Broker {
				e.mu.Unlock()
				return nil, fmt.Errorf("%w: %s is still affecting the brokers", api.ErrBrokerFaultActive, e.displayName(other))
			}
		}
		e.mu.Unlock()
	}
	now := e.cfg.Now().UTC()
	id := newID("exp")
	r := &run{scenario: sc, plan: plan, env: e.env(id)}
	r.rec = record{
		Exp: api.Experiment{ID: id, Scenario: sc.Def.ID, Name: sc.Def.Name, Status: api.StatusScheduled, Params: params,
			CreatedAt: now, StartsAt: now.Add(delay), EndsAt: now.Add(delay + dur), AutoRecover: req.AutoRecover,
			Affected: slices.Clone(plan.Affected), Observations: []api.Observation{}},
		Hidden: drillID != "", DrillID: drillID, DrillNo: drillNo,
	}
	if drillID != "" {
		r.rec.Exp.Name = sc.Def.Name // real name stays in the record, redacted on the way out
	}
	r.rec.Deadline = r.rec.Exp.EndsAt
	if !req.AutoRecover {
		r.rec.Deadline = r.rec.Exp.StartsAt.Add(e.cfg.SafetyTimeout)
	}
	e.mu.Lock()
	e.runs[id] = r
	e.order = append(e.order, id)
	e.mu.Unlock()
	if delay > 0 {
		if err := e.store.saveRecord(ctx, &r.rec); err != nil {
			e.forget(id)
			return nil, err
		}
		e.audit(ctx, actor, "experiment.scheduled", id, e.auditScenario(r), e.auditDetails(r))
		e.timeline(api.KindExperiment, api.SeverityInfo, e.displayName(r)+" scheduled",
			"starts "+r.rec.Exp.StartsAt.Format(time.TimeOnly)+" UTC", id)
		e.changed(r)
		return r, nil
	}
	if err := e.activate(ctx, r, actor); err != nil {
		return r, err
	}
	return r, nil
}

func (e *Engine) forget(id string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.runs, id)
	e.order = slices.DeleteFunc(e.order, func(x string) bool { return x == id })
}

func (e *Engine) env(id string) *Env {
	return &Env{Kube: e.cfg.Kube, Cfg: &e.cfg, ExperimentID: id, Now: e.cfg.Now}
}

// activate applies the plan (called with op held). On failure the experiment ends as failed
// and everything applied so far is reverted.
func (e *Engine) activate(ctx context.Context, r *run, actor string) error {
	now := e.cfg.Now().UTC()
	e.mu.Lock()
	r.rec.Exp.Status = api.StatusRunning
	r.rec.StartedAt = &now
	r.rec.Report.start(e.cfg.Observer, now)
	e.mu.Unlock()
	r.rec.Revert = map[string]json.RawMessage{}
	for i, a := range r.plan.Actions {
		data, err := a.Apply(ctx, r.env)
		if err != nil {
			e.fail(ctx, r, fmt.Errorf("apply step %d: %w", i+1, err))
			return fmt.Errorf("%w: %s could not be started: %v", api.ErrUnavailable, e.displayName(r), err)
		}
		r.rec.Revert[strconv.Itoa(i)] = data
	}
	if err := e.publishFor(ctx, r.plan.affectedServices(0), false); err != nil {
		e.fail(ctx, r, err)
		return err
	}
	e.annotateStart(ctx, r)
	e.met.started(r)
	if err := e.store.saveRecord(ctx, &r.rec); err != nil {
		e.cfg.Log.Warn("experiment not persisted", "id", r.rec.Exp.ID, "error", err)
	}
	e.audit(ctx, actor, "experiment.started", r.rec.Exp.ID, e.auditScenario(r), e.auditDetails(r))
	e.timeline(api.KindExperiment, severityOf(r), e.displayName(r)+" started", e.timelineDetail(r), r.rec.Exp.ID)
	e.changed(r)
	return nil
}

// fail ends an experiment whose plan could not be applied.
func (e *Engine) fail(ctx context.Context, r *run, cause error) {
	r.rec.Reason = cause.Error()
	e.revert(ctx, r, api.StatusFailed, "scheduler")
	e.finish(ctx, r)
}

// Stop implements api.Experiments.
func (e *Engine) Stop(ctx context.Context, id string) (api.Experiment, error) {
	e.op.Lock()
	defer e.op.Unlock()
	r, err := e.find(id)
	if err != nil {
		return api.Experiment{}, err
	}
	e.stopLocked(ctx, r, "user", "stopped by user")
	return e.view(r), nil
}

// stopLocked ends an experiment early: it is reverted and immediately "aborted" (the board
// shows a stopped experiment as aborted, without a recovering phase). A recovering one is
// finished with the outcome decided at its revert.
func (e *Engine) stopLocked(ctx context.Context, r *run, actor, reason string) {
	switch r.rec.Exp.Status {
	case api.StatusScheduled:
		r.rec.Reason = reason
		r.rec.Outcome = api.StatusAborted
		e.finish(ctx, r)
	case api.StatusRunning:
		r.rec.Reason = reason
		e.revert(ctx, r, api.StatusAborted, actor)
		e.finish(ctx, r)
	case api.StatusRecovering:
		e.finish(ctx, r)
	}
}

func (e *Engine) find(id string) (*run, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	r, ok := e.runs[id]
	if !ok {
		return nil, fmt.Errorf("%w: experiment %s", api.ErrNotFound, id)
	}
	return r, nil
}

// revert ends the fault: actions are undone in reverse order and every affected service gets
// a freshly computed snapshot without this experiment. The experiment then recovers.
func (e *Engine) revert(ctx context.Context, r *run, outcome, actor string) {
	now := e.cfg.Now().UTC()
	e.mu.Lock()
	wasRunning := r.rec.Exp.Status == api.StatusRunning
	r.rec.Exp.Status = api.StatusRecovering
	r.rec.RevertedAt = &now
	r.rec.Outcome = outcome
	e.mu.Unlock()
	var errs []error
	for i := len(r.plan.Actions) - 1; i >= 0; i-- {
		data, applied := r.rec.Revert[strconv.Itoa(i)]
		if !applied {
			continue
		}
		if err := r.plan.Actions[i].Revert(ctx, r.env, data); err != nil {
			errs = append(errs, err)
		}
	}
	services := r.plan.affectedServices(time.Duration(r.step) * r.plan.StepEvery)
	if err := e.publishFor(ctx, services, false); err != nil {
		errs = append(errs, err)
	}
	e.mu.Lock()
	r.rec.RevertGen = map[string]int64{}
	for _, s := range services {
		r.rec.RevertGen[s] = e.published[s].gen
	}
	e.mu.Unlock()
	if len(errs) > 0 {
		r.rec.Reason = joinReason(r.rec.Reason, errors.Join(errs...).Error())
		e.timeline(api.KindRecovery, api.SeverityCritical, e.displayName(r)+": revert incomplete", errors.Join(errs...).Error(), r.rec.Exp.ID)
		e.cfg.Log.Error("experiment revert incomplete", "id", r.rec.Exp.ID, "error", errors.Join(errs...))
	}
	if wasRunning {
		e.met.reverted(r)
	}
	if err := e.store.saveRecord(ctx, &r.rec); err != nil {
		e.cfg.Log.Warn("experiment not persisted", "id", r.rec.Exp.ID, "error", err)
	}
	e.audit(ctx, actor, "experiment.reverted", r.rec.Exp.ID, e.auditScenario(r), map[string]string{"outcome": outcome})
	if outcome != api.StatusFailed {
		e.timeline(api.KindRecovery, api.SeverityInfo, e.displayName(r)+" reverted, recovering", r.rec.Reason, r.rec.Exp.ID)
	}
	e.changed(r)
}

func joinReason(a, b string) string {
	if a == "" {
		return b
	}
	return a + "; " + b
}

// finish moves a recovering (or scheduled) experiment to its final status.
func (e *Engine) finish(ctx context.Context, r *run) {
	now := e.cfg.Now().UTC()
	e.mu.Lock()
	r.rec.Exp.Status = r.rec.Outcome
	r.rec.Exp.EndedAt = &now
	r.rec.Exp.Report = r.report()
	e.trimLocked()
	e.mu.Unlock()
	e.met.finished(r)
	e.annotateEnd(ctx, r)
	if err := e.store.saveRecord(ctx, &r.rec); err != nil {
		e.cfg.Log.Warn("experiment not persisted", "id", r.rec.Exp.ID, "error", err)
	}
	e.audit(ctx, "scheduler", "experiment."+r.rec.Exp.Status, r.rec.Exp.ID, e.auditScenario(r), map[string]string{"reason": e.auditReason(r)})
	sev := map[string]string{api.StatusCompleted: api.SeveritySuccess, api.StatusAborted: api.SeverityWarning, api.StatusFailed: api.SeverityCritical}[r.rec.Exp.Status]
	e.timeline(api.KindExperiment, sev, e.displayName(r)+" "+r.rec.Exp.Status, e.auditReason(r), r.rec.Exp.ID)
	e.changed(r)
	if r.rec.DrillID != "" {
		e.drills.ExperimentEnded(ctx, r.rec.Exp.ID)
	}
}

// trimLocked keeps the active experiments plus the newest keepEnded ended ones in memory.
func (e *Engine) trimLocked() {
	ended := 0
	for i := len(e.order) - 1; i >= 0; i-- {
		id := e.order[i]
		if e.runs[id].active() {
			continue
		}
		ended++
		if ended > keepEnded {
			delete(e.runs, id)
			e.order = slices.Delete(e.order, i, i+1)
		}
	}
}

// List implements api.Experiments: active plus the last 20 ended, newest first.
func (e *Engine) List() []api.Experiment {
	e.mu.Lock()
	ids := slices.Clone(e.order)
	e.mu.Unlock()
	out := make([]api.Experiment, 0, len(ids))
	for i := len(ids) - 1; i >= 0; i-- {
		if r, err := e.find(ids[i]); err == nil {
			out = append(out, e.view(r))
		}
	}
	return out
}

// changed notifies subscribers about an experiment change.
func (e *Engine) changed(r *run) {
	x := e.view(r)
	e.emit(api.Event{Experiment: &x})
}

// Tick advances every experiment: scheduled ones start, running ones tick their actions,
// step their overlays and end at their deadline, recovering ones finish once recovered.
// The background loop calls it every TickEvery; tests call it directly.
func (e *Engine) Tick(ctx context.Context) {
	e.op.Lock()
	defer e.op.Unlock()
	now := e.cfg.Now()
	e.mu.Lock()
	var runs []*run
	for _, id := range e.order {
		if r := e.runs[id]; r.active() {
			runs = append(runs, r)
		}
	}
	e.mu.Unlock()
	for _, r := range runs {
		switch r.rec.Exp.Status {
		case api.StatusScheduled:
			if !now.Before(r.rec.Exp.StartsAt) {
				_ = e.activate(ctx, r, "scheduler") // a failure is recorded on the experiment
			}
		case api.StatusRunning:
			e.tickRunning(ctx, r, now)
		case api.StatusRecovering:
			e.tickRecovering(ctx, r, now)
		}
	}
	e.diskGuard(ctx, now)
}

func (e *Engine) tickRunning(ctx context.Context, r *run, now time.Time) {
	elapsed := now.Sub(*r.rec.StartedAt)
	for _, a := range r.plan.Actions {
		if err := a.Tick(ctx, r.env, elapsed); err != nil {
			e.cfg.Log.Warn("experiment action tick failed", "id", r.rec.Exp.ID, "error", err)
		}
	}
	if r.plan.StepEvery > 0 {
		if step := int64(elapsed / r.plan.StepEvery); step != r.step {
			r.step = step
			if err := e.publishFor(ctx, r.plan.affectedServices(elapsed), false); err != nil {
				e.cfg.Log.Warn("experiment step not published", "id", r.rec.Exp.ID, "error", err)
			}
			e.met.intensity(r, elapsed)
		}
	}
	e.observe(ctx, r, now, r.plan.Checks)
	if !now.Before(r.rec.Deadline) {
		outcome := api.StatusCompleted
		if !r.rec.Exp.AutoRecover {
			outcome = api.StatusAborted
			r.rec.Reason = "safety timeout reached"
		}
		e.revert(ctx, r, outcome, "scheduler")
	}
}

func (e *Engine) tickRecovering(ctx context.Context, r *run, now time.Time) {
	checks := append(r.recoveryChecks(e), r.plan.RecoveryChecks...)
	recovered := e.observe(ctx, r, now, checks)
	if recovered {
		e.mu.Lock()
		d := now.Sub(*r.rec.RevertedAt).Seconds()
		r.rec.Report.TimeToRecoverSec = &d
		e.mu.Unlock()
	}
	if recovered || now.Sub(*r.rec.RevertedAt) >= e.cfg.RecoveryTimeout {
		e.finish(ctx, r)
	}
}

// recoveryChecks are the implicit recovery conditions of every experiment: each affected
// service runs the reverted snapshot and the consumer lag is back near its starting level.
func (r *run) recoveryChecks(e *Engine) []Check {
	var out []Check
	for _, svc := range sortedKeys(r.rec.RevertGen) {
		out = append(out, generationCheck(svc, r.rec.RevertGen[svc]))
	}
	return append(out, lagRecoveredCheck())
}
