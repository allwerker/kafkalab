package experiments

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"kafkalab/controlboard/backend/internal/api"
	"kafkalab/controlboard/backend/internal/k8s"
	"kafkalab/controlboard/backend/internal/tuning"
	"kafkalab/pkg/contracts"
)

// Armed implements api.Experiments.
func (e *Engine) Armed() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.settings.Armed
}

// Arm implements api.Experiments. The state is persisted: chaos stays disarmed after a restart
// unless it was armed before.
func (e *Engine) Arm(ctx context.Context, armed bool) error {
	e.op.Lock()
	defer e.op.Unlock()
	if err := e.saveSettings(ctx, func(s *settings) { s.Armed = armed }); err != nil {
		return err
	}
	action, title, sev := "chaos.disarmed", "Chaos disarmed", api.SeverityInfo
	if armed {
		action, title, sev = "chaos.armed", "Chaos armed: destructive scenarios enabled", api.SeverityWarning
	}
	e.audit(ctx, "user", action, "", "", nil)
	e.timeline(api.KindSystem, sev, title, "", "")
	return nil
}

// saveSettings applies change to a copy, persists it, then commits it in memory.
func (e *Engine) saveSettings(ctx context.Context, change func(*settings)) error {
	e.mu.Lock()
	next := cloneSettings(e.settings)
	e.mu.Unlock()
	change(&next)
	if err := e.store.saveSettings(ctx, next); err != nil {
		return err
	}
	e.mu.Lock()
	e.settings = next
	e.mu.Unlock()
	return nil
}

func cloneSettings(s settings) settings {
	b, _ := json.Marshal(s)
	var out settings
	_ = json.Unmarshal(b, &out)
	out.init()
	return out
}

// SetKnobs implements api.Experiments.
func (e *Engine) SetKnobs(ctx context.Context, service string, knobs api.Values) error {
	if err := validService(service); err != nil {
		return err
	}
	if err := validateKnobs(knobs); err != nil {
		return err
	}
	e.op.Lock()
	defer e.op.Unlock()
	err := e.saveSettings(ctx, func(s *settings) {
		m := s.Manual[service]
		if m == nil {
			m = api.Values{}
		}
		for k, v := range knobs {
			if v == nil {
				delete(m, k)
			} else {
				m[k] = v
			}
		}
		if len(m) == 0 {
			delete(s.Manual, service)
		} else {
			s.Manual[service] = m
		}
	})
	if err != nil {
		return err
	}
	if err := e.publishFor(ctx, []string{service}, false); err != nil {
		return err
	}
	e.audit(ctx, "user", "knobs.set", "", "", knobDetails(service, knobs))
	e.timeline(api.KindConfig, api.SeverityInfo, "Knobs changed on "+service, describeValues(knobs), "")
	return nil
}

func knobDetails(service string, v api.Values) map[string]string {
	d := map[string]string{"service": service}
	for k, x := range v {
		d[k] = fmt.Sprint(x)
	}
	return d
}

func describeValues(v api.Values) string {
	var parts []string
	for _, k := range sortedKeys(v) {
		x := v[k]
		if x == nil {
			x = "default"
		}
		parts = append(parts, fmt.Sprintf("%s=%v", k, x))
	}
	return strings.Join(parts, ", ")
}

// ResetKnobs implements api.Experiments.
func (e *Engine) ResetKnobs(ctx context.Context, service string) error {
	if err := validService(service); err != nil {
		return err
	}
	e.op.Lock()
	defer e.op.Unlock()
	if err := e.saveSettings(ctx, func(s *settings) { delete(s.Manual, service) }); err != nil {
		return err
	}
	if err := e.publishFor(ctx, []string{service}, false); err != nil {
		return err
	}
	e.audit(ctx, "user", "knobs.reset", "", "", map[string]string{"service": service})
	e.timeline(api.KindConfig, api.SeverityInfo, "Knobs of "+service+" back to baseline", "", "")
	return nil
}

// Tuning implements api.Experiments.
func (e *Engine) Tuning() api.TuningState {
	e.mu.Lock()
	defer e.mu.Unlock()
	st := api.TuningState{Settings: tuning.Settings(), Services: map[string]api.Values{}}
	for _, svc := range contracts.Services {
		st.Services[svc] = copyValues(e.settings.Tuning[svc])
	}
	return st
}

// SetTuning implements api.Experiments.
func (e *Engine) SetTuning(ctx context.Context, service string, values api.Values) (api.TuningState, error) {
	if err := validService(service); err != nil {
		return api.TuningState{}, err
	}
	if err := tuning.Validate(values); err != nil {
		return api.TuningState{}, err
	}
	e.op.Lock()
	err := e.saveSettings(ctx, func(s *settings) {
		if m := tuning.Merge(s.Tuning[service], values); len(m) > 0 {
			s.Tuning[service] = m
		} else {
			delete(s.Tuning, service)
		}
	})
	if err == nil {
		err = e.publishFor(ctx, []string{service}, false)
	}
	e.op.Unlock()
	if err != nil {
		return api.TuningState{}, err
	}
	d := knobDetails(service, values)
	sev := api.SeverityInfo
	detail := describeValues(values)
	if lossy := tuning.Lossy(e.Tuning().Services[service]); len(lossy) > 0 {
		sev = api.SeverityWarning
		d["lossy"] = strings.Join(lossy, ",")
		detail += " — weakens delivery guarantees: " + strings.Join(lossy, ", ")
	}
	e.audit(ctx, "user", "tuning.set", "", "", d)
	e.timeline(api.KindConfig, sev, "Client tuning changed on "+service, detail, "")
	return e.Tuning(), nil
}

// revertAnnotation is stored on scaled deployments (lab.kafka/revert) so a panic can undo
// scale changes with the API server alone.
type revertAnnotation struct {
	Replicas int    `json:"replicas"`
	Owner    string `json:"owner"`
}

const revertKey = "lab.kafka/revert"

// Scale implements api.Experiments.
func (e *Engine) Scale(ctx context.Context, service string, replicas int) error {
	if err := validService(service); err != nil {
		return err
	}
	if replicas < 0 || replicas > 6 {
		return fmt.Errorf("%w: replicas must be 0..6", api.ErrInvalid)
	}
	if e.cfg.Kube == nil {
		return fmt.Errorf("%w: scaling needs the Kubernetes API", api.ErrUnavailable)
	}
	e.op.Lock()
	defer e.op.Unlock()
	e.mu.Lock()
	orig, known := e.settings.Scaled[service]
	e.mu.Unlock()
	ns := e.cfg.CommerceNamespace
	if !known {
		cur, err := e.cfg.Kube.GetScale(ctx, ns, service)
		if err != nil {
			return kubeErr(err)
		}
		orig = cur
		if _, err := claimRevert(ctx, e.cfg.Kube, k8s.DeploymentPath(ns, service), cur, "manual"); err != nil {
			return kubeErr(err)
		}
	}
	if err := e.cfg.Kube.SetScale(ctx, ns, service, replicas); err != nil {
		return kubeErr(err)
	}
	back := replicas == orig
	if back {
		_ = releaseRevert(ctx, e.cfg.Kube, k8s.DeploymentPath(ns, service), "manual")
	}
	err := e.saveSettings(ctx, func(s *settings) {
		if back {
			delete(s.Scaled, service)
			delete(s.Desired, service)
			return
		}
		s.Scaled[service] = orig
		s.Desired[service] = replicas
	})
	if err != nil {
		return err
	}
	e.audit(ctx, "user", "service.scaled", "", "", map[string]string{"service": service, "replicas": strconv.Itoa(replicas)})
	e.timeline(api.KindConfig, api.SeverityInfo, fmt.Sprintf("%s scaled to %d", service, replicas), "", "")
	return nil
}

func kubeErr(err error) error {
	return fmt.Errorf("%w: kubernetes: %v", api.ErrUnavailable, err)
}

// claimRevert records the original replicas on the deployment unless another owner already
// did (the oldest original wins, so a panic restores the state before any chaos). It reports
// whether owner holds the annotation now.
func claimRevert(ctx context.Context, kube *k8s.Client, path string, replicas int, owner string) (bool, error) {
	ann, err := kube.Annotations(ctx, path)
	if err != nil {
		return false, err
	}
	if _, taken := ann[revertKey]; taken {
		return false, nil
	}
	b, _ := json.Marshal(revertAnnotation{Replicas: replicas, Owner: owner})
	v := string(b)
	return true, kube.SetAnnotations(ctx, path, map[string]*string{revertKey: &v})
}

// releaseRevert removes the annotation if owner set it.
func releaseRevert(ctx context.Context, kube *k8s.Client, path, owner string) error {
	ann, err := kube.Annotations(ctx, path)
	if err != nil {
		return err
	}
	var ra revertAnnotation
	if json.Unmarshal([]byte(ann[revertKey]), &ra) != nil || ra.Owner != owner {
		return nil
	}
	return kube.SetAnnotations(ctx, path, map[string]*string{revertKey: nil})
}

// SimulationState implements api.Experiments.
func (e *Engine) SimulationState() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.settings.Simulation
}

// Panic implements api.Experiments: every experiment is stopped and reverted, manual knobs and
// tuning are cleared, scale changes and leftover Kubernetes faults are undone (found through
// the lab.kafka/revert annotations and running chaos-net helpers), chaos is disarmed and every
// service gets a fresh baseline snapshot.
func (e *Engine) Panic(ctx context.Context) (int, error) {
	e.op.Lock()
	defer e.op.Unlock()
	n := 0
	e.mu.Lock()
	var active []*run
	for _, id := range e.order {
		if r := e.runs[id]; r.active() {
			active = append(active, r)
		}
	}
	cleared := len(e.settings.Manual) + len(e.settings.Tuning)
	scaled := e.settings.Scaled
	e.mu.Unlock()
	for _, r := range active {
		e.stopLocked(ctx, r, "user", "panic")
		n++
	}
	n += cleared
	var errs []error
	if e.cfg.Kube != nil {
		swept, err := e.sweep(ctx, scaled)
		n += swept
		if err != nil {
			errs = append(errs, err)
		}
	}
	if err := e.saveSettings(ctx, func(s *settings) {
		s.Armed = false
		s.Manual = map[string]api.Values{}
		s.Tuning = map[string]api.Values{}
		s.Scaled = map[string]int{}
		s.Desired = map[string]int{}
	}); err != nil {
		errs = append(errs, err)
	}
	if err := e.publishFor(ctx, contracts.Services, true); err != nil {
		errs = append(errs, err)
	}
	e.audit(ctx, "user", "panic", "", "", map[string]string{"reverted": strconv.Itoa(n)})
	detail := ""
	if len(errs) > 0 {
		detail = "incomplete: " + errors.Join(errs...).Error()
	}
	e.timeline(api.KindSystem, api.SeverityCritical, fmt.Sprintf("PANIC: everything reverted (%d), chaos disarmed", n), detail, "")
	if len(errs) > 0 {
		return n, fmt.Errorf("%w: panic incomplete: %v", api.ErrUnavailable, errors.Join(errs...))
	}
	return n, nil
}

// sweep undoes Kubernetes changes using only the API server's state.
func (e *Engine) sweep(ctx context.Context, scaled map[string]int) (int, error) {
	kube := e.cfg.Kube
	n := 0
	var errs []error
	restore := func(ns, name string, ann map[string]string) {
		var ra revertAnnotation
		if json.Unmarshal([]byte(ann[revertKey]), &ra) != nil {
			return
		}
		if err := kube.SetScale(ctx, ns, name, ra.Replicas); err != nil {
			errs = append(errs, err)
			return
		}
		if err := kube.SetAnnotations(ctx, k8s.DeploymentPath(ns, name), map[string]*string{revertKey: nil}); err != nil {
			errs = append(errs, err)
		}
		n++
	}
	deps, err := kube.ListDeployments(ctx, e.cfg.CommerceNamespace, "app.kubernetes.io/part-of=kafka-lab")
	if err != nil {
		errs = append(errs, err)
	}
	for _, d := range deps {
		if _, ok := d.Metadata.Annotations[revertKey]; ok {
			restore(e.cfg.CommerceNamespace, d.Metadata.Name, d.Metadata.Annotations)
			delete(scaled, d.Metadata.Name)
		}
	}
	for svc, orig := range scaled { // manual scale whose annotation got lost
		if err := kube.SetScale(ctx, e.cfg.CommerceNamespace, svc, orig); err != nil {
			errs = append(errs, err)
			continue
		}
		n++
	}
	opPath := k8s.DeploymentPath(e.cfg.KafkaNamespace, e.cfg.OperatorDeployment)
	if ann, err := kube.Annotations(ctx, opPath); err == nil {
		if _, ok := ann[revertKey]; ok {
			restore(e.cfg.KafkaNamespace, e.cfg.OperatorDeployment, ann)
		}
	} else {
		errs = append(errs, err)
	}
	pods, err := kube.ListPods(ctx, e.cfg.KafkaNamespace, "strimzi.io/cluster="+e.cfg.KafkaCluster)
	if err != nil {
		errs = append(errs, err)
	}
	for _, p := range pods {
		if !strings.HasPrefix(p.Metadata.Name, e.cfg.BrokerPodPrefix) {
			continue
		}
		for _, name := range p.RunningEphemeral(chaosPrefix) {
			if strings.HasSuffix(name, undoSuffix) {
				continue
			}
			if err := injectUndo(ctx, kube, &e.cfg, p.Metadata.Name, name+"-"+newID("p")[2:8]+undoSuffix); err != nil {
				errs = append(errs, err)
				continue
			}
			n++
			break
		}
	}
	kpath := k8s.KafkaPath(e.cfg.KafkaNamespace, e.cfg.KafkaCluster)
	if ann, err := kube.Annotations(ctx, kpath); err == nil {
		if _, ok := ann[pauseKey]; ok {
			if err := kube.SetAnnotations(ctx, kpath, map[string]*string{pauseKey: nil}); err != nil {
				errs = append(errs, err)
			} else {
				n++
			}
		}
	} else {
		errs = append(errs, err)
	}
	return n, errors.Join(errs...)
}

const pauseKey = "strimzi.io/pause-reconciliation"

// reconcile restores the in-memory state at startup: unfinished experiments of a previous
// controlboard are reverted (from their persisted revert data) and recorded as aborted, then
// every service gets a fresh snapshot.
func (e *Engine) reconcile(ctx context.Context) error {
	recs, err := e.store.loadRecords(ctx, keepEnded)
	if err != nil {
		return err
	}
	e.op.Lock()
	defer e.op.Unlock()
	for _, rec := range recs {
		sc, ok := e.byID[rec.Exp.Scenario]
		if !ok {
			continue // scenario no longer exists; nothing can be reverted
		}
		r := &run{rec: *rec, scenario: sc, env: e.env(rec.Exp.ID)}
		if plan, err := sc.Plan(PlanInput{Params: rec.Exp.Params, Duration: rec.Exp.EndsAt.Sub(rec.Exp.StartsAt), Cfg: &e.cfg}); err == nil {
			r.plan = plan
		}
		e.runs[r.rec.Exp.ID] = r
		e.order = append(e.order, r.rec.Exp.ID)
		if !r.active() {
			continue
		}
		r.rec.Reason = joinReason(r.rec.Reason, "controlboard restarted")
		r.rec.Hidden = false // its drill was expired by the drills package on startup
		switch r.rec.Exp.Status {
		case api.StatusScheduled:
			r.rec.Outcome = api.StatusAborted
		case api.StatusRunning:
			e.revert(ctx, r, api.StatusAborted, "system")
		}
		e.finish(ctx, r)
	}
	if err := e.publishFor(ctx, contracts.Services, true); err != nil {
		return err
	}
	e.timeline(api.KindSystem, api.SeverityInfo, "Experiment engine started", fmt.Sprintf("chaos armed: %v, simulation %s", e.settings.Armed, e.settings.Simulation), "")
	return nil
}
