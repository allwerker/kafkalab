package experiments

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"kafkalab/controlboard/backend/internal/api"
	"kafkalab/controlboard/backend/internal/k8s"
	"kafkalab/pkg/contracts"
)

// Simulation implements api.Experiments.
//
//   - start: the generator runs at its normal (or experiment) rate again.
//   - stop:  the generator's ordersPerSecond is forced to 0 (overrides every experiment).
//   - reset: stop, then in the background: scale the five consumer deployments to 0, wait until
//     their pods are gone (their groups are empty), delete every record of the business topics
//     and orders.dlq (DeleteRecords up to the end offsets), commit the consumer groups to the end
//     offsets, and scale the deployments back. Service tables are not truncated (the
//     controlboard has no rights in the service schemas): orders that were in flight simply stay
//     unfinished in the services' state tables, idempotency rows are never hit again because new
//     orders have new ids. The simulation stays stopped; "start" resumes with an empty pipeline.
func (e *Engine) Simulation(ctx context.Context, action string) (string, error) {
	switch action {
	case "start", "stop":
	case "reset":
		if e.cfg.Kube == nil || e.cfg.Admin == nil {
			return "", fmt.Errorf("%w: reset needs the Kubernetes API and the Kafka admin client", api.ErrUnavailable)
		}
	default:
		return "", fmt.Errorf("%w: unknown simulation action %q", api.ErrInvalid, action)
	}
	e.op.Lock()
	defer e.op.Unlock()
	if action == "reset" {
		e.mu.Lock()
		busy := e.resetting
		e.resetting = true
		e.mu.Unlock()
		if busy {
			return "", fmt.Errorf("%w: a reset is already running", api.ErrConflict)
		}
	}
	state := "stopped"
	if action == "start" {
		state = "running"
	}
	if err := e.saveSettings(ctx, func(s *settings) { s.Simulation = state }); err != nil {
		e.endReset(action)
		return "", err
	}
	if err := e.publishFor(ctx, []string{contracts.ServiceOrderGenerator}, false); err != nil {
		e.endReset(action)
		return "", err
	}
	e.audit(ctx, "user", "simulation."+action, "", "", nil)
	if action == "reset" {
		e.timeline(api.KindSimulation, api.SeverityWarning, "Simulation reset started", "generator stopped, consumers scaled down", "")
		go e.reset(context.WithoutCancel(ctx))
	} else {
		e.timeline(api.KindSimulation, api.SeverityInfo, "Simulation "+map[string]string{"start": "started", "stop": "stopped"}[action], "", "")
	}
	return state, nil
}

func (e *Engine) endReset(action string) {
	if action != "reset" {
		return
	}
	e.mu.Lock()
	e.resetting = false
	e.mu.Unlock()
}

// reset runs the background part of a simulation reset.
func (e *Engine) reset(ctx context.Context) {
	defer e.endReset("reset")
	kube, ns := e.cfg.Kube, e.cfg.CommerceNamespace
	var errs []error
	replicas := map[string]int{}
	for _, svc := range consumers {
		n, err := kube.GetScale(ctx, ns, svc)
		if errors.Is(err, k8s.ErrNotFound) {
			continue // service not deployed
		}
		if err == nil {
			_, err = claimRevert(ctx, kube, k8s.DeploymentPath(ns, svc), n, "reset")
		}
		if err == nil {
			err = kube.SetScale(ctx, ns, svc, 0)
		}
		if err != nil {
			errs = append(errs, err)
			continue
		}
		replicas[svc] = n
	}
	if len(errs) == 0 {
		errs = append(errs, e.waitPodsGone(ctx, sortedKeys(replicas))...)
	}
	if len(errs) == 0 {
		topics := append(slices.Clone(contracts.BusinessTopics), contracts.TopicOrdersDLQ)
		if err := e.cfg.Admin.DeleteRecordsToEnd(ctx, topics); err != nil {
			errs = append(errs, err)
		} else if err := e.cfg.Admin.CommitGroupsToEnd(ctx, consumers); err != nil {
			errs = append(errs, err)
		}
	}
	for _, svc := range sortedKeys(replicas) {
		if err := kube.SetScale(ctx, ns, svc, replicas[svc]); err != nil {
			errs = append(errs, err)
			continue
		}
		_ = releaseRevert(ctx, kube, k8s.DeploymentPath(ns, svc), "reset")
	}
	if len(errs) > 0 {
		e.timeline(api.KindSimulation, api.SeverityCritical, "Simulation reset failed", errors.Join(errs...).Error(), "")
		e.audit(ctx, "system", "simulation.reset.failed", "", "", map[string]string{"error": errors.Join(errs...).Error()})
		return
	}
	e.timeline(api.KindSimulation, api.SeveritySuccess, "Simulation reset: topics empty, consumer groups at the end",
		"press start to run the simulation again", "")
	e.audit(ctx, "system", "simulation.reset.done", "", "", nil)
}

func (e *Engine) waitPodsGone(ctx context.Context, services []string) []error {
	deadline := time.Now().Add(e.cfg.ResetPodTimeout) // wall clock: pods stop in real time
	for {
		var left []string
		for _, svc := range services {
			pods, err := e.cfg.Kube.ListPods(ctx, e.cfg.CommerceNamespace, kubeSelector+svc)
			if err != nil {
				return []error{err}
			}
			if len(pods) > 0 {
				left = append(left, svc)
			}
		}
		if len(left) == 0 {
			return nil
		}
		if !time.Now().Before(deadline) {
			return []error{fmt.Errorf("pods of %s did not stop within %s", strings.Join(left, ", "), e.cfg.ResetPodTimeout)}
		}
		time.Sleep(e.cfg.PollEvery)
	}
}

// diskGuard warns in the timeline when a broker volume is fuller than DiskWarnRatio (once per
// volume until it drops 5 points below the threshold again).
func (e *Engine) diskGuard(ctx context.Context, now time.Time) {
	if e.cfg.Query == nil || now.Sub(e.lastDisk) < e.cfg.DiskGuardEvery {
		return
	}
	e.lastDisk = now
	samples, err := e.cfg.Query(ctx, `max by (persistentvolumeclaim) (kubelet_volume_stats_used_bytes{namespace="`+e.cfg.KafkaNamespace+
		`"} / kubelet_volume_stats_capacity_bytes{namespace="`+e.cfg.KafkaNamespace+`"})`)
	if err != nil {
		e.cfg.Log.Warn("disk guard query failed", "error", err)
		return
	}
	for _, s := range samples {
		pvc := s.Labels["persistentvolumeclaim"]
		switch {
		case s.Value > e.cfg.DiskWarnRatio && !e.diskWarn[pvc]:
			e.diskWarn[pvc] = true
			e.timeline(api.KindSystem, api.SeverityWarning, fmt.Sprintf("Broker disk %s at %.0f %%", pvc, s.Value*100),
				"retention is size-based (256 MiB per partition); stop Fat Messages / Black Friday or reset the simulation", "")
		case s.Value < e.cfg.DiskWarnRatio-0.05:
			delete(e.diskWarn, pvc)
		}
	}
}
