package experiments

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"kafkalab/controlboard/backend/internal/k8s"
)

// Kubernetes actions. Every Apply returns JSON revert data; Revert must work from that data
// alone (a restarted controlboard reverts with a freshly computed plan).

func noRevert() json.RawMessage { return json.RawMessage(`{}`) }

// scaleAction scales a deployment to Replicas and back.
type scaleAction struct {
	Namespace, Deployment string
	Replicas              int
}

type scaleRevert struct {
	Replicas int  `json:"replicas"`
	Claimed  bool `json:"claimed"`
}

func (a *scaleAction) Apply(ctx context.Context, env *Env) (json.RawMessage, error) {
	cur, err := env.Kube.GetScale(ctx, a.Namespace, a.Deployment)
	if err != nil {
		return nil, err
	}
	claimed, err := claimRevert(ctx, env.Kube, k8s.DeploymentPath(a.Namespace, a.Deployment), cur, env.ExperimentID)
	if err != nil {
		return nil, err
	}
	data, _ := json.Marshal(scaleRevert{Replicas: cur, Claimed: claimed})
	if err := env.Kube.SetScale(ctx, a.Namespace, a.Deployment, a.Replicas); err != nil {
		_ = a.Revert(ctx, env, data)
		return nil, err
	}
	return data, nil
}

func (a *scaleAction) Tick(context.Context, *Env, time.Duration) error { return nil }

func (a *scaleAction) Revert(ctx context.Context, env *Env, data json.RawMessage) error {
	var rv scaleRevert
	_ = json.Unmarshal(data, &rv)
	if err := env.Kube.SetScale(ctx, a.Namespace, a.Deployment, rv.Replicas); err != nil {
		return fmt.Errorf("scale %s back to %d: %w", a.Deployment, rv.Replicas, err)
	}
	if rv.Claimed {
		return releaseRevert(ctx, env.Kube, k8s.DeploymentPath(a.Namespace, a.Deployment), env.ExperimentID)
	}
	return nil
}

// flipScaleAction alternates a deployment between its replicas and replicas+Extra every Every
// (Rebalance Storm) and restores the original count.
type flipScaleAction struct {
	scaleAction
	Extra int
	Every time.Duration
	base  int
	up    bool
	last  time.Duration
}

func (a *flipScaleAction) Apply(ctx context.Context, env *Env) (json.RawMessage, error) {
	cur, err := env.Kube.GetScale(ctx, a.Namespace, a.Deployment)
	if err != nil {
		return nil, err
	}
	a.base, a.Replicas, a.up = cur, cur+a.Extra, true
	return a.scaleAction.Apply(ctx, env)
}

func (a *flipScaleAction) Tick(ctx context.Context, env *Env, elapsed time.Duration) error {
	if elapsed-a.last < a.Every {
		return nil
	}
	a.last = elapsed
	a.up = !a.up
	n := a.base
	if a.up {
		n = a.base + a.Extra
	}
	return env.Kube.SetScale(ctx, a.Namespace, a.Deployment, n)
}

// killPodsAction deletes the pods matching Selector (grace 0) at start and every Every
// (Consumer Crash Loop). Every = 0 kills once.
type killPodsAction struct {
	Namespace, Selector string
	Every               time.Duration
	last                time.Duration
	Kills               int
}

func (a *killPodsAction) kill(ctx context.Context, env *Env) error {
	pods, err := env.Kube.ListPods(ctx, a.Namespace, a.Selector)
	if err != nil {
		return err
	}
	zero := int64(0)
	for _, p := range pods {
		if p.Metadata.DeletionTimestamp != nil {
			continue
		}
		if err := env.Kube.DeletePod(ctx, a.Namespace, p.Metadata.Name, &zero); err != nil && !errors.Is(err, k8s.ErrNotFound) {
			return err
		}
		a.Kills++
	}
	return nil
}

func (a *killPodsAction) Apply(ctx context.Context, env *Env) (json.RawMessage, error) {
	return noRevert(), a.kill(ctx, env)
}

func (a *killPodsAction) Tick(ctx context.Context, env *Env, elapsed time.Duration) error {
	if a.Every == 0 || elapsed-a.last < a.Every {
		return nil
	}
	a.last = elapsed
	return a.kill(ctx, env)
}

func (a *killPodsAction) Revert(context.Context, *Env, json.RawMessage) error { return nil }

// brokerDownAction keeps a broker pod deleted: the Strimzi operator (which runs the
// StrimziPodSet controller) is scaled to 0 first, see ADR 0003. If the pod comes back
// anyway (someone restarted the operator) it is killed again on every tick.
type brokerDownAction struct {
	Pod   string
	Grace *int64
	Kills int
}

func (a *brokerDownAction) Apply(ctx context.Context, env *Env) (json.RawMessage, error) {
	err := env.Kube.DeletePod(ctx, env.Cfg.KafkaNamespace, a.Pod, a.Grace)
	if err != nil && !errors.Is(err, k8s.ErrNotFound) {
		return nil, err
	}
	a.Kills++
	return noRevert(), nil
}

func (a *brokerDownAction) Tick(ctx context.Context, env *Env, _ time.Duration) error {
	p, err := env.Kube.GetPod(ctx, env.Cfg.KafkaNamespace, a.Pod)
	if errors.Is(err, k8s.ErrNotFound) {
		return nil
	}
	if err != nil || p.Metadata.DeletionTimestamp != nil {
		return err
	}
	zero := int64(0)
	a.Kills++
	return env.Kube.DeletePod(ctx, env.Cfg.KafkaNamespace, a.Pod, &zero)
}

func (a *brokerDownAction) Revert(context.Context, *Env, json.RawMessage) error { return nil }

// rollingRestartAction restarts the brokers one after another (graceful delete), waiting until
// the previous one is ready again plus Pause (Rolling Restart).
type rollingRestartAction struct {
	Brokers []int
	Pause   time.Duration
	next    int
	waiting bool
	readyAt time.Duration
}

func (a *rollingRestartAction) podName(env *Env, i int) string {
	return env.Cfg.BrokerPodPrefix + strconv.Itoa(a.Brokers[i])
}

func (a *rollingRestartAction) restart(ctx context.Context, env *Env) error {
	if err := env.Kube.DeletePod(ctx, env.Cfg.KafkaNamespace, a.podName(env, a.next), nil); err != nil && !errors.Is(err, k8s.ErrNotFound) {
		return err
	}
	a.waiting, a.readyAt = true, 0
	return nil
}

func (a *rollingRestartAction) Apply(ctx context.Context, env *Env) (json.RawMessage, error) {
	return noRevert(), a.restart(ctx, env)
}

func (a *rollingRestartAction) Tick(ctx context.Context, env *Env, elapsed time.Duration) error {
	if a.next >= len(a.Brokers) {
		return nil
	}
	if a.waiting {
		p, err := env.Kube.GetPod(ctx, env.Cfg.KafkaNamespace, a.podName(env, a.next))
		if err != nil || !p.Ready() {
			return nil // still restarting
		}
		a.waiting, a.readyAt = false, elapsed
		return nil
	}
	if elapsed-a.readyAt < a.Pause {
		return nil
	}
	a.next++
	if a.next >= len(a.Brokers) {
		return nil
	}
	return a.restart(ctx, env)
}

// Done reports whether every broker was restarted.
func (a *rollingRestartAction) Done() bool { return a.next >= len(a.Brokers) }

func (a *rollingRestartAction) Revert(context.Context, *Env, json.RawMessage) error { return nil }

// chaosNetAction injects the chaos-net helper into a broker pod (ADR 0003).
type chaosNetAction struct {
	Pod      string
	Env      []k8s.EnvVar
	Duration time.Duration
}

const (
	chaosPrefix = "chaos-"
	undoSuffix  = "-undo"
)

type chaosNetRevert struct {
	Pod       string `json:"pod"`
	Container string `json:"container"`
}

func containerName(expID string) string { return chaosPrefix + strings.TrimPrefix(expID, "exp-") }

func helperContainer(cfg *Config, name string, env []k8s.EnvVar) k8s.EphemeralContainer {
	root := int64(0)
	return k8s.EphemeralContainer{Name: name, Image: cfg.ChaosNetImage, Env: env,
		SecurityContext: &k8s.SecurityContext{RunAsUser: &root, Capabilities: &k8s.Capabilities{Add: []string{"NET_ADMIN"}}}}
}

func (a *chaosNetAction) Apply(ctx context.Context, env *Env) (json.RawMessage, error) {
	name := containerName(env.ExperimentID)
	vars := append([]k8s.EnvVar{
		{Name: "ID", Value: strings.TrimPrefix(env.ExperimentID, "exp-")},
		{Name: "DURATION", Value: strconv.Itoa(int(a.Duration.Seconds()))},
		{Name: "HEALTH_URL", Value: env.Cfg.HealthURL},
	}, a.Env...)
	if err := env.Kube.AddEphemeralContainer(ctx, env.Cfg.KafkaNamespace, a.Pod, helperContainer(env.Cfg, name, vars)); err != nil {
		return nil, err
	}
	data, _ := json.Marshal(chaosNetRevert{Pod: a.Pod, Container: name})
	return data, nil
}

func (a *chaosNetAction) Tick(context.Context, *Env, time.Duration) error { return nil }

// Revert injects an undo helper while the fault helper may still be active. If the pod was
// restarted meanwhile, its network namespace (and the rules) are gone already.
func (a *chaosNetAction) Revert(ctx context.Context, env *Env, data json.RawMessage) error {
	var rv chaosNetRevert
	_ = json.Unmarshal(data, &rv)
	p, err := env.Kube.GetPod(ctx, env.Cfg.KafkaNamespace, rv.Pod)
	if errors.Is(err, k8s.ErrNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("network fault on %s: %w", rv.Pod, err)
	}
	if !p.EphemeralRunning(rv.Container) {
		return nil
	}
	return injectUndo(ctx, env.Kube, env.Cfg, rv.Pod, rv.Container+undoSuffix)
}

// injectUndo starts a MODE=undo helper that removes every lab network fault in the pod.
func injectUndo(ctx context.Context, kube *k8s.Client, cfg *Config, pod, name string) error {
	ec := helperContainer(cfg, name, []k8s.EnvVar{{Name: "MODE", Value: "undo"}})
	if err := kube.AddEphemeralContainer(ctx, cfg.KafkaNamespace, pod, ec); err != nil {
		return fmt.Errorf("undo network fault on %s: %w", pod, err)
	}
	return nil
}
