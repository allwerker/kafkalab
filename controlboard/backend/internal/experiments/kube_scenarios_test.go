package experiments

import (
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"kafkalab/controlboard/backend/internal/api"
	"kafkalab/controlboard/backend/internal/k8s"
	"kafkalab/controlboard/backend/internal/k8s/k8sfake"
)

func armed(t *testing.T, opts ...func(*Config)) *harness {
	h := newHarness(t, opts...)
	if err := h.e.Arm(ctx, true); err != nil {
		t.Fatal(err)
	}
	return h
}

// recreate makes the fake API server behave like a Deployment/StrimziPodSet controller.
func recreate(h *harness) {
	h.kube.OnDeletePod = func(s *k8sfake.Server, ns, name string, _ *int64) {
		labels := map[string]string{"app.kubernetes.io/name": strings.TrimSuffix(name, "-pod")}
		if ns == "kafka" {
			labels = map[string]string{"strimzi.io/cluster": "kafka-lab", "strimzi.io/pod-name": name}
		}
		s.AddPodLocked(ns, name, labels)
	}
}

func Test_shouldKillTheServicesPodsRepeatedlyWhenAConsumerCrashLoopRuns(t *testing.T) {
	// GIVEN: inventory pods that Kubernetes recreates after every kill
	h := newHarness(t)
	recreate(h)

	// WHEN: a crash loop with a 12 s interval runs for 30 s
	x := h.start(api.StartRequest{Scenario: "consumer-crash-loop", AutoRecover: true})
	h.advance(6 * time.Second)
	h.advance(6 * time.Second)
	h.advance(12 * time.Second)

	// THEN: the pods were killed without grace at the start and every 12 s, and the observation says so
	if n := h.kube.Count("DELETE /api/v1/namespaces/commerce/pods/inventory-pod"); n != 3 {
		t.Fatalf("%d kills", n)
	}
	obs := h.experiment(x.ID).Observations
	if obs[len(obs)-1].Observed != "3 pods killed" || !obs[len(obs)-1].Confirmed {
		t.Fatalf("observations %+v", obs)
	}
}

func Test_shouldRestoreTheReplicasWhenARegionalOutageEnds(t *testing.T) {
	// GIVEN: payment with 2 replicas and inventory with 1
	h := armed(t)
	_ = h.kube.Client().SetScale(ctx, "commerce", "payment", 2)

	// WHEN: region-a goes down, then the outage ends
	x := h.start(api.StartRequest{Scenario: "regional-outage", AutoRecover: true, Confirm: "regional-outage"})
	downPay, downInv := h.kube.Replicas("commerce", "payment"), h.kube.Replicas("commerce", "inventory")
	ann := h.kube.DeploymentAnnotations("commerce", "payment")[revertKey]
	h.advance(90 * time.Second)

	// THEN: both were at 0 with the original count stored on the deployment, and are back afterwards
	if downPay != 0 || downInv != 0 || !strings.Contains(ann, `"replicas":2`) || !strings.Contains(ann, x.ID) {
		t.Fatalf("down %d/%d annotation %q", downPay, downInv, ann)
	}
	if h.kube.Replicas("commerce", "payment") != 2 || h.kube.Replicas("commerce", "inventory") != 1 {
		t.Fatal("replicas not restored")
	}
	if _, ok := h.kube.DeploymentAnnotations("commerce", "payment")[revertKey]; ok {
		t.Fatal("annotation not removed")
	}
}

func Test_shouldFlipTheReplicasAndRestoreThemWhenARebalanceStormRuns(t *testing.T) {
	// GIVEN: payment with 1 replica
	h := newHarness(t)

	// WHEN: a rebalance storm (every 8 s, +1 replica) runs and ends
	h.start(api.StartRequest{Scenario: "rebalance-storm", AutoRecover: true})
	up := h.kube.Replicas("commerce", "payment")
	h.advance(4 * time.Second)
	still := h.kube.Replicas("commerce", "payment")
	h.advance(4 * time.Second)
	down := h.kube.Replicas("commerce", "payment")
	h.advance(8 * time.Second)
	upAgain := h.kube.Replicas("commerce", "payment")
	h.advance(110 * time.Second)

	// THEN: it alternated 2 → 1 → 2 and ended at the original single replica
	if up != 2 || still != 2 || down != 1 || upAgain != 2 || h.kube.Replicas("commerce", "payment") != 1 {
		t.Fatalf("replicas %d %d %d %d → %d", up, still, down, upAgain, h.kube.Replicas("commerce", "payment"))
	}
}

func Test_shouldKeepTheBrokerDownAndRestoreTheOperatorWhenAKafkaBrokerFailureRuns(t *testing.T) {
	// GIVEN: armed chaos and a broker pod that comes back once (the operator was still shutting down)
	h := armed(t)
	once := false
	h.kube.OnDeletePod = func(s *k8sfake.Server, ns, name string, _ *int64) {
		if !once {
			once = true
			s.AddPodLocked(ns, name, nil)
		}
	}

	// WHEN: broker 1 fails for 90 s
	x := h.start(api.StartRequest{Scenario: "kafka-broker-failure", AutoRecover: true, Confirm: "kafka-broker-failure"})
	opDown := h.kube.Replicas("kafka", "strimzi-cluster-operator")
	_, podBack := h.kube.Pod("kafka", "kafka-lab-dual-role-1")
	h.obs.set(func(o *fakeObserver) { o.brokers[1].Up = false })
	h.advance(5 * time.Second)

	// THEN: the operator was scaled to 0 first, the broker pod is killed again whenever it reappears
	_, stillThere := h.kube.Pod("kafka", "kafka-lab-dual-role-1")
	if opDown != 0 || !podBack || stillThere || h.kube.Count("DELETE /api/v1/namespaces/kafka/pods/kafka-lab-dual-role-1") != 2 {
		t.Fatalf("operator %d podBack %v stillThere %v", opDown, podBack, stillThere)
	}
	obs := h.experiment(x.ID).Observations
	if !obs[0].Confirmed || obs[0].Label != "Broker 1 stopped" {
		t.Fatalf("observations %+v", obs)
	}
	// AND: at the end the operator is back at 1 replica without the revert annotation
	h.advance(85 * time.Second)
	if h.kube.Replicas("kafka", "strimzi-cluster-operator") != 1 || h.experiment(x.ID).Status != api.StatusRecovering {
		t.Fatalf("operator %d status %s", h.kube.Replicas("kafka", "strimzi-cluster-operator"), h.experiment(x.ID).Status)
	}
	if _, ok := h.kube.DeploymentAnnotations("kafka", "strimzi-cluster-operator")[revertKey]; ok {
		t.Fatal("operator annotation left")
	}
	// AND: it completes only when all brokers are up and nothing is under-replicated
	h.obs.set(func(o *fakeObserver) { o.brokers[1].Up = true; o.brokers[0].UnderReplicated = 4 })
	h.advance(5 * time.Second)
	notYet := h.experiment(x.ID).Status
	h.obs.set(func(o *fakeObserver) { o.brokers[0].UnderReplicated = 0 })
	h.advance(5 * time.Second)
	if notYet != api.StatusRecovering || h.experiment(x.ID).Status != api.StatusCompleted {
		t.Fatalf("statuses %s → %s", notYet, h.experiment(x.ID).Status)
	}
}

func Test_shouldToleratePodsThatAreTerminatingOrGoneWhenTheBrokerIsKeptDown(t *testing.T) {
	// GIVEN: a broker failure where the pod is already terminating, then gone
	h := armed(t)
	h.start(api.StartRequest{Scenario: "kafka-broker-failure", AutoRecover: true, Confirm: "kafka-broker-failure"})
	now := time.Now()
	h.kube.AddPod("kafka", "kafka-lab-dual-role-1", nil).Metadata.DeletionTimestamp = &now

	// WHEN: the engine ticks twice
	h.advance(time.Second)
	h.kube.Mu.Lock()
	delete(h.kube.Pods, "kafka/kafka-lab-dual-role-1")
	h.kube.Mu.Unlock()
	h.advance(time.Second)

	// THEN: it does not kill a terminating pod again
	if n := h.kube.Count("DELETE /api/v1/namespaces/kafka/pods/kafka-lab-dual-role-1"); n != 1 {
		t.Fatalf("%d deletes", n)
	}
}

func Test_shouldKillTheBrokerOnceWithoutGraceWhenABrokerCrashRuns(t *testing.T) {
	// GIVEN: armed chaos
	h := armed(t)
	var grace *int64
	h.kube.OnDeletePod = func(_ *k8sfake.Server, _, _ string, g *int64) { grace = g }

	// WHEN: broker 2 crashes
	x := h.start(api.StartRequest{Scenario: "broker-crash", AutoRecover: true, Confirm: "broker-crash", Params: api.Values{"broker": 2.0}})

	// THEN: its pod was deleted with grace 0, the operator is untouched (Strimzi restarts it)
	if grace == nil || *grace != 0 || h.kube.Replicas("kafka", "strimzi-cluster-operator") != 1 || x.Affected[0] != "broker-2" {
		t.Fatalf("grace %v operator %d affected %v", grace, h.kube.Replicas("kafka", "strimzi-cluster-operator"), x.Affected)
	}
}

func Test_shouldRestartTheBrokersOneAfterAnotherWhenARollingRestartRuns(t *testing.T) {
	// GIVEN: brokers that come back ready after a graceful delete, and a 20 s pause between them
	h := armed(t)
	recreate(h)
	var graces []*int64
	h.kube.OnDeletePod = func(s *k8sfake.Server, ns, name string, g *int64) {
		graces = append(graces, g)
		s.AddPodLocked(ns, name, nil)
	}

	// WHEN: the rolling restart runs
	x := h.start(api.StartRequest{Scenario: "broker-rolling-restart", AutoRecover: true, Confirm: "broker-rolling-restart"})
	h.advance(time.Second) // broker 0 ready again
	h.advance(10 * time.Second)
	afterPause := h.kube.Count("DELETE /api/v1/namespaces/kafka/pods/kafka-lab-dual-role-1")
	h.advance(10 * time.Second) // pause over → broker 1
	h.advance(time.Second)      // ready
	h.advance(20 * time.Second) // → broker 2
	h.advance(time.Second)
	h.advance(20 * time.Second)
	h.advance(5 * time.Second)

	// THEN: each broker was deleted gracefully exactly once, in order, respecting the pause
	if afterPause != 0 || len(graces) != 3 || graces[0] != nil {
		t.Fatalf("after pause %d, deletes %d", afterPause, len(graces))
	}
	for _, b := range []string{"0", "1", "2"} {
		if h.kube.Count("DELETE /api/v1/namespaces/kafka/pods/kafka-lab-dual-role-"+b) != 1 {
			t.Fatalf("broker %s not restarted exactly once", b)
		}
	}
	obs := h.experiment(x.ID).Observations
	if obs[0].Observed != "3 of 3" || !obs[0].Confirmed {
		t.Fatalf("observations %+v", obs)
	}
}

func Test_shouldWaitForTheBrokerWhenItIsNotReadyYetDuringARollingRestart(t *testing.T) {
	// GIVEN: broker 0 comes back but is not ready
	h := armed(t)
	h.kube.OnDeletePod = func(s *k8sfake.Server, ns, name string, _ *int64) {
		s.AddPodLocked(ns, name, nil).Status.Conditions = nil
	}
	h.start(api.StartRequest{Scenario: "broker-rolling-restart", AutoRecover: true, Confirm: "broker-rolling-restart", Params: api.Values{"pauseSec": 0.0}})

	// WHEN: a minute passes
	for range 6 {
		h.advance(10 * time.Second)
	}

	// THEN: broker 1 was never touched
	if h.kube.Count("DELETE /api/v1/namespaces/kafka/pods/kafka-lab-dual-role-1") != 0 {
		t.Fatal("restarted broker 1 while broker 0 was not ready")
	}
}

func Test_shouldPartitionTheBrokerAndUndoItEarlyWhenABrokerOutageIsStopped(t *testing.T) {
	// GIVEN: armed chaos and the chaos-net image
	h := armed(t)

	// WHEN: broker 1 is partitioned and the experiment is stopped while the helper still runs
	x := h.start(api.StartRequest{Scenario: "broker-outage", AutoRecover: true, Confirm: "broker-outage"})
	pod, _ := h.kube.Pod("kafka", "kafka-lab-dual-role-1")
	_, err := h.e.Stop(ctx, x.ID)
	after, _ := h.kube.Pod("kafka", "kafka-lab-dual-role-1")

	// THEN: the helper ran with NET_ADMIN, the experiment's id, duration and dead-man switch,
	// and the stop injected an undo helper
	ec := pod.Spec.EphemeralContainers[0]
	env := map[string]string{}
	for _, v := range ec.Env {
		env[v.Name] = v.Value
	}
	if err != nil || ec.Image != "chaos-net:test" || ec.SecurityContext.Capabilities.Add[0] != "NET_ADMIN" ||
		env["MODE"] != "partition" || env["DURATION"] != "60" || env["ID"] != strings.TrimPrefix(x.ID, "exp-") || env["HEALTH_URL"] == "" {
		t.Fatalf("container %+v env %v err %v", ec, env, err)
	}
	undo := after.Spec.EphemeralContainers[1]
	if undo.Name != ec.Name+"-undo" || undo.Env[0].Value != "undo" {
		t.Fatalf("undo %+v", undo)
	}
}

func Test_shouldNotInjectAnUndoHelperWhenTheFaultAlreadyEndedOnItsOwn(t *testing.T) {
	// GIVEN: a broker network degradation whose helper already finished, and one whose pod was restarted
	h := armed(t)
	x := h.start(api.StartRequest{Scenario: "broker-network", AutoRecover: true, Confirm: "broker-network", Params: api.Values{"mode": "loss"}})
	h.kube.FinishEphemeral("kafka", "kafka-lab-dual-role-1", containerName(x.ID))

	// WHEN: the experiment ends
	h.advance(90 * time.Second)

	// THEN: no undo helper is added; the loss settings had reached the helper
	pod, _ := h.kube.Pod("kafka", "kafka-lab-dual-role-1")
	if len(pod.Spec.EphemeralContainers) != 1 || pod.Spec.EphemeralContainers[0].Env[3].Value != "loss" {
		t.Fatalf("containers %+v", pod.Spec.EphemeralContainers)
	}
}

func Test_shouldReportAnIncompleteRevertWhenTheBrokerCannotBeReached(t *testing.T) {
	// GIVEN: a running broker partition and an API server that now fails pod reads
	h := armed(t)
	x := h.start(api.StartRequest{Scenario: "broker-outage", AutoRecover: true, Confirm: "broker-outage"})
	h.kube.Fail = func(r *http.Request) int {
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/pods/") {
			return http.StatusInternalServerError
		}
		return 0
	}

	// WHEN: it is stopped
	got, _ := h.e.Stop(ctx, x.ID)

	// THEN: it is aborted and the timeline tells that the revert is incomplete
	found := false
	for _, e := range h.timeline() {
		found = found || strings.Contains(e.Title, "revert incomplete")
	}
	if got.Status != api.StatusAborted || !found {
		t.Fatalf("status %s, timeline warning %v", got.Status, found)
	}
}

func Test_shouldIgnoreAPodThatIsGoneWhenRevertingABrokerPartition(t *testing.T) {
	// GIVEN: a partitioned broker whose pod was deleted meanwhile (new netns, rules gone)
	h := armed(t)
	x := h.start(api.StartRequest{Scenario: "broker-outage", AutoRecover: true, Confirm: "broker-outage"})
	_ = h.kube.Client().DeletePod(ctx, "kafka", "kafka-lab-dual-role-1", nil)

	// WHEN: the experiment is stopped
	got, _ := h.e.Stop(ctx, x.ID)

	// THEN: the revert is clean
	if got.Status != api.StatusAborted || strings.Contains(h.e.runs[x.ID].rec.Reason, "network fault") {
		t.Fatalf("%+v reason %q", got, h.e.runs[x.ID].rec.Reason)
	}
}

func Test_shouldRefuseBrokerNetworkScenariosWhenTheHelperImageIsMissing(t *testing.T) {
	// GIVEN: no chaos-net image configured
	h := armed(t, func(c *Config) { c.ChaosNetImage = "" })

	// WHEN: a broker partition is requested
	_, err := h.e.Start(ctx, api.StartRequest{Scenario: "broker-outage", AutoRecover: true, Confirm: "broker-outage"})

	// THEN: it is unavailable
	if !errors.Is(err, api.ErrUnavailable) {
		t.Fatalf("err %v", err)
	}
}

func Test_shouldFailTheExperimentWhenTheKubernetesApiRefusesTheFirstStep(t *testing.T) {
	// GIVEN: an API server refusing every call
	h := armed(t)
	h.kube.Fail = func(*http.Request) int { return http.StatusForbidden }
	cases := []api.StartRequest{
		{Scenario: "kafka-broker-failure", Confirm: "kafka-broker-failure"},
		{Scenario: "broker-crash", Confirm: "broker-crash"},
		{Scenario: "broker-rolling-restart", Confirm: "broker-rolling-restart"},
		{Scenario: "broker-outage", Confirm: "broker-outage"},
		{Scenario: "rebalance-storm"},
		{Scenario: "regional-outage", Confirm: "regional-outage"},
	}
	for _, req := range cases {
		req.AutoRecover = true
		// WHEN: the scenario is started
		_, err := h.e.Start(ctx, req)

		// THEN: it fails as unavailable
		if !errors.Is(err, api.ErrUnavailable) {
			t.Fatalf("%s: %v", req.Scenario, err)
		}
	}
}

func Test_shouldNotFailWhenARestartingBrokerIsAlreadyGone(t *testing.T) {
	// GIVEN: broker pods 0 and 1 that are already gone and a scale annotation owned by someone else
	h := armed(t)
	for _, p := range []string{"0", "1"} {
		_ = h.kube.Client().DeletePod(ctx, "kafka", "kafka-lab-dual-role-"+p, nil)
	}
	v := `{"replicas":1,"owner":"manual"}`
	_ = h.kube.Client().SetAnnotations(ctx, k8s.DeploymentPath("kafka", "strimzi-cluster-operator"), map[string]*string{revertKey: &v})

	// WHEN: a rolling restart and (after it) a broker failure start
	x, err1 := h.e.Start(ctx, api.StartRequest{Scenario: "broker-rolling-restart", AutoRecover: true, Confirm: "broker-rolling-restart"})
	_, _ = h.e.Stop(ctx, x.ID)
	h.advance(10 * time.Minute)
	_, err2 := h.e.Start(ctx, api.StartRequest{Scenario: "kafka-broker-failure", AutoRecover: true, Confirm: "kafka-broker-failure"})

	// THEN: both start, and the foreign annotation is kept (oldest original wins)
	if err1 != nil || err2 != nil || h.kube.DeploymentAnnotations("kafka", "strimzi-cluster-operator")[revertKey] != v {
		t.Fatalf("%v %v %v", err1, err2, h.kube.DeploymentAnnotations("kafka", "strimzi-cluster-operator"))
	}
}

func Test_shouldKeepGoingWhenACrashLoopTickFails(t *testing.T) {
	// GIVEN: a crash loop and an API server that starts failing after the first kill
	h := newHarness(t)
	h.start(api.StartRequest{Scenario: "consumer-crash-loop", AutoRecover: true})
	h.kube.Fail = func(*http.Request) int { return http.StatusServiceUnavailable }

	// WHEN: the next kill is due
	h.advance(12 * time.Second)

	// THEN: the engine logs it and the experiment keeps running
	if h.e.List()[0].Status != api.StatusRunning {
		t.Fatal("experiment stopped")
	}
}

func Test_shouldSkipPodsThatAreAlreadyTerminatingWhenKillingConsumers(t *testing.T) {
	// GIVEN: an inventory pod that is already terminating, and one that disappears while deleting
	h := newHarness(t)
	now := time.Now()
	h.kube.AddPod("commerce", "inventory-old", map[string]string{"app.kubernetes.io/name": "inventory"}).Metadata.DeletionTimestamp = &now
	h.kube.Fail = func(r *http.Request) int {
		if r.Method == http.MethodDelete {
			return http.StatusNotFound
		}
		return 0
	}

	// WHEN: the crash loop starts
	_, err := h.e.Start(ctx, api.StartRequest{Scenario: "consumer-crash-loop", AutoRecover: true})

	// THEN: it starts and only tried to delete the live pod
	if err != nil || h.kube.Count("DELETE /api/v1/namespaces/commerce/pods/inventory-old") != 0 {
		t.Fatalf("err %v", err)
	}
}
