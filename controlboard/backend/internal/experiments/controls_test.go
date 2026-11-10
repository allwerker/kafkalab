package experiments

import (
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"kafkalab/controlboard/backend/internal/api"
	"kafkalab/controlboard/backend/internal/k8s"
	"kafkalab/pkg/contracts"
)

func Test_shouldPublishAManualOverrideWithoutExpiryWhenKnobsAreSet(t *testing.T) {
	// GIVEN: a running shop
	h := newHarness(t)

	// WHEN: payment gets a 30 % business failure rate and 10 ms jitter, then the jitter is removed
	err1 := h.e.SetKnobs(ctx, contracts.ServicePayment, api.Values{"businessFailureProbability": 0.3, "processingJitterMs": 10.0})
	err2 := h.e.SetKnobs(ctx, contracts.ServicePayment, api.Values{"processingJitterMs": nil})

	// THEN: payment's snapshot carries only the failure rate, never expires, and Controls shows it
	s := h.last(contracts.ServicePayment)
	if err1 != nil || err2 != nil || *s.Knobs.BusinessFailureProbability != 0.3 || s.Knobs.ProcessingJitterMs != 0 || s.ExpiresAt != nil {
		t.Fatalf("snapshot %+v %v %v", s, err1, err2)
	}
	for _, c := range h.e.Controls() {
		if c.Service == contracts.ServicePayment && (c.Knobs["businessFailureProbability"] != 0.3 || len(c.Knobs) != 1 || c.DesiredGeneration != s.Generation) {
			t.Fatalf("controls %+v", c)
		}
	}
	if !contains(h.auditActions(), "knobs.set") {
		t.Fatal("not audited")
	}
}

func Test_shouldGoBackToBaselineWhenTheKnobsOfAServiceAreReset(t *testing.T) {
	// GIVEN: shipping paused manually
	h := newHarness(t)
	_ = h.e.SetKnobs(ctx, contracts.ServiceShipping, api.Values{"paused": true})
	paused := h.e.Controls()[3].Paused

	// WHEN: its knobs are reset (also after a restart nothing comes back)
	err := h.e.ResetKnobs(ctx, contracts.ServiceShipping)
	h.restart()

	// THEN: shipping runs on the baseline
	if !paused || err != nil || h.last(contracts.ServiceShipping).Knobs.Paused || h.e.Controls()[3].Paused {
		t.Fatalf("paused %v err %v", paused, err)
	}
}

func Test_shouldRejectBadKnobRequestsWhenTheyDoNotMatchTheSchema(t *testing.T) {
	// GIVEN: a running engine
	h := newHarness(t)
	bad := []api.Values{
		{"turbo": true},
		{"paused": "yes"},
		{"processingLatencyMs": 1.5},
		{"processingErrorProbability": 1.5},
		{"popularSku": ""},
		{"ordersPerSecond": "many"},
		{"client": map[string]any{}},
	}
	for _, v := range bad {
		// WHEN: the knobs are set
		err := h.e.SetKnobs(ctx, contracts.ServicePayment, v)

		// THEN: they are refused as invalid
		if !errors.Is(err, api.ErrInvalid) {
			t.Fatalf("%v: %v", v, err)
		}
	}
	if err := h.e.SetKnobs(ctx, "billing", api.Values{}); !errors.Is(err, api.ErrUnknownService) {
		t.Fatalf("unknown service: %v", err)
	}
	if err := h.e.ResetKnobs(ctx, "billing"); !errors.Is(err, api.ErrUnknownService) {
		t.Fatalf("unknown service: %v", err)
	}
}

func Test_shouldFailKnobChangesWhenKafkaOrTheDatabaseIsDown(t *testing.T) {
	// GIVEN: payment paused manually, then Kafka goes down
	h := newHarness(t)
	_ = h.e.SetKnobs(ctx, contracts.ServicePayment, api.Values{"paused": true})
	h.pub.failSnap = errBoom

	// WHEN: knobs are set and reset
	err1 := h.e.SetKnobs(ctx, contracts.ServicePayment, api.Values{"processingJitterMs": 5.0})
	err2 := h.e.ResetKnobs(ctx, contracts.ServicePayment)
	_, err3 := h.e.SetTuning(ctx, contracts.ServicePayment, api.Values{"acks": "all"})
	h.pub.failSnap = nil
	h.db.Close()
	err4 := h.e.SetKnobs(ctx, contracts.ServicePayment, api.Values{"paused": true})
	err5 := h.e.ResetKnobs(ctx, contracts.ServicePayment)
	err6 := h.e.Arm(ctx, true)

	// THEN: every change reports an unavailable dependency
	for i, err := range []error{err1, err2, err3, err4, err5, err6} {
		if !errors.Is(err, api.ErrUnavailable) {
			t.Fatalf("change %d: %v", i+1, err)
		}
	}
}

func Test_shouldPublishTheClientSettingsAndWarnWhenTuningWeakensDelivery(t *testing.T) {
	// GIVEN: a running engine
	h := newHarness(t)

	// WHEN: payment's producer switches to acks=none with 100 ms linger
	st, err := h.e.SetTuning(ctx, contracts.ServicePayment, api.Values{"acks": "none", "lingerMs": 100.0})

	// THEN: knobs.client is published, the state lists it, and the timeline warns about the loss risk
	c := h.last(contracts.ServicePayment).Knobs.Client
	if err != nil || *c.Acks != "none" || *c.LingerMs != 100 || st.Services[contracts.ServicePayment]["acks"] != "none" || len(st.Settings) != 8 {
		t.Fatalf("client %+v state %+v err %v", c, st, err)
	}
	var warned bool
	for _, e := range h.timeline() {
		warned = warned || (e.Severity == api.SeverityWarning && strings.Contains(e.Detail, "acks=none"))
	}
	if !warned {
		t.Fatal("no lossy warning")
	}
	// AND: Controls shows no client settings among the knobs, resetting all removes the entry
	for _, ctl := range h.e.Controls() {
		if _, ok := ctl.Knobs["client"]; ok {
			t.Fatal("client leaked into knobs")
		}
	}
	st, _ = h.e.SetTuning(ctx, contracts.ServicePayment, api.Values{"acks": nil, "lingerMs": nil})
	if len(st.Services[contracts.ServicePayment]) != 0 || h.last(contracts.ServicePayment).Knobs.Client.Acks != nil {
		t.Fatalf("not reset: %+v", st.Services)
	}
}

func Test_shouldRejectTuningOutsideTheAllowlistWhenSet(t *testing.T) {
	// GIVEN: a running engine
	h := newHarness(t)

	// WHEN: an unknown service and an unknown setting are tuned
	_, err1 := h.e.SetTuning(ctx, "billing", api.Values{})
	_, err2 := h.e.SetTuning(ctx, contracts.ServicePayment, api.Values{"retries": 5.0})

	// THEN: both are refused
	if !errors.Is(err1, api.ErrUnknownService) || !errors.Is(err2, api.ErrInvalid) {
		t.Fatalf("%v %v", err1, err2)
	}
}

func Test_shouldRememberTheOriginalReplicasWhenAServiceIsScaledManually(t *testing.T) {
	// GIVEN: analytics with 1 replica
	h := newHarness(t)

	// WHEN: it is scaled to 3, then to 2
	err1 := h.e.Scale(ctx, contracts.ServiceAnalytics, 3)
	err2 := h.e.Scale(ctx, contracts.ServiceAnalytics, 2)

	// THEN: the deployment runs 2, the original 1 is stored on it, and Controls reports the desired 2
	ann := h.kube.DeploymentAnnotations("commerce", "analytics")[revertKey]
	var desired *int
	for _, c := range h.e.Controls() {
		if c.Service == contracts.ServiceAnalytics {
			desired = c.Replicas
		}
	}
	if err1 != nil || err2 != nil || h.kube.Replicas("commerce", "analytics") != 2 || !strings.Contains(ann, `"replicas":1`) || desired == nil || *desired != 2 {
		t.Fatalf("replicas %d ann %q desired %v: %v %v", h.kube.Replicas("commerce", "analytics"), ann, desired, err1, err2)
	}
	// AND: scaling back to the original forgets the change
	_ = h.e.Scale(ctx, contracts.ServiceAnalytics, 1)
	if _, ok := h.kube.DeploymentAnnotations("commerce", "analytics")[revertKey]; ok || h.e.Controls()[5].Replicas != nil {
		t.Fatal("manual scale not forgotten")
	}
}

func Test_shouldRefuseToScaleWhenTheRequestOrTheClusterDoesNotAllowIt(t *testing.T) {
	// GIVEN: an engine with a failing API server and one without Kubernetes
	h := newHarness(t)
	noKube := newHarness(t, func(c *Config) { c.Kube = nil })

	// WHEN: invalid and impossible scale requests are made
	errs := []error{
		h.e.Scale(ctx, "billing", 1),
		h.e.Scale(ctx, contracts.ServicePayment, 7),
		noKube.e.Scale(ctx, contracts.ServicePayment, 2),
	}
	h.kube.Fail = func(r *http.Request) int {
		if strings.HasSuffix(r.URL.Path, "/scale") && r.Method == http.MethodGet {
			return 500
		}
		return 0
	}
	errs = append(errs, h.e.Scale(ctx, contracts.ServicePayment, 2))
	h.kube.Fail = func(r *http.Request) int {
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/deployments/payment") {
			return 500
		}
		return 0
	}
	errs = append(errs, h.e.Scale(ctx, contracts.ServicePayment, 2))
	h.kube.Fail = func(r *http.Request) int {
		if r.Method == http.MethodPatch && strings.HasSuffix(r.URL.Path, "/scale") {
			return 500
		}
		return 0
	}
	errs = append(errs, h.e.Scale(ctx, contracts.ServicePayment, 2))

	// THEN: each is refused with the matching error
	want := []error{api.ErrUnknownService, api.ErrInvalid, api.ErrUnavailable, api.ErrUnavailable, api.ErrUnavailable, api.ErrUnavailable}
	for i, err := range errs {
		if !errors.Is(err, want[i]) {
			t.Fatalf("request %d: %v", i+1, err)
		}
	}
}

func Test_shouldUndoEverythingAndDisarmWhenThePanicButtonIsPressed(t *testing.T) {
	// GIVEN: armed chaos, a broker partition, Black Friday, manual knobs, tuning, a manual scale,
	// and leftovers from a crashed controlboard: a scaled-down service with revert annotation and a
	// paused Kafka CR
	h := armed(t)
	h.start(api.StartRequest{Scenario: "broker-outage", AutoRecover: true, Confirm: "broker-outage"})
	h.start(api.StartRequest{Scenario: "black-friday", AutoRecover: true})
	_ = h.e.SetKnobs(ctx, contracts.ServiceShipping, api.Values{"paused": true})
	_, _ = h.e.SetTuning(ctx, contracts.ServicePayment, api.Values{"acks": "none"})
	_ = h.e.Scale(ctx, contracts.ServiceAnalytics, 4)
	cl := h.kube.Client()
	v := `{"replicas":1,"owner":"exp-crashed"}`
	_ = cl.SetAnnotations(ctx, k8s.DeploymentPath("commerce", "notification"), map[string]*string{revertKey: &v})
	_ = cl.SetScale(ctx, "commerce", "notification", 0)
	paused := "true"
	_ = cl.SetAnnotations(ctx, k8s.KafkaPath("kafka", "kafka-lab"), map[string]*string{pauseKey: &paused})
	lone := h.kube.AddPod("kafka", "kafka-lab-dual-role-2", map[string]string{"strimzi.io/cluster": "kafka-lab"})
	lone.Spec.EphemeralContainers = append(lone.Spec.EphemeralContainers, k8s.EphemeralContainer{Name: "chaos-orphan"})
	h.kube.AddPod("kafka", "kafka-lab-entity-operator", map[string]string{"strimzi.io/cluster": "kafka-lab"})

	// WHEN: panic
	n, err := h.e.Panic(ctx)

	// THEN: experiments aborted, overrides/tuning/scale cleared, leftovers repaired, chaos disarmed
	if err != nil || n != 9 || h.e.Armed() {
		t.Fatalf("reverted %d err %v armed %v", n, err, h.e.Armed())
	}
	for _, x := range h.e.List() {
		if x.Status != api.StatusAborted {
			t.Fatalf("%s is %s", x.Scenario, x.Status)
		}
	}
	if h.last(contracts.ServiceShipping).Knobs.Paused || h.last(contracts.ServicePayment).Knobs.Client.Acks != nil ||
		h.last(contracts.ServiceOrderGenerator).Knobs.OrdersPerSecond != nil {
		t.Fatal("knobs not back to baseline")
	}
	if h.kube.Replicas("commerce", "analytics") != 1 || h.kube.Replicas("commerce", "notification") != 1 {
		t.Fatalf("replicas analytics %d notification %d", h.kube.Replicas("commerce", "analytics"), h.kube.Replicas("commerce", "notification"))
	}
	if _, ok := h.kube.KafkaAnnotations("kafka", "kafka-lab")[pauseKey]; ok {
		t.Fatal("Kafka CR still paused")
	}
	p, _ := h.kube.Pod("kafka", "kafka-lab-dual-role-2")
	if len(p.Spec.EphemeralContainers) != 2 || !strings.HasSuffix(p.Spec.EphemeralContainers[1].Name, undoSuffix) {
		t.Fatalf("orphan helper not undone: %+v", p.Spec.EphemeralContainers)
	}
	if !contains(h.auditActions(), "panic") {
		t.Fatal("panic not audited")
	}
}

func Test_shouldReportAnIncompletePanicWhenTheClusterRefusesTheRepairs(t *testing.T) {
	// GIVEN: a manual scale and an API server that fails everything but reads of the operator
	h := newHarness(t)
	_ = h.e.Scale(ctx, contracts.ServiceAnalytics, 3)
	cl := h.kube.Client()
	v := `{"replicas":1,"owner":"x"}`
	_ = cl.SetAnnotations(ctx, k8s.DeploymentPath("kafka", "strimzi-cluster-operator"), map[string]*string{revertKey: &v})
	h.kube.Fail = func(r *http.Request) int { return http.StatusInternalServerError }
	h.pub.failSnap = errBoom

	// WHEN: panic is pressed
	_, err := h.e.Panic(ctx)

	// THEN: it reports that not everything could be undone, but chaos is disarmed anyway
	if !errors.Is(err, api.ErrUnavailable) || h.e.Armed() {
		t.Fatalf("err %v", err)
	}
}

func Test_shouldReportFailedRepairsWhenSomeCallsAreRefusedDuringPanic(t *testing.T) {
	// GIVEN: leftovers on the operator, notification and a broker, and an API server failing every PATCH
	h := newHarness(t)
	cl := h.kube.Client()
	v := `{"replicas":1,"owner":"x"}`
	for _, p := range []string{k8s.DeploymentPath("kafka", "strimzi-cluster-operator"), k8s.DeploymentPath("commerce", "notification")} {
		_ = cl.SetAnnotations(ctx, p, map[string]*string{revertKey: &v})
	}
	bad := "not json"
	_ = cl.SetAnnotations(ctx, k8s.DeploymentPath("commerce", "shipping"), map[string]*string{revertKey: &bad})
	paused := "true"
	_ = cl.SetAnnotations(ctx, k8s.KafkaPath("kafka", "kafka-lab"), map[string]*string{pauseKey: &paused})
	_ = h.e.Scale(ctx, contracts.ServiceAnalytics, 2)
	h.kube.Mu.Lock()
	delete(h.kube.Deployments["commerce/analytics"].Metadata.Annotations, revertKey)
	p := h.kube.Pods["kafka/kafka-lab-dual-role-0"]
	p.Spec.EphemeralContainers = []k8s.EphemeralContainer{{Name: "chaos-x"}}
	h.kube.Mu.Unlock()
	h.kube.Fail = func(r *http.Request) int {
		if r.Method == http.MethodPatch {
			return http.StatusInternalServerError
		}
		return 0
	}

	// WHEN: panic is pressed
	_, err := h.e.Panic(ctx)

	// THEN: the refused repairs are listed in the error
	if err == nil || !strings.Contains(err.Error(), "undo network fault") {
		t.Fatalf("err %v", err)
	}
	// AND: once the cluster accepts the calls, a second panic repairs annotations it can parse
	h.kube.Fail = func(r *http.Request) int {
		if r.Method == http.MethodPatch && strings.HasSuffix(r.URL.Path, "/deployments/notification") {
			return http.StatusInternalServerError
		}
		return 0
	}
	if _, err := h.e.Panic(ctx); err == nil || h.kube.Replicas("kafka", "strimzi-cluster-operator") != 1 {
		t.Fatalf("second panic: %v", err)
	}
}

func Test_shouldStopTheGeneratorAndResumeItWhenTheSimulationIsStoppedAndStarted(t *testing.T) {
	// GIVEN: Black Friday is running
	h := newHarness(t)
	h.start(api.StartRequest{Scenario: "black-friday", AutoRecover: true})

	// WHEN: the simulation is stopped, the engine restarts, then the simulation is started
	stopped, err1 := h.e.Simulation(ctx, "stop")
	rate := *h.last(contracts.ServiceOrderGenerator).Knobs.OrdersPerSecond
	h.restart()
	afterRestart := h.e.SimulationState()
	running, err2 := h.e.Simulation(ctx, "start")

	// THEN: stop forces 0 orders/s over the experiment, the state survives the restart, start releases it
	if err1 != nil || err2 != nil || stopped != "stopped" || rate != 0 || afterRestart != "stopped" || running != "running" {
		t.Fatalf("%s %v %s %s: %v %v", stopped, rate, afterRestart, running, err1, err2)
	}
	if h.last(contracts.ServiceOrderGenerator).Knobs.OrdersPerSecond != nil {
		t.Fatal("generator still forced")
	}
	if _, err := h.e.Simulation(ctx, "pause"); !errors.Is(err, api.ErrInvalid) {
		t.Fatalf("unknown action: %v", err)
	}
}

func Test_shouldEmptyTopicsAndGroupsAndScaleBackWhenTheSimulationIsReset(t *testing.T) {
	// GIVEN: consumers whose pods go away when scaled to 0, payment with 2 replicas
	h := newHarness(t)
	_ = h.kube.Client().SetScale(ctx, "commerce", "payment", 2)
	h.kube.Mu.Lock()
	for _, s := range consumers {
		delete(h.kube.Pods, "commerce/"+s+"-pod")
	}
	h.kube.Mu.Unlock()

	// WHEN: the simulation is reset
	state, err := h.e.Simulation(ctx, "reset")
	_, busy := h.e.Simulation(ctx, "reset")
	waitTimeline(t, h, "Simulation reset:")

	// THEN: generator stopped, every business topic and the DLQ emptied, groups moved to the end,
	// replicas restored; a second reset while one runs is refused
	if err != nil || state != "stopped" || !errors.Is(busy, api.ErrConflict) || *h.last(contracts.ServiceOrderGenerator).Knobs.OrdersPerSecond != 0 {
		t.Fatalf("state %s err %v busy %v", state, err, busy)
	}
	if len(h.admin.deleted) != len(contracts.BusinessTopics)+1 || !contains(h.admin.deleted, contracts.TopicOrdersDLQ) || len(h.admin.committed) != 5 {
		t.Fatalf("deleted %v committed %v", h.admin.deleted, h.admin.committed)
	}
	if h.kube.Replicas("commerce", "payment") != 2 || h.kube.Count("PATCH /apis/apps/v1/namespaces/commerce/deployments/payment/scale") != 3 {
		t.Fatal("payment not scaled down and back")
	}
	if _, ok := h.kube.DeploymentAnnotations("commerce", "payment")[revertKey]; ok {
		t.Fatal("reset annotation left")
	}
}

func waitTimeline(t *testing.T, h *harness, prefix string) api.TimelineEntry {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, e := range h.timeline() {
			if strings.HasPrefix(e.Title, prefix) {
				return e
			}
		}
	}
	t.Fatalf("no timeline entry %q", prefix)
	return api.TimelineEntry{}
}

func Test_shouldReportAFailedResetWhenConsumerPodsDoNotStop(t *testing.T) {
	// GIVEN: consumer pods that never terminate (scaling does not remove them in the fake)
	h := newHarness(t, func(c *Config) { c.ResetPodTimeout = 50 * time.Millisecond })

	// WHEN: the simulation is reset
	_, err := h.e.Simulation(ctx, "reset")
	e := waitTimeline(t, h, "Simulation reset failed")

	// THEN: nothing was deleted, the replicas are restored and the failure names the services
	if err != nil || len(h.admin.deleted) != 0 || !strings.Contains(e.Detail, "did not stop") || h.kube.Replicas("commerce", "payment") != 1 {
		t.Fatalf("err %v deleted %v detail %q", err, h.admin.deleted, e.Detail)
	}
}

func Test_shouldReportAFailedResetWhenKafkaRefusesTheCleanup(t *testing.T) {
	for _, tc := range []struct {
		name string
		prep func(h *harness)
		want string
	}{
		{"delete records refused", func(h *harness) { h.admin.failDel = errBoom }, "boom"},
		{"group commit refused", func(h *harness) { h.admin.failCom = errBoom }, "boom"},
		{"pods unlistable", func(h *harness) {
			h.kube.Fail = func(r *http.Request) int {
				if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/pods") {
					return 500
				}
				return 0
			}
		}, "500"},
		{"scale refused", func(h *harness) {
			h.kube.Fail = func(r *http.Request) int {
				if r.Method == http.MethodPatch {
					return 500
				}
				return 0
			}
		}, "500"},
		{"one service missing", func(h *harness) {
			h.kube.Mu.Lock()
			delete(h.kube.Deployments, "commerce/analytics")
			h.kube.Mu.Unlock()
			h.admin.failDel = errBoom
		}, "boom"},
	} {
		// GIVEN: consumer pods that stop, and the failure of the case
		h := newHarness(t)
		h.kube.Mu.Lock()
		for _, s := range consumers {
			delete(h.kube.Pods, "commerce/"+s+"-pod")
		}
		h.kube.Mu.Unlock()
		tc.prep(h)

		// WHEN: the simulation is reset
		_, _ = h.e.Simulation(ctx, "reset")
		e := waitTimeline(t, h, "Simulation reset failed")

		// THEN: the failure is reported
		if !strings.Contains(e.Detail, tc.want) {
			t.Fatalf("%s: %q", tc.name, e.Detail)
		}
	}
}

func Test_shouldRefuseAResetWhenTheClusterOrKafkaAdminIsMissing(t *testing.T) {
	// GIVEN: an engine without Kubernetes and one where publishing and persisting fail
	h := newHarness(t, func(c *Config) { c.Kube = nil })
	broken := newHarness(t)
	broken.pub.failSnap = errBoom

	// WHEN: reset is requested
	_, err1 := h.e.Simulation(ctx, "reset")
	_, err2 := broken.e.Simulation(ctx, "reset")
	broken.pub.failSnap = nil
	broken.db.Close()
	_, err3 := broken.e.Simulation(ctx, "reset")

	// THEN: it is unavailable, and a failed reset does not block the next one
	if !errors.Is(err1, api.ErrUnavailable) || !errors.Is(err2, api.ErrUnavailable) || !errors.Is(err3, api.ErrUnavailable) {
		t.Fatalf("%v / %v / %v", err1, err2, err3)
	}
}

func Test_shouldWarnOnceWhenABrokerDiskIsAlmostFull(t *testing.T) {
	// GIVEN: broker 1's volume at 74 %, broker 0 at 40 %
	usage := 0.74
	var queries int
	h := newHarness(t, func(c *Config) {
		c.Query = func(_ ctxT, q string) ([]Sample, error) {
			queries++
			if !strings.Contains(q, "kubelet_volume_stats_used_bytes") {
				return nil, errBoom
			}
			return []Sample{{Labels: map[string]string{"persistentvolumeclaim": "data-0-kafka-lab-dual-role-1"}, Value: usage},
				{Labels: map[string]string{"persistentvolumeclaim": "data-0-kafka-lab-dual-role-0"}, Value: 0.4}}, nil
		}
	})

	// WHEN: the guard runs three times a minute apart, the volume shrinks to 60 % and grows again
	h.advance(time.Minute)
	h.advance(time.Second) // too early, no query
	h.advance(time.Minute)
	usage = 0.6
	h.advance(time.Minute)
	usage = 0.8
	h.advance(time.Minute)

	// THEN: two warnings (one per crossing), none for broker 0
	var warnings int
	for _, e := range h.timeline() {
		if strings.HasPrefix(e.Title, "Broker disk data-0-kafka-lab-dual-role-1") {
			warnings++
		}
		if strings.Contains(e.Title, "role-0") {
			t.Fatal("warned about a healthy disk")
		}
	}
	if warnings != 2 || queries != 4 {
		t.Fatalf("%d warnings, %d queries", warnings, queries)
	}
}

func Test_shouldKeepRunningWhenTheDiskQueryFails(t *testing.T) {
	// GIVEN: VictoriaMetrics unreachable
	h := newHarness(t, func(c *Config) { c.Query = func(ctxT, string) ([]Sample, error) { return nil, errBoom } })

	// WHEN: the guard runs
	h.advance(time.Minute)

	// THEN: no timeline warning about disks
	for _, e := range h.timeline() {
		if strings.HasPrefix(e.Title, "Broker disk") {
			t.Fatal("unexpected warning")
		}
	}
}

func Test_shouldPersistTheArmedStateWhenChaosIsArmedAndDisarmed(t *testing.T) {
	// GIVEN: chaos armed
	h := armed(t)
	exposed := strings.Contains(h.exposition(), "lab_chaos_armed 1")

	// WHEN: the controlboard restarts, then chaos is disarmed
	h.restart()
	after := h.e.Armed()
	_ = h.e.Arm(ctx, false)

	// THEN: it stays armed across the restart and the metric follows
	if !exposed || !after || h.e.Armed() || !strings.Contains(h.exposition(), "lab_chaos_armed 0") {
		t.Fatalf("exposed %v after %v", exposed, after)
	}
	if !contains(h.auditActions(), "chaos.armed") || !contains(h.auditActions(), "chaos.disarmed") {
		t.Fatalf("audit %v", h.auditActions())
	}
}
