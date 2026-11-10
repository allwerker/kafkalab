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

func Test_shouldRestoreTheOperatorWhenTheBrokerPodCannotBeStopped(t *testing.T) {
	// GIVEN: armed chaos and an API server that refuses pod deletions
	h := armed(t)
	h.kube.Fail = func(r *http.Request) int {
		if r.Method == http.MethodDelete {
			return http.StatusForbidden
		}
		return 0
	}

	// WHEN: a broker failure and a consumer crash loop are started
	_, err1 := h.e.Start(ctx, api.StartRequest{Scenario: "kafka-broker-failure", AutoRecover: true, Confirm: "kafka-broker-failure"})
	_, err2 := h.e.Start(ctx, api.StartRequest{Scenario: "consumer-crash-loop", AutoRecover: true})

	// THEN: both fail, and the operator that was already scaled down is running again
	if !errors.Is(err1, api.ErrUnavailable) || !errors.Is(err2, api.ErrUnavailable) || h.kube.Replicas("kafka", "strimzi-cluster-operator") != 1 {
		t.Fatalf("%v %v operator %d", err1, err2, h.kube.Replicas("kafka", "strimzi-cluster-operator"))
	}
}

func Test_shouldFailARegionalOutageWhenTheRevertDataCannotBeStored(t *testing.T) {
	// GIVEN: armed chaos and an API server that cannot read the payment deployment
	h := armed(t)
	h.kube.Fail = func(r *http.Request) int {
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/deployments/payment") {
			return 500
		}
		return 0
	}

	// WHEN: region-a goes down
	_, err := h.e.Start(ctx, api.StartRequest{Scenario: "regional-outage", AutoRecover: true, Confirm: "regional-outage"})

	// THEN: it fails before scaling anything
	if !errors.Is(err, api.ErrUnavailable) || h.kube.Replicas("commerce", "payment") != 1 {
		t.Fatalf("%v", err)
	}
}

func Test_shouldLeaveAForeignRevertAnnotationAloneWhenAnExperimentEnds(t *testing.T) {
	// GIVEN: the operator carries revert data of a manual change
	h := armed(t)
	v := `{"replicas":1,"owner":"manual"}`
	_ = h.kube.Client().SetAnnotations(ctx, k8s.DeploymentPath("kafka", "strimzi-cluster-operator"), map[string]*string{revertKey: &v})

	// WHEN: a broker failure runs and is stopped
	x := h.start(api.StartRequest{Scenario: "kafka-broker-failure", AutoRecover: true, Confirm: "kafka-broker-failure"})
	_, _ = h.e.Stop(ctx, x.ID)

	// THEN: the operator is scaled back, the foreign annotation is still there
	if h.kube.Replicas("kafka", "strimzi-cluster-operator") != 1 || h.kube.DeploymentAnnotations("kafka", "strimzi-cluster-operator")[revertKey] != v {
		t.Fatal("foreign annotation touched")
	}
}

func Test_shouldNotTakeOverAnExperimentsRevertDataWhenTheUserScalesDuringIt(t *testing.T) {
	// GIVEN: a regional outage holds payment at 0 with its revert annotation
	h := armed(t)
	x := h.start(api.StartRequest{Scenario: "regional-outage", AutoRecover: true, Confirm: "regional-outage"})

	// WHEN: the user scales payment to 1 and back to 0 (the API cannot read the deployment by then)
	err1 := h.e.Scale(ctx, contracts.ServicePayment, 1)
	h.kube.Fail = func(r *http.Request) int {
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/deployments/inventory") {
			return 500
		}
		return 0
	}
	_ = h.e.Scale(ctx, contracts.ServiceInventory, 2)
	h.kube.Fail = nil
	err2 := h.e.Scale(ctx, contracts.ServicePayment, 0)

	// THEN: the experiment still owns the annotation
	if err1 != nil || err2 != nil || !strings.Contains(h.kube.DeploymentAnnotations("commerce", "payment")[revertKey], x.ID) {
		t.Fatalf("%v %v %v", err1, err2, h.kube.DeploymentAnnotations("commerce", "payment"))
	}
}

func Test_shouldIgnoreAFailedAnnotationCleanupWhenTheUserScalesBack(t *testing.T) {
	// GIVEN: analytics scaled to 3 by the user
	h := newHarness(t)
	_ = h.e.Scale(ctx, contracts.ServiceAnalytics, 3)
	h.kube.Fail = func(r *http.Request) int {
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/deployments/analytics") {
			return 500
		}
		return 0
	}

	// WHEN: it is scaled back to 1 while its annotation cannot be read
	err := h.e.Scale(ctx, contracts.ServiceAnalytics, 1)

	// THEN: the scale succeeds (the stale annotation is repaired by the next panic)
	if err != nil || h.kube.Replicas("commerce", "analytics") != 1 {
		t.Fatalf("%v", err)
	}
}

func Test_shouldStillRevealAnExpiredDrillWhenItCannotBePersisted(t *testing.T) {
	// GIVEN: an unsolved drill and a database that went away
	h, _ := drillHarness(t)
	h.db.Close()

	// WHEN: the drill's experiment runs out and the system recovers
	h.advance(5 * time.Minute)
	h.appliedAll()
	h.advance(10 * time.Second)

	// THEN: the experiment is shown with its real scenario
	if x := h.e.List()[0]; x.Scenario != "black-friday" {
		t.Fatalf("%+v", x)
	}
}

func Test_shouldStopFlippingWhenTheClusterRefusesARebalanceStep(t *testing.T) {
	// GIVEN: a rebalance storm and an API server that starts refusing scale changes
	h := newHarness(t)
	h.start(api.StartRequest{Scenario: "rebalance-storm", AutoRecover: true})
	h.kube.Fail = func(r *http.Request) int {
		if r.Method == http.MethodPatch {
			return 500
		}
		return 0
	}

	// WHEN: the next flip is due
	h.advance(8 * time.Second)

	// THEN: the experiment keeps running at its last size
	if h.e.List()[0].Status != api.StatusRunning || h.kube.Replicas("commerce", "payment") != 2 {
		t.Fatalf("replicas %d", h.kube.Replicas("commerce", "payment"))
	}
}
