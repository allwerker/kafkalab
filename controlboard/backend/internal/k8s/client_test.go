package k8s_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"kafkalab/controlboard/backend/internal/k8s"
	"kafkalab/controlboard/backend/internal/k8s/k8sfake"
)

var ctx = context.Background()

func Test_shouldListOnlyTheServicesPodsWhenSelectingByLabel(t *testing.T) {
	// GIVEN: two payment pods and one inventory pod in the commerce namespace
	api := k8sfake.New(t)
	api.AddPod("commerce", "payment-a", map[string]string{"app.kubernetes.io/name": "payment"})
	api.AddPod("commerce", "payment-b", map[string]string{"app.kubernetes.io/name": "payment"})
	api.AddPod("commerce", "inventory-a", map[string]string{"app.kubernetes.io/name": "inventory"})

	// WHEN: the payment pods are listed
	pods, err := api.Client().ListPods(ctx, "commerce", "app.kubernetes.io/name=payment")

	// THEN: exactly the two ready payment pods come back
	if err != nil || len(pods) != 2 || pods[0].Metadata.Name != "payment-a" || !pods[1].Ready() {
		t.Fatalf("pods %+v, err %v", pods, err)
	}
}

func Test_shouldKillAPodImmediatelyWhenDeletingWithZeroGrace(t *testing.T) {
	// GIVEN: a running broker pod
	api := k8sfake.New(t)
	api.AddPod("kafka", "kafka-lab-dual-role-1", nil)
	var grace *int64
	api.OnDeletePod = func(_ *k8sfake.Server, _, _ string, g *int64) { grace = g }
	zero := int64(0)

	// WHEN: it is deleted without grace period
	err := api.Client().DeletePod(ctx, "kafka", "kafka-lab-dual-role-1", &zero)

	// THEN: the API server got gracePeriodSeconds=0 and the pod is gone
	_, exists := api.Pod("kafka", "kafka-lab-dual-role-1")
	if err != nil || grace == nil || *grace != 0 || exists {
		t.Fatalf("err %v grace %v exists %v", err, grace, exists)
	}
}

func Test_shouldReportNotFoundWhenThePodIsAlreadyGone(t *testing.T) {
	// GIVEN: no broker pod (Strimzi has not recreated it yet)
	api := k8sfake.New(t)

	// WHEN: the pod is read and deleted gracefully
	_, getErr := api.Client().GetPod(ctx, "kafka", "kafka-lab-dual-role-1")
	delErr := api.Client().DeletePod(ctx, "kafka", "kafka-lab-dual-role-1", nil)

	// THEN: both calls say "not found" in a way callers can test for
	var se *k8s.StatusError
	if !errors.Is(getErr, k8s.ErrNotFound) || !errors.Is(delErr, k8s.ErrNotFound) || !errors.As(getErr, &se) || se.Code != 404 {
		t.Fatalf("get %v delete %v", getErr, delErr)
	}
	if !strings.Contains(getErr.Error(), "not found") {
		t.Fatalf("message %q", getErr)
	}
}

func Test_shouldStartTheNetworkHelperWhenAnEphemeralContainerIsAdded(t *testing.T) {
	// GIVEN: a running broker pod
	api := k8sfake.New(t)
	api.AddPod("kafka", "kafka-lab-dual-role-1", nil)
	root := int64(0)
	ec := k8s.EphemeralContainer{Name: "chaos-e1", Image: "chaos-net:1", Env: []k8s.EnvVar{{Name: "MODE", Value: "partition"}},
		SecurityContext: &k8s.SecurityContext{RunAsUser: &root, Capabilities: &k8s.Capabilities{Add: []string{"NET_ADMIN"}}}}

	// WHEN: the chaos helper is injected
	err := api.Client().AddEphemeralContainer(ctx, "kafka", "kafka-lab-dual-role-1", ec)

	// THEN: the pod runs it with NET_ADMIN, and it counts as running until it terminates
	p, _ := api.Client().GetPod(ctx, "kafka", "kafka-lab-dual-role-1")
	if err != nil || p.Spec.EphemeralContainers[0].SecurityContext.Capabilities.Add[0] != "NET_ADMIN" || !p.EphemeralRunning("chaos-e1") {
		t.Fatalf("pod %+v err %v", p, err)
	}
	if got := p.RunningEphemeral("chaos-"); len(got) != 1 {
		t.Fatalf("running %v", got)
	}
	api.FinishEphemeral("kafka", "kafka-lab-dual-role-1", "chaos-e1")
	p, _ = api.Client().GetPod(ctx, "kafka", "kafka-lab-dual-role-1")
	if p.EphemeralRunning("chaos-e1") || p.EphemeralRunning("chaos-other") || len(p.RunningEphemeral("chaos-")) != 0 {
		t.Fatalf("still running: %+v", p.Status)
	}
}

func Test_shouldTreatAnInjectedContainerAsRunningWhenItHasNoStatusYet(t *testing.T) {
	// GIVEN: a pod whose spec lists the helper but the kubelet has not reported it yet
	p := k8s.Pod{Spec: k8s.PodSpec{EphemeralContainers: []k8s.EphemeralContainer{{Name: "chaos-e2"}}}}

	// WHEN: asked whether the helper is running
	running := p.EphemeralRunning("chaos-e2")

	// THEN: it is assumed to be starting (never "already done")
	if !running {
		t.Fatal("expected running")
	}
}

func Test_shouldNotCountAPodAsReadyWhenItIsTerminatingOrHasNoReadyCondition(t *testing.T) {
	// GIVEN: a terminating pod, a pod without conditions and an unready pod
	now := time.Now()
	terminating := k8s.Pod{Metadata: k8s.ObjectMeta{DeletionTimestamp: &now}}
	fresh := k8s.Pod{}
	unready := k8s.Pod{Status: k8s.PodStatus{Conditions: []k8s.PodCondition{{Type: "PodScheduled", Status: "True"}, {Type: "Ready", Status: "False"}}}}

	// WHEN / THEN: none of them is ready
	if terminating.Ready() || fresh.Ready() || unready.Ready() {
		t.Fatal("expected not ready")
	}
}

func Test_shouldScaleADeploymentWhenTheScaleSubresourceIsPatched(t *testing.T) {
	// GIVEN: the payment deployment with 2 replicas
	api := k8sfake.New(t)
	api.AddDeployment("commerce", "payment", 2, nil)
	cl := api.Client()

	// WHEN: it is scaled to 0 and read back
	before, err1 := cl.GetScale(ctx, "commerce", "payment")
	err2 := cl.SetScale(ctx, "commerce", "payment", 0)
	after, err3 := cl.GetScale(ctx, "commerce", "payment")

	// THEN: the replicas went from 2 to 0
	if err1 != nil || err2 != nil || err3 != nil || before != 2 || after != 0 {
		t.Fatalf("before %d after %d: %v %v %v", before, after, err1, err2, err3)
	}
}

func Test_shouldRememberAndRemoveRevertDataWhenAnnotatingObjects(t *testing.T) {
	// GIVEN: the Strimzi operator deployment and the Kafka resource
	api := k8sfake.New(t)
	api.AddDeployment("kafka", "strimzi-cluster-operator", 1, map[string]string{"app": "strimzi"})
	api.AddKafka("kafka", "kafka-lab")
	cl := api.Client()
	v := `{"replicas":1}`
	paused := "true"

	// WHEN: revert data is stored on the deployment, the Kafka CR is paused, then both are cleaned up
	err1 := cl.SetAnnotations(ctx, k8s.DeploymentPath("kafka", "strimzi-cluster-operator"), map[string]*string{"lab.kafka/revert": &v})
	err2 := cl.SetAnnotations(ctx, k8s.KafkaPath("kafka", "kafka-lab"), map[string]*string{"strimzi.io/pause-reconciliation": &paused})
	stored, err3 := cl.Annotations(ctx, k8s.DeploymentPath("kafka", "strimzi-cluster-operator"))
	deps, err4 := cl.ListDeployments(ctx, "kafka", "app=strimzi")
	err5 := cl.SetAnnotations(ctx, k8s.KafkaPath("kafka", "kafka-lab"), map[string]*string{"strimzi.io/pause-reconciliation": nil})
	kafkaAnn, err6 := cl.Annotations(ctx, k8s.KafkaPath("kafka", "kafka-lab"))

	// THEN: the revert data is readable directly and via the list, and the pause annotation is gone
	for _, err := range []error{err1, err2, err3, err4, err5, err6} {
		if err != nil {
			t.Fatal(err)
		}
	}
	if stored["lab.kafka/revert"] != v || len(deps) != 1 || deps[0].Metadata.Annotations["lab.kafka/revert"] != v || deps[0].Spec.Replicas != 1 {
		t.Fatalf("stored %v deps %+v", stored, deps)
	}
	if _, ok := kafkaAnn["strimzi.io/pause-reconciliation"]; ok {
		t.Fatalf("pause annotation still there: %v", kafkaAnn)
	}
	if all, err := cl.ListDeployments(ctx, "kafka", ""); err != nil || len(all) != 1 {
		t.Fatalf("all %v %v", all, err)
	}
}

func Test_shouldSendTheBearerTokenWhenOneIsConfigured(t *testing.T) {
	// GIVEN: an API server that records the Authorization header
	var auth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth = r.Header.Get("Authorization")
		_, _ = w.Write([]byte(`{"items":[]}`))
	}))
	defer srv.Close()
	cl := k8s.New(k8s.Config{BaseURL: srv.URL + "/", Token: func() (string, error) { return "sa-token", nil }})

	// WHEN: pods are listed
	_, err := cl.ListPods(ctx, "kafka", "x=y")

	// THEN: the ServiceAccount token was presented
	if err != nil || auth != "Bearer sa-token" {
		t.Fatalf("auth %q err %v", auth, err)
	}
}

func Test_shouldFailWithoutCallingTheServerWhenTheTokenCannotBeRead(t *testing.T) {
	// GIVEN: a client whose token file vanished
	cl := k8s.New(k8s.Config{BaseURL: "http://127.0.0.1:1", Token: func() (string, error) { return "", os.ErrNotExist }})

	// WHEN: a pod is read
	_, err := cl.GetPod(ctx, "kafka", "p")

	// THEN: the error names the token
	if err == nil || !strings.Contains(err.Error(), "token") {
		t.Fatalf("err %v", err)
	}
}

func Test_shouldReturnTheServerMessageWhenTheCallIsForbidden(t *testing.T) {
	// GIVEN: an API server that rejects the call (RBAC) with a plain-text body, and one with a Status body
	plain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte("forbidden: no rights"))
	}))
	defer plain.Close()

	// WHEN: a deployment is scaled
	err := k8s.New(k8s.Config{BaseURL: plain.URL}).SetScale(ctx, "kafka", "x", 1)

	// THEN: the error carries status and text, and is not a not-found
	var se *k8s.StatusError
	if !errors.As(err, &se) || se.Code != 403 || se.Message != "forbidden: no rights" || errors.Is(err, k8s.ErrNotFound) {
		t.Fatalf("err %v", err)
	}
}

func Test_shouldFailWhenTheServerAnswersGarbageOrIsUnreachable(t *testing.T) {
	// GIVEN: an API server that answers non-JSON, a truncated body, and an unreachable one
	garbage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("<html>")) }))
	defer garbage.Close()
	truncated := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "100")
		_, _ = w.Write([]byte(`{"items"`))
	}))
	defer truncated.Close()

	// WHEN: pods are read from each
	_, err1 := k8s.New(k8s.Config{BaseURL: garbage.URL}).GetPod(ctx, "kafka", "p")
	_, err2 := k8s.New(k8s.Config{BaseURL: truncated.URL}).ListPods(ctx, "kafka", "")
	_, err3 := k8s.New(k8s.Config{BaseURL: "http://127.0.0.1:1"}).GetScale(ctx, "commerce", "payment")
	_, err4 := k8s.New(k8s.Config{BaseURL: "http://bad\x7f"}).GetScale(ctx, "commerce", "payment")

	// THEN: every call fails instead of returning an empty object
	if err1 == nil || !strings.Contains(err1.Error(), "decode") || err2 == nil || err3 == nil || err4 == nil {
		t.Fatalf("errors %v / %v / %v / %v", err1, err2, err3, err4)
	}
}

func Test_shouldBuildAnInClusterConfigWhenTheServiceAccountIsMounted(t *testing.T) {
	// GIVEN: a mounted ServiceAccount (CA + token) and the in-cluster environment
	dir := t.TempDir()
	writeCA(t, filepath.Join(dir, "ca.crt"))
	_ = os.WriteFile(filepath.Join(dir, "token"), []byte("tok\n"), 0o600)
	env := map[string]string{"KUBERNETES_SERVICE_HOST": "10.43.0.1", "KUBERNETES_SERVICE_PORT": "443"}

	// WHEN: the config is built
	cfg, err := k8s.InCluster(dir, func(k string) string { return env[k] })

	// THEN: it points at the API service with TLS and reads the (rotating) token on demand
	if err != nil || cfg.BaseURL != "https://10.43.0.1:443" || cfg.HTTP == nil {
		t.Fatalf("cfg %+v err %v", cfg, err)
	}
	if tok, err := cfg.Token(); err != nil || tok != "tok" {
		t.Fatalf("token %q %v", tok, err)
	}
}

func Test_shouldRefuseAnInClusterConfigWhenSomethingIsMissing(t *testing.T) {
	// GIVEN: no environment, a missing CA, an invalid CA and a missing token
	env := map[string]string{"KUBERNETES_SERVICE_HOST": "10.43.0.1", "KUBERNETES_SERVICE_PORT": "443"}
	getenv := func(k string) string { return env[k] }
	noCA := t.TempDir()
	badCA := t.TempDir()
	_ = os.WriteFile(filepath.Join(badCA, "ca.crt"), []byte("nope"), 0o600)
	noToken := t.TempDir()
	writeCA(t, filepath.Join(noToken, "ca.crt"))

	// WHEN: configs are built for each case
	_, e1 := k8s.InCluster(noCA, func(string) string { return "" })
	_, e2 := k8s.InCluster(noCA, getenv)
	_, e3 := k8s.InCluster(badCA, getenv)
	_, e4 := k8s.InCluster(noToken, getenv)

	// THEN: every case is refused with a reason
	if e1 == nil || e2 == nil || e3 == nil || e4 == nil {
		t.Fatalf("%v %v %v %v", e1, e2, e3, e4)
	}
}

func writeCA(t *testing.T, path string) {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "k3s"}, NotBefore: time.Now(), NotAfter: time.Now().Add(time.Hour), IsCA: true}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	_ = os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600)
}
