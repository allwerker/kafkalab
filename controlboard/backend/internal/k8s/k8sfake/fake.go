// Package k8sfake is an in-memory Kubernetes API server (httptest) with exactly the resources
// the controlboard uses: pods (incl. ephemeral containers), deployments with their scale
// subresource and annotations, and Strimzi Kafka resources (annotations only).
package k8sfake

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"

	"kafkalab/controlboard/backend/internal/k8s"
)

// Server is the fake API server. Lock Mu before touching the maps directly in tests.
type Server struct {
	URL string
	Mu  sync.Mutex
	// Pods by "namespace/name".
	Pods map[string]*k8s.Pod
	// Deployments by "namespace/name".
	Deployments map[string]*k8s.Deployment
	// Kafkas holds the annotations of Kafka resources by "namespace/name".
	Kafkas map[string]map[string]string
	// Requests logs every request as "METHOD path" (query stripped).
	Requests []string
	// Fail, when set, is asked before every request; a non-zero status is returned instead.
	Fail func(r *http.Request) int
	// OnDeletePod is called (with Mu held) after a pod was deleted, e.g. to let a "controller"
	// recreate it.
	OnDeletePod func(s *Server, ns, name string, grace *int64)
}

// New starts a fake API server that is closed when the test ends.
func New(t testing.TB) *Server {
	s := &Server{Pods: map[string]*k8s.Pod{}, Deployments: map[string]*k8s.Deployment{}, Kafkas: map[string]map[string]string{}}
	srv := httptest.NewServer(http.HandlerFunc(s.serve))
	t.Cleanup(srv.Close)
	s.URL = srv.URL
	return s
}

// Client returns a k8s client for this server.
func (s *Server) Client() *k8s.Client { return k8s.New(k8s.Config{BaseURL: s.URL}) }

// AddPod adds a ready pod with labels.
func (s *Server) AddPod(ns, name string, labels map[string]string) *k8s.Pod {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	return s.addPodLocked(ns, name, labels)
}

// AddPodLocked is AddPod for callers that hold Mu (OnDeletePod hooks).
func (s *Server) AddPodLocked(ns, name string, labels map[string]string) *k8s.Pod {
	return s.addPodLocked(ns, name, labels)
}

func (s *Server) addPodLocked(ns, name string, labels map[string]string) *k8s.Pod {
	p := &k8s.Pod{Metadata: k8s.ObjectMeta{Name: name, Namespace: ns, Labels: labels},
		Status: k8s.PodStatus{Phase: "Running", Conditions: []k8s.PodCondition{{Type: "Ready", Status: "True"}}}}
	s.Pods[ns+"/"+name] = p
	return p
}

// AddDeployment adds a deployment with replicas and labels.
func (s *Server) AddDeployment(ns, name string, replicas int, labels map[string]string) {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	d := &k8s.Deployment{Metadata: k8s.ObjectMeta{Name: name, Namespace: ns, Labels: labels, Annotations: map[string]string{}}}
	d.Spec.Replicas = replicas
	s.Deployments[ns+"/"+name] = d
}

// AddKafka adds a Kafka resource.
func (s *Server) AddKafka(ns, name string) {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	s.Kafkas[ns+"/"+name] = map[string]string{}
}

// Replicas returns a deployment's replicas (-1 if missing).
func (s *Server) Replicas(ns, name string) int {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	if d, ok := s.Deployments[ns+"/"+name]; ok {
		return d.Spec.Replicas
	}
	return -1
}

// DeploymentAnnotations returns a copy of a deployment's annotations.
func (s *Server) DeploymentAnnotations(ns, name string) map[string]string {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	return copyMap(s.Deployments[ns+"/"+name].Metadata.Annotations)
}

// KafkaAnnotations returns a copy of a Kafka resource's annotations.
func (s *Server) KafkaAnnotations(ns, name string) map[string]string {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	return copyMap(s.Kafkas[ns+"/"+name])
}

// Pod returns a copy of a pod and whether it exists.
func (s *Server) Pod(ns, name string) (k8s.Pod, bool) {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	p, ok := s.Pods[ns+"/"+name]
	if !ok {
		return k8s.Pod{}, false
	}
	return *p, true
}

// FinishEphemeral marks an ephemeral container as terminated (the helper script ended).
func (s *Server) FinishEphemeral(ns, pod, container string) {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	p := s.Pods[ns+"/"+pod]
	for i := range p.Status.EphemeralContainerStatuses {
		if p.Status.EphemeralContainerStatuses[i].Name == container {
			p.Status.EphemeralContainerStatuses[i].State = k8s.ContainerState{Terminated: &struct {
				ExitCode int `json:"exitCode"`
			}{}}
		}
	}
}

// Count returns how many logged requests start with prefix (e.g. "DELETE /api/v1/namespaces/kafka/pods").
func (s *Server) Count(prefix string) int {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	n := 0
	for _, r := range s.Requests {
		if strings.HasPrefix(r, prefix) {
			n++
		}
	}
	return n
}

func copyMap(m map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range m {
		out[k] = v
	}
	return out
}

func (s *Server) serve(w http.ResponseWriter, r *http.Request) {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	s.Requests = append(s.Requests, r.Method+" "+r.URL.Path)
	if s.Fail != nil {
		if code := s.Fail(r); code != 0 {
			writeStatus(w, code, "injected failure")
			return
		}
	}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	switch {
	case len(parts) >= 5 && parts[0] == "api" && parts[4] == "pods":
		s.pods(w, r, parts[3], parts[5:])
	case len(parts) >= 6 && parts[0] == "apis" && parts[1] == "apps" && parts[5] == "deployments":
		s.deployments(w, r, parts[4], parts[6:])
	case len(parts) == 7 && parts[0] == "apis" && parts[1] == "kafka.strimzi.io" && parts[5] == "kafkas":
		s.kafka(w, r, parts[4]+"/"+parts[6])
	default:
		writeStatus(w, http.StatusNotFound, "unsupported path")
	}
}

func (s *Server) pods(w http.ResponseWriter, r *http.Request, ns string, rest []string) {
	if len(rest) == 0 {
		var items []k8s.Pod
		sel := parseSelector(r.URL.Query().Get("labelSelector"))
		for key, p := range s.Pods {
			if strings.HasPrefix(key, ns+"/") && matches(p.Metadata.Labels, sel) {
				items = append(items, *p)
			}
		}
		sort.Slice(items, func(i, j int) bool { return items[i].Metadata.Name < items[j].Metadata.Name })
		writeJSON(w, map[string]any{"items": items})
		return
	}
	key := ns + "/" + rest[0]
	p, ok := s.Pods[key]
	if !ok {
		writeStatus(w, http.StatusNotFound, "pods \""+rest[0]+"\" not found")
		return
	}
	switch {
	case len(rest) == 2 && rest[1] == "ephemeralcontainers" && r.Method == http.MethodPatch:
		var patch struct {
			Spec k8s.PodSpec `json:"spec"`
		}
		_ = json.NewDecoder(r.Body).Decode(&patch)
		for _, ec := range patch.Spec.EphemeralContainers {
			p.Spec.EphemeralContainers = append(p.Spec.EphemeralContainers, ec)
			p.Status.EphemeralContainerStatuses = append(p.Status.EphemeralContainerStatuses,
				k8s.ContainerStatus{Name: ec.Name, State: k8s.ContainerState{Running: &struct{}{}}})
		}
		writeJSON(w, p)
	case r.Method == http.MethodDelete:
		var body struct {
			GracePeriodSeconds *int64 `json:"gracePeriodSeconds"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		delete(s.Pods, key)
		if s.OnDeletePod != nil {
			s.OnDeletePod(s, ns, rest[0], body.GracePeriodSeconds)
		}
		writeJSON(w, p)
	default:
		writeJSON(w, p)
	}
}

func (s *Server) deployments(w http.ResponseWriter, r *http.Request, ns string, rest []string) {
	if len(rest) == 0 {
		var items []k8s.Deployment
		sel := parseSelector(r.URL.Query().Get("labelSelector"))
		for key, d := range s.Deployments {
			if strings.HasPrefix(key, ns+"/") && matches(d.Metadata.Labels, sel) {
				items = append(items, *d)
			}
		}
		sort.Slice(items, func(i, j int) bool { return items[i].Metadata.Name < items[j].Metadata.Name })
		writeJSON(w, map[string]any{"items": items})
		return
	}
	d, ok := s.Deployments[ns+"/"+rest[0]]
	if !ok {
		writeStatus(w, http.StatusNotFound, "deployments.apps \""+rest[0]+"\" not found")
		return
	}
	var patch struct {
		Metadata struct {
			Annotations map[string]*string `json:"annotations"`
		} `json:"metadata"`
		Spec struct {
			Replicas *int `json:"replicas"`
		} `json:"spec"`
	}
	if r.Method == http.MethodPatch {
		_ = json.NewDecoder(r.Body).Decode(&patch)
		if patch.Spec.Replicas != nil {
			d.Spec.Replicas = *patch.Spec.Replicas
		}
		mergeAnnotations(d.Metadata.Annotations, patch.Metadata.Annotations)
	}
	if len(rest) == 2 && rest[1] == "scale" {
		writeJSON(w, map[string]any{"spec": map[string]int{"replicas": d.Spec.Replicas}})
		return
	}
	writeJSON(w, d)
}

func (s *Server) kafka(w http.ResponseWriter, r *http.Request, key string) {
	ann, ok := s.Kafkas[key]
	if !ok {
		writeStatus(w, http.StatusNotFound, "kafkas \""+key+"\" not found")
		return
	}
	if r.Method == http.MethodPatch {
		var patch struct {
			Metadata struct {
				Annotations map[string]*string `json:"annotations"`
			} `json:"metadata"`
		}
		_ = json.NewDecoder(r.Body).Decode(&patch)
		mergeAnnotations(ann, patch.Metadata.Annotations)
	}
	writeJSON(w, map[string]any{"metadata": map[string]any{"annotations": ann}})
}

func mergeAnnotations(dst map[string]string, patch map[string]*string) {
	for k, v := range patch {
		if v == nil {
			delete(dst, k)
		} else {
			dst[k] = *v
		}
	}
}

func parseSelector(s string) map[string]string {
	out := map[string]string{}
	for _, kv := range strings.Split(s, ",") {
		if k, v, ok := strings.Cut(kv, "="); ok {
			out[k] = v
		}
	}
	return out
}

func matches(labels, sel map[string]string) bool {
	for k, v := range sel {
		if labels[k] != v {
			return false
		}
	}
	return true
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func writeStatus(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{"kind": "Status", "code": code, "message": msg})
}
