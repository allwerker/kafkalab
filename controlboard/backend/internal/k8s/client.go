// Package k8s is a deliberately tiny Kubernetes REST client for the controlboard's chaos
// actions: pods (get, list, delete, ephemeral containers), deployment scale and annotations
// on a handful of objects. It speaks plain JSON over net/http with the in-cluster
// ServiceAccount token, so the controlboard needs no client-go. Everything it may touch is
// limited by the RBAC allowlist in deploy/charts/controlboard-rbac.
package k8s

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// ErrNotFound is returned (wrapped) when the API server answers 404.
var ErrNotFound = errors.New("kubernetes object not found")

// StatusError is a non-2xx answer of the API server.
type StatusError struct {
	Code    int
	Message string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("kubernetes api: %d %s", e.Code, e.Message)
}

// Unwrap maps 404 to ErrNotFound.
func (e *StatusError) Unwrap() error {
	if e.Code == http.StatusNotFound {
		return ErrNotFound
	}
	return nil
}

// Config configures New.
type Config struct {
	// BaseURL of the API server, e.g. https://10.43.0.1:443.
	BaseURL string
	// Token returns the bearer token; it is re-read on every request because projected
	// ServiceAccount tokens rotate.
	Token func() (string, error)
	// HTTP is the client to use (TLS configured); nil uses a client with a 10 s timeout.
	HTTP *http.Client
}

// Client talks to the Kubernetes API.
type Client struct {
	c Config
}

// New returns a client.
func New(c Config) *Client {
	if c.HTTP == nil {
		c.HTTP = &http.Client{Timeout: 10 * time.Second}
	}
	if c.Token == nil {
		c.Token = func() (string, error) { return "", nil }
	}
	c.BaseURL = strings.TrimRight(c.BaseURL, "/")
	return &Client{c: c}
}

// ServiceAccountDir is where Kubernetes mounts the pod's ServiceAccount credentials.
const ServiceAccountDir = "/var/run/secrets/kubernetes.io/serviceaccount"

// InCluster builds a Config from the ServiceAccount files in dir and the
// KUBERNETES_SERVICE_HOST/PORT environment (getenv).
func InCluster(dir string, getenv func(string) string) (Config, error) {
	host, port := getenv("KUBERNETES_SERVICE_HOST"), getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return Config{}, errors.New("not running in a cluster: KUBERNETES_SERVICE_HOST/PORT unset")
	}
	ca, err := os.ReadFile(filepath.Join(dir, "ca.crt"))
	if err != nil {
		return Config{}, fmt.Errorf("service account ca: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return Config{}, errors.New("service account ca: no certificate found")
	}
	tokenFile := filepath.Join(dir, "token")
	if _, err := os.Stat(tokenFile); err != nil {
		return Config{}, fmt.Errorf("service account token: %w", err)
	}
	return Config{
		BaseURL: "https://" + net.JoinHostPort(host, port),
		Token: func() (string, error) {
			b, err := os.ReadFile(tokenFile)
			return strings.TrimSpace(string(b)), err
		},
		HTTP: &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{
			TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12},
		}},
	}, nil
}

// Content types for PATCH.
const (
	MergePatch     = "application/merge-patch+json"
	StrategicPatch = "application/strategic-merge-patch+json"
)

func (c *Client) do(ctx context.Context, method, path, contentType string, body, out any) error {
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body) // bodies are maps and structs of plain values only
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.c.BaseURL+path, rd)
	if err != nil {
		return err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	req.Header.Set("Accept", "application/json")
	tok, err := c.c.Token()
	if err != nil {
		return fmt.Errorf("kubernetes token: %w", err)
	}
	if tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	resp, err := c.c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode/100 != 2 {
		var st struct {
			Message string `json:"message"`
		}
		_ = json.Unmarshal(raw, &st)
		if st.Message == "" {
			st.Message = strings.TrimSpace(string(raw))
		}
		return &StatusError{Code: resp.StatusCode, Message: st.Message}
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("kubernetes api: decode %s %s: %w", method, path, err)
	}
	return nil
}

// ObjectMeta is the subset of metadata the controlboard reads.
type ObjectMeta struct {
	Name              string            `json:"name"`
	Namespace         string            `json:"namespace,omitempty"`
	Labels            map[string]string `json:"labels,omitempty"`
	Annotations       map[string]string `json:"annotations,omitempty"`
	DeletionTimestamp *time.Time        `json:"deletionTimestamp,omitempty"`
}

// EnvVar is a container environment variable.
type EnvVar struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// Capabilities of a container.
type Capabilities struct {
	Add []string `json:"add,omitempty"`
}

// SecurityContext of a container.
type SecurityContext struct {
	RunAsUser    *int64        `json:"runAsUser,omitempty"`
	Capabilities *Capabilities `json:"capabilities,omitempty"`
}

// EphemeralContainer is a debug container added to a running pod.
type EphemeralContainer struct {
	Name            string           `json:"name"`
	Image           string           `json:"image"`
	Env             []EnvVar         `json:"env,omitempty"`
	SecurityContext *SecurityContext `json:"securityContext,omitempty"`
}

// PodSpec is the subset of the pod spec the controlboard reads.
type PodSpec struct {
	EphemeralContainers []EphemeralContainer `json:"ephemeralContainers,omitempty"`
}

// ContainerState is one of running, terminated or waiting.
type ContainerState struct {
	Running    *struct{} `json:"running,omitempty"`
	Terminated *struct {
		ExitCode int `json:"exitCode"`
	} `json:"terminated,omitempty"`
	Waiting *struct {
		Reason string `json:"reason"`
	} `json:"waiting,omitempty"`
}

// ContainerStatus is the status of one container.
type ContainerStatus struct {
	Name  string         `json:"name"`
	Ready bool           `json:"ready"`
	State ContainerState `json:"state"`
}

// PodCondition is one pod condition (Ready, ...).
type PodCondition struct {
	Type   string `json:"type"`
	Status string `json:"status"`
}

// PodStatus is the subset of the pod status the controlboard reads.
type PodStatus struct {
	Phase                      string            `json:"phase"`
	Conditions                 []PodCondition    `json:"conditions,omitempty"`
	EphemeralContainerStatuses []ContainerStatus `json:"ephemeralContainerStatuses,omitempty"`
}

// Pod is a Kubernetes pod.
type Pod struct {
	Metadata ObjectMeta `json:"metadata"`
	Spec     PodSpec    `json:"spec"`
	Status   PodStatus  `json:"status"`
}

// Ready reports whether the pod is ready and not being deleted.
func (p Pod) Ready() bool {
	if p.Metadata.DeletionTimestamp != nil {
		return false
	}
	for _, c := range p.Status.Conditions {
		if c.Type == "Ready" {
			return c.Status == "True"
		}
	}
	return false
}

// EphemeralRunning reports whether the named ephemeral container is (still) running or about
// to start.
func (p Pod) EphemeralRunning(name string) bool {
	for _, s := range p.Status.EphemeralContainerStatuses {
		if s.Name == name {
			return s.State.Terminated == nil
		}
	}
	// Added to the spec but no status yet: it is starting.
	for _, c := range p.Spec.EphemeralContainers {
		if c.Name == name {
			return true
		}
	}
	return false
}

// RunningEphemeral returns the names of ephemeral containers with the prefix that may still run.
func (p Pod) RunningEphemeral(prefix string) []string {
	var out []string
	for _, c := range p.Spec.EphemeralContainers {
		if strings.HasPrefix(c.Name, prefix) && p.EphemeralRunning(c.Name) {
			out = append(out, c.Name)
		}
	}
	return out
}

func podsPath(ns string) string { return "/api/v1/namespaces/" + url.PathEscape(ns) + "/pods" }

// PodPath returns the API path of a pod.
func PodPath(ns, name string) string { return podsPath(ns) + "/" + url.PathEscape(name) }

// DeploymentPath returns the API path of a deployment.
func DeploymentPath(ns, name string) string {
	return "/apis/apps/v1/namespaces/" + url.PathEscape(ns) + "/deployments/" + url.PathEscape(name)
}

// KafkaPath returns the API path of a Strimzi Kafka resource.
func KafkaPath(ns, name string) string {
	return "/apis/kafka.strimzi.io/v1/namespaces/" + url.PathEscape(ns) + "/kafkas/" + url.PathEscape(name)
}

// GetPod returns one pod.
func (c *Client) GetPod(ctx context.Context, ns, name string) (Pod, error) {
	var p Pod
	err := c.do(ctx, http.MethodGet, PodPath(ns, name), "", nil, &p)
	return p, err
}

// ListPods returns the pods matching a label selector (e.g. "app.kubernetes.io/name=payment").
func (c *Client) ListPods(ctx context.Context, ns, selector string) ([]Pod, error) {
	var list struct {
		Items []Pod `json:"items"`
	}
	err := c.do(ctx, http.MethodGet, podsPath(ns)+"?labelSelector="+url.QueryEscape(selector), "", nil, &list)
	return list.Items, err
}

// DeletePod deletes a pod. grace nil uses the pod's default grace period, 0 kills it.
func (c *Client) DeletePod(ctx context.Context, ns, name string, grace *int64) error {
	var body any
	if grace != nil {
		body = map[string]any{"gracePeriodSeconds": *grace}
	}
	return c.do(ctx, http.MethodDelete, PodPath(ns, name), "application/json", body, nil)
}

// AddEphemeralContainer injects an ephemeral container into a running pod.
func (c *Client) AddEphemeralContainer(ctx context.Context, ns, pod string, ec EphemeralContainer) error {
	patch := map[string]any{"spec": map[string]any{"ephemeralContainers": []EphemeralContainer{ec}}}
	return c.do(ctx, http.MethodPatch, PodPath(ns, pod)+"/ephemeralcontainers", StrategicPatch, patch, nil)
}

// GetScale returns a deployment's desired replicas.
func (c *Client) GetScale(ctx context.Context, ns, deployment string) (int, error) {
	var s struct {
		Spec struct {
			Replicas int `json:"replicas"`
		} `json:"spec"`
	}
	err := c.do(ctx, http.MethodGet, DeploymentPath(ns, deployment)+"/scale", "", nil, &s)
	return s.Spec.Replicas, err
}

// SetScale sets a deployment's desired replicas.
func (c *Client) SetScale(ctx context.Context, ns, deployment string, replicas int) error {
	patch := map[string]any{"spec": map[string]any{"replicas": replicas}}
	return c.do(ctx, http.MethodPatch, DeploymentPath(ns, deployment)+"/scale", MergePatch, patch, nil)
}

// Annotations returns the annotations of the object at path.
func (c *Client) Annotations(ctx context.Context, path string) (map[string]string, error) {
	var o struct {
		Metadata ObjectMeta `json:"metadata"`
	}
	err := c.do(ctx, http.MethodGet, path, "", nil, &o)
	return o.Metadata.Annotations, err
}

// SetAnnotations merges annotations into the object at path; a nil value removes the key.
func (c *Client) SetAnnotations(ctx context.Context, path string, ann map[string]*string) error {
	patch := map[string]any{"metadata": map[string]any{"annotations": ann}}
	return c.do(ctx, http.MethodPatch, path, MergePatch, patch, nil)
}

// Deployment is the subset of a deployment the controlboard reads.
type Deployment struct {
	Metadata ObjectMeta `json:"metadata"`
	Spec     struct {
		Replicas int `json:"replicas"`
	} `json:"spec"`
}

// ListDeployments returns the deployments of a namespace matching a label selector ("" = all).
func (c *Client) ListDeployments(ctx context.Context, ns, selector string) ([]Deployment, error) {
	var list struct {
		Items []Deployment `json:"items"`
	}
	path := "/apis/apps/v1/namespaces/" + url.PathEscape(ns) + "/deployments"
	if selector != "" {
		path += "?labelSelector=" + url.QueryEscape(selector)
	}
	err := c.do(ctx, http.MethodGet, path, "", nil, &list)
	return list.Items, err
}
