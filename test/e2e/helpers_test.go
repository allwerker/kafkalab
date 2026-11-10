//go:build e2e

// Package e2e checks the running stack (make up) from the outside, through the ingress on
// 127.0.0.1:8080, like a user would. Run with `make e2e`.
package e2e

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

var client = &http.Client{Timeout: 15 * time.Second}

// get fetches http://<host>.localhost:8080<path> through the k3d load balancer.
func get(t *testing.T, host, path string) []byte {
	t.Helper()
	b, code := do(t, http.MethodGet, host, path, nil)
	if code >= 300 {
		t.Fatalf("GET %s%s: %d %s", host, path, code, b)
	}
	return b
}

func do(t *testing.T, method, host, path string, body io.Reader) ([]byte, int) {
	t.Helper()
	req, _ := http.NewRequest(method, "http://127.0.0.1:8080"+path, body)
	req.Host = host + ".localhost"
	req.SetBasicAuth("admin", "admin")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("%s %s%s: %v", method, host, path, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return b, resp.StatusCode
}

type sample struct {
	Metric map[string]string `json:"metric"`
	Value  [2]any            `json:"value"`
}

// query runs an instant query against VictoriaMetrics.
func query(t *testing.T, q string) []sample {
	t.Helper()
	b := get(t, "vm", "/api/v1/query?query="+url.QueryEscape(q))
	var r struct {
		Status string `json:"status"`
		Data   struct {
			Result []sample `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(b, &r); err != nil || r.Status != "success" {
		t.Fatalf("query %q: %v %s", q, err, b)
	}
	return r.Data.Result
}

// scalar returns the single value of q (0 when empty).
func scalar(t *testing.T, q string) float64 {
	t.Helper()
	res := query(t, q)
	if len(res) == 0 {
		return 0
	}
	f, _ := strconv.ParseFloat(fmt.Sprint(res[0].Value[1]), 64)
	return f
}

// eventually retries cond until it holds or the timeout passes.
func eventually(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for: %s", timeout, what)
		}
		time.Sleep(5 * time.Second)
	}
}

func contains(s []byte, sub string) bool { return strings.Contains(string(s), sub) }
