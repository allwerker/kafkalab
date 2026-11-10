// Package observe is what the board sees of the running system: business KPIs, broker and
// service state from VictoriaMetrics, and per-partition consumer lag from the Kafka admin API.
package observe

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// Sample is one series of an instant query.
type Sample struct {
	Labels map[string]string
	Value  float64
}

// VM queries the Prometheus API of VictoriaMetrics.
type VM struct {
	Base string // e.g. http://vmsingle-vmks.monitoring:8428
	HTTP *http.Client
}

// Query runs an instant query. Series whose value is NaN or infinite are left out.
func (v VM) Query(ctx context.Context, expr string) ([]Sample, error) {
	u := strings.TrimRight(v.Base, "/") + "/api/v1/query?" + url.Values{"query": {expr}}.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := v.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var body struct {
		Status string `json:"status"`
		Error  string `json:"error"`
		Data   struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Value  [2]any            `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, fmt.Errorf("query %q: HTTP %d: %w", expr, resp.StatusCode, err)
	}
	if body.Status != "success" {
		return nil, fmt.Errorf("query %q: %s", expr, body.Error)
	}
	out := make([]Sample, 0, len(body.Data.Result))
	for _, r := range body.Data.Result {
		s, _ := r.Value[1].(string)
		f, err := strconv.ParseFloat(s, 64)
		if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
			continue
		}
		out = append(out, Sample{Labels: r.Metric, Value: f})
	}
	return out, nil
}

// scalar runs a query that yields at most one series; no series yields (0, false).
func (v VM) scalar(ctx context.Context, expr string) (float64, bool, error) {
	s, err := v.Query(ctx, expr)
	if err != nil || len(s) == 0 {
		return 0, false, err
	}
	return s[0].Value, true, nil
}
