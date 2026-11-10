package experiments

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

var httpClient = &http.Client{Timeout: 5 * time.Second}

// VMQuery returns a Query against VictoriaMetrics' Prometheus API, e.g.
// VMQuery("http://vmsingle-vmks.monitoring:8428").
func VMQuery(base string) Query {
	base = strings.TrimRight(base, "/")
	return func(ctx context.Context, promql string) ([]Sample, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/api/v1/query?query="+url.QueryEscape(promql), nil)
		if err != nil {
			return nil, err
		}
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
		if err := doJSON(req, &body); err != nil {
			return nil, err
		}
		if body.Status != "success" {
			return nil, fmt.Errorf("query failed: %s", body.Error)
		}
		out := make([]Sample, 0, len(body.Data.Result))
		for _, r := range body.Data.Result {
			s, _ := r.Value[1].(string)
			v, err := strconv.ParseFloat(s, 64)
			if err != nil {
				continue
			}
			out = append(out, Sample{Labels: r.Metric, Value: v})
		}
		return out, nil
	}
}

func doJSON(req *http.Request, out any) error {
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("%s %s: %d %s", req.Method, req.URL.Path, resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(raw, out)
}

// GrafanaAnnotator writes annotations through Grafana's HTTP API (basic auth).
type GrafanaAnnotator struct {
	Base, User, Password string
}

func (g *GrafanaAnnotator) request(ctx context.Context, method, path string, body any) (*http.Request, error) {
	b, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(g.Base, "/")+path, bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(g.User, g.Password)
	return req, nil
}

// Start implements Annotator.
func (g *GrafanaAnnotator) Start(ctx context.Context, t time.Time, text string, tags []string) (int64, error) {
	req, err := g.request(ctx, http.MethodPost, "/api/annotations", map[string]any{"time": t.UnixMilli(), "text": text, "tags": tags})
	if err != nil {
		return 0, err
	}
	var res struct {
		ID int64 `json:"id"`
	}
	err = doJSON(req, &res)
	return res.ID, err
}

// Update implements Annotator (turns the point annotation into a region when end is set).
func (g *GrafanaAnnotator) Update(ctx context.Context, id int64, end time.Time, text string, tags []string) error {
	body := map[string]any{"text": text, "tags": tags}
	if !end.IsZero() {
		body["timeEnd"] = end.UnixMilli()
	}
	req, err := g.request(ctx, http.MethodPatch, "/api/annotations/"+strconv.FormatInt(id, 10), body)
	if err != nil {
		return err
	}
	return doJSON(req, nil)
}
