// Package links serves the board's tool links (GET /api/links) and probes their health.
// Each link has a public URL for the browser and an in-cluster probe URL for the board.
package links

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"kafkalab/controlboard/backend/internal/api"
)

// Config is one link of the LINKS_FILE (a JSON array of these objects).
type Config struct {
	ID          string `json:"id"`
	Title       string `json:"title"`
	URL         string `json:"url"`
	ProbeURL    string `json:"probeUrl"` // empty: health stays "unknown"
	Description string `json:"description"`
	Icon        string `json:"icon"` // lucide icon name
}

// Defaults match the ingress URLs and in-cluster services of docs/contracts/platform.md.
var Defaults = []Config{
	{ID: "grafana", Title: "Grafana", URL: "http://grafana.localhost:8080", ProbeURL: "http://grafana.monitoring:80/api/health",
		Description: "Dashboards for Kafka, the services and the business", Icon: "layout-dashboard"},
	{ID: "akhq", Title: "AKHQ", URL: "http://akhq.localhost:8080", ProbeURL: "http://akhq.kafka:28081/health",
		Description: "Browse topics, messages and consumer groups", Icon: "database"},
	{ID: "vmui", Title: "VictoriaMetrics", URL: "http://vm.localhost:8080/vmui", ProbeURL: "http://vmsingle-vmks.monitoring:8428/health",
		Description: "Ad-hoc PromQL / MetricsQL queries", Icon: "chart-line"},
	{ID: "vmagent", Title: "Scrape targets", URL: "http://vmagent.localhost:8080/targets", ProbeURL: "http://vmagent-vmks.monitoring:8429/health",
		Description: "What vmagent scrapes and whether it succeeds", Icon: "target"},
	{ID: "vmalert", Title: "vmalert", URL: "http://vmalert.localhost:8080", ProbeURL: "http://vmalert-vmks.monitoring:8080/health",
		Description: "Alerting rules and their state", Icon: "siren"},
	{ID: "alertmanager", Title: "Alertmanager", URL: "http://alertmanager.localhost:8080", ProbeURL: "http://vmalertmanager-vmks.monitoring:9093/-/healthy",
		Description: "Routing, grouping and silences of alerts", Icon: "bell"},
	{ID: "logs", Title: "VictoriaLogs", URL: "http://logs.localhost:8080/select/vmui", ProbeURL: "http://victoria-logs.monitoring:9428/health",
		Description: "Structured logs of every lab service", Icon: "scroll-text"},
	{ID: "traces", Title: "Traces", URL: "http://traces.localhost:8080", ProbeURL: "http://victoria-traces.monitoring:10428/health",
		Description: "Distributed traces of sampled orders", Icon: "waypoints"},
}

// Load reads the links file; an empty path yields the defaults.
func Load(path string) ([]Config, error) {
	if path == "" {
		return Defaults, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var list []Config
	if err := json.Unmarshal(b, &list); err != nil {
		return nil, fmt.Errorf("links file %s: %w", path, err)
	}
	for _, l := range list {
		if l.ID == "" || l.URL == "" {
			return nil, fmt.Errorf("links file %s: every link needs an id and a url", path)
		}
	}
	return list, nil
}

// Checker keeps the health of the links.
type Checker struct {
	links  []Config
	hc     *http.Client
	mu     sync.RWMutex
	health map[string]string
}

// NewChecker returns a checker with every link "unknown" until the first probe.
func NewChecker(links []Config, hc *http.Client) *Checker {
	return &Checker{links: links, hc: hc, health: map[string]string{}}
}

// Links returns the links with their latest health.
func (c *Checker) Links() []api.Link {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]api.Link, 0, len(c.links))
	for _, l := range c.links {
		h := c.health[l.ID]
		if h == "" {
			h = api.HealthUnknown
		}
		out = append(out, api.Link{ID: l.ID, Title: l.Title, URL: l.URL, Description: l.Description, Icon: l.Icon, Health: h})
	}
	return out
}

// Probe checks every link concurrently: 2xx/3xx up, other answers degraded, no answer down.
func (c *Checker) Probe(ctx context.Context) {
	var wg sync.WaitGroup
	for _, l := range c.links {
		if l.ProbeURL == "" {
			continue
		}
		wg.Go(func() {
			h := c.probe(ctx, l.ProbeURL)
			c.mu.Lock()
			c.health[l.ID] = h
			c.mu.Unlock()
		})
	}
	wg.Wait()
}

func (c *Checker) probe(ctx context.Context, url string) string {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return api.HealthUnknown
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return api.HealthDown
	}
	resp.Body.Close()
	if resp.StatusCode < 400 {
		return api.HealthUp
	}
	return api.HealthDegraded
}

// Run probes every interval until ctx ends.
func (c *Checker) Run(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		c.Probe(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
