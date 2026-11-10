package alerts

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"kafkalab/controlboard/backend/internal/api"
	"kafkalab/controlboard/backend/internal/grafana"
)

// Webhook is the Alertmanager webhook payload (version 4); unknown fields are ignored.
type Webhook struct {
	Version string         `json:"version"`
	Status  string         `json:"status"`
	Alerts  []WebhookAlert `json:"alerts"`
}

// WebhookAlert is one alert of a webhook notification.
type WebhookAlert struct {
	Status       string            `json:"status"`
	Labels       map[string]string `json:"labels"`
	Annotations  map[string]string `json:"annotations"`
	StartsAt     time.Time         `json:"startsAt"`
	EndsAt       time.Time         `json:"endsAt"`
	GeneratorURL string            `json:"generatorURL"`
	Fingerprint  string            `json:"fingerprint"`
}

// ParseWebhook decodes a notification into board alerts.
func ParseWebhook(r io.Reader, g grafana.Links) ([]api.Alert, error) {
	var w Webhook
	if err := json.NewDecoder(r).Decode(&w); err != nil {
		return nil, fmt.Errorf("%w: alertmanager webhook: %v", api.ErrInvalid, err)
	}
	out := make([]api.Alert, 0, len(w.Alerts))
	for _, wa := range w.Alerts {
		if wa.Fingerprint == "" || wa.StartsAt.IsZero() {
			return nil, fmt.Errorf("%w: alert without fingerprint or startsAt", api.ErrInvalid)
		}
		a := fromLabels(g, wa.Labels, wa.Annotations)
		a.Fingerprint, a.StartsAt, a.GeneratorURL, a.Status = wa.Fingerprint, wa.StartsAt.UTC(), wa.GeneratorURL, api.AlertFiring
		if wa.Status == api.AlertResolved {
			a.Status = api.AlertResolved
			end := wa.EndsAt.UTC()
			if wa.EndsAt.IsZero() {
				end = a.StartsAt
			}
			a.EndsAt = &end
		}
		out = append(out, a)
	}
	return out, nil
}

// VMAlert polls pending alerts from vmalert's /api/v1/alerts.
type VMAlert struct {
	Base    string // e.g. http://vmalert-vmks.monitoring:8080
	HTTP    *http.Client
	Grafana grafana.Links
}

// Pending returns the alerts vmalert currently holds in state "pending".
func (v VMAlert) Pending(ctx context.Context) ([]api.Alert, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(v.Base, "/")+"/api/v1/alerts", nil)
	if err != nil {
		return nil, err
	}
	resp, err := v.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var body struct {
		Data struct {
			Alerts []struct {
				ID          string            `json:"id"`
				Name        string            `json:"name"`
				State       string            `json:"state"`
				Labels      map[string]string `json:"labels"`
				Annotations map[string]string `json:"annotations"`
				ActiveAt    time.Time         `json:"activeAt"`
			} `json:"alerts"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, fmt.Errorf("vmalert alerts: HTTP %d: %w", resp.StatusCode, err)
	}
	out := []api.Alert{}
	for _, va := range body.Data.Alerts {
		if va.State != api.AlertPending {
			continue
		}
		a := fromLabels(v.Grafana, va.Labels, va.Annotations)
		if a.Name == "" {
			a.Name = va.Name
		}
		a.Fingerprint, a.Status, a.StartsAt = "vmalert-"+va.ID, api.AlertPending, va.ActiveAt.UTC()
		a.GeneratorURL = strings.TrimRight(v.Base, "/") + "/vmalert/alert?alert_id=" + va.ID
		out = append(out, a)
	}
	return out, nil
}

// PollPending feeds vmalert's pending alerts into m every interval until ctx ends.
func (m *Manager) PollPending(ctx context.Context, src VMAlert, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		if list, err := src.Pending(ctx); err != nil {
			m.o.Log.WarnContext(ctx, "vmalert unavailable", "error", err)
		} else {
			m.SetPending(list)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// TestAlertName is the alert POST /api/alerts/test fires.
const TestAlertName = "LabTestAlert"

// FireTest posts LabTestAlert (severity info, ends in 2 min) to Alertmanager's /api/v2/alerts,
// so it travels Alertmanager → webhook → board like any real alert.
func FireTest(ctx context.Context, hc *http.Client, alertmanager, generatorURL string, now time.Time) error {
	body, _ := json.Marshal([]map[string]any{{
		"labels": map[string]string{"alertname": TestAlertName, "severity": api.SeverityInfo, "component": "platform", "dashboard": "lab-monitoring"},
		"annotations": map[string]string{"summary": "Test alert fired from the controlboard",
			"description": "Checks the path Alertmanager → webhook → controlboard. Resolves itself after 2 minutes."},
		"startsAt":     now.UTC().Format(time.RFC3339),
		"endsAt":       now.Add(2 * time.Minute).UTC().Format(time.RFC3339),
		"generatorURL": generatorURL,
	}})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(alertmanager, "/")+"/api/v2/alerts", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := hc.Do(req)
	if err != nil {
		return fmt.Errorf("%w: alertmanager: %v", api.ErrUnavailable, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("%w: alertmanager answered %d: %s", api.ErrUnavailable, resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	return nil
}
