package experiments

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/VictoriaMetrics/metrics"

	"kafkalab/controlboard/backend/internal/api"
)

// Every outbound channel goes through the helpers in this file so an unsolved Incident Drill
// never leaks its scenario: API/WebSocket experiments (view), timeline texts (displayName,
// timelineDetail), audit events (auditScenario, auditDetails, auditReason), metric labels
// (labels) and Grafana annotations (annotationText). Snapshots only carry the opaque id.

const hidden = "hidden"

func (e *Engine) view(r *run) api.Experiment {
	e.mu.Lock()
	defer e.mu.Unlock()
	x := r.rec.Exp
	x.Params = copyValues(x.Params)
	x.Affected = slices.Clone(x.Affected)
	x.Observations = slices.Clone(x.Observations)
	x.Category = r.scenario.Def.Category
	x.Severity = r.scenario.Def.Severity
	if x.Report == nil {
		x.Report = r.report()
	}
	if r.rec.Hidden {
		x.Scenario = hidden
		x.Name = drillName(r.rec.DrillNo)
		x.Category = ""
		x.Severity = ""
		x.Params = api.Values{}
		x.Affected = []string{}
		x.Observations = []api.Observation{}
		x.Report = nil
	}
	return x
}

func drillName(n int) string { return "Incident Drill #" + strconv.Itoa(n) }

func (e *Engine) displayName(r *run) string {
	if r.rec.Hidden {
		return drillName(r.rec.DrillNo)
	}
	return r.rec.Exp.Name
}

func (e *Engine) auditScenario(r *run) string {
	if r.rec.Hidden {
		return hidden
	}
	return r.rec.Exp.Scenario
}

func (e *Engine) auditDetails(r *run) map[string]string {
	if r.rec.Hidden {
		return map[string]string{"drill": r.rec.DrillID}
	}
	d := map[string]string{
		"durationSec": strconv.Itoa(int(r.rec.Exp.EndsAt.Sub(r.rec.Exp.StartsAt).Seconds())),
		"autoRecover": strconv.FormatBool(r.rec.Exp.AutoRecover),
		"affected":    strings.Join(r.rec.Exp.Affected, ","),
	}
	for k, v := range r.rec.Exp.Params {
		d["param."+k] = fmt.Sprint(v)
	}
	return d
}

func (e *Engine) auditReason(r *run) string {
	if r.rec.Hidden {
		return ""
	}
	return r.rec.Reason
}

func (e *Engine) timelineDetail(r *run) string {
	if r.rec.Hidden {
		return ""
	}
	return "affects " + strings.Join(r.rec.Exp.Affected, ", ")
}

func severityOf(r *run) string {
	if r.rec.Hidden {
		return api.SeverityWarning
	}
	switch r.scenario.Def.Severity {
	case "critical":
		return api.SeverityCritical
	case "high":
		return api.SeverityWarning
	}
	return api.SeverityInfo
}

// Metrics.

type engineMetrics struct {
	set *metrics.Set
}

func newEngineMetrics(set *metrics.Set, e *Engine) *engineMetrics {
	set.GetOrCreateGauge("lab_chaos_armed", func() float64 {
		if e.Armed() {
			return 1
		}
		return 0
	})
	return &engineMetrics{set: set}
}

func metricName(name string, labels ...string) string {
	var b strings.Builder
	b.WriteString(name)
	b.WriteByte('{')
	for i := 0; i < len(labels); i += 2 {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(labels[i])
		b.WriteString("=")
		b.WriteString(strconv.Quote(labels[i+1]))
	}
	b.WriteByte('}')
	return b.String()
}

func (r *run) labels() (scenario, category, severity string) {
	if r.rec.Hidden {
		return hidden, hidden, hidden
	}
	return r.rec.Exp.Scenario, r.scenario.Def.Category, r.scenario.Def.Severity
}

func (r *run) activeName() string {
	s, c, v := r.labels()
	return metricName("lab_experiment_active", "experiment_id", r.rec.Exp.ID, "scenario", s, "category", c, "severity", v)
}

func (r *run) intensityName() string {
	return metricName("lab_experiment_intensity", "experiment_id", r.rec.Exp.ID)
}

func (m *engineMetrics) started(r *run) {
	r.metricActive = r.activeName()
	m.set.GetOrCreateGauge(r.metricActive, nil).Set(1)
	m.intensity(r, 0)
}

func (m *engineMetrics) intensity(r *run, elapsed time.Duration) {
	x := 1.0
	if r.plan.Intensity != nil {
		x = r.plan.Intensity(elapsed)
	}
	m.set.GetOrCreateGauge(r.intensityName(), nil).Set(x)
}

func (m *engineMetrics) reverted(r *run) {
	if r.metricActive != "" {
		m.set.UnregisterMetric(r.metricActive)
	}
	m.set.UnregisterMetric(r.intensityName())
}

func (m *engineMetrics) finished(r *run) {
	s, _, _ := r.labels()
	m.set.GetOrCreateCounter(metricName("lab_experiments_total", "scenario", s, "outcome", r.rec.Exp.Status)).Inc()
	if t := r.rec.Report.TimeToRecoverSec; t != nil {
		m.set.GetOrCreateHistogram(metricName("lab_experiment_recovery_seconds", "scenario", s)).Update(*t)
	}
}

// Grafana annotations: a region from start to end, tagged chaos + scenario (+ category).

func (r *run) annotationText() (string, []string) {
	if r.rec.Hidden {
		return drillName(r.rec.DrillNo), []string{"chaos", hidden}
	}
	text := r.rec.Exp.Name
	if r.rec.DrillID != "" {
		text = drillName(r.rec.DrillNo) + ": " + r.rec.Exp.Name
	}
	tags := []string{"chaos", r.rec.Exp.Scenario, r.scenario.Def.Category}
	sort.Strings(tags[1:])
	return text, tags
}

func (e *Engine) annotateStart(ctx context.Context, r *run) {
	if e.cfg.Annotator == nil {
		return
	}
	text, tags := r.annotationText()
	id, err := e.cfg.Annotator.Start(ctx, *r.rec.StartedAt, text, tags)
	if err != nil {
		e.cfg.Log.Warn("grafana annotation not created", "id", r.rec.Exp.ID, "error", err)
		return
	}
	r.rec.Annotation = id
}

func (e *Engine) annotateEnd(ctx context.Context, r *run) {
	e.annotateUpdate(ctx, r, *r.rec.Exp.EndedAt)
}

// annotateUpdate rewrites text and tags (drill reveal backfill) and optionally the end.
func (e *Engine) annotateUpdate(ctx context.Context, r *run, end time.Time) {
	if e.cfg.Annotator == nil || r.rec.Annotation == 0 {
		return
	}
	text, tags := r.annotationText()
	if err := e.cfg.Annotator.Update(ctx, r.rec.Annotation, end, text, tags); err != nil {
		e.cfg.Log.Warn("grafana annotation not updated", "id", r.rec.Exp.ID, "error", err)
	}
}
