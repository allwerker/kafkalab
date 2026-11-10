package lint

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

// Profile describes what is allowed in one dashboard set.
type Profile struct {
	// Bundle: strict PromQL, every panel on the `${datasource}` prometheus variable.
	Bundle bool
	// Known reports whether a metric name is documented (catalog / metrics.md).
	Known func(string) bool
	// UIDs are the dashboard UIDs that drill-down links may point to.
	UIDs map[string]bool
}

// Report collects problems and the metric names a dashboard uses.
type Report struct {
	Problems []string
	Used     map[string]bool
}

func (r *Report) addf(format string, args ...any) {
	r.Problems = append(r.Problems, fmt.Sprintf(format, args...))
}

// Kinds that show numbers and therefore need a unit; kinds that colour values need thresholds.
var (
	needsUnit       = map[string]bool{"timeseries": true, "stat": true, "gauge": true, "bargauge": true, "table": true, "heatmap": true, "state-timeline": true, "status-history": true, "piechart": true}
	needsThresholds = map[string]bool{"stat": true, "gauge": true, "bargauge": true, "table": true, "state-timeline": true, "status-history": true}
	dashLinkRe      = regexp.MustCompile(`/d/([a-z0-9-]+)/`)
	labelValuesRe   = regexp.MustCompile(`^label_values\((.*),\s*\w+\)$`)
)

type panel struct {
	ID          int             `json:"id"`
	Type        string          `json:"type"`
	Title       string          `json:"title"`
	Description string          `json:"description"`
	Datasource  *dsRef          `json:"datasource"`
	Targets     []target        `json:"targets"`
	Panels      []panel         `json:"panels"`
	FieldConfig json.RawMessage `json:"fieldConfig"`
	Options     json.RawMessage `json:"options"`
}

type dsRef struct {
	Type string `json:"type"`
	UID  string `json:"uid"`
}

type target struct {
	Expr       string `json:"expr"`
	Datasource *dsRef `json:"datasource"`
}

type fieldConfig struct {
	Defaults struct {
		Unit       string `json:"unit"`
		Thresholds *struct {
			Steps []any `json:"steps"`
		} `json:"thresholds"`
		Links []struct {
			URL string `json:"url"`
		} `json:"links"`
	} `json:"defaults"`
}

type dashboardJSON struct {
	UID           string  `json:"uid"`
	Title         string  `json:"title"`
	SchemaVersion int     `json:"schemaVersion"`
	Panels        []panel `json:"panels"`
	Templating    struct {
		List []struct {
			Name  string `json:"name"`
			Type  string `json:"type"`
			Query any    `json:"query"`
		} `json:"list"`
	} `json:"templating"`
	Annotations struct {
		List []struct {
			Name       string `json:"name"`
			Expr       string `json:"expr"`
			Datasource dsRef  `json:"datasource"`
		} `json:"list"`
	} `json:"annotations"`
}

// Dashboard lints one dashboard JSON document.
func Dashboard(raw []byte, p Profile) Report {
	r := Report{Used: map[string]bool{}}
	var d dashboardJSON
	if err := json.Unmarshal(raw, &d); err != nil {
		r.addf("invalid JSON: %v", err)
		return r
	}
	where := d.UID
	if d.UID == "" || d.Title == "" {
		r.addf("%s: uid and title are required", where)
	}
	if d.SchemaVersion < 39 {
		r.addf("%s: schemaVersion %d < 39 (Grafana 11)", where, d.SchemaVersion)
	}
	lintVariables(&r, d, p)
	for _, a := range d.Annotations.List {
		if a.Expr != "" {
			r.query(fmt.Sprintf("%s annotation %q", where, a.Name), a.Expr, dialect(&a.Datasource, p), p)
		}
	}
	ids := map[int]bool{}
	for _, pn := range flatten(d.Panels) {
		lintPanel(&r, where, pn, p, ids)
	}
	return r
}

func lintVariables(r *Report, d dashboardJSON, p Profile) {
	hasDS := false
	for _, v := range d.Templating.List {
		switch v.Type {
		case "datasource":
			hasDS = hasDS || (v.Name == "datasource" && v.Query == "prometheus")
		case "query":
			q := v.Query
			if m, ok := q.(map[string]any); ok {
				q = m["query"]
			}
			s, _ := q.(string)
			if mm := labelValuesRe.FindStringSubmatch(s); mm != nil {
				s = mm[1]
			}
			r.query(fmt.Sprintf("%s variable %s", d.UID, v.Name), s, dialectFor(p), p)
		}
	}
	if p.Bundle && !hasDS {
		r.addf("%s: bundle dashboards need a variable 'datasource' of type datasource (prometheus)", d.UID)
	}
}

func flatten(ps []panel) []panel {
	var out []panel
	for _, pn := range ps {
		if pn.Type == "row" {
			out = append(out, flatten(pn.Panels)...)
			continue
		}
		out = append(out, pn)
	}
	return out
}

func lintPanel(r *Report, dash string, pn panel, p Profile, ids map[int]bool) {
	where := fmt.Sprintf("%s panel %q", dash, pn.Title)
	if ids[pn.ID] {
		r.addf("%s: duplicate panel id %d", where, pn.ID)
	}
	ids[pn.ID] = true
	if pn.Title == "" {
		r.addf("%s panel id %d: missing title", dash, pn.ID)
	}
	if len(strings.TrimSpace(pn.Description)) < 30 {
		r.addf("%s: description missing or too short (explain meaning, healthy range, where to look next)", where)
	}
	var fc fieldConfig
	_ = json.Unmarshal(pn.FieldConfig, &fc) // generated JSON; a missing fieldConfig shows up as missing unit
	if needsUnit[pn.Type] && fc.Defaults.Unit == "" {
		r.addf("%s: no unit", where)
	}
	if needsThresholds[pn.Type] && (fc.Defaults.Thresholds == nil || len(fc.Defaults.Thresholds.Steps) == 0) {
		r.addf("%s: %s panel without thresholds (use a single neutral step if there is no good/bad)", where, pn.Type)
	}
	for _, l := range fc.Defaults.Links {
		for _, m := range dashLinkRe.FindAllStringSubmatch(l.URL, -1) {
			if !p.UIDs[m[1]] {
				r.addf("%s: link to unknown dashboard %q", where, m[1])
			}
		}
	}
	if p.Bundle && len(pn.Targets) > 0 && (pn.Datasource == nil || pn.Datasource.UID != "${datasource}" || pn.Datasource.Type != "prometheus") {
		r.addf("%s: bundle panels must use the ${datasource} prometheus variable", where)
	}
	for _, t := range pn.Targets {
		r.query(where, t.Expr, dialect(t.Datasource, p), p)
	}
}

func dialectFor(p Profile) Dialect {
	if p.Bundle {
		return PromQL
	}
	return MetricsQL
}

func dialect(ds *dsRef, p Profile) Dialect {
	if ds != nil && ds.Type == "victoriametrics-logs-datasource" {
		return LogsQL
	}
	return dialectFor(p)
}

func (r *Report) query(where, expr string, d Dialect, p Profile) {
	if strings.TrimSpace(expr) == "" {
		r.addf("%s: empty query", where)
		return
	}
	names, err := Metrics(expr, d)
	if err != nil {
		r.addf("%s: %v in %q", where, err, expr)
		return
	}
	for _, n := range names {
		r.Used[n] = true
		if !p.Known(n) {
			r.addf("%s: metric %s is not documented (catalog / metrics.md)", where, n)
		}
	}
}
