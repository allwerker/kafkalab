package grafana

import (
	"bytes"
	"encoding/json"
	"sort"
	"strings"
)

// JSON renders the dashboard as indented, deterministic JSON (keys sorted, trailing newline).
// ds is the default datasource for panels and query variables without an explicit one.
func JSON(d *Dashboard, ds Datasource) []byte {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	_ = enc.Encode(Render(d, ds)) // maps of plain values cannot fail to encode
	return buf.Bytes()
}

// Render converts the dashboard into Grafana's classic JSON model.
func Render(d *Dashboard, ds Datasource) map[string]any {
	from := d.From
	if from == "" {
		from = "now-1h"
	}
	refresh := d.Refresh
	if refresh == "" {
		refresh = "30s"
	}
	return map[string]any{
		"uid":                  d.UID,
		"title":                d.Title,
		"description":          d.Description,
		"tags":                 nonNil(d.Tags),
		"timezone":             "browser",
		"editable":             true,
		"graphTooltip":         1,
		"fiscalYearStartMonth": 0,
		"liveNow":              false,
		"schemaVersion":        SchemaVersion,
		"version":              1,
		"refresh":              refresh,
		"time":                 map[string]any{"from": from, "to": "now"},
		"timepicker":           map[string]any{"refresh_intervals": []string{"10s", "30s", "1m", "5m", "15m"}},
		"links":                renderDashLinks(d.Links),
		"annotations":          map[string]any{"list": renderAnnotations(d.Annotations)},
		"templating":           map[string]any{"list": renderVariables(d.Variables, ds)},
		"panels":               layout(d.Rows, ds),
	}
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

func renderDashLinks(links []Link) []any {
	out := []any{}
	for _, l := range links {
		if len(l.Tags) > 0 {
			out = append(out, map[string]any{
				"title": l.Title, "type": "dashboards", "tags": l.Tags, "asDropdown": true,
				"includeVars": true, "keepTime": true, "targetBlank": false, "icon": "external link",
			})
			continue
		}
		out = append(out, map[string]any{
			"title": l.Title, "type": "link", "url": l.URL, "tags": []string{}, "asDropdown": false,
			"includeVars": false, "keepTime": false, "targetBlank": l.NewTab, "icon": "external link",
		})
	}
	return out
}

func renderAnnotations(as []Annotation) []any {
	out := []any{map[string]any{
		"builtIn": 1, "datasource": GrafanaDS, "enable": true, "hide": true,
		"iconColor": "rgba(0, 211, 255, 1)", "name": "Annotations & Alerts", "type": "dashboard",
	}}
	for _, a := range as {
		m := map[string]any{
			"name": a.Name, "datasource": a.DS, "enable": true, "hide": a.Hide, "iconColor": a.Color,
		}
		if len(a.Tags) > 0 {
			m["target"] = map[string]any{"type": "tags", "tags": a.Tags, "matchAny": true, "limit": 100}
		} else {
			m["expr"] = a.Expr
			m["step"] = "30s"
			m["titleFormat"] = a.Title
			m["textFormat"] = a.Text
			m["tagKeys"] = a.TagKey
			m["useValueForTime"] = false
		}
		out = append(out, m)
	}
	return out
}

func renderVariables(vs []Variable, ds Datasource) []any {
	out := []any{}
	for _, v := range vs {
		m := map[string]any{
			"name": v.Name, "label": v.Label, "type": v.Kind, "hide": 0, "skipUrlSync": false,
			"multi": v.Multi, "includeAll": v.All, "options": []any{}, "regex": v.Regex,
			"current": map[string]any{},
		}
		if v.Hide {
			m["hide"] = 2
		}
		if v.AllValue != "" {
			m["allValue"] = v.AllValue
		}
		if v.Default != "" {
			m["current"] = map[string]any{"text": v.Default, "value": v.Default, "selected": true}
		}
		switch v.Kind {
		case "datasource":
			m["query"] = v.Query
			m["refresh"] = 1
		case "query":
			m["datasource"] = ds
			m["definition"] = v.Query
			m["query"] = map[string]any{"query": v.Query, "refId": "PrometheusVariableQueryEditor-VariableQuery"}
			m["refresh"] = 2
			m["sort"] = v.Sort
		default: // custom, constant, textbox, interval
			m["query"] = v.Query
			m["options"] = customOptions(v.Query)
		}
		out = append(out, m)
	}
	return out
}

func customOptions(q string) []any {
	out := []any{}
	for _, o := range strings.Split(q, ",") {
		o = strings.TrimSpace(o)
		out = append(out, map[string]any{"text": o, "value": o, "selected": false})
	}
	return out
}

// layout assigns ids and grid positions. Rows with a title become row panels; collapsed rows
// carry their panels inside, expanded rows are followed by their panels.
func layout(rows []*Row, ds Datasource) []any {
	out := []any{}
	id, y := 1, 0
	for _, r := range rows {
		var row map[string]any
		if r.Title != "" {
			row = map[string]any{
				"type": "row", "title": r.Title, "collapsed": r.Collapsed, "id": id,
				"gridPos": grid(0, y, 24, 1), "panels": []any{},
			}
			id++
			y++
			out = append(out, row)
		}
		x, rowH := 0, 0
		var inner []any
		for _, p := range r.Panels {
			if x+p.W > 24 {
				x, y, rowH = 0, y+rowH, 0
			}
			inner = append(inner, renderPanel(p, id, grid(x, y, p.W, p.H), ds))
			id++
			x += p.W
			rowH = max(rowH, p.H)
		}
		y += rowH
		if row != nil && r.Collapsed {
			row["panels"] = inner
			continue
		}
		out = append(out, inner...)
	}
	return out
}

func grid(x, y, w, h int) map[string]any { return map[string]any{"x": x, "y": y, "w": w, "h": h} }

func renderPanel(p *Panel, id int, pos map[string]any, ds Datasource) map[string]any {
	pds := ds
	if p.DS != nil {
		pds = *p.DS
	}
	m := map[string]any{
		"id": id, "type": p.Kind, "title": p.Title, "description": p.Description, "gridPos": pos,
		"options": panelOptions(p), "fieldConfig": fieldConfig(p),
	}
	if len(p.Targets) > 0 {
		m["datasource"] = pds
		m["targets"] = renderTargets(p.Targets, pds)
	}
	if len(p.Transforms) > 0 {
		m["transformations"] = p.Transforms
	}
	if p.Repeat != "" {
		m["repeat"] = p.Repeat
		m["repeatDirection"] = "h"
		m["maxPerRow"] = 3
	}
	return m
}

func renderTargets(ts []Target, ds Datasource) []any {
	out := []any{}
	for i, t := range ts {
		m := map[string]any{
			"refId": string(rune('A' + i)), "expr": t.Expr, "datasource": ds, "hide": false,
		}
		if ds.Type == VLogs.Type {
			m["queryType"] = t.QueryType
			m["legendFormat"] = t.Legend
			out = append(out, m)
			continue
		}
		m["editorMode"] = "code"
		m["legendFormat"] = t.Legend
		m["range"] = !t.Instant
		m["instant"] = t.Instant
		m["format"] = "time_series"
		if t.Format != "" {
			m["format"] = t.Format
		}
		out = append(out, m)
	}
	return out
}

func fieldConfig(p *Panel) map[string]any {
	defaults := map[string]any{"links": renderDataLinks(p.Links), "mappings": renderMappings(p.Mappings)}
	if p.Unit != "" {
		defaults["unit"] = p.Unit
	}
	if p.NoValue != "" {
		defaults["noValue"] = p.NoValue
	}
	if p.Decimals != nil {
		defaults["decimals"] = *p.Decimals
	}
	if p.Min != nil {
		defaults["min"] = *p.Min
	}
	if p.Max != nil {
		defaults["max"] = *p.Max
	}
	if len(p.Steps) > 0 {
		defaults["thresholds"] = map[string]any{"mode": "absolute", "steps": renderSteps(p.Steps)}
	}
	if p.ColorMode != "" {
		c := map[string]any{"mode": p.ColorMode}
		if p.ColorFixed != "" {
			c["fixedColor"] = p.ColorFixed
		}
		defaults["color"] = c
	}
	if custom := customDefaults(p); len(custom) > 0 {
		defaults["custom"] = custom
	}
	return map[string]any{"defaults": defaults, "overrides": renderOverrides(p.Overrides)}
}

func renderSteps(steps []Step) []any {
	out := []any{}
	for i, s := range steps {
		var v any = s.Value
		if i == 0 {
			v = nil
		}
		out = append(out, map[string]any{"color": s.Color, "value": v})
	}
	return out
}

func renderDataLinks(links []Link) []any {
	out := []any{}
	for _, l := range links {
		out = append(out, map[string]any{"title": l.Title, "url": l.URL, "targetBlank": l.NewTab})
	}
	return out
}

func renderMappings(ms []Mapping) []any {
	out := []any{}
	for i, mp := range ms {
		res := map[string]any{"text": mp.Text, "index": i, "color": mp.Color}
		out = append(out, map[string]any{"type": "value", "options": map[string]any{mp.Value: res}})
	}
	return out
}

func renderOverrides(os []Override) []any {
	out := []any{}
	for _, o := range os {
		props := []any{}
		for _, k := range sortedKeys(o.Properties) {
			props = append(props, map[string]any{"id": k, "value": o.Properties[k]})
		}
		out = append(out, map[string]any{
			"matcher":    map[string]any{"id": o.Matcher, "options": o.Option},
			"properties": props,
		})
	}
	return out
}

// customDefaults merges the per-kind custom field defaults with the panel's explicit ones.
func customDefaults(p *Panel) map[string]any {
	base := map[string]any{}
	switch p.Kind {
	case "timeseries":
		style := p.ThresholdStyle
		if style == "" {
			style = "off"
		}
		base = map[string]any{
			"drawStyle": "line", "lineWidth": 2, "fillOpacity": 12, "gradientMode": "opacity",
			"showPoints": "never", "spanNulls": 60000, "lineInterpolation": "smooth",
			"axisSoftMin": 0, "axisBorderShow": false, "axisCenteredZero": false,
			"stacking":        map[string]any{"mode": "none", "group": "A"},
			"thresholdsStyle": map[string]any{"mode": style},
			"barAlignment":    0, "pointSize": 4, "insertNulls": false,
			"hideFrom": map[string]any{"legend": false, "tooltip": false, "viz": false},
		}
	case "table":
		base = map[string]any{"align": "auto", "cellOptions": map[string]any{"type": "auto"}, "inspect": false, "filterable": true}
	case "state-timeline":
		base = map[string]any{"fillOpacity": 80, "lineWidth": 0}
	case "bargauge", "stat", "gauge", "heatmap":
		base = map[string]any{"hideFrom": map[string]any{"legend": false, "tooltip": false, "viz": false}}
	}
	for k, v := range p.Custom {
		base[k] = v
	}
	return base
}

func reduce(calc string) map[string]any {
	return map[string]any{"calcs": []string{calc}, "fields": "", "values": false}
}

// panelOptions merges the per-kind option defaults with the panel's explicit options.
func panelOptions(p *Panel) map[string]any {
	legend := map[string]any{"displayMode": "list", "placement": "bottom", "showLegend": true, "calcs": []string{}}
	tooltip := map[string]any{"mode": "multi", "sort": "desc", "hideZeros": false}
	var o map[string]any
	switch p.Kind {
	case "timeseries":
		o = map[string]any{"legend": legend, "tooltip": tooltip}
	case "stat":
		o = map[string]any{
			"reduceOptions": reduce("lastNotNull"), "colorMode": "value", "graphMode": "area",
			"justifyMode": "center", "textMode": "value_and_name", "orientation": "auto",
			"wideLayout": true, "showPercentChange": false,
		}
		if len(p.Targets) == 1 {
			o["textMode"] = "value"
		}
	case "gauge":
		o = map[string]any{
			"reduceOptions": reduce("lastNotNull"), "showThresholdMarkers": true,
			"showThresholdLabels": false, "orientation": "auto", "sizing": "auto",
		}
	case "bargauge":
		o = map[string]any{
			"reduceOptions": map[string]any{"calcs": []string{"lastNotNull"}, "fields": "", "values": false},
			"displayMode":   "gradient", "orientation": "horizontal", "showUnfilled": true,
			"valueMode": "color", "namePlacement": "left", "sizing": "manual", "minVizHeight": 14,
			"maxVizHeight": 26, "minVizWidth": 8,
			"legend": map[string]any{"displayMode": "list", "placement": "bottom", "showLegend": false, "calcs": []string{}},
		}
	case "table":
		o = map[string]any{"showHeader": true, "cellHeight": "sm", "footer": map[string]any{"show": false, "reducer": []string{"sum"}, "fields": ""}}
	case "heatmap":
		o = map[string]any{
			"calculate": false, "cellGap": 1, "cellValues": map[string]any{"unit": p.Unit},
			"color": map[string]any{
				"mode": "scheme", "scheme": "Turbo", "steps": 64, "exponent": 0.5, "fill": "dark-orange",
				"reverse": false,
			},
			"filterValues": map[string]any{"le": 1e-9},
			"legend":       map[string]any{"show": true},
			"rowsFrame":    map[string]any{"layout": "auto"},
			"tooltip":      map[string]any{"mode": "single", "yHistogram": false, "showColorScale": true},
			"yAxis":        map[string]any{"axisPlacement": "left", "reverse": false},
			"exemplars":    map[string]any{"color": "rgba(255,0,255,0.7)"},
			"showValue":    "never",
		}
	case "state-timeline":
		o = map[string]any{
			"mergeValues": true, "showValue": "auto", "alignValue": "left", "rowHeight": 0.85,
			"legend": legend, "tooltip": tooltip,
		}
	case "logs":
		o = map[string]any{
			"showTime": true, "wrapLogMessage": true, "sortOrder": "Descending", "enableLogDetails": true,
			"dedupStrategy": "none", "prettifyLogMessage": false, "showCommonLabels": false,
			"showLabels": false, "enableInfiniteScrolling": true,
		}
	case "dashlist":
		o = map[string]any{
			"showStarred": false, "showRecentlyViewed": false, "showSearch": true, "showHeadings": false,
			"showFolderNames": false, "maxItems": 30, "query": "", "includeVars": true, "keepTime": true,
		}
	default: // text
		o = map[string]any{"code": map[string]any{"language": "plaintext", "showLineNumbers": false, "showMiniMap": false}}
	}
	for k, v := range p.Options {
		o[k] = v
	}
	return o
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
