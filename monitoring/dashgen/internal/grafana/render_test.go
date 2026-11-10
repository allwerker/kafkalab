package grafana

import (
	"encoding/json"
	"strings"
	"testing"
)

func decode(t *testing.T, d *Dashboard, ds Datasource) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(JSON(d, ds), &m); err != nil {
		t.Fatalf("rendered dashboard is not valid JSON: %v", err)
	}
	return m
}

func panelsOf(m map[string]any) []map[string]any {
	var out []map[string]any
	for _, p := range m["panels"].([]any) {
		out = append(out, p.(map[string]any))
	}
	return out
}

func gridOf(p map[string]any) (x, y, w, h int) {
	g := p["gridPos"].(map[string]any)
	return int(g["x"].(float64)), int(g["y"].(float64)), int(g["w"].(float64)), int(g["h"].(float64))
}

func Test_shouldPlacePanelsSideBySideAndWrapWhenARowIsFull(t *testing.T) {
	// GIVEN: a headerless row with three half-width panels of different heights
	d := &Dashboard{UID: "u", Title: "T", Rows: []*Row{{Panels: []*Panel{
		TimeSeries("a", "d", "s").Size(12, 8),
		TimeSeries("b", "d", "s").Size(12, 6),
		TimeSeries("c", "d", "s").Size(12, 4),
	}}}}

	// WHEN: the dashboard is rendered
	ps := panelsOf(decode(t, d, PromVar))

	// THEN: the first two share a line and the third starts below the taller of them
	if len(ps) != 3 {
		t.Fatalf("want 3 panels, got %d", len(ps))
	}
	x0, y0, _, _ := gridOf(ps[0])
	x1, y1, _, _ := gridOf(ps[1])
	x2, y2, _, _ := gridOf(ps[2])
	if x0 != 0 || y0 != 0 || x1 != 12 || y1 != 0 || x2 != 0 || y2 != 8 {
		t.Errorf("unexpected layout: (%d,%d) (%d,%d) (%d,%d)", x0, y0, x1, y1, x2, y2)
	}
}

func Test_shouldKeepPanelsInsideTheRowWhenTheRowIsCollapsed(t *testing.T) {
	// GIVEN: an expanded row followed by a collapsed row, each with one panel
	d := &Dashboard{UID: "u", Title: "T", Rows: []*Row{
		{Title: "open", Panels: []*Panel{Stat("s", "d", "short").Neutral()}},
		{Title: "closed", Collapsed: true, Panels: []*Panel{Stat("t", "d", "short").Neutral()}},
	}}

	// WHEN: the dashboard is rendered
	ps := panelsOf(decode(t, d, PromVar))

	// THEN: the open row's panel follows its header, the collapsed row carries its panel inside
	if len(ps) != 3 || ps[0]["type"] != "row" || ps[1]["type"] != "stat" || ps[2]["type"] != "row" {
		t.Fatalf("unexpected top-level panels: %v", ps)
	}
	inner := ps[2]["panels"].([]any)
	if len(inner) != 1 || inner[0].(map[string]any)["title"] != "t" || ps[2]["collapsed"] != true {
		t.Errorf("collapsed row should contain panel t, got %v", inner)
	}
	if ps[0]["id"] == ps[1]["id"] || ps[1]["id"] == inner[0].(map[string]any)["id"] {
		t.Error("panel ids must be unique")
	}
}

func Test_shouldFillDefaultsWhenTimeRangeAndRefreshAreNotSet(t *testing.T) {
	// GIVEN: a dashboard without time range and refresh, and one with both set
	plain := &Dashboard{UID: "a", Title: "A"}
	custom := &Dashboard{UID: "b", Title: "B", From: "now-6h", Refresh: "1m", Tags: []string{"x"}}

	// WHEN: both are rendered
	p, c := decode(t, plain, PromVar), decode(t, custom, PromVar)

	// THEN: the plain one shows the last hour refreshing every 30 s, the custom one keeps its choice
	if p["refresh"] != "30s" || p["time"].(map[string]any)["from"] != "now-1h" || len(p["tags"].([]any)) != 0 {
		t.Errorf("defaults not applied: %v %v %v", p["refresh"], p["time"], p["tags"])
	}
	if c["refresh"] != "1m" || c["time"].(map[string]any)["from"] != "now-6h" || p["schemaVersion"].(float64) != SchemaVersion {
		t.Errorf("custom values lost: %v %v", c["refresh"], c["time"])
	}
}

func Test_shouldRenderTagDropdownsAndExternalLinksWhenDashboardHasLinks(t *testing.T) {
	// GIVEN: a dashboard linking to a tag group and to an external page in a new tab
	d := &Dashboard{UID: "u", Title: "T", Links: []Link{
		{Title: "Kafka", Tags: []string{"kafka"}},
		{Title: "Board", URL: "http://board", NewTab: true},
	}}

	// WHEN: it is rendered
	links := decode(t, d, PromVar)["links"].([]any)

	// THEN: the tag link is a dropdown keeping time and variables, the URL link opens in a new tab
	tag, ext := links[0].(map[string]any), links[1].(map[string]any)
	if tag["type"] != "dashboards" || tag["asDropdown"] != true || tag["keepTime"] != true {
		t.Errorf("tag link wrong: %v", tag)
	}
	if ext["type"] != "link" || ext["url"] != "http://board" || ext["targetBlank"] != true {
		t.Errorf("external link wrong: %v", ext)
	}
}

func Test_shouldRenderQueryAndTagAnnotationsNextToTheBuiltInOneWhenConfigured(t *testing.T) {
	// GIVEN: an alert annotation from a query and a chaos annotation by Grafana tag
	d := &Dashboard{UID: "u", Title: "T", Annotations: []Annotation{
		{Name: "alerts", DS: PromVar, Color: "red", Expr: "ALERTS", Title: "{{alertname}}", Text: "x", TagKey: "severity"},
		{Name: "chaos", DS: GrafanaDS, Color: "orange", Tags: []string{"chaos"}},
	}}

	// WHEN: it is rendered
	list := decode(t, d, PromVar)["annotations"].(map[string]any)["list"].([]any)

	// THEN: Grafana's built-in annotation comes first, then the query and the tag annotation
	if len(list) != 3 || list[0].(map[string]any)["builtIn"].(float64) != 1 {
		t.Fatalf("unexpected annotations: %v", list)
	}
	q, tag := list[1].(map[string]any), list[2].(map[string]any)
	if q["expr"] != "ALERTS" || q["titleFormat"] != "{{alertname}}" {
		t.Errorf("query annotation wrong: %v", q)
	}
	if tag["target"].(map[string]any)["type"] != "tags" {
		t.Errorf("tag annotation wrong: %v", tag)
	}
}

func Test_shouldRenderEveryVariableKindWhenDashboardHasFilters(t *testing.T) {
	// GIVEN: datasource, query (hidden, multi with all), custom and textbox variables
	d := &Dashboard{UID: "u", Title: "T", Variables: []Variable{
		{Name: "datasource", Kind: "datasource", Query: "prometheus"},
		{Name: "pod", Kind: "query", Query: "label_values(up, pod)", Multi: true, All: true, AllValue: ".*", Default: "All", Hide: true, Sort: 1},
		{Name: "ns", Kind: "custom", Query: "a, b"},
		{Name: "filter", Kind: "textbox", Query: "*"},
	}}

	// WHEN: it is rendered
	list := decode(t, d, PromVar)["templating"].(map[string]any)["list"].([]any)

	// THEN: each kind carries the fields Grafana needs to show and refresh it
	ds, pod, ns := list[0].(map[string]any), list[1].(map[string]any), list[2].(map[string]any)
	if ds["query"] != "prometheus" || ds["refresh"].(float64) != 1 {
		t.Errorf("datasource variable wrong: %v", ds)
	}
	if pod["hide"].(float64) != 2 || pod["allValue"] != ".*" || pod["datasource"].(map[string]any)["uid"] != "${datasource}" ||
		pod["current"].(map[string]any)["value"] != "All" || pod["refresh"].(float64) != 2 {
		t.Errorf("query variable wrong: %v", pod)
	}
	if len(ns["options"].([]any)) != 2 || ns["options"].([]any)[1].(map[string]any)["value"] != "b" {
		t.Errorf("custom variable options wrong: %v", ns["options"])
	}
}

func Test_shouldGiveEveryPanelKindItsOptionsWhenRendered(t *testing.T) {
	// GIVEN: one panel of every supported kind, decorated with the helpers dashboards use
	ts := TimeSeries("ts", "d", "s", Q("up", "{{pod}}")).Stack().Bars().Lines("dashed").
		Thresholds(Green, Step{Color: Red, Value: 1}).SetMin(0).SetMax(1).Dec(2).
		LegendTable("right", "mean").SeriesColor("x", Red).Link("go", "/d/x/").RepeatBy("request")
	panels := []*Panel{
		ts,
		TimeSeries("plain", "d", "s", Q("up", "")),
		Stat("one", "d", "short", Q("up", "")).Neutral().Empty("none").Map("1", "up", Green).Opt("graphMode", "none"),
		Stat("two", "d", "short", Q("up", "a"), Q("up", "b")).Neutral(),
		Gauge("g", "d", "percentunit", Q("up", "")).Neutral(),
		BarGauge("b", "d", "short", I("up", "{{pod}}")).Hue(Purple).Neutral(),
		Table("t", "d", "short", I("up", "")).Neutral().Organize(map[string]string{"pod": "Pod"}, "Value").Order("pod"),
		Heatmap("h", "d", "s", Target{Expr: "x_bucket", Format: "heatmap"}),
		StateTimeline("st", "d", "none", Q("up", "")).Neutral(),
		TextPanel("txt", "d", "# hi"),
		Logs("l", "d", Target{Expr: "*", QueryType: "instant"}).Using(VLogs),
		DashList("dl", "d", "kafka"),
	}
	d := &Dashboard{UID: "u", Title: "T", Rows: []*Row{{Panels: panels}}}

	// WHEN: the dashboard is rendered
	ps := panelsOf(decode(t, d, PromVar))

	// THEN: each kind gets sensible defaults and the explicit decorations survive
	byTitle := map[string]map[string]any{}
	for _, p := range ps {
		byTitle[p["title"].(string)] = p
	}
	tsd := byTitle["ts"]["fieldConfig"].(map[string]any)["defaults"].(map[string]any)
	custom := tsd["custom"].(map[string]any)
	if custom["drawStyle"] != "bars" || custom["thresholdsStyle"].(map[string]any)["mode"] != "dashed" || tsd["decimals"].(float64) != 2 {
		t.Errorf("timeseries decorations lost: %v", tsd)
	}
	if byTitle["ts"]["repeat"] != "request" || byTitle["plain"]["repeat"] != nil {
		t.Error("repeat should only be set where requested")
	}
	if byTitle["one"]["fieldConfig"].(map[string]any)["defaults"].(map[string]any)["noValue"] != "none" {
		t.Error("no-data text lost")
	}
	if byTitle["one"]["options"].(map[string]any)["textMode"] != "value" || byTitle["two"]["options"].(map[string]any)["textMode"] != "value_and_name" {
		t.Error("stat text mode should depend on the number of queries")
	}
	if byTitle["b"]["targets"].([]any)[0].(map[string]any)["format"] != "time_series" {
		t.Error("bar gauges need one series per bar, not a table frame")
	}
	org := byTitle["t"]["transformations"].([]any)[0].(map[string]any)["options"].(map[string]any)
	if byTitle["t"]["targets"].([]any)[0].(map[string]any)["format"] != "table" || org["indexByName"].(map[string]any)["pod"].(float64) != 0 {
		t.Error("tables need table-format instant queries and their transformations")
	}
	logsTarget := byTitle["l"]["targets"].([]any)[0].(map[string]any)
	if logsTarget["queryType"] != "instant" || logsTarget["editorMode"] != nil {
		t.Errorf("logs target wrong: %v", logsTarget)
	}
	if byTitle["txt"]["targets"] != nil || byTitle["txt"]["options"].(map[string]any)["content"] != "# hi" {
		t.Error("text panel must have content and no queries")
	}
	if byTitle["dl"]["options"].(map[string]any)["keepTime"] != true {
		t.Error("dashboard list must keep the time range")
	}
	if !strings.Contains(string(JSON(d, PromVar)), `"calculate": false`) {
		t.Error("heatmap must use pre-bucketed data")
	}
}
