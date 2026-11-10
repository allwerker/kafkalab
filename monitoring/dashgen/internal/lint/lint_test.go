package lint

import (
	"strings"
	"testing"

	g "kafkalab/monitoring/dashgen/internal/grafana"
)

func known(names ...string) func(string) bool {
	set := map[string]bool{}
	for _, n := range names {
		set[n] = true
	}
	return func(n string) bool { return set[n] }
}

func joined(r Report) string { return strings.Join(r.Problems, "\n") }

func Test_shouldReplaceGrafanaVariablesWithParseablePlaceholdersWhenExpanding(t *testing.T) {
	// GIVEN: a query using interval built-ins, a range in seconds and dashboard variables
	expr := `rate(x{a="$a",b=~"${b:regex}"}[$__rate_interval]) * $__range_s`

	// WHEN: it is expanded
	got := Expand(expr)

	// THEN: built-ins become durations or numbers and variables become a label value
	want := `rate(x{a="x",b=~"x"}[5m]) * 300`
	if got != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

func Test_shouldReturnSelectedMetricNamesWhenAPromQLQueryParses(t *testing.T) {
	// GIVEN: a PromQL query selecting two metrics (one through a __name__ matcher) and a nameless selector
	expr := `sum(rate(a_total[5m])) / sum({__name__="b_total"}) + count({job="x"})`

	// WHEN: its metrics are extracted
	names, err := Metrics(expr, PromQL)

	// THEN: both names are reported once, sorted
	if err != nil || strings.Join(names, ",") != "a_total,b_total" {
		t.Errorf("got %v, %v", names, err)
	}
}

func Test_shouldRejectMetricsQLFunctionsWhenTheQueryMustBePortablePromQL(t *testing.T) {
	// GIVEN: a MetricsQL-only histogram query
	expr := `buckets_limit(20, prometheus_buckets(sum by (vmrange) (rate(h_bucket[5m]))))`

	// WHEN: it is parsed as PromQL and as MetricsQL
	_, promErr := Metrics(expr, PromQL)
	names, mqlErr := Metrics(expr, MetricsQL)

	// THEN: strict PromQL refuses it, MetricsQL accepts it and finds the bucket metric
	if promErr == nil {
		t.Error("PromQL must reject MetricsQL functions")
	}
	if mqlErr != nil || strings.Join(names, ",") != "h_bucket" {
		t.Errorf("MetricsQL: %v %v", names, mqlErr)
	}
}

func Test_shouldReportSyntaxErrorWhenAMetricsQLQueryIsBroken(t *testing.T) {
	// GIVEN: a demo query with an unbalanced parenthesis
	expr := `sum(rate(x[5m])`

	// WHEN: it is parsed as MetricsQL
	_, err := Metrics(expr, MetricsQL)

	// THEN: the error names the dialect
	if err == nil || !strings.Contains(err.Error(), "MetricsQL") {
		t.Errorf("want MetricsQL error, got %v", err)
	}
}

func Test_shouldCheckLogsQLStructureWhenTheQueryTargetsVictoriaLogs(t *testing.T) {
	// GIVEN: valid and broken LogsQL queries
	cases := map[string]bool{
		`ns:in(a, b) "ERROR (x" | stats by (ns) count() n`: true,
		`   `:                  false,
		`stats by (ns count()`: false,
		`a) (`:                 false,
		`"open`:                false,
		`x | fields [a)`:       false,
	}

	for q, ok := range cases {
		// WHEN: the query is checked
		_, err := Metrics(q, LogsQL)

		// THEN: only structurally sound queries pass
		if (err == nil) != ok {
			t.Errorf("%q: got err=%v, want ok=%v", q, err, ok)
		}
	}
}

func bundleDash(panels ...*g.Panel) []byte {
	d := &g.Dashboard{
		UID: "kafka-x", Title: "X",
		Variables: []g.Variable{
			{Name: "datasource", Kind: "datasource", Query: "prometheus"},
			{Name: "kafka_cluster", Kind: "query", Query: `label_values(up{job="kafka"}, kafka_cluster)`},
			{Name: "filter", Kind: "textbox", Query: "*"},
		},
		Annotations: []g.Annotation{{Name: "alerts", DS: g.PromVar, Expr: `ALERTS`}},
		Rows:        []*g.Row{{Title: "r", Collapsed: true, Panels: panels}},
	}
	return g.JSON(d, g.PromVar)
}

func Test_shouldAcceptAWellDocumentedBundleDashboardWhenEveryRuleIsMet(t *testing.T) {
	// GIVEN: a bundle dashboard with a documented stat on a catalogued metric and a drill-down link
	raw := bundleDash(g.Stat("Brokers", "Number of brokers that are up right now, should equal the cluster size.", "short",
		g.Q(`sum(up{kafka_cluster="$kafka_cluster"})`, "")).Neutral().Link("deep dive", "/d/kafka-broker/?x"))
	p := Profile{Bundle: true, Known: known("up", "ALERTS"), UIDs: map[string]bool{"kafka-broker": true}}

	// WHEN: it is linted
	r := Dashboard(raw, p)

	// THEN: there are no problems and the used metrics are reported
	if len(r.Problems) > 0 || !r.Used["up"] || !r.Used["ALERTS"] {
		t.Errorf("unexpected: %v used=%v", r.Problems, r.Used)
	}
}

func Test_shouldReportEveryViolationWhenABundlePanelIsSloppy(t *testing.T) {
	// GIVEN: an undocumented stat without unit and thresholds on an unknown metric, on a fixed
	// datasource, linking to a dashboard that does not exist, plus an empty and a broken query
	sloppy := g.Stat("", "short", "", g.Q(`rate(mystery_total[5m])`, ""), g.Q(``, ""), g.Q(`sum(`, "")).
		Using(g.VM).Link("x", "/d/nowhere/")
	raw := bundleDash(sloppy, g.TextPanel("note", "A text panel needs no unit nor thresholds at all.", "hi"))
	p := Profile{Bundle: true, Known: known("ALERTS", "up"), UIDs: map[string]bool{}}

	// WHEN: it is linted
	out := joined(Dashboard(raw, p))

	// THEN: each problem is reported in a way that says what to fix
	for _, want := range []string{
		"missing title", "description missing", "no unit", "without thresholds", "link to unknown dashboard \"nowhere\"",
		"${datasource} prometheus variable", "metric mystery_total is not documented", "empty query", "PromQL:",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing problem %q in:\n%s", want, out)
		}
	}
	if strings.Contains(out, `"note": no unit`) {
		t.Error("text panels must not need a unit")
	}
}

func Test_shouldRequireTheDatasourceVariableWhenTheDashboardBelongsToTheBundle(t *testing.T) {
	// GIVEN: a dashboard without variables, an outdated schema and no uid
	raw := []byte(`{"title":"x","schemaVersion":27,"panels":[],"templating":{"list":[]}}`)

	// WHEN: it is linted as bundle and as demo dashboard
	bundle := joined(Dashboard(raw, Profile{Bundle: true, Known: known()}))
	demo := joined(Dashboard(raw, Profile{Known: known()}))

	// THEN: only the bundle needs the datasource variable; both flag schema and uid
	if !strings.Contains(bundle, "need a variable 'datasource'") || strings.Contains(demo, "need a variable") {
		t.Errorf("datasource variable rule wrong:\nbundle: %s\ndemo: %s", bundle, demo)
	}
	if !strings.Contains(demo, "schemaVersion 27") || !strings.Contains(demo, "uid and title are required") {
		t.Errorf("schema/uid not flagged: %s", demo)
	}
}

func Test_shouldLintLogsAndMetricsQLTargetsWhenTheDashboardIsADemo(t *testing.T) {
	// GIVEN: a demo dashboard with a MetricsQL heatmap, a duplicate panel id and a VictoriaLogs panel
	d := &g.Dashboard{UID: "lab-x", Title: "X", Rows: []*g.Row{{Panels: []*g.Panel{
		g.Heatmap("Latency", "Order latency distribution from vmrange buckets of the shop.", "s",
			g.Target{Expr: `buckets_limit(10, prometheus_buckets(rate(lab_x_bucket[5m])))`, Format: "heatmap"}),
		g.Logs("Logs", "All log lines of the lab, newest first, filtered by namespace.",
			g.Target{Expr: `ns:in($namespace) | limit 10`, QueryType: "instant"}).Using(g.VLogs),
	}}}}
	raw := strings.Replace(string(g.JSON(d, g.VM)), `"id": 2`, `"id": 1`, 1)

	// WHEN: it is linted with the demo profile
	r := Dashboard([]byte(raw), Profile{Known: known("lab_x_bucket")})

	// THEN: both queries are accepted and only the duplicate id is reported
	if len(r.Problems) != 1 || !strings.Contains(r.Problems[0], "duplicate panel id 1") {
		t.Errorf("unexpected problems: %v", r.Problems)
	}
}

func Test_shouldReportInvalidJSONWhenTheFileIsCorrupt(t *testing.T) {
	// GIVEN: a truncated dashboard file
	raw := []byte(`{"uid":`)

	// WHEN: it is linted
	r := Dashboard(raw, Profile{Known: known()})

	// THEN: the JSON error is reported
	if len(r.Problems) != 1 || !strings.Contains(r.Problems[0], "invalid JSON") {
		t.Errorf("got %v", r.Problems)
	}
}
