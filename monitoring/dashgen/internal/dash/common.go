// Package dash defines every dashboard of the Kafka Lab: the portable bundle (folder
// "Kafka (bundle)", PromQL only, `${datasource}` variable) and the demo dashboards (folder
// "Kafka Lab", may use MetricsQL and lab-specific metrics).
package dash

import (
	"strings"

	g "kafkalab/monitoring/dashgen/internal/grafana"
)

// Set names the dashboard folder a dashboard belongs to.
type Set string

// The two dashboard sets.
const (
	Bundle Set = "bundle"
	Demo   Set = "demo"
)

// Board is a dashboard together with its folder and default datasource.
type Board struct {
	Set  Set
	DS   g.Datasource
	Dash *g.Dashboard
}

// File is the JSON file name of the board (uid.json).
func (b Board) File() string { return b.Dash.UID + ".json" }

// JSON renders the board.
func (b Board) JSON() []byte { return g.JSON(b.Dash, b.DS) }

// All returns every dashboard, bundle first.
func All() []Board {
	out := []Board{}
	for _, d := range []*g.Dashboard{
		Overview(), Broker(), KRaft(), Topics(), Lag(), E2E(), JVM(), Pods(),
	} {
		out = append(out, Board{Set: Bundle, DS: g.PromVar, Dash: d})
	}
	for _, d := range []*g.Dashboard{
		Home(), Business(), Services(), Chaos(), Footprint(), Monitoring(), Postgres(), LogsBoard(),
	} {
		out = append(out, Board{Set: Demo, DS: g.VM, Dash: d})
	}
	return out
}

// Selector tokens expanded in every bundle query, so label filters are spelled once.
var tokens = strings.NewReplacer(
	"@BT", `kafka_cluster="$kafka_cluster",kafka_broker=~"$kafka_broker",topic=~"$topic"`,
	"@B", `kafka_cluster="$kafka_cluster",kafka_broker=~"$kafka_broker"`,
	"@KG", `kafka_cluster="$kafka_cluster",group_id=~"$group",topic_name=~"$topic"`,
	"@KT", `kafka_cluster="$kafka_cluster",topic_name=~"$topic"`,
	"@C", `kafka_cluster="$kafka_cluster"`,
	"@P", `namespace=~"$namespace",pod=~"$pod"`,
	"@RI", `$__rate_interval`,
)

// X expands the selector tokens of a query.
func X(expr string) string { return tokens.Replace(expr) }

// q is a range query with expanded tokens.
func q(expr, legend string) g.Target { return g.Q(X(expr), legend) }

// iq is an instant table query with expanded tokens.
func iq(expr, legend string) g.Target { return g.I(X(expr), legend) }

// drill builds a drill-down URL to another dashboard keeping time range, datasource and
// cluster, plus extra query parameters (e.g. "var-kafka_broker=${__field.labels.kafka_broker}").
func drill(uid, extra string) string {
	u := "/d/" + uid + "/?${__url_time_range}&${datasource:queryparam}&${kafka_cluster:queryparam}"
	if extra != "" {
		u += "&" + extra
	}
	return u
}

// ---------------------------------------------------------------------------------------------
// Variables
// ---------------------------------------------------------------------------------------------

func dsVar() g.Variable {
	return g.Variable{Name: "datasource", Label: "Data source", Kind: "datasource", Query: "prometheus"}
}

// clusterVar selects one Kafka cluster from the label of the given metric.
func clusterVar(metric string) g.Variable {
	return g.Variable{
		Name: "kafka_cluster", Label: "Cluster", Kind: "query", Sort: 1,
		Query: "label_values(" + metric + ", kafka_cluster)",
	}
}

func brokerVar() g.Variable {
	return g.Variable{
		Name: "kafka_broker", Label: "Broker", Kind: "query", Multi: true, All: true, AllValue: ".+",
		Default: "All", Sort: 3,
		Query: X(`label_values(kafka_server_kafkaserver_brokerstate{@C}, kafka_broker)`),
	}
}

// topicVar: allValue ".+" so JMX aggregate series (no topic label) never match "All".
func topicVar() g.Variable {
	return g.Variable{
		Name: "topic", Label: "Topic", Kind: "query", Multi: true, All: true, AllValue: ".+",
		Default: "All", Sort: 1,
		Query: X(`label_values(kminion_kafka_topic_info{@C}, topic_name)`),
	}
}

func groupVar() g.Variable {
	return g.Variable{
		Name: "group", Label: "Consumer group", Kind: "query", Multi: true, All: true, AllValue: ".+",
		Default: "All", Sort: 1,
		Query: X(`label_values(kminion_kafka_consumer_group_info{@C}, group_id)`),
	}
}

// ---------------------------------------------------------------------------------------------
// Annotations and links shared by every bundle dashboard
// ---------------------------------------------------------------------------------------------

func bundleAnnotations() []g.Annotation {
	return []g.Annotation{
		{Name: "Chaos & incidents (tag chaos)", DS: g.GrafanaDS, Color: "orange", Tags: []string{"chaos", "incident"}},
		{
			Name: "Firing Kafka alerts", DS: g.PromVar, Color: "red",
			Expr:  `ALERTS{alertstate="firing",component=~"kafka|consumer"}`,
			Title: "{{alertname}}", Text: "severity {{severity}} · component {{component}}", TagKey: "severity,component",
		},
	}
}

func bundleLinks() []g.Link {
	return []g.Link{{Title: "Kafka dashboards", Tags: []string{"kafka-bundle"}}}
}

// bundle creates a bundle dashboard skeleton.
func bundle(uid, title, desc string, vars ...g.Variable) *g.Dashboard {
	return &g.Dashboard{
		UID: uid, Title: title, Description: desc,
		Tags:        []string{"kafka", "kafka-bundle"},
		Variables:   append([]g.Variable{dsVar()}, vars...),
		Annotations: bundleAnnotations(),
		Links:       bundleLinks(),
	}
}

func row(title string, collapsed bool, panels ...*g.Panel) *g.Row {
	return &g.Row{Title: title, Collapsed: collapsed, Panels: panels}
}

// Common thresholds.
func okIfZero(p *g.Panel) *g.Panel { return p.Thresholds(g.Green, g.Step{Color: g.Red, Value: 1}) }

func idleThresholds(p *g.Panel) *g.Panel {
	return p.Thresholds(g.Red, g.Step{Color: g.Yellow, Value: 0.2}, g.Step{Color: g.Green, Value: 0.4})
}
