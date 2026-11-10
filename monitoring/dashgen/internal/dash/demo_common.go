package dash

import g "kafkalab/monitoring/dashgen/internal/grafana"

// BoardURL is the gamified controlboard of the lab.
const BoardURL = "http://board.localhost:8080"

// lab builds a URL to another dashboard keeping the time range; bundle dashboards get the lab's
// datasource and cluster.
func lab(uid, extra string) string {
	u := "/d/" + uid + "/?${__url_time_range}"
	if len(uid) > 6 && uid[:6] == "kafka-" {
		u += "&var-datasource=vm&var-kafka_cluster=kafka-lab"
	}
	if extra != "" {
		u += "&" + extra
	}
	return u
}

func demoAnnotations() []g.Annotation {
	return []g.Annotation{
		{
			Name: "Chaos experiments", DS: g.VM, Color: "orange",
			Expr:  `max by (experiment_id, scenario, category, severity) (lab_experiment_active) > 0`,
			Title: "{{scenario}}", Text: "{{category}} · severity {{severity}} · id {{experiment_id}}", TagKey: "category,severity",
		},
		{Name: "Chaos & incidents (tag chaos)", DS: g.GrafanaDS, Color: "yellow", Tags: []string{"chaos", "incident"}},
		{
			Name: "Firing alerts", DS: g.VM, Color: "red",
			Expr:  `ALERTS{alertstate="firing",severity=~"critical|warning",component!=""}`,
			Title: "{{alertname}}", Text: "severity {{severity}} · component {{component}}", TagKey: "severity,component",
		},
	}
}

func demoLinks() []g.Link {
	return []g.Link{
		{Title: "Kafka Lab", Tags: []string{"kafka-lab"}},
		{Title: "Kafka (bundle)", Tags: []string{"kafka-bundle"}},
		{Title: "Controlboard", URL: BoardURL, NewTab: true},
	}
}

// demo creates a demo dashboard skeleton.
func demo(uid, title, desc string, vars ...g.Variable) *g.Dashboard {
	return &g.Dashboard{
		UID: uid, Title: title, Description: desc,
		Tags:        []string{"kafka-lab"},
		Variables:   vars,
		Annotations: demoAnnotations(),
		Links:       demoLinks(),
	}
}

func serviceVar(metric string) g.Variable {
	return g.Variable{
		Name: "service", Label: "Service", Kind: "query", Multi: true, All: true, AllValue: ".*", Default: "All", Sort: 1,
		Query: "label_values(" + metric + ", service)",
	}
}
