package dash

import g "kafkalab/monitoring/dashgen/internal/grafana"

// Chaos is the chaos experiments dashboard (uid lab-chaos).
func Chaos() *g.Dashboard {
	d := demo("lab-chaos", "Kafka Lab · Chaos Experiments",
		"What the controlboard did and what it caused: experiment timeline, intensity, outcomes and time to recovery, side by side with the technical and business symptoms they produce.")
	d.Rows = []*g.Row{
		row("", false,
			g.Stat("Active experiments", "Experiments running right now.", "short",
				q(`count(lab_experiment_active > 0) or vector(0)`, "")).Thresholds(g.Green, g.Step{Color: g.Orange, Value: 1}),
			g.Stat("Chaos armed", "1 when the controlboard allows experiments to start.", "none",
				q(`max(lab_chaos_armed)`, "")).Thresholds(g.Blue, g.Step{Color: g.Orange, Value: 1}).Map("0", "safe", g.Blue).Map("1", "armed", g.Orange).Opt("graphMode", "none"),
			g.Stat("Experiments (window)", "Experiments finished within the time range by outcome.", "short",
				q(`sum by (outcome) (increase(lab_experiments_total[$__range]))`, "{{outcome}}")).Hue(g.Purple).Neutral().Dec(0).Opt("textMode", "value_and_name"),
			g.Stat("Median recovery", "Median time to recovery over the time range (all scenarios).", "s",
				g.Target{Expr: `histogram_quantile(0.5, sum by (vmrange) (increase(lab_experiment_recovery_seconds_bucket[$__range])))`}).
				Using(g.VMNative).Thresholds(g.Green, g.Step{Color: g.Yellow, Value: 60}, g.Step{Color: g.Red, Value: 300}),
			g.Stat("Drill score", "Score of the current incident drill (controlboard game).", "none",
				q(`max(lab_drill_score)`, "")).Hue(g.Green).Neutral(),
			g.Stat("Alerts received", "Alert notifications the controlboard received from Alertmanager in the time range (firing + resolved).", "short",
				q(`sum(increase(lab_alerts_received_total[$__range]))`, "")).Hue(g.Red).Neutral().Dec(0),
		),
		row("Experiments", false,
			g.StateTimeline("Experiment timeline",
				"One lane per experiment: when it was active and its scenario. 'hidden' is shown during an unsolved incident drill (the scenario is a secret until you solved it).",
				"none",
				q(`max by (scenario, experiment_id) (lab_experiment_active) > 0`, "{{scenario}} · {{experiment_id}}")).
				Thresholds(g.Orange).Map("1", "active", g.Orange).Size(16, 8),
			g.TimeSeries("Intensity",
				"Configured failure intensity of each running experiment (e.g. latency in ms, drop percentage), as set on the controlboard.",
				"short",
				q(`max by (experiment_id) (lab_experiment_intensity)`, "{{experiment_id}}")).
				Hue(g.Orange).Size(8, 8),
			g.TimeSeries("Time to recovery by scenario (p50 / p95)",
				"How long the system needed to return to normal after each scenario ended (VictoriaMetrics histogram).",
				"s",
				g.Target{Expr: `histogram_quantile(0.5, sum by (vmrange, scenario) (increase(lab_experiment_recovery_seconds_bucket[1h])))`, Legend: "p50 · {{scenario}}"},
				g.Target{Expr: `histogram_quantile(0.95, sum by (vmrange, scenario) (increase(lab_experiment_recovery_seconds_bucket[1h])))`, Legend: "p95 · {{scenario}}"}).
				Using(g.VMNative).Hue(g.Purple).Size(12, 8),
			g.BarGauge("Outcomes by scenario (window)",
				"Experiments per scenario and outcome (completed, aborted, failed) in the time range.",
				"short",
				iq(`sum by (scenario, outcome) (increase(lab_experiments_total[$__range])) > 0`, "{{scenario}} · {{outcome}}")).
				Hue(g.Purple).Neutral().Size(12, 8),
		),
		row("Observed degradation", false,
			g.TimeSeries("Business: orders and revenue per minute",
				"The shop's heartbeat. Compare dips with the experiment timeline above.",
				"short",
				q(`sum(rate(lab_orders_created_total[$__rate_interval])) * 60`, "orders created / min"),
				q(`sum(rate(lab_orders_shipped_total[$__rate_interval])) * 60`, "orders shipped / min")).
				Hue(g.Blue).Link("Commerce business", lab("lab-business", "")).Size(8, 8),
			g.TimeSeries("Kafka: consumer lag",
				"Total consumer lag per group (KMinion). Most experiments show up here first.",
				"short",
				q(`sum by (group_id) (kminion_kafka_consumer_group_topic_lag{kafka_cluster="kafka-lab"})`, "{{group_id}}")).
				Hue(g.Orange).Link("Consumer lag", lab("kafka-lag", "")).Size(8, 8),
			g.TimeSeries("Kafka: probe latency and partition health",
				"End-to-end probe roundtrip p99 (left) and under-replicated partitions (right axis).",
				"s",
				q(`histogram_quantile(0.99, sum by (le) (rate(kminion_end_to_end_roundtrip_latency_seconds_bucket{kafka_cluster="kafka-lab"}[$__rate_interval])))`, "probe p99"),
				q(`sum(kafka_server_replicamanager_underreplicatedpartitions{kafka_cluster="kafka-lab"})`, "under-replicated")).
				Over("byName", "under-replicated", map[string]any{"unit": "short", "custom.axisPlacement": "right", "color": map[string]any{"mode": "fixed", "fixedColor": g.Red}}).
				Hue(g.Green).Link("Cluster overview", lab("kafka-overview", "")).Size(8, 8),
			g.Table("Alerts firing now",
				"Alerts currently firing (vmalert), with severity and component.",
				"short",
				iq(`max by (alertname, severity, component) (ALERTS{alertstate="firing"})`, "")).
				Organize(map[string]string{"alertname": "Alert", "severity": "Severity", "component": "Component"}, "Value").
				Thresholds(g.Text).
				Over("byName", "Severity", map[string]any{
					"custom.cellOptions": map[string]any{"type": "color-text"},
					"mappings": []any{map[string]any{"type": "value", "options": map[string]any{
						"critical": map[string]any{"color": g.Red, "index": 0},
						"warning":  map[string]any{"color": g.Orange, "index": 1},
						"info":     map[string]any{"color": g.Blue, "index": 2},
					}}},
				}).Size(24, 6),
		),
	}
	return d
}
