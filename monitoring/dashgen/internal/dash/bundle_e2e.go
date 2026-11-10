package dash

import g "kafkalab/monitoring/dashgen/internal/grafana"

func e2eQuantile(quantile, metric, by string) string {
	return `histogram_quantile(` + quantile + `, sum by (` + by + `) (rate(kminion_end_to_end_` + metric + `_bucket{@C}[@RI])))`
}

// E2E is the end-to-end probe dashboard (uid kafka-e2e).
func E2E() *g.Dashboard {
	d := bundle("kafka-e2e", "Kafka · End-to-End Probe",
		"KMinion continuously produces small probe messages (acks=all) to its own topic with one partition per broker and consumes them again. This is the closest thing to an application's view: if the probe is slow or loses messages, applications are affected too.",
		clusterVar("kminion_kafka_cluster_info"))
	d.Rows = []*g.Row{
		row("", false,
			g.Stat("Probe success (5 m)",
				"Probe messages received back divided by messages produced during the last 5 minutes. 100 % = healthy; below 99 % messages are lost or exceed the roundtrip SLA.",
				"percentunit", q(`sum(increase(kminion_end_to_end_messages_received_total{@C}[5m])) / clamp_min(sum(increase(kminion_end_to_end_messages_produced_total{@C}[5m])), 1)`, "")).
				Thresholds(g.Red, g.Step{Color: g.Yellow, Value: 0.95}, g.Step{Color: g.Green, Value: 0.99}).SetMax(1).Dec(1),
			g.Stat("Roundtrip p99", "99th percentile time from produce to consume of a probe message.",
				"s", q(e2eQuantile("0.99", "roundtrip_latency_seconds", "le"), "")).
				Thresholds(g.Green, g.Step{Color: g.Yellow, Value: 0.5}, g.Step{Color: g.Red, Value: 2}),
			g.Stat("Produce p99 (acks=all)", "99th percentile of the produce acknowledgement time: leader append + replication to the ISR.",
				"s", q(e2eQuantile("0.99", "produce_latency_seconds", "le"), "")).
				Thresholds(g.Green, g.Step{Color: g.Yellow, Value: 0.25}, g.Step{Color: g.Red, Value: 1}),
			g.Stat("Offset commit p99", "99th percentile of the probe consumer's offset commit latency (group coordinator health).",
				"s", q(e2eQuantile("0.99", "offset_commit_latency_seconds", "le"), "")).
				Thresholds(g.Green, g.Step{Color: g.Yellow, Value: 0.25}, g.Step{Color: g.Red, Value: 1}),
			okIfZero(g.Stat("Lost messages (window)", "Probe messages that never arrived within the dashboard time range. Lost = produced and acknowledged but not consumed within the SLA.",
				"short", q(`sum(increase(kminion_end_to_end_messages_lost_total{@C}[$__range]))`, ""))),
			okIfZero(g.Stat("Produce failures (window)", "Probe produce requests that failed (no leader, not enough replicas, timeouts) during the time range.",
				"short", q(`sum(increase(kminion_end_to_end_messages_produced_failed_total{@C}[$__range]))`, ""))),
		),
		row("Latency", false,
			g.Heatmap("Roundtrip latency distribution",
				"Distribution of probe roundtrip times over time (histogram buckets). A tight band at the bottom is healthy; a second band or a smear upwards shows intermittent slowness (GC pauses, leader elections, slow disks).",
				"s",
				g.Target{Expr: X(`sum by (le) (increase(kminion_end_to_end_roundtrip_latency_seconds_bucket{@C}[@RI]))`), Legend: "{{le}}", Format: "heatmap"}).
				Opt("calculate", false).Opt("yAxis", map[string]any{"axisPlacement": "left", "unit": "s"}).Size(12, 9),
			g.TimeSeries("Roundtrip latency p50 / p95 / p99",
				"Percentiles of the produce→consume roundtrip of probe messages. The probe SLA is configured in KMinion (roundtripSla).",
				"s",
				q(e2eQuantile("0.5", "roundtrip_latency_seconds", "le"), "p50"),
				q(e2eQuantile("0.95", "roundtrip_latency_seconds", "le"), "p95"),
				q(e2eQuantile("0.99", "roundtrip_latency_seconds", "le"), "p99")).
				Thresholds(g.Green, g.Step{Color: g.Yellow, Value: 0.5}, g.Step{Color: g.Red, Value: 2}).Lines("dashed").
				Hue(g.Green).Size(12, 9),
			g.TimeSeries("Produce latency p99 per partition",
				"Produce acknowledgement p99 per probe partition. KMinion creates one partition per broker and spreads the leaders, so a single slow line points to one slow leader broker (check its leader in Topics, Partitions & Storage).",
				"s",
				q(e2eQuantile("0.99", "produce_latency_seconds", "le, partition_id"), "partition {{partition_id}}")).
				Hue(g.Blue).Size(12, 8),
			g.TimeSeries("Roundtrip latency p99 per partition",
				"Roundtrip p99 per probe partition: isolates a slow partition leader or a slow follower in its ISR.",
				"s",
				q(e2eQuantile("0.99", "roundtrip_latency_seconds", "le, partition_id"), "partition {{partition_id}}")).
				Hue(g.Purple).Size(12, 8),
		),
		row("Messages", false,
			g.TimeSeries("Probe messages produced / received / lost",
				"Rates of probe messages. Produced and received should overlap; any 'lost' or 'failed' series means an availability problem.",
				"ops",
				q(`sum(rate(kminion_end_to_end_messages_produced_total{@C}[@RI]))`, "produced"),
				q(`sum(rate(kminion_end_to_end_messages_received_total{@C}[@RI]))`, "received"),
				q(`sum(rate(kminion_end_to_end_messages_lost_total{@C}[@RI]))`, "lost"),
				q(`sum(rate(kminion_end_to_end_messages_produced_failed_total{@C}[@RI]))`, "produce failed")).
				SeriesColor("produced", g.Blue).SeriesColor("received", g.Green).SeriesColor("lost", g.Red).SeriesColor("produce failed", "dark-red").Size(12, 8),
			g.TimeSeries("In-flight probe messages",
				"Probe messages produced but not yet acknowledged, per partition. Rising values mean a partition leader does not answer.",
				"short",
				q(`sum by (partition_id) (kminion_end_to_end_messages_produced_in_flight{@C})`, "partition {{partition_id}}")).
				Hue(g.Orange).Size(6, 8),
			g.TimeSeries("Offset commits",
				"Offset commits of the probe consumer and failed commits. Failing commits mean the group coordinator (on one broker) has a problem.",
				"ops",
				q(`sum(rate(kminion_end_to_end_offset_commits_total{@C}[@RI]))`, "commits"),
				q(`sum(rate(kminion_end_to_end_offset_commits_failed_total{@C}[@RI]))`, "failed")).
				SeriesColor("commits", g.Green).SeriesColor("failed", g.Red).Size(6, 8),
		),
	}
	return d
}
