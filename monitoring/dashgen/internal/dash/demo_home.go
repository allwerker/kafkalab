package dash

import g "kafkalab/monitoring/dashgen/internal/grafana"

const homeIntro = `### Welcome to the Kafka Lab

A simulated online shop runs on a 3-node Kafka cluster. Break things on the **[controlboard](` + BoardURL + `)**, then find out what happened here.

| Start with | When you want to know … |
|---|---|
| [Cluster Overview](/d/kafka-overview/?var-datasource=vm&var-kafka_cluster=kafka-lab) | is Kafka healthy? |
| [Consumer Groups & Lag](/d/kafka-lag/?var-datasource=vm&var-kafka_cluster=kafka-lab) | who is falling behind, and by how long? |
| [Commerce Business](/d/lab-business/) | does the shop still make money? |
| [Chaos Experiments](/d/lab-chaos/) | what did the last experiment break, and how fast did it recover? |

Orange markers on every graph are chaos experiments, red ones firing alerts. Tools: [AKHQ](http://akhq.localhost:8080) · [VMUI](http://vm.localhost:8080/vmui) · [vmalert](http://vmalert.localhost:8080) · [Alertmanager](http://alertmanager.localhost:8080) · [Logs](http://logs.localhost:8080/select/vmui)`

// Home is the lab entry page (uid lab-home).
func Home() *g.Dashboard {
	d := demo("lab-home", "Kafka Lab · Home",
		"Entry page of the Kafka Lab: health tiles for Kafka, the shop and the lab itself, plus navigation to every dashboard and to the controlboard.")
	d.Refresh = "15s"
	d.Rows = []*g.Row{
		row("", false,
			g.TextPanel("Kafka Lab", "How to use the lab and where to go next.", homeIntro).Size(12, 11),
			g.DashList("Kafka (bundle)", "The portable Kafka dashboards. Links keep the current time range.", "kafka-bundle").Size(6, 11),
			g.DashList("Kafka Lab", "Dashboards of the lab: business, services, chaos, footprint and logs.", "kafka-lab").Size(6, 11),
		),
		row("Kafka", false,
			g.Stat("Brokers active", "Brokers registered and unfenced at the active controller. The lab runs 3.",
				"short", q(`max(kafka_controller_kafkacontroller_activebrokercount{kafka_cluster="kafka-lab"})`, "")).
				Thresholds(g.Red, g.Step{Color: g.Yellow, Value: 2}, g.Step{Color: g.Green, Value: 3}).Link("Cluster overview", lab("kafka-overview", "")),
			okIfZero(g.Stat("Partitions in trouble", "Under-replicated + offline partitions. 0 = every partition is fully replicated.",
				"short", q(`sum(kafka_server_replicamanager_underreplicatedpartitions{kafka_cluster="kafka-lab"}) + max(kafka_controller_kafkacontroller_offlinepartitionscount{kafka_cluster="kafka-lab"})`, ""))).
				Link("Cluster overview", lab("kafka-overview", "")),
			g.Stat("Consumer lag", "Messages not yet consumed by all groups together.",
				"short", q(`sum(kminion_kafka_consumer_group_topic_lag{kafka_cluster="kafka-lab"})`, "")).
				Thresholds(g.Green, g.Step{Color: g.Yellow, Value: 1000}, g.Step{Color: g.Red, Value: 10000}).Link("Consumer lag", lab("kafka-lag", "")),
			g.Stat("Probe roundtrip p99", "KMinion end-to-end probe: produce + consume roundtrip, 99th percentile.",
				"s", q(`histogram_quantile(0.99, sum by (le) (rate(kminion_end_to_end_roundtrip_latency_seconds_bucket{kafka_cluster="kafka-lab"}[$__rate_interval])))`, "")).
				Thresholds(g.Green, g.Step{Color: g.Yellow, Value: 0.5}, g.Step{Color: g.Red, Value: 2}).Link("End-to-end probe", lab("kafka-e2e", "")),
		),
		row("Shop", false,
			g.Stat("Orders per minute", "Orders created per minute (analytics service).",
				"short", q(`sum(rate(lab_orders_created_total[$__rate_interval])) * 60`, "")).Hue(g.Blue).Neutral().Link("Commerce business", lab("lab-business", "")),
			g.Stat("Payment success", "Share of payments that succeeded during the last 5 minutes.",
				"percentunit", q(`sum(rate(lab_payments_total{result="completed"}[5m])) / sum(rate(lab_payments_total[5m]))`, "")).
				Thresholds(g.Red, g.Step{Color: g.Yellow, Value: 0.8}, g.Step{Color: g.Green, Value: 0.9}).SetMax(1).Link("Commerce business", lab("lab-business", "")),
			g.Stat("Revenue per minute", "Value of shipped orders per minute.",
				"currencyEUR", q(`sum(rate(lab_revenue_cents_total[$__rate_interval])) * 60 / 100`, "")).Hue(g.Green).Neutral().Link("Commerce business", lab("lab-business", "")),
			okIfZero(g.Stat("Dead-lettered (15 m)", "Records sent to a dead-letter topic during the last 15 minutes.",
				"short", q(`sum(increase(lab_dlq_records_total[15m])) or vector(0)`, ""))).Link("Commerce business", lab("lab-business", "")),
		),
		row("Lab", false,
			g.Stat("Active experiments", "Chaos experiments running right now (controlboard).",
				"short", q(`count(lab_experiment_active > 0) or vector(0)`, "")).
				Thresholds(g.Green, g.Step{Color: g.Orange, Value: 1}).Link("Chaos experiments", lab("lab-chaos", "")),
			okIfZero(g.Stat("Firing alerts", "Alerts firing in vmalert (warning and critical).",
				"short", q(`count(ALERTS{alertstate="firing",severity=~"warning|critical"}) or vector(0)`, ""))).
				Link("vmalert", "http://vmalert.localhost:8080/vmalert/alerts"),
			g.Stat("Stack memory", "Working-set memory of all containers vs. the 10 GB budget of the lab.",
				"decbytes", q(`sum(container_memory_working_set_bytes{container!=""})`, "")).
				Thresholds(g.Green, g.Step{Color: g.Yellow, Value: 8e9}, g.Step{Color: g.Red, Value: 10e9}).Link("Stack footprint", lab("lab-footprint", "")),
			g.Stat("Max JMX scrape time", "Slowest JMX exporter scrape of the brokers (runs inside the broker JVM). Keep below 25 % of the 15 s interval.",
				"s", q(`max(jmx_scrape_duration_seconds{kafka_cluster="kafka-lab"})`, "")).
				Thresholds(g.Green, g.Step{Color: g.Yellow, Value: 2}, g.Step{Color: g.Red, Value: 3.75}).Link("Monitoring overhead", lab("lab-monitoring", "")),
		),
	}
	for _, r := range d.Rows[1:] {
		for _, p := range r.Panels {
			p.Size(6, 4)
		}
	}
	return d
}
