package dash

import g "kafkalab/monitoring/dashgen/internal/grafana"

const svc = `service=~"$service"`

func vmQuantile(quantile, metric, by string) g.Target {
	return g.Target{
		Expr:   `histogram_quantile(` + quantile + `, sum by (vmrange, ` + by + `) (rate(` + metric + `_bucket{` + svc + `}[5m])))`,
		Legend: "p" + quantile[2:] + " · {{" + by + "}}",
	}
}

// Services is the service & client runtime dashboard (uid lab-services).
func Services() *g.Dashboard {
	d := demo("lab-services", "Kafka Lab · Service & Client Runtime",
		"Inside the commerce services: record processing, retries and dead letters, consumer rebalances, the applied controlboard configuration, franz-go client metrics and the Go runtime.",
		serviceVar("kafka_client_connects_total"))
	d.Rows = []*g.Row{
		row("", false,
			g.Stat("Records processed / s", "Records finished by the selected services (all results).", "short",
				q(`sum(rate(lab_records_processed_total{`+svc+`}[$__rate_interval]))`, "")).Hue(g.Blue).Neutral(),
			g.Stat("Records produced / s", "Records produced and acknowledged by the selected services (franz-go).", "short",
				q(`sum(rate(kafka_client_produce_records_total{`+svc+`}[$__rate_interval]))`, "")).Hue(g.Purple).Neutral(),
			g.Stat("Retries / min", "Retried batches per minute (database, produce, transient and schema registry errors).", "short",
				q(`sum(rate(lab_processing_retries_total{`+svc+`}[$__rate_interval])) * 60 or vector(0)`, "")).
				Thresholds(g.Green, g.Step{Color: g.Yellow, Value: 1}, g.Step{Color: g.Red, Value: 30}),
			g.Stat("Rebalances (window)", "Consumer group membership changes (assigned/revoked/lost) in the time range. Each rebalance pauses consumption briefly.", "short",
				q(`sum(increase(lab_rebalances_total{`+svc+`}[$__range]))`, "")).
				Thresholds(g.Green, g.Step{Color: g.Yellow, Value: 5}, g.Step{Color: g.Red, Value: 20}).Dec(0),
			g.Stat("Paused consumers", "Pods whose consumer is paused by the 'Silent Failure' knob of the controlboard.", "short",
				q(`sum(lab_consumer_paused{`+svc+`}) or vector(0)`, "")).Thresholds(g.Green, g.Step{Color: g.Red, Value: 1}),
			g.Stat("Control generation", "Applied control snapshot generation per service. All services should show the same number shortly after a change on the controlboard; a lagging service did not apply the latest settings.", "none",
				q(`max by (service) (lab_control_generation{`+svc+`})`, "{{service}}")).Hue(g.Purple).Neutral().Opt("graphMode", "none"),
		),
		row("Processing", false,
			g.TimeSeries("Records processed by result",
				"Records finished per second by service and result: ok, duplicate (skipped idempotently) and dlq (dead-lettered).",
				"short",
				q(`sum by (service, result) (rate(lab_records_processed_total{`+svc+`}[$__rate_interval]))`, "{{service}} · {{result}}")).
				Over("byRegexp", ".*dlq", map[string]any{"color": map[string]any{"mode": "fixed", "fixedColor": g.Red}}).
				Hue(g.Blue).Size(12, 8),
			g.TimeSeries("Processing time p95 per record",
				"Handler time per record (95th percentile, VictoriaMetrics histogram). Includes database work; compare with database transaction time below.",
				"s", vmQuantile("0.95", "lab_processing_duration_seconds", "service")).
				Using(g.VMNative).Hue(g.Purple).Size(12, 8),
			g.TimeSeries("Event age at processing p95",
				"How old an event is when a service processes it (now − occurredAt): end-to-end freshness including Kafka lag. Rising age with stable processing time = the consumer is lagging.",
				"s", vmQuantile("0.95", "lab_event_age_seconds", "service")).
				Using(g.VMNative).Thresholds(g.Green, g.Step{Color: g.Yellow, Value: 5}, g.Step{Color: g.Red, Value: 30}).Lines("dashed").
				Hue(g.Orange).Size(8, 8),
			g.TimeSeries("Batch duration p95 (poll → commit)",
				"Time from polling a partition batch to committing its offsets (95th percentile).",
				"s", vmQuantile("0.95", "lab_batch_duration_seconds", "service")).
				Using(g.VMNative).Hue(g.Blue).Size(8, 8),
			g.TimeSeries("Database transaction time p95",
				"PostgreSQL transaction duration per service (95th percentile). Slow transactions slow down every consumer that writes state.",
				"s", vmQuantile("0.95", "lab_db_tx_duration_seconds", "service")).
				Using(g.VMNative).Hue(g.Green).Size(8, 8),
			g.TimeSeries("Retries by reason",
				"Retried batches per minute by service and reason (db, produce, transient, registry).",
				"short",
				q(`sum by (service, reason) (rate(lab_processing_retries_total{`+svc+`}[$__rate_interval])) * 60`, "{{service}} · {{reason}}")).
				Hue(g.Orange).Bars().Size(12, 7),
			g.TimeSeries("Dead-lettered records by service",
				"Records dead-lettered per minute by service, topic and reason.",
				"short",
				q(`sum by (service, topic, reason) (rate(lab_dlq_records_total{`+svc+`}[$__rate_interval])) * 60`, "{{service}} · {{topic}} · {{reason}}")).
				Hue(g.Red).Bars().Size(12, 7),
		),
		row("Consumer group membership", false,
			g.TimeSeries("Rebalances by kind",
				"Partition assignment changes per service: assigned, revoked, lost (lost = the member was kicked out, e.g. after a session timeout).",
				"short",
				q(`sum by (service, kind) (increase(lab_rebalances_total{`+svc+`}[$__rate_interval]))`, "{{service}} · {{kind}}")).
				SeriesColor(".*lost", g.Red).Hue(g.Purple).Bars().Size(8, 8),
			g.TimeSeries("Partitions assigned per pod",
				"Partitions each pod owns per topic. Uneven numbers mean uneven load between replicas of a service.",
				"short",
				q(`sum by (pod, topic) (lab_partitions_assigned{`+svc+`})`, "{{pod}} · {{topic}}")).
				Hue(g.Blue).Size(8, 8),
			g.TimeSeries("Applied controlboard knobs",
				"Numeric knob values the services applied from the controlboard (only non-default knobs are exported), and Tuning Lab client rebuilds.",
				"short",
				q(`max by (service, knob) (lab_control_knob{`+svc+`})`, "{{service}} · {{knob}}"),
				q(`sum by (service) (increase(lab_client_rebuilds_total{`+svc+`}[$__rate_interval]))`, "rebuilds · {{service}}")).
				Hue(g.Orange).Size(8, 8),
		),
		row("franz-go client", false,
			g.TimeSeries("Produce throughput",
				"Records and bytes produced per second by topic (acknowledged batches).",
				"short",
				q(`sum by (topic) (rate(kafka_client_produce_records_total{`+svc+`}[$__rate_interval]))`, "{{topic}}")).
				Hue(g.Purple).Size(8, 8),
			g.TimeSeries("Average records per produce batch",
				"Records per produce batch by topic. Small batches (≈1) cost broker CPU; tuning linger/batch size on the controlboard changes this line.",
				"short",
				q(`sum by (topic) (rate(kafka_client_produce_batch_records_sum{`+svc+`}[$__rate_interval])) / sum by (topic) (rate(kafka_client_produce_batch_records_count{`+svc+`}[$__rate_interval]))`, "{{topic}}")).
				Hue(g.Green).Size(8, 8),
			g.TimeSeries("Fetch throughput",
				"Records fetched per second by topic (before processing).",
				"short",
				q(`sum by (topic) (rate(kafka_client_fetch_records_total{`+svc+`}[$__rate_interval]))`, "{{topic}}")).
				Hue(g.Blue).Size(8, 8),
			g.TimeSeries("Broker request latency p99 (client view)",
				"Write+read time of requests per broker as seen by the clients (99th percentile). Compare with the broker-side request time on Broker Deep Dive: a gap means network or client-side queuing.",
				"s", vmQuantile("0.99", "kafka_client_request_duration_seconds", "node")).
				Using(g.VMNative).Hue(g.Orange).Size(8, 8),
			g.TimeSeries("Connections",
				"Connects, connect errors and disconnects per minute by broker node. Connect errors during a broker outage are expected; constant churn is not.",
				"short",
				q(`sum by (node) (rate(kafka_client_connects_total{`+svc+`}[$__rate_interval])) * 60`, "connects · node {{node}}"),
				q(`sum by (node) (rate(kafka_client_connect_errors_total{`+svc+`}[$__rate_interval])) * 60`, "connect errors · node {{node}}"),
				q(`sum by (node) (rate(kafka_client_disconnects_total{`+svc+`}[$__rate_interval])) * 60`, "disconnects · node {{node}}")).
				Over("byRegexp", "connect errors.*", map[string]any{"color": map[string]any{"mode": "fixed", "fixedColor": g.Red}}).Hue(g.Blue).Size(8, 8),
			g.TimeSeries("Buffered records and throttling",
				"Records buffered in the clients waiting to be produced or processed, and broker throttle time (quotas).",
				"short",
				q(`sum by (service) (kafka_client_buffered_produce_records{`+svc+`})`, "produce buffer · {{service}}"),
				q(`sum by (service) (kafka_client_buffered_fetch_records{`+svc+`})`, "fetch buffer · {{service}}"),
				q(`sum by (service) (rate(kafka_client_throttle_seconds_total{`+svc+`}[$__rate_interval]))`, "throttled s/s · {{service}}")).
				Hue(g.Purple).Size(8, 8),
		),
		row("Generator", true,
			g.TimeSeries("Generator target vs actual rate",
				"Orders per second the generator should produce (controlboard) and actually produces, by kind (normal, duplicate, poison, hot).",
				"short",
				q(`max(lab_generator_target_rate)`, "target"),
				q(`sum by (kind) (rate(lab_generator_orders_total[$__rate_interval]))`, "{{kind}}")).
				Over("byName", "target", map[string]any{"custom.lineStyle": map[string]any{"fill": "dash", "dash": []int{10, 10}}, "color": map[string]any{"mode": "fixed", "fixedColor": g.Text}}).
				Hue(g.Blue).Size(16, 7),
			g.Stat("Generator lagging", "1 while the generator cannot keep up with its target rate (Kafka is pushing back).", "none",
				q(`max(lab_generator_lagging)`, "")).Thresholds(g.Green, g.Step{Color: g.Red, Value: 1}).Map("0", "keeping up", g.Green).Map("1", "lagging", g.Red).Size(8, 7),
		),
		row("Go runtime", true,
			g.TimeSeries("Memory: heap in use vs GOMEMLIMIT",
				"Go heap in use and the soft memory limit (80 % of the container limit). Heap close to the limit makes the GC run continuously.",
				"bytes",
				q(`sum by (service) (go_memstats_heap_inuse_bytes{`+svc+`})`, "heap · {{service}}"),
				q(`max by (service) (go_gc_gomemlimit_bytes{`+svc+`})`, "limit · {{service}}")).
				Over("byRegexp", "limit.*", map[string]any{"custom.lineStyle": map[string]any{"fill": "dash", "dash": []int{10, 10}}, "custom.fillOpacity": 0}).
				Hue(g.Purple).Size(8, 8),
			g.TimeSeries("Goroutines",
				"Goroutines per service. A steadily growing number is a leak.",
				"short",
				q(`sum by (service) (go_goroutines{`+svc+`})`, "{{service}}")).
				Hue(g.Blue).Size(8, 8),
			g.TimeSeries("GC CPU and pauses",
				"Time spent in Go GC pauses per second (stop-the-world).",
				"s",
				q(`sum by (service) (rate(go_gc_duration_seconds_sum{`+svc+`}[$__rate_interval]))`, "{{service}}")).
				Hue(g.Orange).Size(8, 8),
		),
	}
	return d
}
