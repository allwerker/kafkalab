package dash

import g "kafkalab/monitoring/dashgen/internal/grafana"

// BudgetBytes is the lab's memory budget (10 GB, decimal).
const BudgetBytes = 10e9

const ws = `container_memory_working_set_bytes{container!="",container!="POD"}`

// Footprint is the stack footprint dashboard (uid lab-footprint).
func Footprint() *g.Dashboard {
	d := demo("lab-footprint", "Kafka Lab · Stack Footprint",
		"Does the lab fit into 10 GB of RAM? Working-set memory of every container by namespace and pod against the budget, the biggest consumers, memory against limits and CPU use.")
	d.Rows = []*g.Row{
		row("", false,
			g.Gauge("Total working set vs budget", "Working-set memory of all containers (cAdvisor) against the 10 GB budget. Yellow from 8 GB.",
				"decbytes", q(`sum(`+ws+`)`, "")).
				Thresholds(g.Green, g.Step{Color: g.Yellow, Value: 8e9}, g.Step{Color: g.Red, Value: BudgetBytes}).SetMin(0).SetMax(12e9).Size(6, 7),
			g.BarGauge("Working set by namespace", "Memory per namespace right now.",
				"decbytes", iq(`sort_desc(sum by (namespace) (`+ws+`))`, "{{namespace}}")).
				Hue(g.Purple).Neutral().Size(8, 7),
			g.BarGauge("Top 12 containers", "The twelve biggest containers by working set (pod / container).",
				"decbytes", iq(`topk(12, sum by (namespace, pod, container) (`+ws+`))`, "{{pod}} / {{container}}")).
				Hue(g.Blue).Neutral().Size(10, 7),
		),
		row("Memory over time", false,
			g.TimeSeries("Working set by namespace vs 10 GB budget",
				"Stacked working-set memory per namespace with the 10 GB budget line. The top of the stack is the whole lab.",
				"decbytes",
				q(`sum by (namespace) (`+ws+`)`, "{{namespace}}")).
				Stack().Thresholds(g.Green, g.Step{Color: g.Red, Value: BudgetBytes}).Lines("dashed").SetMax(11e9).
				Hue(g.Purple).Size(24, 9),
			g.Table("Memory vs limit per pod",
				"Working set, limit and usage share per pod. Pods close to 100 % will be OOM-killed under load.",
				"decbytes",
				iq(`sum by (namespace, pod) (`+ws+`)`, ""),
				iq(`sum by (namespace, pod) (kube_pod_container_resource_limits{resource="memory"})`, ""),
				iq(`sum by (namespace, pod) (`+ws+`) / sum by (namespace, pod) (kube_pod_container_resource_limits{resource="memory"})`, "")).
				Transform("merge", map[string]any{}).
				Organize(map[string]string{"namespace": "Namespace", "pod": "Pod", "Value #A": "Working set", "Value #B": "Limit", "Value #C": "Used"}).
				Neutral().
				Over("byName", "Used", map[string]any{
					"unit": "percentunit", "min": 0, "max": 1,
					"custom.cellOptions": map[string]any{"type": "gauge", "mode": "basic", "valueDisplayMode": "text"},
					"thresholds": map[string]any{"mode": "absolute", "steps": []any{
						map[string]any{"color": g.Green, "value": nil}, map[string]any{"color": g.Yellow, "value": 0.8},
						map[string]any{"color": g.Red, "value": 0.95},
					}},
				}).
				Opt("sortBy", []any{map[string]any{"displayName": "Used", "desc": true}}).Size(24, 9),
		),
		row("CPU", true,
			g.TimeSeries("CPU by namespace",
				"CPU cores used per namespace, stacked.",
				"short",
				q(`sum by (namespace) (rate(container_cpu_usage_seconds_total{container!="",container!="POD"}[$__rate_interval]))`, "{{namespace}}")).
				Stack().Hue(g.Orange).Size(12, 8),
			g.BarGauge("Top 12 containers by CPU", "Containers using the most CPU (5-minute rate).",
				"short", iq(`topk(12, sum by (pod, container) (rate(container_cpu_usage_seconds_total{container!="",container!="POD"}[5m])))`, "{{pod}} / {{container}}")).
				Hue(g.Orange).Neutral().Size(12, 8),
		),
	}
	return d
}

// Monitoring is the monitoring overhead dashboard (uid lab-monitoring).
func Monitoring() *g.Dashboard {
	d := demo("lab-monitoring", "Kafka Lab · Monitoring Overhead",
		"What does watching cost? JMX scrape time inside the brokers against the scrape interval, samples and series per scrape job, VictoriaMetrics ingestion and the resources of the monitoring stack itself. See docs/metrics-performance-impact.md.")
	d.Rows = []*g.Row{
		row("", false,
			g.Stat("Max JMX scrape time", "Slowest broker JMX exporter collection. It runs on broker threads; alert at 25 % of the interval (3.75 s at 15 s).",
				"s", q(`max(jmx_scrape_duration_seconds)`, "")).
				Thresholds(g.Green, g.Step{Color: g.Yellow, Value: 2}, g.Step{Color: g.Red, Value: 3.75}),
			g.Stat("JMX scrape share of interval", "Slowest JMX scrape divided by the lab's 15 s scrape interval. Above 25 % the exporter competes noticeably with Kafka for CPU and the JMX lock.",
				"percentunit", q(`max(jmx_scrape_duration_seconds) / 15`, "")).
				Thresholds(g.Green, g.Step{Color: g.Yellow, Value: 0.15}, g.Step{Color: g.Red, Value: 0.25}),
			g.Stat("Samples per scrape (brokers)", "Samples one broker scrape returns (max of the three brokers). Every sample costs JMX attribute reads in the broker.",
				"short", q(`max(scrape_samples_scraped{job=~".*strimzi-kafka"})`, "")).Hue(g.Blue).Neutral(),
			g.Stat("Active series", "Time series active in VictoriaMetrics during the last hour.",
				"short", q(`max(vm_cache_entries{type="storage/hour_metric_ids"})`, "")).Hue(g.Purple).Neutral(),
			g.Stat("Ingestion", "Samples ingested per second by vmsingle.",
				"short", q(`sum(rate(vm_rows_inserted_total[$__rate_interval]))`, "")).Hue(g.Blue).Neutral(),
			g.Stat("Monitoring memory", "Working set of everything in the monitoring namespace.",
				"decbytes", q(`sum(container_memory_working_set_bytes{namespace="monitoring",container!="",container!="POD"})`, "")).Hue(g.Purple).Neutral(),
		),
		row("Broker scrape cost", false,
			g.TimeSeries("JMX scrape duration per broker vs interval",
				"Time the JMX exporter agent needs to walk the allowlisted MBeans and render the response, per broker. The dashed lines are 25 % and 100 % of the 15 s scrape interval. Growing values mean more beans (topics, clients) — revisit the allowlist.",
				"s",
				q(`max by (kafka_broker) (jmx_scrape_duration_seconds)`, "broker {{kafka_broker}}"),
				q(`max by (kafka_broker) (scrape_duration_seconds{job=~".*strimzi-kafka"})`, "vmagent view · broker {{kafka_broker}}")).
				Thresholds(g.Green, g.Step{Color: g.Yellow, Value: 3.75}, g.Step{Color: g.Red, Value: 15}).Lines("dashed").
				Hue(g.Orange).Size(12, 8),
			g.TimeSeries("Samples per broker scrape",
				"Samples returned by each broker scrape after relabelling. Each per-topic metric adds one series per topic and broker.",
				"short",
				q(`max by (kafka_broker) (scrape_samples_post_metric_relabeling{job=~".*strimzi-kafka"})`, "broker {{kafka_broker}}")).
				Hue(g.Blue).Size(12, 8),
		),
		row("Cardinality", false,
			g.BarGauge("Series per scrape job",
				"Samples per scrape summed over all targets of a job (≈ active series contributed by the job).",
				"short", iq(`sort_desc(sum by (job) (scrape_samples_post_metric_relabeling))`, "{{job}}")).
				Hue(g.Purple).Neutral().Size(12, 10),
			g.TimeSeries("Scrape duration per job (max)",
				"Slowest target per scrape job as seen by vmagent.",
				"s", q(`max by (job) (scrape_duration_seconds)`, "{{job}}")).
				Hue(g.Blue).Size(12, 10),
		),
		row("Monitoring stack resources", false,
			g.TimeSeries("Memory of the monitoring components",
				"Working set per pod in the monitoring namespace (vmsingle, vmagent, vmalert, Grafana, exporters, logs, traces).",
				"decbytes",
				q(`sum by (pod) (container_memory_working_set_bytes{namespace="monitoring",container!="",container!="POD"})`, "{{pod}}")).
				Stack().Hue(g.Purple).Size(12, 8),
			g.TimeSeries("CPU of the monitoring components",
				"CPU cores per pod in the monitoring namespace.",
				"short",
				q(`sum by (pod) (rate(container_cpu_usage_seconds_total{namespace="monitoring",container!="",container!="POD"}[$__rate_interval]))`, "{{pod}}")).
				Stack().Hue(g.Orange).Size(12, 8),
			g.TimeSeries("VictoriaMetrics storage",
				"Data size on disk of vmsingle and the ingestion rate.",
				"decbytes",
				q(`sum(vm_data_size_bytes)`, "data size")).
				Hue(g.Green).Size(12, 7),
			g.TimeSeries("vmalert evaluation time",
				"Average rule group evaluation duration (vmalert). Must stay well below the 15 s evaluation interval.",
				"s",
				q(`max by (group) (rate(vmalert_iteration_duration_seconds_sum[$__rate_interval]) / rate(vmalert_iteration_duration_seconds_count[$__rate_interval]))`, "{{group}}")).
				Hue(g.Blue).Size(12, 7),
		),
	}
	return d
}

// Postgres is the PostgreSQL dashboard (uid lab-postgres).
func Postgres() *g.Dashboard {
	d := demo("lab-postgres", "Kafka Lab · PostgreSQL",
		"The services' state store (postgres_exporter): availability, connections, transactions, cache hit ratio, row activity, locks, dead tuples and sizes.")
	db := `datname="lab"`
	d.Rows = []*g.Row{
		row("", false,
			g.Stat("PostgreSQL up", "1 when the exporter can reach the database.", "none", q(`max(pg_up)`, "")).
				Thresholds(g.Red, g.Step{Color: g.Green, Value: 1}).Map("0", "down", g.Red).Map("1", "up", g.Green).Opt("graphMode", "none"),
			g.Stat("Connections", "Backends connected to the lab database vs max_connections.", "percentunit",
				q(`sum(pg_stat_database_numbackends{`+db+`}) / max(pg_settings_max_connections)`, "")).
				Thresholds(g.Green, g.Step{Color: g.Yellow, Value: 0.7}, g.Step{Color: g.Red, Value: 0.9}),
			g.Stat("Transactions / s", "Commits + rollbacks per second.", "short",
				q(`sum(rate(pg_stat_database_xact_commit{`+db+`}[$__rate_interval]) + rate(pg_stat_database_xact_rollback{`+db+`}[$__rate_interval]))`, "")).Hue(g.Blue).Neutral(),
			g.Stat("Cache hit ratio", "Share of block reads served from shared buffers (5 m). Below 95 % the database reads from disk a lot.", "percentunit",
				q(`sum(rate(pg_stat_database_blks_hit{`+db+`}[5m])) / (sum(rate(pg_stat_database_blks_hit{`+db+`}[5m])) + sum(rate(pg_stat_database_blks_read{`+db+`}[5m])))`, "")).
				Thresholds(g.Red, g.Step{Color: g.Yellow, Value: 0.9}, g.Step{Color: g.Green, Value: 0.95}),
			okIfZero(g.Stat("Deadlocks (window)", "Deadlocks detected in the time range.", "short",
				q(`sum(increase(pg_stat_database_deadlocks{`+db+`}[$__range]))`, ""))).Dec(0),
			g.Stat("Database size", "Size of the lab database (all service schemas). Grows with orders, reservations and outbox/inbox tables.", "bytes", q(`max(pg_database_size_bytes{`+db+`})`, "")).Hue(g.Green).Neutral(),
		),
		row("Activity", false,
			g.TimeSeries("Transactions", "Commits and rollbacks per second. Rollbacks come from failed handler transactions (retries).", "short",
				q(`sum(rate(pg_stat_database_xact_commit{`+db+`}[$__rate_interval]))`, "commit"),
				q(`sum(rate(pg_stat_database_xact_rollback{`+db+`}[$__rate_interval]))`, "rollback")).
				SeriesColor("rollback", g.Red).Hue(g.Blue).Size(8, 8),
			g.TimeSeries("Rows", "Rows inserted, updated, deleted and returned per second.", "short",
				q(`sum(rate(pg_stat_database_tup_inserted{`+db+`}[$__rate_interval]))`, "inserted"),
				q(`sum(rate(pg_stat_database_tup_updated{`+db+`}[$__rate_interval]))`, "updated"),
				q(`sum(rate(pg_stat_database_tup_deleted{`+db+`}[$__rate_interval]))`, "deleted"),
				q(`sum(rate(pg_stat_database_tup_fetched{`+db+`}[$__rate_interval]))`, "fetched")).
				Hue(g.Green).Size(8, 8),
			g.TimeSeries("Connections by state", "Connections by state (active, idle, idle in transaction). Many 'idle in transaction' connections hold locks.", "short",
				q(`sum by (state) (pg_stat_activity_count{`+db+`})`, "{{state}}")).
				Stack().Hue(g.Purple).Size(8, 8),
			g.TimeSeries("Locks by mode", "Locks held per lock mode. Many exclusive row locks with rising transaction time point to contention between service replicas.", "short",
				q(`sum by (mode) (pg_locks_count{`+db+`})`, "{{mode}}")).
				Hue(g.Orange).Size(8, 8),
			g.TimeSeries("Longest transaction", "Duration of the longest running transaction. Long transactions block vacuum and hold locks.", "s",
				q(`max(pg_stat_activity_max_tx_duration{`+db+`})`, "max tx duration")).
				Thresholds(g.Green, g.Step{Color: g.Yellow, Value: 10}, g.Step{Color: g.Red, Value: 60}).Lines("dashed").Hue(g.Purple).Size(8, 8),
			g.TimeSeries("Temp files", "Bytes written to temporary files per second (sorts/hashes exceeding work_mem).", "Bps",
				q(`sum(rate(pg_stat_database_temp_bytes{`+db+`}[$__rate_interval]))`, "temp bytes")).
				Hue(g.Yellow).Size(8, 8),
		),
		row("Tables", true,
			g.BarGauge("Largest tables", "Table size (incl. TOAST) per schema/table — one schema per service.", "bytes",
				iq(`topk(15, sum by (schemaname, relname) (pg_stat_user_tables_table_size_bytes))`, "{{schemaname}}.{{relname}}")).
				Hue(g.Green).Neutral().Size(12, 9),
			g.BarGauge("Dead tuples", "Dead row versions waiting for (auto)vacuum per table. High numbers bloat tables and slow scans.", "short",
				iq(`topk(15, sum by (schemaname, relname) (pg_stat_user_tables_n_dead_tup))`, "{{schemaname}}.{{relname}}")).
				Thresholds(g.Green, g.Step{Color: g.Yellow, Value: 10000}, g.Step{Color: g.Red, Value: 100000}).Size(12, 9),
		),
	}
	return d
}

// LogsBoard is the logs dashboard (uid lab-logs) on VictoriaLogs.
func LogsBoard() *g.Dashboard {
	d := demo("lab-logs", "Kafka Lab · Logs",
		"Container logs of the whole lab from VictoriaLogs (LogsQL). Narrow down by namespace and a free-text filter; error volume per namespace shows where things go wrong.",
		g.Variable{Name: "namespace", Label: "Namespace", Kind: "custom", Multi: true, All: true, Default: "All",
			Query: "commerce,kafka,data,control,monitoring,kube-system"},
		g.Variable{Name: "filter", Label: "Filter (LogsQL)", Kind: "textbox", Query: "*", Default: "*"},
	)
	ns := `kubernetes.pod_namespace:in($namespace)`
	logsTarget := func(expr, legend, kind string) g.Target {
		return g.Target{Expr: expr, Legend: legend, QueryType: kind}
	}
	d.Rows = []*g.Row{
		row("", false,
			g.TimeSeries("Log volume by namespace",
				"Log lines per interval and namespace. A sudden burst usually is a crash loop or a retry storm.",
				"short",
				logsTarget(ns+` $filter | stats by (kubernetes.pod_namespace) count() lines`, "{{kubernetes.pod_namespace}}", "statsRange")).
				Using(g.VLogs).Stack().Bars().Hue(g.Blue).Size(12, 8),
			g.TimeSeries("Errors and warnings",
				"Lines at level error/warn (JSON logs of the Go services) or containing ERROR/Exception (Kafka, JVM), per namespace.",
				"short",
				logsTarget(ns+` (level:in(ERROR, error, WARN, warn) or "ERROR" or "Exception") $filter | stats by (kubernetes.pod_namespace) count() errors`, "{{kubernetes.pod_namespace}}", "statsRange")).
				Using(g.VLogs).Stack().Bars().Hue(g.Red).Size(12, 8),
			g.Logs("Log lines",
				"Newest first. Click a line for all fields (pod, container, trace_id → jump to the trace).",
				logsTarget(ns+` $filter | sort by (_time) desc | limit 500`, "", "instant")).
				Using(g.VLogs).Size(24, 16),
		),
	}
	return d
}
