package dash

import g "kafkalab/monitoring/dashgen/internal/grafana"

// Container selector for the Kafka broker containers (cAdvisor: container="" is the pod cgroup).
const kc = `@P,container!="",container!="POD"`

func podVars() []g.Variable {
	return []g.Variable{
		{
			Name: "namespace", Label: "Namespace", Kind: "query", Multi: true, All: true, Hide: true, Default: "All",
			Query: X(`label_values(kafka_server_kafkaserver_brokerstate{@B}, namespace)`),
		},
		{
			Name: "pod", Label: "Pod", Kind: "query", Multi: true, All: true, Default: "All", Sort: 1,
			Query: X(`label_values(kafka_server_kafkaserver_brokerstate{@B}, pod)`),
		},
		{
			Name: "node_instance", Label: "Node (node-exporter)", Kind: "query", Multi: true, All: true, Default: "All",
			Query: `label_values(node_uname_info, instance)`,
		},
	}
}

// pvc restricts kubelet volume stats to the claims mounted by the selected pods (kube-state-metrics join).
func pvc(metric string) string {
	return `(sum by (persistentvolumeclaim) (` + metric + `{namespace=~"$namespace"}) and on (persistentvolumeclaim) (max by (persistentvolumeclaim) (kube_pod_spec_volumes_persistentvolumeclaims_info{@P})))`
}

// Pods is the Kafka pods & nodes dashboard (uid kafka-pods).
func Pods() *g.Dashboard {
	vars := append([]g.Variable{clusterVar("kafka_server_kafkaserver_brokerstate"), brokerVar()}, podVars()...)
	d := bundle("kafka-pods", "Kafka · Pods & Nodes",
		"Resources of the Kafka broker pods (cAdvisor, kube-state-metrics, kubelet) and their nodes (node-exporter): CPU incl. throttling, memory vs. limit incl. page cache, disk I/O, network, pressure stall information (PSI) and persistent volume usage with a fill-up forecast.",
		vars...)
	d.Rows = []*g.Row{
		row("", false,
			g.Stat("Memory vs limit (max pod)", "Working set of the fullest broker container relative to its memory limit. Above ~90 % the kernel reclaims page cache aggressively and the OOM killer is close. Kafka needs headroom above the heap for page cache, direct and mapped buffers.",
				"percentunit", q(`max(sum by (pod) (container_memory_working_set_bytes{`+kc+`}) / sum by (pod) (kube_pod_container_resource_limits{@P,resource="memory"}))`, "")).
				Thresholds(g.Green, g.Step{Color: g.Yellow, Value: 0.85}, g.Step{Color: g.Red, Value: 0.95}).SetMax(1),
			g.Stat("CPU cores used", "CPU used by all selected broker containers.",
				"short", q(`sum(rate(container_cpu_usage_seconds_total{`+kc+`}[@RI]))`, "")).Hue(g.Orange).Neutral().Dec(2),
			g.Stat("CPU throttled", "Share of CFS periods in which the broker containers were throttled by their CPU limit. Throttling directly adds latency; > 5 % is noticeable. No data = no CPU limit set (fine).",
				"percentunit", q(`max(sum by (pod) (rate(container_cpu_cfs_throttled_periods_total{`+kc+`}[@RI])) / sum by (pod) (rate(container_cpu_cfs_periods_total{`+kc+`}[@RI])))`, "")).
				Thresholds(g.Green, g.Step{Color: g.Yellow, Value: 0.05}, g.Step{Color: g.Red, Value: 0.25}).Empty("no CPU limit"),
			g.Stat("Restarts (window)", "Container restarts of the selected pods during the dashboard time range.",
				"short", q(`sum(increase(kube_pod_container_status_restarts_total{@P}[$__range]))`, "")).
				Thresholds(g.Green, g.Step{Color: g.Yellow, Value: 1}, g.Step{Color: g.Red, Value: 3}).Dec(0),
			g.Stat("Volume used (max)", "Fullest Kafka data volume (used ÷ capacity from the kubelet). Kafka stops a broker when a log dir is full — act at 80 %.",
				"percentunit", q(`max(`+pvc("kubelet_volume_stats_used_bytes")+` / `+pvc("kubelet_volume_stats_capacity_bytes")+`)`, "")).
				Thresholds(g.Green, g.Step{Color: g.Yellow, Value: 0.8}, g.Step{Color: g.Red, Value: 0.9}),
			g.Stat("OOM kills (window)", "Out-of-memory kills of the broker containers in the time range (cAdvisor). Any OOM kill means the memory limit is too small for heap + off-heap + page cache needs.",
				"short", q(`sum(increase(container_oom_events_total{`+kc+`}[$__range]))`, "")).
				Thresholds(g.Green, g.Step{Color: g.Red, Value: 1}).Dec(0),
		),
		row("CPU", false,
			g.TimeSeries("CPU usage vs request / limit",
				"CPU cores used per broker container with the container's CPU request (dashed). Usage permanently above the request means the pod competes for CPU with neighbours.",
				"short",
				q(`sum by (pod) (rate(container_cpu_usage_seconds_total{`+kc+`}[@RI]))`, "{{pod}}"),
				q(`sum by (pod) (kube_pod_container_resource_requests{@P,resource="cpu"})`, "request · {{pod}}"),
				q(`sum by (pod) (kube_pod_container_resource_limits{@P,resource="cpu"})`, "limit · {{pod}}")).
				Over("byRegexp", "(request|limit).*", map[string]any{"custom.lineStyle": map[string]any{"fill": "dash", "dash": []int{10, 10}}, "custom.fillOpacity": 0}).
				Hue(g.Orange).Size(12, 8),
			g.TimeSeries("CPU throttling",
				"Share of scheduling periods in which the container hit its CPU limit, per pod. Kafka is latency sensitive: prefer no CPU limit or a generous one.",
				"percentunit",
				q(`sum by (pod) (rate(container_cpu_cfs_throttled_periods_total{`+kc+`}[@RI])) / sum by (pod) (rate(container_cpu_cfs_periods_total{`+kc+`}[@RI]))`, "{{pod}}")).
				Thresholds(g.Green, g.Step{Color: g.Yellow, Value: 0.05}, g.Step{Color: g.Red, Value: 0.25}).Lines("dashed").
				Hue(g.Red).Size(12, 8),
		),
		row("Memory", false,
			g.TimeSeries("Memory: working set, RSS and page cache vs limit",
				"Working set (what the kubelet compares with the limit for eviction/OOM), RSS (JVM heap + off-heap) and page cache per broker. Kafka reads and writes through the page cache: a large cache is GOOD, as long as the working set stays below the limit.",
				"bytes",
				q(`sum by (pod) (container_memory_working_set_bytes{`+kc+`})`, "working set · {{pod}}"),
				q(`sum by (pod) (container_memory_rss{`+kc+`})`, "rss · {{pod}}"),
				q(`sum by (pod) (container_memory_cache{`+kc+`})`, "page cache · {{pod}}"),
				q(`sum by (pod) (kube_pod_container_resource_limits{@P,resource="memory"})`, "limit · {{pod}}")).
				Over("byRegexp", "limit.*", map[string]any{"custom.lineStyle": map[string]any{"fill": "dash", "dash": []int{10, 10}}, "custom.fillOpacity": 0, "color": map[string]any{"mode": "fixed", "fixedColor": g.Red}}).
				Hue(g.Purple).Size(16, 9),
			g.TimeSeries("Memory pressure (PSI)",
				"Share of time tasks in the container were stalled waiting for memory (pressure stall information, 'some'). Non-zero values mean reclaim is slowing Kafka down even before an OOM.",
				"percentunit",
				q(`sum by (pod) (rate(container_pressure_memory_waiting_seconds_total{`+kc+`}[@RI]))`, "{{pod}}")).
				Thresholds(g.Green, g.Step{Color: g.Yellow, Value: 0.01}, g.Step{Color: g.Red, Value: 0.1}).Hue(g.Red).Size(8, 9),
		),
		row("Disk & volumes", false,
			g.TimeSeries("Disk throughput per pod",
				"Bytes read and written by the broker containers (cAdvisor block I/O). Writes ≈ produced bytes × replication factor; reads > 0 under load means consumers read data that is no longer in the page cache (lagging consumers).",
				"Bps",
				q(`sum by (pod) (rate(container_fs_writes_bytes_total{`+kc+`}[@RI]))`, "write · {{pod}}"),
				q(`sum by (pod) (rate(container_fs_reads_bytes_total{`+kc+`}[@RI]))`, "read · {{pod}}")).
				Over("byRegexp", "read.*", map[string]any{"custom.transform": "negative-Y"}).Hue(g.Blue).Size(8, 8),
			g.TimeSeries("I/O pressure (PSI)",
				"Share of time the container's tasks were stalled on I/O. The best indicator of a saturated disk from Kafka's point of view.",
				"percentunit",
				q(`sum by (pod) (rate(container_pressure_io_waiting_seconds_total{`+kc+`}[@RI]))`, "some · {{pod}}"),
				q(`sum by (pod) (rate(container_pressure_io_stalled_seconds_total{`+kc+`}[@RI]))`, "full · {{pod}}")).
				Thresholds(g.Green, g.Step{Color: g.Yellow, Value: 0.05}, g.Step{Color: g.Red, Value: 0.2}).Hue(g.Orange).Size(8, 8),
			g.TimeSeries("Volume usage and forecast",
				"Used bytes of every Kafka data volume and a linear forecast 7 days ahead (predict_linear over 6 h), with the volume capacity. If the forecast crosses the capacity, add disk or reduce retention.",
				"bytes",
				q(pvc("kubelet_volume_stats_used_bytes"), "used · {{persistentvolumeclaim}}"),
				q(`predict_linear((`+pvc("kubelet_volume_stats_used_bytes")+`)[6h:5m], 7 * 86400)`, "in 7 d · {{persistentvolumeclaim}}"),
				q(`max by (persistentvolumeclaim) (kube_persistentvolumeclaim_resource_requests_storage_bytes{namespace=~"$namespace"}) and on (persistentvolumeclaim) (max by (persistentvolumeclaim) (kube_pod_spec_volumes_persistentvolumeclaims_info{@P}))`, "requested · {{persistentvolumeclaim}}")).
				Over("byRegexp", "in 7 d.*", map[string]any{"custom.lineStyle": map[string]any{"fill": "dash", "dash": []int{10, 10}}}).
				Over("byRegexp", "requested.*", map[string]any{"custom.fillOpacity": 0, "color": map[string]any{"mode": "fixed", "fixedColor": g.Red}}).
				Hue(g.Green).Size(8, 8),
		),
		row("Network & CPU pressure", true,
			g.TimeSeries("Network throughput per pod",
				"Bytes received (up) and transmitted (down) by each broker pod, all interfaces. Includes client, replication and controller traffic.",
				"Bps",
				q(`sum by (pod) (rate(container_network_receive_bytes_total{@P}[@RI]))`, "rx · {{pod}}"),
				q(`sum by (pod) (rate(container_network_transmit_bytes_total{@P}[@RI]))`, "tx · {{pod}}")).
				Over("byRegexp", "tx.*", map[string]any{"custom.transform": "negative-Y"}).Hue(g.Blue).Size(8, 8),
			g.TimeSeries("Network errors and drops",
				"Dropped and erroneous packets per pod. Should be 0; drops point to overloaded nodes or CNI problems.",
				"pps",
				q(`sum by (pod) (rate(container_network_receive_packets_dropped_total{@P}[@RI]) + rate(container_network_transmit_packets_dropped_total{@P}[@RI]))`, "dropped · {{pod}}"),
				q(`sum by (pod) (rate(container_network_receive_errors_total{@P}[@RI]) + rate(container_network_transmit_errors_total{@P}[@RI]))`, "errors · {{pod}}")).
				Hue(g.Red).Size(8, 8),
			g.TimeSeries("CPU pressure (PSI)",
				"Share of time runnable tasks of the container waited for a CPU. High values with low usage mean the node is overcommitted.",
				"percentunit",
				q(`sum by (pod) (rate(container_pressure_cpu_waiting_seconds_total{`+kc+`}[@RI]))`, "{{pod}}")).
				Thresholds(g.Green, g.Step{Color: g.Yellow, Value: 0.1}, g.Step{Color: g.Red, Value: 0.3}).Hue(g.Orange).Size(8, 8),
		),
		row("Nodes", true,
			g.TimeSeries("Node CPU utilisation",
				"Non-idle CPU share of the nodes (node-exporter), averaged over cores.",
				"percentunit",
				q(`1 - avg by (instance) (rate(node_cpu_seconds_total{instance=~"$node_instance",mode="idle"}[@RI]))`, "{{instance}}")).
				SetMax(1).Thresholds(g.Green, g.Step{Color: g.Yellow, Value: 0.8}, g.Step{Color: g.Red, Value: 0.95}).Hue(g.Orange).Size(8, 8),
			g.TimeSeries("Node memory available",
				"Memory available for new allocations on each node (MemAvailable). Page cache counts as available — Kafka benefits from lots of it.",
				"bytes",
				q(`node_memory_MemAvailable_bytes{instance=~"$node_instance"}`, "available · {{instance}}"),
				q(`node_memory_MemTotal_bytes{instance=~"$node_instance"}`, "total · {{instance}}")).
				Hue(g.Purple).Size(8, 8),
			g.TimeSeries("Node disk utilisation",
				"Share of time each block device was busy (io time). Near 100 % = saturated disk.",
				"percentunit",
				q(`rate(node_disk_io_time_seconds_total{instance=~"$node_instance",device!~"dm-.*|loop.*"}[@RI])`, "{{device}} · {{instance}}")).
				SetMax(1).Thresholds(g.Green, g.Step{Color: g.Yellow, Value: 0.7}, g.Step{Color: g.Red, Value: 0.9}).Hue(g.Blue).Size(8, 8),
			g.TimeSeries("Node pressure (PSI)",
				"Node-wide pressure stall information: share of time some tasks waited for CPU, memory or I/O.",
				"percentunit",
				q(`rate(node_pressure_cpu_waiting_seconds_total{instance=~"$node_instance"}[@RI])`, "cpu · {{instance}}"),
				q(`rate(node_pressure_memory_waiting_seconds_total{instance=~"$node_instance"}[@RI])`, "memory · {{instance}}"),
				q(`rate(node_pressure_io_waiting_seconds_total{instance=~"$node_instance"}[@RI])`, "io · {{instance}}")).
				Hue(g.Red).Size(12, 7),
			g.TimeSeries("Node load per core",
				"1-minute load average divided by the number of cores. Above 1 the node has more runnable tasks than CPUs.",
				"short",
				q(`node_load1{instance=~"$node_instance"} / on (instance) count by (instance) (node_cpu_seconds_total{instance=~"$node_instance",mode="idle"})`, "{{instance}}")).
				Thresholds(g.Green, g.Step{Color: g.Yellow, Value: 1}, g.Step{Color: g.Red, Value: 2}).Lines("dashed").Hue(g.Orange).Size(12, 7),
		),
	}
	return d
}
