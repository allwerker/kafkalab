package dash

import g "kafkalab/monitoring/dashgen/internal/grafana"

// Heap pools are the pools with a "collection usage" (after-GC) value: works for G1, Parallel, CMS, ZGC.
const (
	heapAfterGCRatio = `sum by (kafka_broker) (jvm_memory_pool_collection_used_bytes{@B}) / sum by (kafka_broker) (jvm_memory_max_bytes{@B,area="heap"})`
	gcPauseShare     = `sum by (kafka_broker) (rate(jvm_gc_collection_seconds_sum{@B,gc!~".*Concurrent.*"}[@RI]))`
)

// JVM is the broker JVM dashboard (uid kafka-jvm).
func JVM() *g.Dashboard {
	d := bundle("kafka-jvm", "Kafka · JVM",
		"The brokers' Java runtime: heap by pool and after GC, garbage collection time share and pauses, allocation rate, threads, direct/mapped buffers and file descriptors. Kafka is sensitive to long GC pauses (ISR shrinks, session timeouts).",
		clusterVar("kafka_server_kafkaserver_brokerstate"), brokerVar())
	d.Rows = []*g.Row{
		row("", false,
			g.Stat("Heap used (max broker)", "Current heap usage of the fullest selected broker relative to -Xmx. Sawtooth between 30 % and 90 % is normal for G1; look at 'after GC' for the real pressure.",
				"percentunit", q(`max(sum by (kafka_broker) (jvm_memory_used_bytes{@B,area="heap"}) / sum by (kafka_broker) (jvm_memory_max_bytes{@B,area="heap"}))`, "")).
				Thresholds(g.Green, g.Step{Color: g.Yellow, Value: 0.85}, g.Step{Color: g.Red, Value: 0.95}).SetMax(1),
			g.Stat("Heap after GC (max broker)", "Live data that survived the last collection, relative to -Xmx. This is the real memory pressure: above 80 % the JVM collects constantly and risks OutOfMemoryError.",
				"percentunit", q(`max(`+heapAfterGCRatio+`)`, "")).
				Thresholds(g.Green, g.Step{Color: g.Yellow, Value: 0.7}, g.Step{Color: g.Red, Value: 0.8}).SetMax(1),
			g.Stat("GC pause time share", "Share of wall-clock time the busiest selected broker spends in stop-the-world GC (concurrent cycles excluded). Healthy < 2 %; > 10 % hurts latency.",
				"percentunit", q(`max(`+gcPauseShare+`)`, "")).
				Thresholds(g.Green, g.Step{Color: g.Yellow, Value: 0.02}, g.Step{Color: g.Red, Value: 0.1}).Dec(2),
			g.Stat("Threads", "Live JVM threads on the selected brokers (max). Kafka uses a fixed number; a steadily growing count is a leak.",
				"short", q(`max(jvm_threads_current{@B})`, "")).Hue(g.Blue).Neutral(),
			g.Stat("File descriptors", "Open file descriptors relative to the limit on the fullest broker. Every segment file and connection uses one; Kafka needs a high ulimit.",
				"percentunit", q(`max(process_open_fds{@B} / process_max_fds{@B})`, "")).
				Thresholds(g.Green, g.Step{Color: g.Yellow, Value: 0.7}, g.Step{Color: g.Red, Value: 0.9}),
			g.Stat("Uptime (min broker)", "Time since the most recently restarted selected broker started. A small value explains under-replicated partitions and cold caches.",
				"s", q(`min(time() - process_start_time_seconds{@B})`, "")).Hue(g.Blue).Neutral().Opt("graphMode", "none"),
		),
		row("Heap", false,
			g.TimeSeries("Heap used / committed / max",
				"Heap in use, committed by the JVM and the configured maximum (-Xmx) per broker. With -Xms = -Xmx (Strimzi default when both are set) committed stays flat.",
				"bytes",
				q(`sum by (kafka_broker) (jvm_memory_used_bytes{@B,area="heap"})`, "used · broker {{kafka_broker}}"),
				q(`sum by (kafka_broker) (jvm_memory_committed_bytes{@B,area="heap"})`, "committed · broker {{kafka_broker}}"),
				q(`sum by (kafka_broker) (jvm_memory_max_bytes{@B,area="heap"})`, "max · broker {{kafka_broker}}")).
				Over("byRegexp", "max.*", map[string]any{"custom.lineStyle": map[string]any{"fill": "dash", "dash": []int{10, 10}}, "custom.fillOpacity": 0}).
				Hue(g.Blue).Size(12, 8),
			g.TimeSeries("Heap by pool",
				"Heap usage per memory pool (young/eden, survivor, old generation), stacked, for the selected brokers. The old generation should drop after mixed/old collections; if it only grows, live data is growing.",
				"bytes",
				q(`sum by (pool) (jvm_memory_pool_used_bytes{@B} and on (kafka_broker, pool) jvm_memory_pool_collection_used_bytes{@B})`, "{{pool}}")).
				Stack().Hue(g.Purple).Size(12, 8),
			g.TimeSeries("Heap after GC",
				"Heap still occupied after the last collection relative to the maximum heap, per broker. The single best signal for heap sizing: alert above 80 % for several minutes.",
				"percentunit",
				q(heapAfterGCRatio, "broker {{kafka_broker}}")).
				Thresholds(g.Green, g.Step{Color: g.Yellow, Value: 0.7}, g.Step{Color: g.Red, Value: 0.8}).Lines("dashed").
				Hue(g.Green).SetMax(1).Size(12, 8),
			g.TimeSeries("Allocation and promotion rate",
				"Bytes allocated in the young generation per second (eden) and promoted to the old generation. Kafka should allocate modestly; high promotion rates lead to frequent mixed/old collections.",
				"Bps",
				q(`sum by (kafka_broker) (rate(jvm_memory_pool_allocated_bytes_total{@B,pool=~".*Eden.*"}[@RI]))`, "allocated · broker {{kafka_broker}}"),
				q(`sum by (kafka_broker) (rate(jvm_memory_pool_allocated_bytes_total{@B,pool=~".*Old.*|.*Tenured.*"}[@RI]))`, "promoted · broker {{kafka_broker}}")).
				Hue(g.Orange).Size(12, 8),
		),
		row("Garbage collection", false,
			g.TimeSeries("GC pause time share per collector",
				"Seconds per second spent in each collector, i.e. the share of time the JVM is collecting. G1 'Concurrent GC' runs alongside the application and is shown dashed — it costs CPU, not latency.",
				"percentunit",
				q(`sum by (kafka_broker, gc) (rate(jvm_gc_collection_seconds_sum{@B}[@RI]))`, "{{gc}} · broker {{kafka_broker}}")).
				Over("byRegexp", ".*Concurrent.*", map[string]any{"custom.lineStyle": map[string]any{"fill": "dash", "dash": []int{6, 6}}}).
				Thresholds(g.Green, g.Step{Color: g.Yellow, Value: 0.02}, g.Step{Color: g.Red, Value: 0.1}).Lines("dashed").
				Hue(g.Orange).Size(12, 8),
			g.TimeSeries("Average GC pause",
				"Average duration of a collection per collector (time spent ÷ collections, per interval). Young pauses should stay below the G1 target (MaxGCPauseMillis, e.g. 20 ms); multi-second 'Old Generation' pauses are full GCs and can expire broker sessions.",
				"s",
				q(`sum by (kafka_broker, gc) (rate(jvm_gc_collection_seconds_sum{@B,gc!~".*Concurrent.*"}[@RI])) / sum by (kafka_broker, gc) (rate(jvm_gc_collection_seconds_count{@B,gc!~".*Concurrent.*"}[@RI]))`, "{{gc}} · broker {{kafka_broker}}")).
				Thresholds(g.Green, g.Step{Color: g.Yellow, Value: 0.1}, g.Step{Color: g.Red, Value: 1}).Lines("dashed").
				Hue(g.Purple).Size(12, 8),
			g.TimeSeries("Collections per second",
				"Number of collections per second per collector. Many young collections are cheap; any 'Old Generation' collection with G1 means a full GC (bad).",
				"ops",
				q(`sum by (kafka_broker, gc) (rate(jvm_gc_collection_seconds_count{@B}[@RI]))`, "{{gc}} · broker {{kafka_broker}}")).
				SeriesColor(".*Old Generation.*", g.Red).Hue(g.Blue).Bars().Size(24, 6),
		),
		row("Threads, buffers & non-heap", true,
			g.TimeSeries("Thread states",
				"JVM threads by state, summed over the selected brokers. Many BLOCKED threads indicate lock contention; deadlocked threads are shown in red when present.",
				"short",
				q(`sum by (state) (jvm_threads_state{@B})`, "{{state}}"),
				q(`sum(jvm_threads_deadlocked{@B}) > 0`, "deadlocked")).
				SeriesColor("deadlocked", g.Red).SeriesColor("BLOCKED", g.Orange).Stack().Hue(g.Blue).Size(8, 8),
			g.TimeSeries("Direct and mapped buffers",
				"Off-heap buffers: 'direct' (network I/O buffers, bounded by MaxDirectMemorySize) and 'mapped' (memory-mapped index files — they grow with the number of segments and count against the container memory limit!).",
				"bytes",
				q(`sum by (kafka_broker, pool) (jvm_buffer_pool_used_bytes{@B,pool=~"direct|mapped"})`, "{{pool}} · broker {{kafka_broker}}")).
				Hue(g.Purple).Size(8, 8),
			g.TimeSeries("Non-heap: metaspace and code cache",
				"Class metadata (Metaspace) and JIT code cache. Stable after warm-up; continuous growth means a class-loading leak.",
				"bytes",
				q(`sum by (kafka_broker) (jvm_memory_used_bytes{@B,area="nonheap"})`, "non-heap · broker {{kafka_broker}}")).
				Hue(g.Green).Size(8, 8),
			g.TimeSeries("Process CPU",
				"CPU cores used by the broker process (JVM view, includes GC and JIT threads). Compare with the container limit on Kafka Pods & Nodes.",
				"short",
				q(`sum by (kafka_broker) (rate(process_cpu_seconds_total{@B}[@RI]))`, "broker {{kafka_broker}}")).
				Hue(g.Orange).Size(12, 7),
			g.TimeSeries("Open file descriptors",
				"Open file descriptors per broker (segments, indexes, sockets) and the process limit.",
				"short",
				q(`max by (kafka_broker) (process_open_fds{@B})`, "open · broker {{kafka_broker}}")).
				Hue(g.Blue).Size(12, 7),
		),
	}
	return d
}
