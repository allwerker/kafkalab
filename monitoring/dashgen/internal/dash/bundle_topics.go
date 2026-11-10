package dash

import g "kafkalab/monitoring/dashgen/internal/grafana"

const topicLink = "var-topic=${__field.labels.topic_name}"

// Topics is the topics, partitions & storage dashboard (uid kafka-topics).
func Topics() *g.Dashboard {
	d := bundle("kafka-topics", "Kafka · Topics, Partitions & Storage",
		"Per-topic throughput, partition skew, retained data and storage growth with a linear forecast. Topic and partition sizes come from KMinion (Kafka admin API), throughput per topic from the brokers' JMX.",
		clusterVar("kminion_kafka_cluster_info"), brokerVar(), topicVar())
	d.Rows = []*g.Row{
		row("", false,
			g.Stat("Topics", "Topics matching the topic filter (internal topics starting with __ are hidden by KMinion).",
				"short", q(`count(kminion_kafka_topic_info{@KT})`, "")).Hue(g.Blue).Neutral(),
			g.Stat("Partitions", "Partitions of the selected topics. Each partition is a directory with segment files on every replica; partitions cost memory, file handles and controller work.",
				"short", q(`count(kminion_kafka_topic_partition_high_water_mark{@KT})`, "")).Hue(g.Blue).Neutral(),
			g.Stat("Data on disk", "Bytes of the selected topics on all brokers' log directories, replicas included (KMinion DescribeLogDirs).",
				"bytes", q(`sum(kminion_kafka_topic_log_dir_size_total_bytes{@KT})`, "")).Hue(g.Green).Neutral(),
			g.Stat("Messages in", "Records appended per second to the selected topics (rate of the partitions' high watermarks, i.e. committed records).",
				"suffix: msg/s", q(`sum(rate(kminion_kafka_topic_high_water_mark_sum{@KT}[@RI]))`, "")).Hue(g.Blue).Neutral(),
			g.Stat("Growth per day", "How much the selected topics grow per day at the current rate (linear trend over the last 6 hours). Negative = retention deletes more than producers write.",
				"bytes", q(`sum(deriv(kminion_kafka_topic_log_dir_size_total_bytes{@KT}[6h])) * 86400`, "")).Hue(g.Purple).Neutral(),
			g.Stat("Broker disk (all log dirs)", "Total size of all log directories per broker (KMinion). Compare with the volume size on Kafka Pods & Nodes.",
				"bytes", q(`sum(kminion_kafka_broker_log_dir_size_total_bytes{@C})`, "")).Hue(g.Green).Neutral(),
		),
		row("Topics", false, topicTable()),
		row("Throughput per topic", false,
			g.TimeSeries("Messages in per topic",
				"Committed records per second per topic, derived from the high watermark (works for every client and includes transactional markers). Click a series to filter the dashboard on that topic.",
				"suffix: msg/s",
				q(`sum by (topic_name) (rate(kminion_kafka_topic_high_water_mark_sum{@KT}[@RI]))`, "{{topic_name}}")).
				Link("Filter on ${__field.labels.topic_name}", drill("kafka-topics", topicLink)).
				LegendTable("right", "mean", "max").Hue(g.Blue).Size(12, 8),
			g.TimeSeries("Bytes in / out per topic",
				"Produced (up) and consumed (down) bytes per topic from the brokers' BrokerTopicMetrics. Topics without traffic are not listed (Kafka creates the per-topic metrics lazily).",
				"Bps",
				q(`sum by (topic) (rate(kafka_server_brokertopicmetrics_bytesin_total{@BT}[@RI]))`, "in · {{topic}}"),
				q(`sum by (topic) (rate(kafka_server_brokertopicmetrics_bytesout_total{@BT}[@RI]))`, "out · {{topic}}")).
				Over("byRegexp", "out.*", map[string]any{"custom.transform": "negative-Y"}).Hue(g.Purple).Size(12, 8),
			g.TimeSeries("Produce / fetch requests per topic",
				"Produce and fetch requests per second per topic. Many requests with few messages each (compare with Messages in) mean small batches: raise linger.ms / batch.size on producers.",
				"reqps",
				q(`sum by (topic) (rate(kafka_server_brokertopicmetrics_totalproducerequests_total{@BT}[@RI]))`, "produce · {{topic}}"),
				q(`sum by (topic) (rate(kafka_server_brokertopicmetrics_totalfetchrequests_total{@BT}[@RI]))`, "fetch · {{topic}}")).
				Hue(g.Blue).Size(12, 8),
			g.TimeSeries("Average batch size (messages per produce request)",
				"Messages per produce request per topic. Values near 1 mean producers send every record on its own — expensive for brokers. 10+ is healthy for busy topics.",
				"short",
				q(`sum by (topic) (rate(kafka_server_brokertopicmetrics_messagesin_total{@BT}[@RI])) / sum by (topic) (rate(kafka_server_brokertopicmetrics_totalproducerequests_total{@BT}[@RI]))`, "{{topic}}")).
				Thresholds(g.Yellow, g.Step{Color: g.Green, Value: 5}).Hue(g.Green).Size(12, 8),
		),
		row("Partitions & skew", false,
			g.Heatmap("Partition skew: messages in per partition",
				"Each row is one partition of the selected topics, the colour its produce rate. A single bright row = a hot partition (skewed message keys); consumers of that partition will lag first. Select one topic for a readable picture.",
				"suffix: msg/s",
				q(`sum by (topic_name, partition_id) (rate(kminion_kafka_topic_partition_high_water_mark{@KT}[@RI]))`, "{{topic_name}} · p{{partition_id}}")).
				Size(12, 10),
			g.BarGauge("Retained messages per partition (top 20)",
				"High watermark minus low watermark: records currently retained per partition. Big differences between partitions of one topic mean skew; a partition that never shrinks may have retention misconfigured.",
				"short",
				iq(`topk(20, sum by (topic_name, partition_id) (kminion_kafka_topic_partition_high_water_mark{@KT} - kminion_kafka_topic_partition_low_water_mark{@KT}))`, "{{topic_name}} · p{{partition_id}}")).
				Hue(g.Blue).Neutral().Size(12, 10),
		),
		row("Storage & forecast", false,
			g.TimeSeries("Size per topic (all replicas)",
				"Bytes on disk per topic summed over all replicas, stacked. Sawtooth patterns are retention deleting whole segments (segment.bytes / segment.ms).",
				"bytes",
				q(`sum by (topic_name) (kminion_kafka_topic_log_dir_size_total_bytes{@KT})`, "{{topic_name}}")).
				Stack().Hue(g.Green).LegendTable("right", "lastNotNull").Size(12, 9),
			g.TimeSeries("Size forecast: now vs. in 24 h and 7 days",
				"Linear forecast (predict_linear over the last 6 h) of the selected topics' total size. A forecast far above 'now' means retention is not keeping up; compare the 7-day value with the disk size of your brokers.",
				"bytes",
				q(`sum(kminion_kafka_topic_log_dir_size_total_bytes{@KT})`, "now"),
				q(`sum(predict_linear(kminion_kafka_topic_log_dir_size_total_bytes{@KT}[6h], 86400))`, "in 24 h"),
				q(`sum(predict_linear(kminion_kafka_topic_log_dir_size_total_bytes{@KT}[6h], 7 * 86400))`, "in 7 days")).
				SeriesColor("now", g.Green).SeriesColor("in 24 h", g.Yellow).SeriesColor("in 7 days", g.Orange).
				Over("byRegexp", "in .*", map[string]any{"custom.lineStyle": map[string]any{"fill": "dash", "dash": []int{10, 10}}}).Size(12, 9),
			g.TimeSeries("Log dir size per broker",
				"Total bytes in each broker's log directories (all topics, all replicas). Imbalance between brokers means unbalanced partition placement.",
				"bytes",
				q(`sum by (broker_id) (kminion_kafka_broker_log_dir_size_total_bytes{@C})`, "broker {{broker_id}}")).
				Hue(g.Green).Size(12, 8),
			g.BarGauge("Fastest growing topics (per day)",
				"Topics ranked by growth rate (deriv over 6 h × 1 day). Topics on top will fill disks first.",
				"bytes",
				iq(`topk(10, deriv(kminion_kafka_topic_log_dir_size_total_bytes{@KT}[6h]) * 86400 > 0)`, "{{topic_name}}")).
				Hue(g.Purple).Neutral().Size(12, 8),
		),
	}
	return d
}

func topicTable() *g.Panel {
	return g.Table("Topic inventory",
		"Configuration and size of every selected topic (KMinion topic info). Sort by any column; click a topic name to filter the dashboard.",
		"short",
		iq(`max by (topic_name, partition_count, replication_factor, min_insync_replicas, cleanup_policy, retention_ms, retention_bytes) (kminion_kafka_topic_info{@KT})`, ""),
		iq(`sum by (topic_name) (kminion_kafka_topic_log_dir_size_total_bytes{@KT})`, ""),
		iq(`sum by (topic_name) (rate(kminion_kafka_topic_high_water_mark_sum{@KT}[5m]))`, ""),
	).Transform("merge", map[string]any{}).
		Organize(map[string]string{
			"topic_name": "Topic", "partition_count": "Partitions", "replication_factor": "RF",
			"min_insync_replicas": "min ISR", "cleanup_policy": "Policy", "retention_ms": "Retention ms",
			"retention_bytes": "Retention bytes", "Value #B": "Size", "Value #C": "Msg/s",
		}, "Value #A").
		Order("topic_name", "partition_count", "replication_factor", "min_insync_replicas", "cleanup_policy", "retention_ms", "retention_bytes", "Value #B", "Value #C").
		Neutral().
		Over("byName", "Retention ms", map[string]any{"unit": "ms"}).
		Over("byName", "Retention bytes", map[string]any{"unit": "bytes"}).
		Over("byName", "Size", map[string]any{"unit": "bytes", "custom.cellOptions": map[string]any{"type": "gauge", "mode": "lcd", "valueDisplayMode": "text"}, "color": map[string]any{"mode": "continuous-BlPu"}}).
		Over("byName", "Msg/s", map[string]any{"unit": "short", "decimals": 1}).
		Over("byName", "Topic", map[string]any{"links": []any{map[string]any{
			"title": "Filter on topic", "url": drill("kafka-topics", "var-topic=${__data.fields.Topic}"),
		}}}).
		Opt("sortBy", []any{map[string]any{"displayName": "Size", "desc": true}}).
		Size(24, 9)
}
