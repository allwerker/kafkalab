package dash

import g "kafkalab/monitoring/dashgen/internal/grafana"

const brokerLink = "var-kafka_broker=${__field.labels.kafka_broker}"

// Overview is the cluster overview (uid kafka-overview): is the cluster healthy, how busy is it,
// and where to look next.
func Overview() *g.Dashboard {
	d := bundle("kafka-overview", "Kafka · Cluster Overview",
		"One screen answering: is the Kafka cluster healthy, how much traffic does it carry, and which broker, topic or consumer group needs attention. Every tile links to the deep-dive dashboard.",
		clusterVar("kafka_server_kafkaserver_brokerstate"), brokerVar())
	d.Rows = []*g.Row{
		row("", false, overviewTiles()...),
		row("Throughput", false,
			g.TimeSeries("Messages in per broker",
				"Records appended per second on each broker (leader side, replication not included). Healthy: roughly equal across brokers. A broker far above the others owns hot partition leaders — check 'Leaders per broker' and the Topics dashboard. Click a series for the broker deep dive.",
				"suffix: msg/s",
				q(`sum by (kafka_broker) (rate(kafka_server_brokertopicmetrics_messagesin_total{@B,topic=""}[@RI]))`, "broker {{kafka_broker}}")).
				Link("Broker deep dive · broker ${__field.labels.kafka_broker}", drill("kafka-broker", brokerLink)).
				LegendTable("right", "mean", "max").Size(12, 8),
			g.TimeSeries("Bytes in (up) / out (down) per broker",
				"Client traffic per broker: produced bytes above the axis, bytes fetched by consumers below (replication excluded). Out > in is normal (several consumer groups read the same data). A sudden drop of 'out' with stable 'in' means consumers stopped — see Consumer Groups & Lag.",
				"Bps",
				q(`sum by (kafka_broker) (rate(kafka_server_brokertopicmetrics_bytesin_total{@B,topic=""}[@RI]))`, "in · broker {{kafka_broker}}"),
				q(`sum by (kafka_broker) (rate(kafka_server_brokertopicmetrics_bytesout_total{@B,topic=""}[@RI]))`, "out · broker {{kafka_broker}}")).
				Over("byRegexp", "out.*", map[string]any{"custom.transform": "negative-Y", "color": map[string]any{"mode": "shades", "fixedColor": g.Purple}}).
				Link("Broker deep dive · broker ${__field.labels.kafka_broker}", drill("kafka-broker", brokerLink)).
				LegendTable("right", "mean", "max").Size(12, 8),
			g.BarGauge("Top topics by bytes in",
				"The ten topics receiving the most produced bytes right now (all brokers). Use it to find the topic behind a traffic spike. Click a bar to open the topic in Topics, Partitions & Storage.",
				"Bps",
				iq(`topk(10, sum by (topic) (rate(kafka_server_brokertopicmetrics_bytesin_total{@B,topic!=""}[@RI])) > 0)`, "{{topic}}")).
				Hue(g.Blue).Neutral().Link("Topic details · ${__field.labels.topic}", drill("kafka-topics", "var-topic=${__field.labels.topic}")).Size(8, 9),
			g.TimeSeries("Replication traffic",
				"Bytes copied between brokers to keep follower replicas in sync (in = this broker fetching as follower). Roughly (replication factor − 1) × bytes in. Spikes without more client traffic mean a broker is catching up after a restart or a reassignment is running.",
				"Bps",
				q(`sum by (kafka_broker) (rate(kafka_server_brokertopicmetrics_replicationbytesin_total{@B}[@RI]))`, "repl in · broker {{kafka_broker}}"),
				q(`sum by (kafka_broker) (rate(kafka_server_brokertopicmetrics_reassignmentbytesin_total{@B}[@RI]))`, "reassignment in · broker {{kafka_broker}}")).
				Hue(g.Purple).Size(8, 9),
			g.TimeSeries("Requests per second by type",
				"Request rate for the main API keys across the selected brokers. FetchFollower is replication; FetchConsumer and Produce are client traffic; Metadata storms (hundreds/s) usually point to a misbehaving client.",
				"reqps",
				q(`sum by (request) (rate(kafka_network_requestmetrics_requests_total{@B,request=~"Produce|FetchConsumer|FetchFollower|Metadata|OffsetCommit|Heartbeat|ConsumerGroupHeartbeat|JoinGroup"}[@RI]))`, "{{request}}")).
				Hue(g.Blue).Size(8, 9),
		),
		row("Health", false,
			g.TimeSeries("Partition health",
				"Partitions in trouble over time. Under-replicated: a follower is behind (broker slow or down). Under min ISR: producers with acks=all are being rejected. Offline: no leader, the partition is unavailable. Healthy: all zero. Open Broker Deep Dive → Replication to find the slow broker.",
				"short",
				q(`sum(kafka_server_replicamanager_underreplicatedpartitions{@B})`, "under-replicated"),
				q(`sum(kafka_server_replicamanager_underminisrpartitioncount{@B})`, "under min ISR"),
				q(`sum(kafka_server_replicamanager_atminisrpartitioncount{@B})`, "at min ISR (no slack)"),
				q(`max(kafka_controller_kafkacontroller_offlinepartitionscount{@C})`, "offline")).
				SeriesColor("under-replicated", g.Orange).SeriesColor("under min ISR", g.Red).
				SeriesColor("offline", "dark-red").SeriesColor("at min.*", g.Yellow).
				Thresholds(g.Green, g.Step{Color: g.Red, Value: 1}).Size(8, 8),
			g.TimeSeries("ISR shrinks / expands",
				"How often replicas leave (shrink) and re-join (expand) the in-sync replica set. Occasional pairs after a restart are normal; a steady shrink/expand flapping means a broker or the network cannot keep up (replica.lag.time.max.ms exceeded).",
				"ops",
				q(`sum by (kafka_broker) (rate(kafka_server_replicamanager_isrshrinks_total{@B}[@RI]))`, "shrink · broker {{kafka_broker}}"),
				q(`sum by (kafka_broker) (rate(kafka_server_replicamanager_isrexpands_total{@B}[@RI]))`, "expand · broker {{kafka_broker}}")).
				Hue(g.Orange).Bars().Size(8, 8),
			g.TimeSeries("Error responses by request and error code",
				"Responses with an error code other than NONE, per request type and error. NOT_LEADER_OR_FOLLOWER bursts are expected during leader moves; persistent errors (e.g. NOT_ENOUGH_REPLICAS, REQUEST_TIMED_OUT, UNKNOWN_TOPIC_OR_PARTITION) need action.",
				"reqps",
				q(`sum by (request, error) (rate(kafka_network_requestmetrics_errors_total{@B,error!="NONE"}[@RI])) > 0`, "{{request}} · {{error}}")).
				Hue(g.Red).Bars().Size(8, 8),
		),
		row("Latency", false,
			g.TimeSeries("Produce latency p99 per broker",
				"99th percentile of the total time a produce request spends in the broker (queue + local write + waiting for replicas with acks=all + response). Healthy on SSDs: < 50 ms; > 200 ms producers will notice. Open Broker Deep Dive → Request time breakdown to see which phase grows.",
				"ms",
				q(`max by (kafka_broker) (kafka_network_requestmetrics_totaltimems{@B,request="Produce",quantile="0.99"})`, "broker {{kafka_broker}}")).
				Thresholds(g.Green, g.Step{Color: g.Yellow, Value: 100}, g.Step{Color: g.Red, Value: 500}).Lines("dashed").
				Link("Broker deep dive · broker ${__field.labels.kafka_broker}", drill("kafka-broker", brokerLink+"&var-request=Produce")).
				Hue(g.Blue).Size(12, 8),
			g.TimeSeries("Consumer fetch latency p99 per broker",
				"99th percentile of the total time of consumer fetch requests. Includes fetch.max.wait.ms (default 500 ms) when there is no new data, so a flat line at the client's max wait (Java default 500 ms, franz-go 5 s) is NORMAL on quiet topics. Values above the max wait, or a high local time on Broker Deep Dive, indicate slow disks or overloaded threads (alert KafkaFetchLatencyHigh uses the local phase for this reason).",
				"ms",
				q(`max by (kafka_broker) (kafka_network_requestmetrics_totaltimems{@B,request="FetchConsumer",quantile="0.99"})`, "broker {{kafka_broker}}")).
				Link("Broker deep dive · broker ${__field.labels.kafka_broker}", drill("kafka-broker", brokerLink+"&var-request=FetchConsumer")).
				Hue(g.Purple).Size(12, 8),
		),
		row("Brokers", false, brokerTable()),
		row("Consumers", false,
			g.BarGauge("Consumer lag by group",
				"Total messages not yet consumed per consumer group (sum over its partitions, from KMinion). The bar colour turns yellow at 1 000 and red at 10 000 messages. Click a bar for per-partition lag, growth rate and time-behind.",
				"short",
				iq(`sort_desc(sum by (group_id) (kminion_kafka_consumer_group_topic_lag{@C}))`, "{{group_id}}")).
				Thresholds(g.Green, g.Step{Color: g.Yellow, Value: 1000}, g.Step{Color: g.Red, Value: 10000}).
				Link("Lag details · ${__field.labels.group_id}", drill("kafka-lag", "var-group=${__field.labels.group_id}")).Size(12, 8),
			g.TimeSeries("End-to-end probe latency",
				"Time for a KMinion probe message to be produced (acks=all) and consumed back again, p50 and p99. This is what an application experiences. Healthy: p99 < 100 ms on an idle cluster. Missing data means the probe itself is failing — see End-to-End Probe.",
				"s",
				q(`histogram_quantile(0.5, sum by (le) (rate(kminion_end_to_end_roundtrip_latency_seconds_bucket{@C}[@RI])))`, "roundtrip p50"),
				q(`histogram_quantile(0.99, sum by (le) (rate(kminion_end_to_end_roundtrip_latency_seconds_bucket{@C}[@RI])))`, "roundtrip p99")).
				Thresholds(g.Green, g.Step{Color: g.Yellow, Value: 0.5}, g.Step{Color: g.Red, Value: 2}).Lines("dashed").
				Link("End-to-end probe", drill("kafka-e2e", "")).Hue(g.Green).Size(12, 8),
		),
	}
	return d
}

func overviewTiles() []*g.Panel {
	return []*g.Panel{
		g.Stat("Active brokers",
			"Brokers registered and unfenced according to the active controller. Should equal the number of brokers you run; a lower value means a broker is down or fenced (check Kafka Pods & Nodes).",
			"short", q(`max(kafka_controller_kafkacontroller_activebrokercount{@C})`, "")).
			Thresholds(g.Red, g.Step{Color: g.Green, Value: 1}).Link("KRaft & controller", drill("kafka-kraft", "")),
		g.Stat("Active controller",
			"Number of nodes that currently act as KRaft active controller. Exactly 1 is healthy; 0 means no metadata changes are possible (no leader elections!), 2 means a split brain during an election.",
			"short", q(`sum(kafka_controller_kafkacontroller_activecontrollercount{@C})`, "")).
			Thresholds(g.Red, g.Step{Color: g.Green, Value: 1}, g.Step{Color: g.Red, Value: 2}).
			Map("0", "none", g.Red).Map("1", "OK", g.Green).Link("KRaft & controller", drill("kafka-kraft", "")),
		okIfZero(g.Stat("Offline partitions",
			"Partitions without a leader (reported by the active controller). Anything above 0 means data in those partitions can be neither written nor read.",
			"short", q(`max(kafka_controller_kafkacontroller_offlinepartitionscount{@C})`, ""))).
			Link("Topics & partitions", drill("kafka-topics", "")),
		okIfZero(g.Stat("Under-replicated",
			"Partitions whose followers are not all in sync, summed over the selected brokers. Short blips during restarts are normal; a persistent value means a broker or disk is too slow.",
			"short", q(`sum(kafka_server_replicamanager_underreplicatedpartitions{@B})`, ""))).
			Link("Broker deep dive", drill("kafka-broker", "")),
		okIfZero(g.Stat("Under min ISR",
			"Partitions with fewer in-sync replicas than min.insync.replicas. Producers using acks=all get NOT_ENOUGH_REPLICAS for them — this is an outage for writers.",
			"short", q(`sum(kafka_server_replicamanager_underminisrpartitioncount{@B})`, ""))).
			Link("Broker deep dive", drill("kafka-broker", "")),
		g.Stat("Probe roundtrip p99",
			"99th percentile end-to-end latency of the KMinion probe (produce with acks=all, then consume). Green < 0.5 s, red > 2 s. The best single number for 'does Kafka work for applications right now'.",
			"s", q(`histogram_quantile(0.99, sum by (le) (rate(kminion_end_to_end_roundtrip_latency_seconds_bucket{@C}[@RI])))`, "")).
			Thresholds(g.Green, g.Step{Color: g.Yellow, Value: 0.5}, g.Step{Color: g.Red, Value: 2}).
			Link("End-to-end probe", drill("kafka-e2e", "")),
		g.Stat("Messages in",
			"Records produced per second across the selected brokers (client traffic only).",
			"suffix: msg/s", q(`sum(rate(kafka_server_brokertopicmetrics_messagesin_total{@B,topic=""}[@RI]))`, "")).
			Hue(g.Blue).Neutral(),
		g.Stat("Bytes in",
			"Bytes produced per second across the selected brokers (client traffic, before replication).",
			"Bps", q(`sum(rate(kafka_server_brokertopicmetrics_bytesin_total{@B,topic=""}[@RI]))`, "")).
			Hue(g.Blue).Neutral(),
		g.Stat("Bytes out",
			"Bytes fetched per second by consumers across the selected brokers (replication excluded).",
			"Bps", q(`sum(rate(kafka_server_brokertopicmetrics_bytesout_total{@B,topic=""}[@RI]))`, "")).
			Hue(g.Purple).Neutral(),
		g.Stat("Total consumer lag",
			"Messages not yet consumed, summed over all consumer groups (KMinion). Absolute numbers depend on throughput; watch the trend in the sparkline and open Consumer Groups & Lag for time-behind estimates.",
			"short", q(`sum(kminion_kafka_consumer_group_topic_lag{@C})`, "")).
			Thresholds(g.Green, g.Step{Color: g.Yellow, Value: 1000}, g.Step{Color: g.Red, Value: 10000}).
			Link("Consumer lag", drill("kafka-lag", "")),
		g.Stat("Partitions / topics",
			"Partitions and topics in the cluster (from the controller's metadata image). Each partition replica costs broker memory and file handles; a few thousand per broker is fine.",
			"short",
			q(`max(kafka_controller_kafkacontroller_globalpartitioncount{@C})`, "partitions"),
			q(`max(kafka_controller_kafkacontroller_globaltopiccount{@C})`, "topics")).
			Hue(g.Blue).Neutral().Link("Topics & partitions", drill("kafka-topics", "")),
		idleThresholds(g.Stat("Min request handler idle",
			"Idle share of the request handler (I/O) threads on the busiest selected broker. Below 40 % the broker gets busy; below 20 % requests queue up and latency rises. Fix: more brokers, num.io.threads, or less load.",
			"percentunit", q(`min(rate(kafka_server_kafkarequesthandlerpool_requesthandleravgidlepercent_count{@B}[@RI]) / 1e9)`, ""))).
			SetMin(0).SetMax(1).Link("Broker deep dive", drill("kafka-broker", "")),
	}
}

func brokerTable() *g.Panel {
	return g.Table("Brokers at a glance",
		"One row per broker (instant values). Leaders should be balanced; under-replicated > 0 or low idle values point to the broker to investigate. Click a broker id for the deep dive.",
		"short",
		iq(`max by (kafka_broker) (kafka_server_kafkaserver_brokerstate{@B})`, ""),
		iq(`sum by (kafka_broker) (kafka_server_replicamanager_leadercount{@B})`, ""),
		iq(`sum by (kafka_broker) (kafka_server_replicamanager_partitioncount{@B})`, ""),
		iq(`sum by (kafka_broker) (kafka_server_replicamanager_underreplicatedpartitions{@B})`, ""),
		iq(`sum by (kafka_broker) (rate(kafka_server_brokertopicmetrics_bytesin_total{@B,topic=""}[5m]))`, ""),
		iq(`sum by (kafka_broker) (rate(kafka_server_kafkarequesthandlerpool_requesthandleravgidlepercent_count{@B}[5m])) / 1e9`, ""),
		iq(`avg by (kafka_broker) (kafka_network_socketserver_networkprocessoravgidle_percent{@B})`, ""),
		iq(`sum by (kafka_broker) (kafka_server_socketservermetrics_connection_count{@B})`, ""),
	).Transform("merge", map[string]any{}).
		Organize(map[string]string{
			"kafka_broker": "Broker", "Value #A": "State", "Value #B": "Leaders", "Value #C": "Partitions",
			"Value #D": "Under-replicated", "Value #E": "Bytes in", "Value #F": "Handler idle",
			"Value #G": "Network idle", "Value #H": "Connections",
		}).
		Neutral().
		Over("byName", "State", map[string]any{
			"mappings": []any{map[string]any{"type": "value", "options": map[string]any{
				"3": map[string]any{"text": "running", "color": g.Green, "index": 0},
				"0": map[string]any{"text": "not running", "color": g.Red, "index": 1},
				"1": map[string]any{"text": "starting", "color": g.Yellow, "index": 2},
				"2": map[string]any{"text": "recovering", "color": g.Yellow, "index": 3},
				"6": map[string]any{"text": "shutting down", "color": g.Orange, "index": 4},
				"7": map[string]any{"text": "shutting down", "color": g.Orange, "index": 5},
			}}},
			"custom.cellOptions": map[string]any{"type": "color-text"},
		}).
		Over("byName", "Under-replicated", map[string]any{
			"custom.cellOptions": map[string]any{"type": "color-background", "mode": "basic"},
			"thresholds": map[string]any{"mode": "absolute", "steps": []any{
				map[string]any{"color": "transparent", "value": nil}, map[string]any{"color": g.Red, "value": 1},
			}},
		}).
		Over("byName", "Bytes in", map[string]any{"unit": "Bps"}).
		Over("byRegexp", ".*idle", map[string]any{
			"unit": "percentunit", "min": 0, "max": 1,
			"custom.cellOptions": map[string]any{"type": "gauge", "mode": "basic", "valueDisplayMode": "text"},
			"thresholds": map[string]any{"mode": "absolute", "steps": []any{
				map[string]any{"color": g.Red, "value": nil}, map[string]any{"color": g.Yellow, "value": 0.2},
				map[string]any{"color": g.Green, "value": 0.4},
			}},
		}).
		Over("byName", "Broker", map[string]any{"links": []any{map[string]any{
			"title": "Broker deep dive", "url": drill("kafka-broker", "var-kafka_broker=${__data.fields.Broker}"),
		}}}).
		Size(24, 7)
}
