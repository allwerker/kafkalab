package dash

import g "kafkalab/monitoring/dashgen/internal/grafana"

func requestVar() g.Variable {
	return g.Variable{
		Name: "request", Label: "Request type", Kind: "query", Multi: true, Default: "Produce", Sort: 1,
		Query: X(`label_values(kafka_network_requestmetrics_totaltimems_count{@C}, request)`),
	}
}

// phase is one stage of Kafka's request processing pipeline.
type phase struct{ metric, name string }

var phases = []phase{
	{"requestqueuetimems", "1 request queue"},
	{"localtimems", "2 local (leader)"},
	{"remotetimems", "3 remote (replicas/purgatory)"},
	{"responsequeuetimems", "4 response queue"},
	{"responsesendtimems", "5 response send"},
}

func phaseTargets(quantile string) []g.Target {
	out := []g.Target{}
	for _, ph := range phases {
		out = append(out, q(`max(kafka_network_requestmetrics_`+ph.metric+`{@B,request=~"$request",quantile="`+quantile+`"})`, ph.name))
	}
	return out
}

// Broker is the broker deep dive (uid kafka-broker).
func Broker() *g.Dashboard {
	d := bundle("kafka-broker", "Kafka · Broker Deep Dive",
		"Everything inside a broker: where request time goes (queue / local / remote / response), thread pool saturation, purgatory, replication, ISR changes, connections and the log layer. Select one or more brokers and request types at the top.",
		clusterVar("kafka_server_kafkaserver_brokerstate"), brokerVar(), requestVar())
	d.Rows = []*g.Row{
		row("", false, brokerTiles()...),
		row("Request time breakdown", false,
			g.TimeSeries("Where does $request time go? (p99 per phase)",
				"Kafka handles a request in five phases: 1 waiting in the request queue for an I/O thread (high = handler threads saturated), 2 local processing on the leader (high = slow disk/page cache misses), 3 remote: waiting for followers (acks=all) or in purgatory for fetch.min.bytes / fetch.max.wait.ms, 4 waiting in the response queue for a network thread, 5 sending the response (high = slow clients or network). Percentiles are per phase and therefore not strictly additive — the tallest band is the bottleneck.",
				"ms", phaseTargets("0.99")...).
				Stack().Hue(g.Purple).RepeatBy("request").LegendTable("bottom", "mean", "max").Size(12, 9),
			g.TimeSeries("$request total time p50 / p99 / p99.9",
				"Total request time percentiles reported by the broker (Yammer histograms with a decaying 5-minute reservoir). The gap between p50 and p99 shows how spiky latency is; compare with the phase breakdown to see which phase causes the tail.",
				"ms",
				q(`max by (kafka_broker) (kafka_network_requestmetrics_totaltimems{@B,request=~"$request",quantile="0.50"})`, "p50 · broker {{kafka_broker}}"),
				q(`max by (kafka_broker) (kafka_network_requestmetrics_totaltimems{@B,request=~"$request",quantile="0.99"})`, "p99 · broker {{kafka_broker}}"),
				q(`max by (kafka_broker) (kafka_network_requestmetrics_totaltimems{@B,request=~"$request",quantile="0.999"})`, "p99.9 · broker {{kafka_broker}}")).
				Hue(g.Blue).RepeatBy("request").Size(12, 9),
			g.TimeSeries("Request rate by type",
				"Requests per second per API key on the selected brokers (summed over API versions). Use it to correlate latency changes with load changes.",
				"reqps",
				q(`sum by (request) (rate(kafka_network_requestmetrics_requests_total{@B}[@RI])) > 0`, "{{request}}")).
				Hue(g.Blue).LegendTable("right", "mean", "max").Size(12, 8),
			g.TimeSeries("Throttle time p99",
				"Time requests were delayed by client quotas (produce/fetch/request quotas). Non-zero only if quotas are configured and a client exceeds them — then the client, not the broker, is the bottleneck.",
				"ms",
				q(`max by (request) (kafka_network_requestmetrics_throttletimems{@B,request=~"Produce|FetchConsumer",quantile="0.99"})`, "{{request}}")).
				Hue(g.Orange).Size(12, 8),
		),
		row("Thread pools & queues", false,
			idleThresholds(g.TimeSeries("Request handler (I/O thread) idle",
				"Share of time the request handler threads (num.io.threads) were idle, computed from the idle-time meter over the selected interval. Healthy > 40 %; < 20 % means requests wait in the request queue. Broker and controller pools are shown separately on combined nodes.",
				"percentunit",
				q(`max by (kafka_broker) (rate(kafka_server_kafkarequesthandlerpool_requesthandleravgidlepercent_count{@B}[@RI])) / 1e9`, "all · broker {{kafka_broker}}"),
				q(`max by (kafka_broker) (rate(kafka_server_kafkarequesthandlerpool_controllerrequesthandleravgidlepercent_count{@B}[@RI])) / 1e9`, "controller pool · broker {{kafka_broker}}"))).
				Lines("dashed").Hue(g.Green).SetMin(0).SetMax(1).Size(8, 8),
			idleThresholds(g.TimeSeries("Network processor idle",
				"Share of time the network threads (num.network.threads) were idle. Below 30 % the broker cannot read requests / write responses fast enough; raise num.network.threads or reduce connections.",
				"percentunit",
				q(`max by (kafka_broker) (kafka_network_socketserver_networkprocessoravgidle_percent{@B})`, "broker {{kafka_broker}}"))).
				Lines("dashed").Hue(g.Green).SetMin(0).SetMax(1).Size(8, 8),
			g.TimeSeries("Request / response queue size",
				"Requests waiting for an I/O thread (request queue, bounded by queued.max.requests) and responses waiting for a network thread. Should hover near 0; a growing request queue is the leading indicator of handler saturation.",
				"short",
				q(`sum by (kafka_broker) (kafka_network_requestchannel_requestqueuesize{@B})`, "request queue · broker {{kafka_broker}}"),
				q(`sum by (kafka_broker) (kafka_network_requestchannel_responsequeuesize{@B,processor!=""})`, "response queue · broker {{kafka_broker}}")).
				Hue(g.Orange).Size(8, 8),
			g.TimeSeries("Group coordinator threads idle",
				"Idle ratio of the new group coordinator runtime threads (KIP-848) and its event queue size. Low idle with a growing queue means offset commits and heartbeats are delayed — consumer rebalances get slow.",
				"percentunit",
				q(`max by (kafka_broker) (kafka_server_group_coordinator_metrics_thread_idle_ratio_avg{@B})`, "idle · broker {{kafka_broker}}")).
				Hue(g.Green).SetMin(0).SetMax(1).Neutral().Size(12, 7),
			g.TimeSeries("Controller event queue (this node)",
				"Time events wait in, and are processed by, the controller event queue on nodes with the controller role (p99). High values delay leader elections and broker registrations — see KRaft Controller & Quorum.",
				"ms",
				q(`max by (kafka_broker) (kafka_controller_controllereventmanager_eventqueuetimems{@B,quantile="0.99"})`, "queue · broker {{kafka_broker}}"),
				q(`max by (kafka_broker) (kafka_controller_controllereventmanager_eventqueueprocessingtimems{@B,quantile="0.99"})`, "processing · broker {{kafka_broker}}")).
				Hue(g.Purple).Size(12, 7),
		),
		row("Purgatory", true,
			g.TimeSeries("Requests waiting in purgatory",
				"Delayed operations parked until a condition is met: Produce waits for follower acknowledgements (acks=all), Fetch waits for fetch.min.bytes or fetch.max.wait.ms. A large Fetch purgatory is normal with long-polling consumers; a growing Produce purgatory means followers are slow.",
				"short",
				q(`sum by (delayedOperation) (kafka_server_delayedoperationpurgatory_purgatorysize{@B,delayedOperation=~"Produce|Fetch|DeleteRecords|ShareFetch|RemoteFetch"})`, "{{delayedOperation}}")).
				Hue(g.Purple).Size(12, 8),
			g.TimeSeries("Delayed operations (not yet completed)",
				"Delayed operations that are still pending (purgatory size also counts completed but not yet purged ones). Compare Produce here with the produce 'remote' phase above.",
				"short",
				q(`sum by (delayedOperation, kafka_broker) (kafka_server_delayedoperationpurgatory_numdelayedoperations{@B,delayedOperation=~"Produce|Fetch"})`, "{{delayedOperation}} · broker {{kafka_broker}}")).
				Hue(g.Blue).Size(12, 8),
		),
		row("Replication & ISR", false,
			g.TimeSeries("Under-replicated / under-min-ISR per broker",
				"Partitions led by this broker whose followers are lagging (under-replicated) or below min.insync.replicas. The broker that LEADS the partitions reports them — the slow FOLLOWER is usually another broker: check the replica fetcher lag below.",
				"short",
				q(`sum by (kafka_broker) (kafka_server_replicamanager_underreplicatedpartitions{@B})`, "under-replicated · broker {{kafka_broker}}"),
				q(`sum by (kafka_broker) (kafka_server_replicamanager_underminisrpartitioncount{@B})`, "under min ISR · broker {{kafka_broker}}")).
				Thresholds(g.Green, g.Step{Color: g.Red, Value: 1}).Hue(g.Orange).Size(8, 8),
			g.TimeSeries("ISR shrinks / expands per broker",
				"Rate of in-sync replica set changes. Shrinks without matching expands = replicas fall out and do not come back. Correlate with GC pauses (JVM) and disk latency (Pods & Nodes).",
				"ops",
				q(`sum by (kafka_broker) (rate(kafka_server_replicamanager_isrshrinks_total{@B}[@RI]))`, "shrink · broker {{kafka_broker}}"),
				q(`sum by (kafka_broker) (rate(kafka_server_replicamanager_isrexpands_total{@B}[@RI]))`, "expand · broker {{kafka_broker}}"),
				q(`sum by (kafka_broker) (rate(kafka_server_replicamanager_failedisrupdates_total{@B}[@RI]))`, "failed ISR update · broker {{kafka_broker}}")).
				Hue(g.Orange).Bars().Size(8, 8),
			g.TimeSeries("Replica fetcher max lag",
				"Largest lag (in messages) of any partition this broker replicates as a follower. Should stay close to 0; a broker that cannot keep up shows a growing line here. Dead fetcher threads or failed partitions are shown as separate series when non-zero.",
				"short",
				q(`max by (kafka_broker) (kafka_server_replicafetchermanager_maxlag{@B,clientId="Replica"})`, "max lag · broker {{kafka_broker}}"),
				q(`sum by (kafka_broker) (kafka_server_replicafetchermanager_deadthreadcount{@B}) > 0`, "dead fetcher threads · broker {{kafka_broker}}"),
				q(`sum by (kafka_broker) (kafka_server_replicafetchermanager_failedpartitionscount{@B}) > 0`, "failed partitions · broker {{kafka_broker}}")).
				Hue(g.Purple).Size(8, 8),
			g.TimeSeries("Leaders and replicas per broker",
				"Partition leaders and replicas hosted by each broker. Leaders carry the client traffic, so an uneven leader count means uneven load; run a preferred leader election or Cruise Control if one broker leads much more than the others.",
				"short",
				q(`sum by (kafka_broker) (kafka_server_replicamanager_leadercount{@B})`, "leaders · broker {{kafka_broker}}"),
				q(`sum by (kafka_broker) (kafka_server_replicamanager_partitioncount{@B})`, "replicas · broker {{kafka_broker}}")).
				Hue(g.Blue).Size(12, 7),
			g.TimeSeries("Replication bytes per broker",
				"Bytes this broker fetched as follower (in) and served to followers (out). Reassignment traffic is shown separately when a reassignment is running.",
				"Bps",
				q(`sum by (kafka_broker) (rate(kafka_server_brokertopicmetrics_replicationbytesin_total{@B}[@RI]))`, "repl in · broker {{kafka_broker}}"),
				q(`sum by (kafka_broker) (rate(kafka_server_brokertopicmetrics_replicationbytesout_total{@B}[@RI]))`, "repl out · broker {{kafka_broker}}"),
				q(`sum by (kafka_broker) (rate(kafka_server_brokertopicmetrics_reassignmentbytesin_total{@B}[@RI])) > 0`, "reassign in · broker {{kafka_broker}}")).
				Over("byRegexp", "repl out.*", map[string]any{"custom.transform": "negative-Y"}).Hue(g.Purple).Size(12, 7),
		),
		row("Connections & network", true,
			g.TimeSeries("Open connections by listener",
				"Client and inter-broker connections per listener. Every client instance opens connections to the brokers it talks to; thousands of connections or a sawtooth pattern indicate connection churn (short-lived clients).",
				"short",
				q(`sum by (listener, kafka_broker) (kafka_server_socketservermetrics_connection_count{@B})`, "{{listener}} · broker {{kafka_broker}}")).
				Hue(g.Blue).Size(8, 8),
			g.TimeSeries("Connection creation / close rate",
				"New and closed connections per second per broker. Steady high rates (connection storms) cost CPU for TLS/SASL handshakes and are a classic cause of high network-thread load.",
				"ops",
				q(`sum by (kafka_broker) (rate(kafka_server_socketservermetrics_connection_creation_total{@B}[@RI]))`, "created · broker {{kafka_broker}}"),
				q(`sum by (kafka_broker) (rate(kafka_server_socketservermetrics_connection_close_total{@B}[@RI]))`, "closed · broker {{kafka_broker}}")).
				Hue(g.Purple).Size(8, 8),
			g.TimeSeries("Network bytes by listener",
				"Bytes received and sent by the socket server per listener (client, replication and controller listeners separately).",
				"Bps",
				q(`sum by (listener) (rate(kafka_server_socketservermetrics_incoming_byte_total{@B}[@RI]))`, "in · {{listener}}"),
				q(`sum by (listener) (rate(kafka_server_socketservermetrics_outgoing_byte_total{@B}[@RI]))`, "out · {{listener}}")).
				Over("byRegexp", "out.*", map[string]any{"custom.transform": "negative-Y"}).Hue(g.Blue).Size(8, 8),
			g.TimeSeries("Failed authentications",
				"Failed SASL/TLS authentications per second. Non-zero values mean misconfigured clients or someone guessing credentials.",
				"ops",
				q(`sum by (listener, kafka_broker) (rate(kafka_server_socketservermetrics_failed_authentication_total{@B}[@RI]))`, "{{listener}} · broker {{kafka_broker}}")).
				Hue(g.Red).Bars().Size(12, 7),
			g.TimeSeries("Network thread I/O ratio",
				"Fraction of time network threads spend doing I/O (io-ratio) versus waiting for I/O (io-wait-ratio), averaged per listener.",
				"percentunit",
				q(`avg by (listener) (kafka_server_socketservermetrics_io_ratio{@B})`, "io · {{listener}}"),
				q(`avg by (listener) (kafka_server_socketservermetrics_io_wait_ratio{@B})`, "io wait · {{listener}}")).
				Hue(g.Green).Size(12, 7),
		),
		row("Errors & rejected data", true,
			g.TimeSeries("Error responses (excluding NONE)",
				"Error codes returned per request type. See the Kafka protocol docs for each code; NOT_LEADER_OR_FOLLOWER and FENCED_LEADER_EPOCH are benign during leader changes.",
				"reqps",
				q(`sum by (request, error) (rate(kafka_network_requestmetrics_errors_total{@B,error!="NONE"}[@RI])) > 0`, "{{request}} · {{error}}")).
				Hue(g.Red).Bars().Size(12, 8),
			g.TimeSeries("Failed produce / fetch requests and rejected records",
				"Requests that failed inside the broker and records rejected for being invalid (bad CRC, wrong magic, out-of-order sequence for idempotent producers) or too large (bytes rejected, see message.max.bytes).",
				"ops",
				q(`sum(rate(kafka_server_brokertopicmetrics_failedproducerequests_total{@B,topic=""}[@RI]))`, "failed produce"),
				q(`sum(rate(kafka_server_brokertopicmetrics_failedfetchrequests_total{@B,topic=""}[@RI]))`, "failed fetch"),
				q(`sum(rate(kafka_server_brokertopicmetrics_invalidoffsetorsequencerecords_total{@B}[@RI]))`, "invalid offset/sequence"),
				q(`sum(rate(kafka_server_brokertopicmetrics_invalidmessagecrcrecords_total{@B}[@RI]))`, "invalid CRC"),
				q(`sum(rate(kafka_server_brokertopicmetrics_bytesrejected_total{@B,topic=""}[@RI]))`, "bytes rejected (B/s)")).
				Hue(g.Red).Size(12, 8),
		),
		row("Log layer & disk", true,
			g.TimeSeries("Log flush latency",
				"Time to fsync a log segment (p50/p99) and flushes per second. Kafka normally relies on the page cache and flushes rarely; high flush latency means the disk is saturated — produce latency (local phase) follows.",
				"ms",
				q(`max by (kafka_broker) (kafka_log_logflushstats_logflushrateandtimems{@B,quantile="0.99"})`, "p99 · broker {{kafka_broker}}"),
				q(`max by (kafka_broker) (kafka_log_logflushstats_logflushrateandtimems{@B,quantile="0.50"})`, "p50 · broker {{kafka_broker}}")).
				Hue(g.Purple).Size(8, 8),
			okIfZero(g.Stat("Offline log directories",
				"Log directories the broker marked offline after an I/O error. Any non-zero value means partitions on that disk are unavailable on this broker.",
				"short", q(`sum(kafka_log_logmanager_offlinelogdirectorycount{@B})`, ""))).Size(4, 8),
			g.TimeSeries("Log cleaner (compaction)",
				"Compaction health: the dirtiest log's dirty ratio and uncleanable partitions. Partitions become uncleanable after errors; then compacted topics grow forever.",
				"percent",
				q(`max by (kafka_broker) (kafka_log_logcleanermanager_max_dirty_percent{@B})`, "max dirty % · broker {{kafka_broker}}"),
				q(`sum by (kafka_broker) (kafka_log_logcleanermanager_uncleanable_partitions_count{@B}) > 0`, "uncleanable partitions · broker {{kafka_broker}}")).
				Hue(g.Green).Size(12, 8),
			g.TimeSeries("Incremental fetch sessions",
				"Fetch session cache: sessions and cached partitions (KIP-227) and evictions. Evictions > 0 mean max.incremental.fetch.session.cache.slots is too small, causing full fetch requests (more CPU and bandwidth).",
				"short",
				q(`sum by (kafka_broker) (kafka_server_fetchsessioncache_numincrementalfetchsessions{@B})`, "sessions · broker {{kafka_broker}}"),
				q(`sum by (kafka_broker) (rate(kafka_server_fetchsessioncache_incrementalfetchsessionevictions_total{@B}[@RI]))`, "evictions/s · broker {{kafka_broker}}")).
				Hue(g.Blue).Size(24, 6),
		),
	}
	return d
}

func brokerTiles() []*g.Panel {
	return []*g.Panel{
		g.Stat("Broker state",
			"Broker state from kafka.server:type=KafkaServer,name=BrokerState per selected broker. 'running' (3) is healthy; starting/recovering after restarts; shutting down during rolling updates.",
			"none", q(`max by (kafka_broker) (kafka_server_kafkaserver_brokerstate{@B})`, "broker {{kafka_broker}}")).
			Thresholds(g.Red, g.Step{Color: g.Yellow, Value: 1}, g.Step{Color: g.Green, Value: 3}, g.Step{Color: g.Orange, Value: 4}).
			Map("0", "not running", g.Red).Map("1", "starting", g.Yellow).Map("2", "recovering", g.Yellow).
			Map("3", "running", g.Green).Map("6", "shutting down", g.Orange).Map("7", "shutting down", g.Orange).
			Opt("graphMode", "none").Size(6, 4),
		g.Stat("Leaders",
			"Partition leaders on the selected brokers. Leaders do the client work: balance them across brokers.",
			"short", q(`sum(kafka_server_replicamanager_leadercount{@B})`, "")).Hue(g.Blue).Neutral().Size(3, 4),
		okIfZero(g.Stat("Under-replicated",
			"Under-replicated partitions led by the selected brokers.",
			"short", q(`sum(kafka_server_replicamanager_underreplicatedpartitions{@B})`, ""))).Size(3, 4),
		idleThresholds(g.Stat("Handler idle",
			"Average idle share of the request handler threads (lowest selected broker). < 20 % = saturated.",
			"percentunit", q(`min(rate(kafka_server_kafkarequesthandlerpool_requesthandleravgidlepercent_count{@B}[@RI]) / 1e9)`, ""))).Size(3, 4),
		idleThresholds(g.Stat("Network idle",
			"Average idle share of the network threads (lowest selected broker). < 30 % = add network threads.",
			"percentunit", q(`min(kafka_network_socketserver_networkprocessoravgidle_percent{@B})`, ""))).Size(3, 4),
		g.Stat("Connections",
			"Open client and inter-broker connections on the selected brokers.",
			"short", q(`sum(kafka_server_socketservermetrics_connection_count{@B})`, "")).Hue(g.Blue).Neutral().Size(3, 4),
		g.Stat("JMX scrape time",
			"How long the JMX exporter needed to collect the metrics of the slowest selected broker. It runs inside the broker JVM: keep it well below the scrape interval (alert at 25 %). See docs/metrics-performance-impact.md.",
			"s", q(`max(jmx_scrape_duration_seconds{@B})`, "")).
			Thresholds(g.Green, g.Step{Color: g.Yellow, Value: 2}, g.Step{Color: g.Red, Value: 3.75}).Size(3, 4),
	}
}
