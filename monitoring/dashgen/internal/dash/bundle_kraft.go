package dash

import g "kafkalab/monitoring/dashgen/internal/grafana"

// KRaft is the KRaft controller & quorum dashboard (uid kafka-kraft).
func KRaft() *g.Dashboard {
	d := bundle("kafka-kraft", "Kafka · KRaft Controller & Quorum",
		"Health of the KRaft metadata quorum: who is the active controller, are voters in sync, how fast metadata changes commit and how far brokers lag behind the metadata log.",
		clusterVar("kafka_server_kafkaserver_brokerstate"), brokerVar())
	d.Rows = []*g.Row{
		row("", false,
			g.Stat("Active controller",
				"Nodes acting as active controller right now. 1 = healthy, 0 = no controller (metadata frozen: no leader elections, no topic changes), 2 = election in progress / split brain.",
				"short", q(`sum(kafka_controller_kafkacontroller_activecontrollercount{@C})`, "")).
				Thresholds(g.Red, g.Step{Color: g.Green, Value: 1}, g.Step{Color: g.Red, Value: 2}).Map("0", "none", g.Red).Size(4, 4),
			g.Stat("Quorum leader (node id)",
				"Node id of the current Raft leader as seen by the selected nodes. All nodes should agree; it is the active controller.",
				"none", q(`max(kafka_server_raftmetrics_current_leader{@B})`, "")).Hue(g.Blue).Neutral().Opt("graphMode", "none").Size(4, 4),
			g.Stat("Leader changes (window)",
				"New active controllers elected within the dashboard time range. Each change briefly pauses metadata updates; frequent changes indicate controller GC pauses, network problems or overloaded controller nodes.",
				"short", q(`sum(max_over_time(kafka_controller_kafkacontroller_newactivecontrollerscount{@C}[$__range]) - min_over_time(kafka_controller_kafkacontroller_newactivecontrollerscount{@C}[$__range]))`, "")).
				Thresholds(g.Green, g.Step{Color: g.Yellow, Value: 1}, g.Step{Color: g.Red, Value: 3}).Opt("graphMode", "none").Size(4, 4),
			g.Stat("Raft epoch",
				"Current leader epoch of the metadata quorum. It increments on every election; a fast-rising epoch means elections keep failing.",
				"none", q(`max(kafka_server_raftmetrics_current_epoch{@B})`, "")).Hue(g.Purple).Neutral().Size(4, 4),
			okIfZero(g.Stat("Unknown voter connections",
				"Voters this node cannot reach or does not know an endpoint for. Any value > 0 reduces quorum fault tolerance.",
				"short", q(`max(kafka_server_raftmetrics_number_unknown_voter_connections{@B})`, ""))).Size(4, 4),
			okIfZero(g.Stat("Fenced brokers",
				"Brokers the controller fenced because their heartbeats stopped. Fenced brokers lead no partitions.",
				"short", q(`max(kafka_controller_kafkacontroller_fencedbrokercount{@C})`, ""))).Size(4, 4),
		),
		row("Quorum membership", false,
			g.StateTimeline("Raft role per node",
				"Role of each node in the metadata quorum over time: leader (active controller), follower (voter), observer (broker-only node) or candidate/unattached during elections. Exactly one leader at any time is healthy.",
				"none",
				q(`max by (kafka_broker) (
  (kafka_server_raftmetrics_current_state{@B,current_state="leader"} * 3)
  or (kafka_server_raftmetrics_current_state{@B,current_state="follower"} * 2)
  or (kafka_server_raftmetrics_current_state{@B,current_state="observer"} * 1)
  or (kafka_server_raftmetrics_current_state{@B,current_state!~"leader|follower|observer"} * 0)
)`, "node {{kafka_broker}}")).
				Thresholds(g.Blue).
				Map("3", "leader", g.Green).Map("2", "follower", g.Blue).Map("1", "observer", g.Purple).
				Map("0", "electing", g.Orange).Size(16, 8),
			g.Table("Voters & observers",
				"Number of voters and observers as seen by each node, plus uncommitted voter changes (a KIP-853 reconfiguration in flight).",
				"short",
				iq(`max by (kafka_broker) (kafka_server_raftmetrics_number_of_voters{@B})`, ""),
				iq(`max by (kafka_broker) (kafka_server_raftmetrics_current_leader{@B})`, ""),
				iq(`max by (kafka_broker) (kafka_server_raftmetrics_current_epoch{@B})`, "")).
				Transform("merge", map[string]any{}).
				Organize(map[string]string{"kafka_broker": "Node", "Value #A": "Voters", "Value #B": "Leader seen", "Value #C": "Epoch"}).
				Neutral().Size(8, 8),
		),
		row("Metadata log replication", false,
			g.TimeSeries("Metadata log end offset vs high watermark",
				"Log end offset (what each node has) and high watermark (what the quorum committed). Lines should overlap; a node whose end offset falls behind cannot keep up with metadata changes.",
				"short",
				q(`max by (kafka_broker) (kafka_server_raftmetrics_log_end_offset{@B})`, "end offset · node {{kafka_broker}}"),
				q(`max(kafka_server_raftmetrics_high_watermark{@B})`, "high watermark")).
				Hue(g.Blue).Size(12, 8),
			g.TimeSeries("Commit latency",
				"Average time from appending a metadata record on the leader to committing it on a majority of voters. Healthy: a few ms to tens of ms; seconds means a slow voter disk or network.",
				"ms",
				q(`max by (kafka_broker) (kafka_server_raftmetrics_commit_latency_avg{@B})`, "avg · node {{kafka_broker}}")).
				Thresholds(g.Green, g.Step{Color: g.Yellow, Value: 100}, g.Step{Color: g.Red, Value: 1000}).Lines("dashed").
				Hue(g.Purple).Size(12, 8),
			g.TimeSeries("Metadata apply lag (brokers)",
				"How old the last metadata record applied by each broker is, and the controller's own last-applied lag. Brokers that lag apply leader changes late: clients see NOT_LEADER errors for longer. Healthy: below a few hundred ms while metadata changes, otherwise it grows with the time since the last change (no-op records every 500 ms keep it small in Kafka 4). Values of a day or more (a broker that has not applied anything since start, lag = epoch time) are filtered out.",
				"ms",
				q(`max by (kafka_broker) (kafka_server_brokermetadatametrics_last_applied_record_lag_ms{@B} < 86400000)`, "broker {{kafka_broker}}"),
				q(`max by (kafka_broker) (kafka_controller_kafkacontroller_lastappliedrecordlagms{@B} < 86400000)`, "controller · node {{kafka_broker}}")).
				Hue(g.Blue).Size(12, 8),
			g.TimeSeries("Raft append / fetch rate",
				"Metadata records appended by the leader and fetched by followers/observers per second. Kafka 4 writes a no-op record about every 500 ms, so a flat ~2/s line is the idle baseline.",
				"ops",
				q(`max by (kafka_broker) (kafka_server_raftmetrics_append_records_rate{@B})`, "append · node {{kafka_broker}}"),
				q(`max by (kafka_broker) (kafka_server_raftmetrics_fetch_records_rate{@B})`, "fetch · node {{kafka_broker}}")).
				Hue(g.Green).Size(12, 8),
		),
		row("Controller", false,
			g.TimeSeries("Controller event queue time / processing time (p99)",
				"Events waiting in and being processed by the QuorumController queue (broker heartbeats, topic creations, ISR changes). Growing queue time means the controller is overloaded and broker heartbeats may time out → brokers fenced.",
				"ms",
				q(`max by (kafka_broker) (kafka_controller_controllereventmanager_eventqueuetimems{@B,quantile="0.99"})`, "queue · node {{kafka_broker}}"),
				q(`max by (kafka_broker) (kafka_controller_controllereventmanager_eventqueueprocessingtimems{@B,quantile="0.99"})`, "processing · node {{kafka_broker}}")).
				Hue(g.Purple).Size(12, 8),
			g.TimeSeries("Controller errors and timeouts",
				"Events that timed out in the controller queue, broker heartbeats that timed out and metadata errors (controller and broker side, cumulative counters → increase per interval). All should stay at 0.",
				"short",
				q(`sum(increase(kafka_controller_kafkacontroller_eventqueueoperationstimedoutcount{@C}[@RI]))`, "timed-out events"),
				q(`sum(increase(kafka_controller_kafkacontroller_timedoutbrokerheartbeatcount{@C}[@RI]))`, "timed-out broker heartbeats"),
				q(`sum(increase(kafka_controller_kafkacontroller_metadataerrorcount{@C}[@RI]))`, "controller metadata errors"),
				q(`sum(increase(kafka_server_brokermetadatametrics_metadata_apply_error_count{@C}[@RI]))`, "broker apply errors"),
				q(`sum(increase(kafka_server_brokermetadatametrics_metadata_load_error_count{@C}[@RI]))`, "broker load errors")).
				Hue(g.Red).Bars().Thresholds(g.Green, g.Step{Color: g.Red, Value: 1}).Size(12, 8),
			g.StateTimeline("Broker registration state (active controller view)",
				"Registration state of every broker as tracked by the active controller (BrokerRegistrationState). Unfenced brokers can lead partitions; fenced ones lost their heartbeat session; controlled shutdown appears during rolling restarts.",
				"none",
				q(`max by (broker) (kafka_controller_kafkacontroller_brokerregistrationstate{@C} and on (kafka_broker) (kafka_controller_kafkacontroller_activecontrollercount{@C} == 1))`, "broker {{broker}}")).
				Thresholds(g.Blue).
				Map("-1", "not registered", g.Red).Map("10", "fenced", g.Red).Map("20", "controlled shutdown", g.Orange).Map("30", "unfenced", g.Green).
				Size(12, 7),
			g.TimeSeries("Controller idle ratio",
				"Idle share of the controller event thread and of the Raft polling thread (poll idle). Values near 1 are healthy; below 0.5 the single-threaded controller is the bottleneck.",
				"percentunit",
				q(`max by (kafka_broker) (kafka_controller_controllereventmanager_avgidleratio{@B})`, "event thread · node {{kafka_broker}}"),
				q(`max by (kafka_broker) (kafka_server_raftmetrics_poll_idle_ratio_avg{@B})`, "raft poll · node {{kafka_broker}}")).
				Hue(g.Green).SetMin(0).SetMax(1).Size(12, 7),
		),
	}
	return d
}
