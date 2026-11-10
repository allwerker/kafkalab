package rules

import (
	"fmt"
	"strings"
)

// Runbook is the operational guidance of an alert.
type Runbook struct {
	Impact     string
	Diagnosis  []string
	Mitigation []string
}

// runbooks holds the guidance per alert. Every alert must have one (checked by the tests).
var runbooks = map[string]Runbook{
	"KafkaBrokerDown": {
		Impact: "Partitions led by the broker fail over; their replicas are under-replicated. With replication factor 3 and min.insync.replicas 2, losing a second broker stops acks=all writers.",
		Diagnosis: []string{
			"Kafka · Pods & Nodes: is the pod running, restarting (OOMKilled?), or pending?",
			"`./kctl -n kafka get pods -l strimzi.io/component-type=kafka` and `./kctl -n kafka logs <pod> --previous`.",
			"If the pod runs but `up` is 0: the JMX exporter port 9404 is not reachable (NetworkPolicy, exporter crashed).",
			"Broker state other than RUNNING (3): the broker is starting, recovering logs after an unclean shutdown, or shutting down.",
		},
		Mitigation: []string{
			"OOMKilled: raise the memory limit (heap + page cache + direct/mapped buffers) in the KafkaNodePool.",
			"Crash loop on startup: read the log for corrupt segments or a full disk (see KafkaDiskAlmostFull).",
			"Log recovery after unclean shutdown can take minutes on big brokers: wait, watch Broker state.",
		},
	},
	"KafkaOfflinePartitions": {
		Impact: "Data in the offline partitions can be neither written nor read. This is an outage for every application using those topics.",
		Diagnosis: []string{
			"Kafka · Cluster Overview → Partition health and Brokers at a glance: which brokers are down?",
			"`kafka-topics.sh --describe --unavailable-partitions` (or AKHQ) lists the partitions and their replicas.",
			"All replicas of the partition must be on unavailable brokers, or only out-of-sync replicas are left (unclean.leader.election.enable=false).",
		},
		Mitigation: []string{
			"Bring the brokers holding the replicas back (see KafkaBrokerDown).",
			"Last resort, accepting data loss: enable unclean leader election for the topic or run `kafka-leader-election.sh --election-type unclean`.",
		},
	},
	"KafkaUnderMinISR": {
		Impact: "Producers with acks=all get NOT_ENOUGH_REPLICAS (or time out) for the affected partitions — writes fail; reads still work.",
		Diagnosis: []string{
			"Kafka · Broker Deep Dive → Replication & ISR: which broker shrinks ISRs, which follower lags (replica fetcher max lag)?",
			"Check the follower brokers for GC pauses (Kafka · JVM), disk saturation and CPU throttling (Kafka · Pods & Nodes).",
		},
		Mitigation: []string{
			"Restore the lagging/down broker; followers re-join the ISR automatically once caught up.",
			"Temporarily lowering min.insync.replicas restores writes at the cost of durability — only with explicit approval.",
		},
	},
	"KafkaUnderReplicatedPartitions": {
		Impact: "Redundancy is reduced: one more failure can make partitions unavailable (or under min ISR).",
		Diagnosis: []string{
			"The alerting broker is the LEADER; the slow follower is another broker. Kafka · Broker Deep Dive → Replica fetcher max lag shows which follower falls behind.",
			"Look for a broker restart (uptime on Kafka · JVM), a running reassignment (reassignment bytes) or a saturated disk/network.",
		},
		Mitigation: []string{
			"After a restart the broker catches up by itself — watch the lag go down.",
			"Persistent: give the slow broker more resources, throttle reassignments, check `num.replica.fetchers`.",
		},
	},
	"KafkaProduceLatencyHigh": {
		Impact: "Producers wait longer for acknowledgements; with max.in.flight limits throughput drops and buffers fill (producer-side timeouts).",
		Diagnosis: []string{
			"Kafka · Broker Deep Dive → 'Where does Produce time go?': request queue (handler saturation), local (disk), remote (waiting for followers, acks=all), response queue/send (network threads).",
			"Correlate with ISR shrinks, GC pauses and disk I/O pressure of the same broker.",
		},
		Mitigation: []string{
			"Remote dominant: fix the slow follower (see KafkaUnderReplicatedPartitions).",
			"Local dominant: disk is too slow — check I/O PSI and log flush latency.",
			"Request queue dominant: see KafkaRequestHandlerSaturated.",
		},
	},
	"KafkaFetchLatencyHigh": {
		Impact: "Consumers or followers receive data late; lagging consumers fall further behind, followers may drop out of the ISR.",
		Diagnosis: []string{
			"Kafka · Pods & Nodes → disk reads: reads > 0 under load mean data is no longer in the page cache (lagging consumers reading old data).",
			"Kafka · Consumer Groups & Lag: which group reads far behind the log end?",
		},
		Mitigation: []string{
			"Give the brokers more memory for page cache (container limit above heap), or let lagging consumers catch up off-peak.",
			"Spread hot partitions over more brokers.",
		},
	},
	"KafkaRequestHandlerSaturated": {
		Impact: "Requests wait in the request queue for an I/O thread; latency grows for every client of the broker.",
		Diagnosis: []string{
			"Kafka · Broker Deep Dive → Request rate by type: did a client start flooding (Metadata, tiny Produce batches)?",
			"Thread pools & queues: request queue size and handler idle per broker.",
		},
		Mitigation: []string{
			"Fix the noisy client (batching: linger.ms/batch.size; metadata refresh), add quotas.",
			"Raise num.io.threads if CPU is available, or add brokers.",
		},
	},
	"KafkaJMXScrapeSlow": {
		Impact: "The JMX exporter consumes broker CPU and holds MBean locks for a large share of every scrape interval; scrapes may time out and dashboards show gaps.",
		Diagnosis: []string{
			"Kafka Lab · Monitoring Overhead: did samples per scrape grow (new topics → per-topic beans)?",
			"Is the broker itself overloaded (CPU throttling, GC)? A slow scrape is then a symptom.",
			"See docs/metrics-performance-impact.md for the cost of each MBean group.",
		},
		Mitigation: []string{
			"Narrow includeObjectNames in monitoring/bundle/jmx/kafka-jmx-exporter.yaml (e.g. drop per-topic BrokerTopicMetrics, fewer RequestMetrics request types).",
			"Raise the scrape interval for brokers (30 s) if you cannot reduce beans.",
		},
	},
	"KafkaNoActiveController": {
		Impact: "Metadata is frozen: no leader elections (failed brokers' partitions stay offline), no topic or config changes, brokers cannot register.",
		Diagnosis: []string{
			"Kafka · KRaft Controller & Quorum → Raft role per node: is any node leader or are they all electing?",
			"Controller pod logs: `./kctl -n kafka logs <controller pod> | grep -i -E 'raft|election|quorum'`.",
			"A majority of controller voters must be up and able to reach each other.",
		},
		Mitigation: []string{
			"Restore enough controller nodes to regain the majority.",
			"Check network policies / DNS between controller nodes (port 9090 controller listener in Strimzi).",
		},
	},
	"KafkaMultipleActiveControllers": {
		Impact: "Conflicting metadata decisions are fenced by epochs, but clients and brokers may see flapping leadership.",
		Diagnosis: []string{
			"Kafka · KRaft Controller & Quorum: Raft epoch and leader per node — do nodes disagree?",
			"Look for a network partition between controller nodes.",
		},
		Mitigation: []string{"Heal the network partition; the node with the stale epoch resigns automatically."},
	},
	"KafkaControllerElectionsFlapping": {
		Impact: "Every election pauses metadata processing; partition leadership changes are delayed.",
		Diagnosis: []string{
			"Kafka · JVM: GC pauses on controller nodes longer than controller.quorum.fetch.timeout.ms?",
			"Kafka · Pods & Nodes: CPU throttling or I/O pressure on controller nodes.",
		},
		Mitigation: []string{"Give controller nodes dedicated, unthrottled resources; separate controller and broker roles on bigger clusters."},
	},
	"KafkaQuorumVoterUnreachable": {
		Impact:     "The quorum tolerates one failure less than designed.",
		Diagnosis:  []string{"Which node reports it (label kafka_broker) and which voter is down: Kafka · KRaft → Raft role per node."},
		Mitigation: []string{"Restore the voter; check DNS/network for the controller listener."},
	},
	"KafkaQuorumCommitSlow": {
		Impact:     "Metadata changes (leader elections, ISR updates, topic creation) take seconds.",
		Diagnosis:  []string{"Disk latency of the metadata log on the voters (log flush latency, I/O PSI) and network between voters."},
		Mitigation: []string{"Faster disks for controllers, fewer concurrent metadata operations (e.g. batch topic creation)."},
	},
	"KafkaBrokerMetadataLagging": {
		Impact:     "The broker serves stale metadata: clients are redirected to old leaders and see NOT_LEADER_OR_FOLLOWER errors and timeouts.",
		Diagnosis:  []string{"Kafka · KRaft → Metadata apply lag per broker; broker logs for metadata loader errors; broker CPU/GC."},
		Mitigation: []string{"Restart the broker if it does not catch up; investigate metadata apply errors."},
	},
	"KafkaBrokerFenced": {
		Impact:     "Fenced brokers lead no partitions; their load shifts to the others and their replicas drop out of ISRs.",
		Diagnosis:  []string{"Kafka · KRaft → Broker registration state; is the broker running but unable to heartbeat (network, GC pause > broker.session.timeout.ms)?"},
		Mitigation: []string{"Restore connectivity / resources; the broker unfences itself after a successful heartbeat."},
	},
	"KafkaConsumerLagGrowing": {
		Impact: "The application processes data later and later; with retention, unconsumed data may be deleted before it is read (data loss for the consumer).",
		Diagnosis: []string{
			"Kafka · Consumer Groups & Lag: consume vs produce rate, lag per partition (one hot partition or all?).",
			"Members per group: did instances disappear? Group state: is it rebalancing all the time?",
			"Lab: Kafka Lab · Service & Client Runtime → processing time, retries, paused consumers.",
		},
		Mitigation: []string{
			"Scale the consumer out (up to the number of partitions) or speed up processing (batching, DB indexes).",
			"Hot partition: fix the key distribution or the slow message.",
		},
	},
	"KafkaConsumerStalled": {
		Impact: "Nothing is processed for the affected topic while the group still holds its partitions, so no other instance takes over.",
		Diagnosis: []string{
			"Kafka · Consumer Groups & Lag → Stalled consumers table and Group state.",
			"Application logs (Kafka Lab · Logs) of the group's pods: blocked handler, poison message retried forever, paused consumer.",
		},
		Mitigation: []string{
			"Restart the stuck instance; move poison messages to a DLQ.",
			"Lab: disable the 'Silent Failure' knob on the controlboard.",
		},
	},
	"KafkaProbeFailing": {
		Impact:     "Kafka does not deliver KMinion's own messages: applications are very likely affected too.",
		Diagnosis:  []string{"Kafka · End-to-End Probe → per-partition latency and failures (each probe partition has a different leader broker); then Cluster Overview."},
		Mitigation: []string{"Follow KafkaOfflinePartitions / KafkaUnderMinISR / KafkaBrokerDown, whichever fires."},
	},
	"KafkaProbeSlow": {
		Impact:     "Producing and consuming takes seconds for a share of messages: latency-sensitive applications breach their SLOs.",
		Diagnosis:  []string{"Kafka · End-to-End Probe → produce latency per partition (slow leader?), then Broker Deep Dive request breakdown and JVM GC pauses."},
		Mitigation: []string{"Fix the slow broker (resources, disk, GC)."},
	},
	"KMinionScrapeFailing": {
		Impact:     "Lag, topic size and probe metrics are stale: lag alerts cannot fire.",
		Diagnosis:  []string{"`./kctl -n kafka logs deploy/kminion` — authentication, connectivity or timeouts against the admin API."},
		Mitigation: []string{"Fix KMinion's Kafka connection settings; restart KMinion."},
	},
	"KafkaHeapAfterGCHigh": {
		Impact:     "Constant garbage collection, growing pauses, eventually OutOfMemoryError and a broker restart.",
		Diagnosis:  []string{"Kafka · JVM → Heap after GC trend: steady growth (more partitions/connections) or sudden jump (large fetch/produce requests)?"},
		Mitigation: []string{"Raise -Xmx together with the container memory limit (keep room for page cache), or reduce partitions per broker."},
	},
	"KafkaGCTimeHigh": {
		Impact:     "Stop-the-world pauses add latency to every request; long pauses expire sessions and shrink ISRs.",
		Diagnosis:  []string{"Kafka · JVM → average pause, collections per second (any Old Generation = full GC), allocation rate."},
		Mitigation: []string{"More heap if heap after GC is high; otherwise tune G1 (MaxGCPauseMillis) or reduce allocation (smaller fetch sizes, fewer connections)."},
	},
	"KafkaDiskFillingUp": {
		Impact:     "When the volume is full the broker marks its log dir offline; all partitions on it become unavailable on this replica.",
		Diagnosis:  []string{"Kafka · Topics, Partitions & Storage → fastest growing topics and size forecast; Kafka · Pods & Nodes → volume usage."},
		Mitigation: []string{"Reduce retention (retention.ms / retention.bytes) of the growing topics, expand the PVC, or move partitions to other brokers."},
	},
	"KafkaDiskAlmostFull": {
		Impact:     "The broker is minutes to hours away from taking its log dir offline.",
		Diagnosis:  []string{"Same as KafkaDiskFillingUp; also check for compacted topics that never compact (log cleaner, uncleanable partitions)."},
		Mitigation: []string{"Act now: lower retention on the biggest topics, delete unused topics, expand the volume."},
	},
	"LabPaymentFailureRateHigh": {
		Impact:     "Orders are cancelled instead of shipped; revenue drops.",
		Diagnosis:  []string{"Kafka Lab · Commerce Business → Payments by result (which reason?); payment service logs; running chaos experiments."},
		Mitigation: []string{"Stop the experiment that degrades the payment provider; check payment's database connections."},
	},
	"LabOrderCompletionSlow": {
		Impact:     "Customers wait long for shipping confirmations; in-flight orders pile up.",
		Diagnosis:  []string{"Kafka Lab · Commerce Business → funnel steps; Kafka · Consumer Groups & Lag for the lagging service."},
		Mitigation: []string{"Remove the bottleneck (scale the lagging service, end the experiment)."},
	},
	"LabDeadLetterActivity": {
		Impact:     "Some orders are not processed and need manual attention.",
		Diagnosis:  []string{"AKHQ: read the DLQ topic, check the reason header; Kafka Lab · Service & Client Runtime → dead-lettered records by service."},
		Mitigation: []string{"Expected while the generator sends poison messages. Otherwise fix the bug and replay the DLQ."},
	},
	"LabRevenueDrop": {
		Impact:     "The shop earns less than half of what it did an hour ago.",
		Diagnosis:  []string{"Kafka Lab · Commerce Business → funnel; Kafka Lab · Chaos Experiments → what is running?"},
		Mitigation: []string{"Find the broken funnel step and fix or stop its cause."},
	},
	"LabNotificationBacklog": {
		Impact:     "Customers are informed late about shipped orders.",
		Diagnosis:  []string{"Kafka · Consumer Groups & Lag, group notification; notification service logs."},
		Mitigation: []string{"Scale the notification service or end the experiment affecting it."},
	},
}

// RunbookFiles renders docs/runbooks/<alert>.md for every alert of the groups.
func RunbookFiles(groups []Group) map[string][]byte {
	out := map[string][]byte{}
	for _, g := range groups {
		for _, a := range g.Alerts {
			rb := runbooks[a.Name]
			var b strings.Builder
			fmt.Fprintf(&b, "# %s\n\n<!-- GENERATED by monitoring/dashgen from internal/rules (make alerts). Do not edit. -->\n\n", a.Name)
			fmt.Fprintf(&b, "| Severity | Component | Dashboard | for (production / demo) |\n|---|---|---|---|\n")
			fmt.Fprintf(&b, "| %s | %s | [%s](http://grafana.localhost:8080/d/%s/) | %s / %s |\n\n", a.Severity, a.Component, a.Dashboard, a.Dashboard, Duration(a.ForProd), Duration(a.ForDemo))
			fmt.Fprintf(&b, "**Summary:** %s\n\n%s\n\n", plain(a.Summary), plain(a.Description))
			fmt.Fprintf(&b, "## Impact\n\n%s\n\n## Diagnosis\n\n", rb.Impact)
			for i, d := range rb.Diagnosis {
				fmt.Fprintf(&b, "%d. %s\n", i+1, d)
			}
			b.WriteString("\n## Mitigation\n\n")
			for _, m := range rb.Mitigation {
				fmt.Fprintf(&b, "- %s\n", m)
			}
			fmt.Fprintf(&b, "\n## Expression\n\n```promql\n%s\n```\n", a.Expr)
			out[a.Name+".md"] = []byte(b.String())
		}
	}
	return out
}

// plain turns {{ $labels.x }} into <x> for prose.
func plain(s string) string {
	return labelTmpl.ReplaceAllString(s, "<$1>")
}
