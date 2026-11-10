# Metrics and Kafka performance — what monitoring costs the brokers

Monitoring is not free. The JMX exporter runs **inside the broker JVM** and competes with Kafka for
CPU, heap and MBean-server locks on every scrape. This document lists every MBean group of the
curated config ([monitoring/bundle/jmx/kafka-jmx-exporter.yaml](../monitoring/bundle/jmx/kafka-jmx-exporter.yaml)),
what it costs, why it is in or out, and how to check the cost on a running cluster.

## ⚠ Notification list: metrics that degrade Kafka performance

You asked to be told about these. Status in the bundle:

| # | Metric / feature | Risk | Status in the bundle |
|---|---|---|---|
| 1 | **Per-partition broker beans** `kafka.log:type=Log,name=Size\|LogEndOffset\|LogStartOffset\|NumLogSegments`, `kafka.cluster:type=Partition,*` | **High** — 5–10 beans × partitions × replicas per broker. 10 000 partition replicas = 50 000+ beans: scrapes take seconds to minutes, allocate hundreds of MB, cause GC pauses and ISR shrinks | **Excluded.** Sizes, offsets and lag come from KMinion via the admin API |
| 2 | **Replica fetcher lag per partition** `kafka.server:type=FetcherLagMetrics,*` | **High** — one bean per followed partition | **Excluded.** `ReplicaFetcherManager MaxLag` (one value per broker) is kept |
| 3 | **Per-client quota beans** `kafka.server:type=Produce\|Fetch\|Request,user=*,client-id=*` | **High, unbounded** — one set per client id; clients with random ids (e.g. `consumer-<uuid>`) grow it forever | **Excluded.** Only the aggregate queue sizes are read |
| 4 | **All request types of `RequestMetrics`** (~70 API keys × ~10 histogram/meter beans) | **Medium-high** — Yammer histograms compute a sorted snapshot (1 028 samples) for *every* percentile/mean/min/max attribute read; ~6 snapshots per histogram per scrape | **Restricted** to 15 request types; percentiles exported only for the time histograms |
| 5 | **Per-topic `BrokerTopicMetrics`** | **Medium at scale** — ~12 meters × topics × brokers; harmless below ~1 000 topics | **Included** (internal `__*` topics excluded). Drop with `excludeObjectNames: kafka.server:type=BrokerTopicMetrics,name=*,topic=*` on clusters with thousands of topics |
| 6 | **JVM thread metrics** (`jvm_threads_state`, `jvm_threads_deadlocked`) | **Low** — `ThreadMXBean.getThreadInfo` / `findDeadlockedThreads` are VM operations that need a safepoint (a short global pause, microseconds with ~150 Kafka threads) | **Included** (built-in collector) |
| 7 | **KMinion `logDirs`** (DescribeLogDirs on every broker per scrape) | **Medium on brokers with many partitions** — the broker walks all log dirs | **Included** (topic sizes and storage forecasts depend on it). Raise KMinion's scrape interval on big clusters |
| 8 | **Scrape interval below 15 s** | Multiplies every item above | Lab and bundle alert assume **15 s** |
| 9 | Rules without `cache: true` | Regex matching of every attribute against ~40 patterns per scrape | **Fixed**: all rules cache except the one that matches attribute values |
| 10 | Duplicate series (`_objectname` label) | Request beans of different API versions collided into one series name; jmx_exporter 1.x then emits an `_objectname` label per bean (higher cardinality, ugly series) | **Fixed**: `version` label kept on `requests_total` |

Rule of thumb: **anything whose bean count grows with partitions, clients or topics must not be in
the JMX allowlist** of a big cluster. Get that data from the admin API (KMinion) instead.

## How the JMX exporter spends broker resources

Per scrape the agent:

1. queries the MBean server for every name in `includeObjectNames` (wildcards are expanded by the
   MBean server — takes its registration lock briefly);
2. reads **all readable attributes** of every matched bean (`getAttributes`), unless excluded with
   `excludeObjectNameAttributes` — the rules only decide what is *exported*, not what is *read*.
   For a Yammer histogram that means Count, Min, Max, Mean, StdDev and six percentiles, each
   percentile computed from a fresh sorted snapshot of the reservoir;
3. matches every attribute against the rules (first match wins; `cache: true` remembers the result
   per bean/attribute and skips the regex work on later scrapes);
4. renders the text exposition (allocation proportional to the number of samples → young-GC pressure).

So the cost is driven by **beans × attributes** read, not only by the series exported.

## Measured in the lab

3 dual-role KRaft nodes, Kafka 4.3.2, Strimzi 1.3.1, jmx_prometheus_javaagent 1.6.0, 16 topics / 54
partitions, ~55 orders/s through 6 services. Host: 8 cores, heavily loaded (load average ~36 while
several agents build) — absolute times are pessimistic, relations hold.

| | Config at T106 (deployed while recording) | Refined config (this ticket) |
|---|---|---|
| Samples per broker scrape | 1 003 – 1 099 (`scrape_samples_scraped`) | ≈ 890 expected (−160: no p99.9 per phase, no byte-size/conversion percentiles, no TemporaryMemory/MessageConversions beans) |
| Share of `RequestMetrics` | 574 of 1 054 samples (54 %), 372 percentile series | ≈ 410 samples |
| `jmx_scrape_duration_seconds` (30 min avg / max) | 1.3 – 2.3 s / up to 9.9 s | to be measured after rollout (rule cache, fewer snapshots) |
| KMinion scrape | 688 samples, no JMX involvement | unchanged |

Fixtures with the full scrape of every broker: `monitoring/bundle/fixtures/jmx-broker-*.prom`.

> The refined config is committed but **not rolled out yet** in the shared lab cluster (rolling the
> Kafka release was outside this ticket's permissions). Roll out with
> `make deploy-one R=kafka-cluster`, then re-record fixtures (`monitoring/bundle/fixtures/record.sh`)
> and compare the numbers on *Kafka Lab · Monitoring Overhead*. The dashboards work with both
> configs (only names common to both are used).

## How to check the cost on your cluster

| What | Query / place |
|---|---|
| Exporter time inside the JVM | `jmx_scrape_duration_seconds{kafka_cluster="…"}` — alert **KafkaJMXScrapeSlow** fires above 25 % of the scrape interval (avg over 5 m) |
| Scrape time as the scraper sees it (incl. network, rendering) | `scrape_duration_seconds{job=~".*kafka.*"}` |
| Samples per scrape (≈ beans × exported attributes) | `scrape_samples_scraped`, `scrape_samples_post_metric_relabeling` |
| Is the rule cache effective? | `jmx_scrape_cached_beans` > 0 after the first scrape (0 = no caching rules) |
| Dashboards | *Kafka Lab · Monitoring Overhead* (scrape time vs interval, samples per broker, series per job), *Kafka · Broker Deep Dive* tile "JMX scrape time" |
| Cost of one MBean group | Temporarily add it to `excludeObjectNames` (the agent reloads the file; `jmx_config_reload_success_total` increments) and compare `jmx_scrape_duration_seconds` and `scrape_samples_scraped` before/after |
| From the command line | `./kctl get --raw /api/v1/namespaces/kafka/pods/kafka-lab-dual-role-0:9404/proxy/metrics \| grep -v '^#' \| sed 's/[{ ].*//' \| sort \| uniq -c \| sort -rn \| head` — series per metric name |

Symptoms that monitoring hurts the broker: `jmx_scrape_duration_seconds` close to the interval,
young-GC rate rising in step with the scrape interval (Kafka · JVM → Collections per second),
request handler idle dropping briefly every scrape, scrape timeouts (`up` flapping).

## Every MBean group of the allowlist

Series counts per broker from the lab fixtures (16 topics, 3 listeners, 6 network threads).

| MBean group | In | Series (lab) | Grows with | Risk | Why / mitigation |
|---|---|---|---|---|---|
| `kafka.server:type=BrokerTopicMetrics,*` | ✔ | 79 (57 per-topic) | topics × brokers | medium at >1 000 topics | traffic per topic; `__*` topics excluded; drop `topic=*` beans if large |
| `kafka.server:type=ReplicaManager,*` | ✔ | 12 | constant | low | URP, ISR changes, leaders |
| `kafka.server:type=ReplicaFetcherManager,*` | ✔ | 4 | fetcher pools | low | max lag, dead threads |
| `kafka.server:type=KafkaRequestHandlerPool,*` | ✔ | 6 | constant | low | handler idle (use `rate(_count)/1e9`) |
| `kafka.server:type=DelayedOperationPurgatory,*` | ✔ | 14 | constant | low | purgatory sizes |
| `kafka.server:type=FetchSessionCache,*` | ✔ | 3 | constant | low | incremental fetch sessions |
| `kafka.server:type=KafkaServer,name=BrokerState` (+ `linux-disk-*`) | ✔ | 1 (+2) | constant | low | broker state, disk bytes (bare metal) |
| `kafka.server:type=socket-server-metrics,*` | ✔ | 96 | listeners × network threads (+ client software name/version) | low | connections, bytes, auth failures, client versions |
| `kafka.server:type=Produce\|Fetch\|Request` (no client id) | ✔ | 3 | constant | low | quota queue sizes; per-client beans are excluded (#3) |
| `kafka.network:type=RequestChannel,*` | ✔ | 8 | network threads | low | request/response queue sizes |
| `kafka.network:type=SocketServer,*`, `Acceptor,*` | ✔ | 4 (+listeners) | listeners | low | network idle, acceptor blocked |
| `kafka.network:type=RequestMetrics` — 15 request types | ✔ | 574 → ≈410 | request types × API versions | **medium-high** (#4) | request time breakdown; percentiles only for TotalTime and the 5 phases |
| `kafka.network:type=RequestMetrics,name=ErrorsPerSec,*` | ✔ | 23 | request types × error codes seen | low | error codes |
| `RequestMetrics name=TemporaryMemoryBytes\|MessageConversionsTimeMs` | ✘ | (16) | request types | low | unused; excluded |
| `kafka.controller:type=KafkaController\|ControllerStats\|ControllerEventManager\|QuorumController` | ✔ | 29 | brokers (registration state) | low | KRaft controller health |
| `kafka.server:type=raft-metrics`, `raft-channel-metrics`, `broker-metadata-metrics` | ✔ | 30 | constant | low | quorum health, metadata lag |
| `kafka.server:type=MetadataLoader,*`, `SnapshotEmitter,*` | ✔ | few | constant | low | metadata version, snapshot age (available after rollout) |
| `kafka.log:type=LogFlushStats,*` | ✔ | 4 | constant | low | fsync latency |
| `kafka.log:type=LogManager,name=OfflineLogDirectoryCount` | ✔ | 1 | constant | low | offline log dirs |
| `kafka.log:type=LogCleanerManager,*` | ✔ | 4 | log dirs | low | compaction health |
| `kafka.server:type=group-coordinator-metrics,*`, `share-group-metrics`, `transaction-coordinator-metrics` | ✔ | 80 | group states (constant) | low | coordinator load, group counts |
| `kafka.log:type=Log,*`, `kafka.cluster:type=Partition,*` | ✘ | — | **partitions × replicas** | **high** (#1) | KMinion |
| `kafka.server:type=FetcherLagMetrics,*` | ✘ | — | **followed partitions** | **high** (#2) | `ReplicaFetcherManager` max lag |
| `kafka.server:type=Produce\|Fetch\|Request,user=*,client-id=*` | ✘ | — | **clients** | **high** (#3) | aggregate queue sizes |
| `kafka.server:type=app-info`, `kafka.server:type=*-metrics,client-id=*` (internal clients), `kafka.utils:type=Throttler` | ✘ | — | — | low | no dashboard use |
| JVM / process collectors (`jvm_*`, `process_*`) | ✔ | 100 | pools, collectors, buffer pools | low (#6) | heap, GC, threads, buffers, fds |

## Other sources

| Source | Cost on Kafka | Notes |
|---|---|---|
| KMinion | admin requests per scrape (Metadata, ListOffsets, DescribeGroups, OffsetFetch, DescribeLogDirs) + probe traffic (`probeInterval` 500 ms → ~2 msg/s per probe partition, acks=all) | visible as request rates on Broker Deep Dive; `scrapeMode: adminApi` avoids consuming `__consumer_offsets` |
| cAdvisor, kube-state-metrics, kubelet, node-exporter | none on Kafka | cardinality in VictoriaMetrics only (kubelet ≈ 70 000 series in the lab) |
| franz-go client hooks in the services | none on brokers | per-node / per-topic series in the services |

## Checklist before adding an MBean

1. Does its count grow with partitions, clients or topics? → don't, or restrict by name.
2. Is it a Yammer histogram? → every scrape sorts its reservoir several times; export only the
   percentiles you use and consider `excludeObjectNameAttributes`.
3. Add it to `includeObjectNames`, add a rule with `cache: true` (unless it matches values), record
   fixtures (`monitoring/bundle/fixtures/record.sh`), add it to `monitoring/bundle/catalog.yaml`,
   run `make dashboards-lint`, and compare `jmx_scrape_duration_seconds` before/after.
