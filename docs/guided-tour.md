# Guided tour (about 30 minutes)

Everything here works on a fresh `make up`. URLs assume the default port 8080.

## 1. The calm before the storm (5 min)
1. Open the **controlboard** at http://board.localhost:8080. The live view shows the order
   generator (left) feeding payment and inventory; both outcomes meet at shipping; analytics and
   notification consume everything. Each lane is one topic partition, a parcel is a sampled
   event, `1 parcel = N orders` tells you the sampling.
2. Open the **Launchpad** tab: every web UI with a health dot.
3. Open Grafana → **Kafka Lab / Home** (`lab-home`), then **Kafka (bundle) / Kafka Cluster
   Overview** (`kafka-overview`): 3 brokers, 1 controller, 0 offline / under-replicated
   partitions, end-to-end probe latency (KMinion), throughput per broker.

## 2. Black Friday — throughput and lag (5 min)
1. Scenarios → **Black Friday** (1,000 orders/s, 3 min) → Start. Chaos does not need to be armed:
   it only changes knobs.
2. Watch parcels pile up in front of notification and analytics; the KPI bar shows total lag.
3. Grafana → **Consumer Groups & Lag** (`kafka-lag`): lag per group/topic, the partition-skew
   heatmap, *estimated time behind*, consume vs. produce rate. Chaos annotations mark the window.
4. **Broker Deep Dive** (`kafka-broker`): request time split into queue / local / remote /
   response time — where does produce latency come from under load?
5. After the end the board reverts automatically and shows the report (time to detect, time to
   recover, peak lag).

## 3. Payment Provider Down — business impact (5 min)
1. Start **Payment Provider Down** (90 s). The board shows *requested vs observed*: "payment
   applied the experiment settings" is confirmed within seconds (the control-plane generation
   is a metric).
2. The alert panel shows `LabPaymentFailureRateHigh` pending, then firing.
3. Grafana → **Commerce Business** (`lab-business`): failed payments by reason, cancelled orders,
   refunds, revenue per minute — the technical incident expressed as lost money.

## 4. A broker dies (arm chaos first) (5 min)
1. Top bar → arm chaos. Start **Kafka Broker Failure** and type the scenario id to confirm.
2. The rack in the live view goes dark; if it was the controller, the crown moves.
3. **KRaft Controller & Quorum** (`kafka-kraft`): leader change, epoch, voters.
   **Cluster Overview**: under-replicated partitions rise, ISR shrinks, then expands when the
   broker returns. **End-to-End Probe** (`kafka-e2e`): client-visible latency during failover.
4. PANIC always stops everything, reverts all changes and disarms chaos.

## 5. Other things to try
- **Notification Meltdown / Silent Failure**: lag grows although every pod is "healthy" —
  why lag alerts beat liveness probes.
- **Hot Partition**: one partition in the skew heatmap glows, one consumer pod drowns.
- **Poison Message / Duplicate Delivery**: DLQ panel and idempotency (duplicates detected, no
  double charges).
- **Tuning Lab**: lower `acks`, raise `linger`, switch compression — watch request rates and
  batch sizes in **Service & Client Runtime** (`lab-services`).
- **Incident Drill**: a hidden failure; diagnose it with the dashboards; leaderboard.
- **Logs and traces**: in the live view click a parcel → order timeline with links to the
  trace (VictoriaTraces) and the logs (VictoriaLogs).
- **Stack Footprint** (`lab-footprint`) and **Monitoring Overhead** (`lab-monitoring`): what the
  monitoring itself costs (JMX scrape duration per broker, series cardinality).

## 6. Taking the monitoring to another Kafka cluster
Everything under `monitoring/bundle/` is portable: JMX exporter config, KMinion values, scrape
examples (Strimzi, plain Kubernetes, VMs), 8 dashboards, 24 alert rules in two profiles with
runbooks. Start with `monitoring/bundle/README.md` and read
`docs/metrics-performance-impact.md` before enabling anything on a production cluster.
