# ADR 0003 — How the controlboard breaks (and repairs) Kafka brokers (T209/T312)

Status: accepted · 2026-11-10 · verified on the live k3d cluster `kafka-lab`
(Strimzi 1.3.1 operator chart, Kafka 4.3.2, KRaft, 3 dual-role nodes, StrimziPodSet `kafka-lab-dual-role`).

## Questions and what the spike showed

| Question | Result |
|---|---|
| Does `strimzi.io/pause-reconciliation=true` on the Kafka CR keep a deleted broker pod down? | **No.** Condition `ReconciliationPaused=True` was set, the pod was deleted and the StrimziPodSet controller recreated it within ~1 s (ready after ~35 s). Pausing only stops the Kafka-CR reconciliation, not the PodSet controller. |
| What keeps it down? | **Scaling `strimzi-cluster-operator` to 0** (the PodSet controller runs inside it), then deleting the pod. The pod stayed absent for the whole test (48 s). Scaling the operator back to 1: operator up after ~35 s, broker pod recreated, ready ~45 s later, ISR complete again shortly after. |
| Can an ephemeral container with `NET_ADMIN` manipulate the broker's network namespace? | **Yes.** `PATCH pods/<pod>/ephemeralcontainers` with `securityContext {runAsUser: 0, capabilities.add: [NET_ADMIN]}` works (no PodSecurity admission in k3d). Image from the k3d registry (`kafka-lab-registry.localhost:5111/chaos-net:<hash>`). iptables backend: `iptables-nft` (host uses nf_tables). |
| Partition (drop 9090–9093 in/out, except `lo`) | **Works.** Connects to `:9092` time out, the broker leaves every ISR within ~10 s (`--under-replicated-partitions` lists all its replicas), the pod stays `Running` and its JMX `up` stays 1 (port 9404 is not blocked). After undo the ISR was complete again within ~20 s. |
| `tc netem` without `sch_netem` loaded | **Works.** The kernel auto-loads `sch_netem` on the first `tc qdisc add … netem` request from the pod's netns (lsmod showed it afterwards; no sudo needed). 300 ms delay measured as 312 ms TCP connect time. |
| Loss via `iptables -m statistic --mode random` | Works (`xt_statistic` loaded). |
| Dead-man switch | Works: with an unreachable `HEALTH_URL` the helper self-reverted after 3 failed checks (12 s). |

## Decisions

1. **Rolling Restart** (`broker-rolling-restart`, not destructive): graceful `DELETE` of one broker pod.
   The StrimziPodSet controller recreates it; observation = pod ready again.
2. **Broker Crash** (`broker-crash`, destructive): scale `strimzi-cluster-operator` to 0 (previous replica
   count stored in annotation `lab.kafka/revert` on the Deployment and in PostgreSQL), delete the broker pod
   with `gracePeriodSeconds=0`, keep it down for the duration. Revert = scale the operator back to the stored
   count and remove the annotation; Strimzi recreates the pod. Recovery takes ~80 s after the revert, which
   is shown as `recovering`. `pause-reconciliation` is **not** used (no effect on the PodSet controller).
   While the operator is down nothing else in the cluster is reconciled (topics are handled by the
   entity operator and keep working) — another reason for the global broker-fault lock.
3. **Broker Partition** (`broker-partition`, destructive) and **Broker Packet Loss / Latency**
   (`broker-network`, destructive): ephemeral container `chaos-<experiment-id>` from `images/chaos-net`.
   It reverts itself on duration end, SIGTERM and when `http://controlboard.control:8080/healthz` fails
   3× in a row. The controlboard cannot stop an ephemeral container, so an early stop/panic injects a
   second container `chaos-<id>-undo` with `MODE=undo` (same netns): it removes every `LCI*`/`LCO*` chain
   and a netem root qdisc. The original helper checks its own marker chain every 5 s and exits without
   touching anything once it is gone, so it can never undo a newer experiment's rules (verified live:
   "undone externally", ISR complete ~15 s later). Ephemeral containers accumulate in the pod spec until
   the next broker restart; names are unique per experiment.
4. **Latency for services** stays client-side (`netLatencyMs`/`netDropProbability` knobs through the kit
   fault dialer): it needs no privileges in the `commerce` namespace and reverts with the snapshot expiry.
   netem is only used for brokers (`broker-network` with `mode=latency`). If netem is unavailable on
   another host the helper exits with code 2 and the experiment fails with that observation.
5. **Panic** undoes all of this from the API server alone: it injects an undo container into every broker
   pod with a running `chaos-*` ephemeral container, scales every Deployment carrying a `lab.kafka/revert`
   annotation (operator, commerce services) back to the recorded replicas, and removes
   `strimzi.io/pause-reconciliation` from the Kafka CR if somebody set it.

## Observations used

- Broker down: no pod / pod not ready via the Kubernetes API (faster than `up{kafka_broker="N"}`).
- Partition: `sum(kafka_server_replicamanager_underreplicatedpartitions) > 0` and the pod still Running.
- Back to normal: all broker pods ready and under-replicated = 0.
