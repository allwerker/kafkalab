#!/usr/bin/env bash
# Records the scrape fixtures the metric catalog (../catalog.yaml) is validated against
# (`make dashboards-lint`). Run against a running lab: monitoring/bundle/fixtures/record.sh
#
#   jmx-broker-<id>.prom       full JMX exporter scrape of every Kafka node (Strimzi pods)
#   kminion.prom               full KMinion scrape
#   node-exporter.prom         node-exporter, metric families used by the bundle
#   cadvisor-kafka.prom        kubelet cAdvisor, Kafka namespace only
#   kubelet-kafka.prom         kubelet volume stats, Kafka namespace only
#   kube-state-metrics-kafka.prom  kube-state-metrics, Kafka namespace only
#   vmagent-up.prom            vmagent's per-target series (up, scrape_*) for the Kafka targets
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../../.." && pwd)"
K="$ROOT/kctl"
NS="${KAFKA_NAMESPACE:-kafka}"
raw() { "$K" get --raw "$1"; }

for pod in $("$K" -n "$NS" get pods -l strimzi.io/component-type=kafka -o name); do
  pod="${pod#pod/}"
  id="${pod##*-}"
  raw "/api/v1/namespaces/$NS/pods/$pod:9404/proxy/metrics" > "$HERE/jmx-broker-$id.prom"
done
kminion="$("$K" -n "$NS" get pods -l app.kubernetes.io/name=kminion -o name | head -1)"
raw "/api/v1/namespaces/$NS/pods/${kminion#pod/}:8080/proxy/metrics" > "$HERE/kminion.prom"

node="$("$K" get nodes -o name | head -1)"
node="${node#node/}"
raw "/api/v1/nodes/$node/proxy/metrics/cadvisor" | grep -E "namespace=\"$NS\"" > "$HERE/cadvisor-kafka.prom"
raw "/api/v1/nodes/$node/proxy/metrics" | grep -E "^kubelet_volume_stats_.*namespace=\"$NS\"" > "$HERE/kubelet-kafka.prom"

ne="$("$K" -n monitoring get pods -l app.kubernetes.io/name=prometheus-node-exporter -o name | head -1)"
raw "/api/v1/namespaces/monitoring/pods/${ne#pod/}:9100/proxy/metrics" \
  | grep -E '^node_(cpu_seconds_total|memory_MemAvailable_bytes|memory_MemTotal_bytes|disk_io_time_seconds_total|pressure_(cpu|memory|io)_(waiting|stalled)_seconds_total|load1|uname_info)[{ ]' > "$HERE/node-exporter.prom"

ksm="$("$K" -n monitoring get pods -l app.kubernetes.io/name=kube-state-metrics -o name | head -1)"
raw "/api/v1/namespaces/monitoring/pods/${ksm#pod/}:8080/proxy/metrics" | grep -E "namespace=\"$NS\"" > "$HERE/kube-state-metrics-kafka.prom"

# vmagent-generated per-target series (not exposed by a target): export them from VictoriaMetrics.
VM="${VM_URL:-http://vm.localhost:8080}"
curl -fsS "$VM/api/v1/export" --data-urlencode "match[]={__name__=~\"up|scrape_duration_seconds|scrape_samples_scraped|scrape_samples_post_metric_relabeling\",namespace=\"$NS\"}" \
  --data-urlencode 'start=-1m' \
  | python3 -c 'import json,sys
for line in sys.stdin:
    s = json.loads(line); m = s["metric"]; n = m.pop("__name__")
    print(n + "{" + ",".join("%s=\"%s\"" % kv for kv in sorted(m.items())) + "} " + str(s["values"][-1]))' > "$HERE/vmagent-kafka.prom"
wc -l "$HERE"/*.prom
