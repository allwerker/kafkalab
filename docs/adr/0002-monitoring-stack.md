# ADR 0002 — Monitoring stack and data plane services (T105/T106)

Status: accepted · 2026-11-10

## What is installed

| Release (file) | Namespace | Content |
|---|---|---|
| `vmks` (`25-monitoring`) | monitoring | `victoria-metrics-k8s-stack` 0.98.1: VM operator (+CRDs), vmsingle v1.156.0, vmagent, vmalert, Alertmanager v0.35.0, Grafana 13.3.0, node-exporter, kube-state-metrics, the chart's sync job (default dashboards + rules) |
| `postgres` (`30-postgres`) | data | local chart: PostgreSQL 18.6-alpine StatefulSet + postgres_exporter v0.20.1 sidecar, bootstrap hook Job, Secret `postgres-credentials` in data/commerce/control |
| `schema-registry` (`31-schema-registry`) | data | local chart: Karapace 6.2.4 (registry only) + hook Job registering 12 subjects, compatibility BACKWARD |
| `akhq` (`32-akhq`) | kafka | `akhq/akhq` 0.28.0, only if `extras.akhq` |
| `lab-scrapes` (`41-kafka-scrape`) | monitoring | `monitoring/bundle/scrape/strimzi-vmpodscrape.yaml` (passed in as a file) + `lab-services` and `akhq` VMPodScrapes |
| `lab-dashboards` (`42-lab-dashboards`) | monitoring | one ConfigMap per dashboard JSON |

The VM stack is numbered **25** (not 40+) because helmfile runs sub-helmfiles in file order and the
VMPodScrape CRDs must exist before `30-postgres`/`31-schema-registry` apply their scrape objects.

### Service names (as in platform.md)
`vmsingle-vmks:8428`, `vmagent-vmks:8429`, `vmalert-vmks:8080`, `vmalertmanager-vmks:9093`, `grafana:80`
(`fullnameOverride: vmks` for the CRs, `grafana.fullnameOverride: grafana`), `postgres.data:5432`,
`schema-registry.data:8081`, `akhq.kafka:80`. Ingress (Traefik): grafana / vm / vmagent / vmalert /
alertmanager / akhq `.localhost:8080`.

## How things get into Grafana

- **Dashboards**: drop `*.json` into `monitoring/bundle/dashboards/` (folder "Kafka (bundle)") or
  `monitoring/demo/dashboards/` (folder "Kafka Lab") and run `make deploy`. `42-lab-dashboards.yaml.gotmpl`
  reads the directories with helmfile's `readDir`/`readFile` (no copy step), the chart renders one
  ConfigMap per file (label `grafana_dashboard=1`, annotation `grafana_folder`), the Grafana sidecar
  provisions it. Deleting a file removes the dashboard on the next deploy. Reference datasources by UID
  (`vm`, `vm-native`, ...). Folders appear once they contain a dashboard.
- **Datasources**: `vm` (prometheus, default), `vm-native` (`victoriametrics-metrics-datasource`,
  installed by Grafana at start → needs internet), `alertmanager` (implementation prometheus) come from
  the stack's ConfigMap `vmks-grafana-ds`. Extension point for `vlogs`/`traces`: ship a ConfigMap in
  `monitoring` labelled `grafana_datasource: "1"` with a `datasource.yaml` (provisioning format); the
  datasource sidecar reloads Grafana.
- Default dashboards are fetched by the chart's sync job from GitHub at deploy time (folder
  "Kubernetes & VictoriaMetrics"); Windows/AIX/macOS/multi-cluster/apiserver/Prometheus ones are disabled.

## Scraping

- Strimzi brokers: VMPodScrape `strimzi-kafka` (`strimzi.io/kind=Kafka`, `component-type=kafka`, port
  `tcp-prometheus`) → `kafka_cluster` from `strimzi.io/cluster`, `kafka_broker` = node id from the pod
  name suffix (also `kafka_pool`, `node`). Strimzi cluster operator (port `http`) and topic operator
  (port `healthcheck-to`) are scraped too. `strimzi-podmonitor.yaml` is the Prometheus-Operator twin (not deployed).
- Go services: VMPodScrape `lab-services`, `part-of=kafka-lab`, any namespace, port `http`, `/metrics`,
  `service` ← `app.kubernetes.io/name`. No double scrape: Strimzi rewrites `part-of` on its pods to
  `strimzi-kafka-lab`, and platform pods (AKHQ, Karapace, Grafana, Postgres) deliberately do not carry
  `part-of=kafka-lab` (AKHQ and Karapace have a port named `http`).
- Karapace exposes Prometheus metrics on `/metrics` (`karapace_schema_reader_*`, `http_requests_*`) —
  scraped. AKHQ `/prometheus` on port `management` — scraped. postgres_exporter port `metrics`.
- k3s: apiserver, controller-manager, scheduler, etcd, kube-proxy scrapes disabled; kubelet, cAdvisor,
  probes, node-exporter, kube-state-metrics, CoreDNS on. Interval 15s, retention 7d.

## Alerting

Alertmanager routes everything to `http://controlboard.control:8080/api/alerts/webhook`
(group_by alertname, group_wait 5s, group_interval 10s, repeat 1h, send_resolved). Exception:
`Alertmanager(Cluster)FailedToSendAlerts` go to a no-op `blackhole` receiver — "the webhook fails" cannot
be delivered through that webhook, and trying keeps the alert firing forever. Disabled default
rules (always/meaningless firing on single-node k3d): `Watchdog`, `InfoInhibitor`, `KubeCPUOvercommit`,
`KubeMemoryOvercommit`, `NodeClockNotSynchronising`, `NodeClockSkewDetected`,
`KubePersistentVolumeFillingUp`, `NodeFilesystemAlmostOutOfSpace`, `NodeFilesystemSpaceFillingUp`
(local-path PVs and the node fs are the host disk), group `kube-apiserver-availability.rules`.

Known, expected until other tickets land: `ScrapePoolHasNoTargets` for `lab-services` (after 30 min
without Go pods) and `AlertmanagerFailedToSendAlerts` (for ~15 min, its rate window) whenever something
fires while the controlboard does not exist yet.

## Deviations from platform.md

- **Grafana 512Mi instead of 256Mi.** Grafana 13.3 OOM-kills at 256Mi during start (Go heap ~170 MB
  plus the VictoriaMetrics plugin backend process; RSS ~375–440 MB). `GOMEMLIMIT=360MiB`. The two
  k8s-sidecar containers (dashboards, datasources) need ~75 MB each (limit 128Mi).
- vmagent/vmsingle/Grafana get `GOMAXPROCS=2` (no CPU limits → Go would size for all host cores;
  vmagent OOM-killed at 192Mi without it). vmagent also `-remoteWrite.queues=2`, `-memory.allowedPercent=40`.
- PostgreSQL image pinned to `postgres:18.6-alpine`; exporter adds 48Mi on top of PostgreSQL's 384Mi.
- Operator-managed pods (vmagent, vmalert, alertmanager) carry a config-reloader sidecar (~16–22 MB each)
  not listed in the resource table.

## Measured memory (working set, kubelet stats/summary, idle lab, no Go services yet)

| Component | Limit | Used |
|---|---|---|
| vmsingle | 512Mi | 351 MB |
| vmagent (+reloader) | 192Mi | 114 (+18) MB |
| vmalert (+reloader) | 96Mi | 46 (+22) MB |
| alertmanager (+reloader) | 64Mi | 32 (+16) MB |
| vm-operator | 160Mi | 55 MB |
| Grafana / 2 sidecars | 512Mi / 128Mi each | 366 / 75 + 72 MB |
| node-exporter / kube-state-metrics | 48Mi / 96Mi | 16 / 31 MB |
| PostgreSQL / exporter | 384Mi / 48Mi | 28 / 14 MB |
| Karapace | 192Mi | 104 MB |
| AKHQ (heap 256m) | 448Mi | 303 MB |

Command: `./kctl get --raw /api/v1/nodes/k3d-kafka-lab-server-0/proxy/stats/summary | jq ...`.
