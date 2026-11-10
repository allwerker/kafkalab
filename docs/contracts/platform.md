# Platform contract (FROZEN)

## Namespaces
| Namespace | Content |
|---|---|
| `kafka` | Strimzi operator, Kafka cluster `kafka-lab`, KMinion, AKHQ |
| `data` | PostgreSQL `postgres` (+ exporter sidecar), Karapace `schema-registry` |
| `monitoring` | victoria-metrics-k8s-stack (vmsingle, vmagent, vmalert, alertmanager, grafana, exporters), VictoriaLogs, VictoriaTraces |
| `commerce` | order-generator, payment, inventory, shipping, notification, analytics |
| `control` | controlboard |

## In-cluster endpoints
| What | Address |
|---|---|
| Kafka bootstrap | `kafka-lab-kafka-bootstrap.kafka:9092` (plain) |
| Broker pods | `kafka-lab-dual-role-{0,1,2}` in `kafka`, JMX exporter on `:9404/metrics` |
| Schema registry | `http://schema-registry.data:8081` |
| PostgreSQL | `postgres.data:5432`, database `lab`, one schema + role per service (role = schema = service name, password in Secret `postgres-credentials` key `<service>`) |
| VictoriaMetrics (query) | `http://vmsingle-vmks.monitoring:8428` (Prometheus API under `/api/v1/...`; prefix `/prometheus` also accepted) |
| vmalert | `http://vmalert-vmks.monitoring:8080` |
| Alertmanager | `http://vmalertmanager-vmks.monitoring:9093` |
| Grafana | `http://grafana.monitoring:80` |
| VictoriaLogs | `http://victoria-logs.monitoring:9428` |
| VictoriaTraces (OTLP HTTP) | `http://victoria-traces.monitoring:10428/insert/opentelemetry/v1/traces` |
| Controlboard | `http://controlboard.control:8080` (alert webhook: `POST /api/alerts/webhook`) |

Release names are chosen so these service names come out exactly (VM stack release `vmks`, `fullnameOverride` where needed).

## Ingress (Traefik, host port 127.0.0.1:8080)
| URL | Target |
|---|---|
| `http://board.localhost:8080` | controlboard |
| `http://grafana.localhost:8080` | Grafana (anonymous Viewer + admin/admin, embedding allowed) |
| `http://akhq.localhost:8080` | AKHQ |
| `http://vm.localhost:8080/vmui` | VictoriaMetrics UI |
| `http://vmagent.localhost:8080/targets` | vmagent targets |
| `http://vmalert.localhost:8080` | vmalert |
| `http://alertmanager.localhost:8080` | Alertmanager |
| `http://logs.localhost:8080/select/vmui` | VictoriaLogs UI |
| `http://traces.localhost:8080` | VictoriaTraces (Jaeger UI API) |

## Ports inside every Go pod
| Port | Use |
|---|---|
| `8080` | HTTP: `/metrics`, `/healthz` (liveness), `/readyz` (readiness), service-specific API |

## Labels
- Every lab pod: `app.kubernetes.io/name=<service>`, `app.kubernetes.io/part-of=kafka-lab`,
  `lab.kafka/tier=<commerce|control|platform>`.
- Scrape: Go services are scraped through a `VMPodScrape` selecting `app.kubernetes.io/part-of=kafka-lab`
  and port name `http`. Resulting labels: `namespace`, `pod`, `service` (= app.kubernetes.io/name).
- Bundle label contract: `kafka_cluster` (cluster name), `kafka_broker` (node id). The demo sets
  `kafka_cluster="kafka-lab"`.

## Environment variables (Go services)
| Var | Default | Meaning |
|---|---|---|
| `SERVICE_NAME` | binary name | service id, e.g. `payment` |
| `KAFKA_BROKERS` | `kafka-lab-kafka-bootstrap.kafka:9092` | comma separated |
| `SCHEMA_REGISTRY_URL` | `http://schema-registry.data:8081` | |
| `DATABASE_URL` | – | `postgres://<svc>:<pw>@postgres.data:5432/lab?search_path=<svc>` |
| `OTLP_TRACES_URL` | empty = tracing off | |
| `TRACE_ROOT_RATE` | `2` | generator: root traces per second |
| `LOG_LEVEL` | `info` | |
| `HTTP_ADDR` | `:8080` | |
| `GOMEMLIMIT` | set by chart to 80% of limit | |

## Resource table (requests = limits for memory)
| Component | Memory |
|---|---|
| Kafka node ×3 | 1Gi (heap 512m) |
| Strimzi operator / entity operator | 384Mi / 320Mi |
| KMinion | 96Mi |
| Karapace | 192Mi |
| PostgreSQL | 384Mi |
| AKHQ | 448Mi (heap 256m) |
| vmsingle / vmagent / vmalert / alertmanager | 512Mi / 192Mi / 96Mi / 64Mi |
| Grafana | 256Mi |
| node-exporter / kube-state-metrics | 48Mi / 96Mi |
| VictoriaLogs / collector | 256Mi / 96Mi |
| VictoriaTraces | 256Mi |
| each Go service | 64Mi (order-generator, analytics 96Mi) |
| controlboard | 128Mi |

## Grafana
- Datasource UIDs: `vm` (type `prometheus`, default), `vm-native` (VictoriaMetrics plugin, MetricsQL),
  `vlogs` (VictoriaLogs), `traces` (Jaeger type → VictoriaTraces), `alertmanager`.
- Dashboard UIDs: `kafka-overview`, `kafka-broker`, `kafka-kraft`, `kafka-topics`, `kafka-lag`,
  `kafka-e2e`, `kafka-jvm`, `kafka-pods`, `lab-home`, `lab-business`, `lab-services`, `lab-chaos`,
  `lab-footprint`, `lab-monitoring`, `lab-postgres`, `lab-logs`.
- Folders: `Kafka (bundle)`, `Kafka Lab`.
- Chaos annotations: metric `lab_experiment_active` (see metrics.md) and Grafana annotation API.

## Alerts
Labels: `severity` = `critical|warning|info`, `component` = `kafka|consumer|business|platform|chaos`,
`dashboard` = dashboard UID, annotation `runbook_url`, `summary`, `description`.
Alertmanager routes everything to `http://controlboard.control:8080/api/alerts/webhook`
(group_wait 5s, group_interval 10s, repeat 1h, send_resolved true).
