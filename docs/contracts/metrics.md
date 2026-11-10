# Application metric contract (FROZEN)

Library: `github.com/VictoriaMetrics/metrics`. Histograms use VictoriaMetrics `vmrange` buckets
(query with `histogram_quantile` in MetricsQL or `histogram_quantiles` — demo dashboards only).
For PromQL-portable panels the `_sum`/`_count` pair is used. All metrics carry the scrape labels
`namespace`, `pod`, `service`.

## Kit (every consuming service)
| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `lab_records_consumed_total` | counter | `topic` | records polled |
| `lab_records_processed_total` | counter | `topic`, `result`=`ok\|duplicate\|dlq` | records finished |
| `lab_records_produced_total` | counter | `topic` | outcomes produced (acked) |
| `lab_processing_retries_total` | counter | `topic`, `reason`=`db\|produce\|transient\|registry` | retried batches |
| `lab_processing_duration_seconds` | histogram | `topic` | handler time per record |
| `lab_batch_duration_seconds` | histogram | `topic` | poll-to-commit per partition batch |
| `lab_event_age_seconds` | histogram | `topic` | now − `meta.occurredAt` at processing |
| `lab_consumer_paused` | gauge | – | 1 while the Silent Failure knob is on |
| `lab_control_generation` | gauge | – | applied control snapshot generation |
| `lab_control_knob` | gauge | `knob` | applied numeric knob values (non-default only) |
| `lab_client_rebuilds_total` | counter | – | Tuning Lab client rebuilds |
| `lab_partitions_assigned` | gauge | `topic` | partitions owned by this pod |
| `lab_rebalances_total` | counter | `kind`=`assigned\|revoked\|lost` | group membership changes |
| `lab_db_tx_duration_seconds` | histogram | – | transaction time |
| `lab_dlq_records_total` | counter | `topic`, `reason` | dead-lettered records |

## franz-go client (kit hooks, every service)
| Metric | Type | Labels |
|---|---|---|
| `kafka_client_connects_total` / `_connect_errors_total` / `_disconnects_total` | counter | `node` |
| `kafka_client_write_bytes_total` / `kafka_client_read_bytes_total` | counter | `node` |
| `kafka_client_request_duration_seconds` | histogram | `node` (write+read time per request) |
| `kafka_client_throttle_seconds_total` | counter | `node` |
| `kafka_client_produce_batches_total` / `_produce_records_total` / `_produce_bytes_total` | counter | `topic` |
| `kafka_client_produce_batch_records` | histogram | `topic` |
| `kafka_client_fetch_records_total` / `_fetch_bytes_total` | counter | `topic` |
| `kafka_client_buffered_produce_records` / `kafka_client_buffered_fetch_records` | gauge | – |

## Business (analytics service only — gauges are never exported by several replicas)
| Metric | Type | Labels |
|---|---|---|
| `lab_orders_created_total` | counter | `region`, `payment_method` |
| `lab_order_value_cents_total` | counter | `region` (revenue = shipped value, see below) |
| `lab_revenue_cents_total` | counter | `region` — value of shipped orders |
| `lab_payments_total` | counter | `result`=`completed\|failed`, `reason` |
| `lab_refunds_total` / `lab_refunded_cents_total` | counter | – |
| `lab_reservations_total` | counter | `result`=`reserved\|rejected`, `reason` |
| `lab_releases_total` | counter | – |
| `lab_orders_shipped_total` | counter | `carrier`, `warehouse` |
| `lab_orders_cancelled_total` | counter | `reason` |
| `lab_notifications_total` | counter | `channel` |
| `lab_order_completion_seconds` | histogram | `outcome`=`shipped\|cancelled` (now − orderCreatedAt) |
| `lab_dlq_total` | counter | `source_topic` |
| `lab_duplicates_total` | counter | `topic` |

Orders in flight = `created − shipped − cancelled` (PromQL, no gauge).

## Generator
| Metric | Type | Labels |
|---|---|---|
| `lab_generator_target_rate` | gauge | – |
| `lab_generator_orders_total` | counter | `kind`=`normal\|duplicate\|poison\|hot` |
| `lab_generator_lagging` | gauge | – (1 when the producer cannot keep up) |

## Service specifics
| Metric | Service | Type | Labels |
|---|---|---|---|
| `lab_stock_units` | inventory | gauge | `sku` (top 20 SKUs only; exported by partition owner 0 only) |
| `lab_join_open_orders` | shipping | gauge | – (rows waiting for the second half; per pod, sum is correct) |
| `lab_join_deadline_exceeded_total` | shipping | counter | – |

## Controlboard
| Metric | Type | Labels |
|---|---|---|
| `lab_experiment_active` | gauge | `experiment_id`, `scenario`, `category`, `severity` (scenario=`hidden` during an unsolved drill) |
| `lab_experiment_intensity` | gauge | `experiment_id` |
| `lab_experiments_total` | counter | `scenario`, `outcome`=`completed\|aborted\|failed` |
| `lab_experiment_recovery_seconds` | histogram | `scenario` |
| `lab_chaos_armed` | gauge | – |
| `lab_alerts_received_total` | counter | `status` |
| `lab_ws_clients` | gauge | – |
| `lab_drill_score` | gauge | – |
