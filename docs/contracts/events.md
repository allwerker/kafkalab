# Event flow contract (FROZEN)

Schemas: `schemas/commerce.avdl`, `schemas/platform.avdl`. Wire format: Confluent
(magic byte + 4-byte schema id + Avro binary). Subject per topic: `<topic>-value`.
Schemas are registered by the `schema-registration` Job; services never auto-register.
Record key: `orderId` (UTF-8). Headers: `traceparent`, `tracestate` (W3C), `lab-producer`.

## Who consumes what
| Service (= consumer group) | Consumes | Produces |
|---|---|---|
| order-generator | – (reads `platform.control` only) | `orders.created` |
| payment | `orders.created`, `orders.cancelled` | `payments.completed` \| `payments.failed`, `payments.refunded` |
| inventory | `orders.created`, `orders.cancelled` | `inventory.reserved` \| `inventory.rejected`, `inventory.released` |
| shipping | `payments.completed`, `payments.failed`, `inventory.reserved`, `inventory.rejected` | `orders.shipped` \| `orders.cancelled` |
| notification | `orders.created`, `orders.shipped`, `orders.cancelled`, `payments.refunded` | `notifications.sent` |
| analytics | all business topics + `orders.dlq` | – (metrics only) |
| controlboard | all business topics (no group, sampled) | `platform.control`, `platform.audit` |

Every service reads `platform.control` with its own non-group client.

## Event ids
- `orders.created`: random UUID v4 (duplicates from the Duplicate Delivery scenario keep the same id).
- Outcome of one input: `DeriveEventID(cause.eventId, "<outcome>")` (`pkg/contracts`).
- Terminal shipping decisions: `DeriveEventID(orderId, "shipped"|"cancelled")` — one per order even
  if two shipping pods race.
- Deadline cancellations: same rule, cause id `deadline:<orderId>` in `causationId`.

## Delivery semantics (kit runner)
1. Poll. Per partition batch: one PostgreSQL transaction.
2. For each record: decode (failure → DLQ, see below), then
   `INSERT INTO processed_events(event_id, consumer) ... ON CONFLICT DO NOTHING`.
   Conflict → duplicate: re-publish the outputs stored for that event (they are deterministic).
   New → call the handler inside the same transaction; outputs are stored with the event row.
3. Commit the transaction, produce all outputs synchronously (acks=all, idempotent producer).
4. Mark and commit offsets only after every output was acknowledged.
5. Infrastructure errors (DB, produce, registry, handler `Transient`) → roll back, back off,
   retry the same batch forever (lag grows; nothing is dead-lettered). Handler `Permanent`
   errors and undecodable records → `orders.dlq` with the original bytes and headers
   `lab-dlq-source-topic`, `-partition`, `-offset`, `-reason`, `-error`, `-consumer`.

## Per-order state machines (one row per order per service, row lock via upsert)
Arrival order of inputs is arbitrary (different topics, different partition counts).

**payment** — `none → charged | declined`, `charged → refunded`, `none → cancelled_early`
- `orders.created`: none → authorise → charged (`payments.completed`) or declined (`payments.failed`);
  cancelled_early → no charge, no output.
- `orders.cancelled`: charged → refunded (`payments.refunded`); none → cancelled_early; else no-op.

**inventory** — `none → reserved | rejected`, `reserved → released`, `none → cancelled_early`
- `orders.created`: reserve all lines atomically (`stock` rows locked in SKU order) or reject.
- `orders.cancelled`: reserved → released (stock returned); none → cancelled_early; else no-op.
- Restock: every SKU below 20% capacity is refilled by a periodic job (`lab_stock_units`).

**shipping** — per order: `payment ∈ {unknown, ok, failed}`, `stock ∈ {unknown, ok, failed}`, `status`
- `status=open` until both are known or the deadline (default 300 s after first sight) passes.
- ok+ok → `shipped` (or `ready` while the warehouseOutage knob is on; a sweeper ships later).
- any failed → `cancelled` immediately with reason `PAYMENT_FAILED` / `OUT_OF_STOCK` (`BOTH_FAILED` only if both failures are already known at that moment),
  `refundRequired = payment==ok`, `releaseRequired = stock==ok`.
- deadline → `cancelled` `DEADLINE_EXCEEDED`, flags = what is known to be ok.
- After a terminal status every further input is a no-op.

**notification** — one notification per input event (channel by customer hash), templates
`order-confirmed`, `order-shipped`, `order-cancelled`, `payment-refunded`.

**analytics** — counts each event id once (processed_events), exports business metrics.
