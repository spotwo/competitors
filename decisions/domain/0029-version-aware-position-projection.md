# ADR 0029 - Version-Aware Inventory Position Quantity Projection

Status: Accepted

Date: 2026-08-18

## Context

ADR 0028 made Inbox deduplication and the transport ACK boundary executable. It guarantees that one `event_id` produces one committed local handler transaction for one `consumer_name`.

That does not guarantee semantic event order. Multiple Outbox publishers may publish different rows concurrently, a retry may arrive late, and an administrative replay may interleave old and current facts. A projection that applies quantity deltas in broker arrival order can therefore produce the wrong result even when every individual event is deduplicated correctly.

`inventory.position.changed` already carries the required ordering identity:

```text
aggregate_type     InventoryPosition
aggregate_id       inventory_positions.id
aggregate_version  post-change inventory_positions.version
```

`inventory_positions.version` starts at zero and increments in the same authoritative transaction before its position event is inserted. The event payload carries physical, reserved, and allocated quantity deltas.

## Decision

Add a concrete `InventoryPositionQuantityProjector` with a PostgreSQL projection cursor and durable gap buffer.

The projection is keyed by:

```text
(consumer_name, position_id)
```

It materializes:

```text
aggregate_version
physical_qty
reserved_qty
allocated_qty
last_event_id
last_recorded_at
```

This is a derived quantity view. It is not the authoritative Inventory Position, journal, or an independently mutable availability bucket.

## Version policy

For projection cursor `V` and incoming event version `E`:

| Condition | Decision | Inbox transaction | Broker ACK |
|---|---|---|---|
| same `event_id` | Inbox duplicate, handler skipped | prior result remains | yes |
| `E < V` | record audited `stale` outcome, do not change quantities | commit receipt and decision | yes |
| `E = V`, different event ID | producer/version conflict | roll back | no |
| `E = V + 1` | apply delta and drain contiguous buffered versions | commit all effects | yes |
| `E > V + 1` | copy full delta fact to durable pending buffer | commit receipt and buffer | yes |

A gap event is ACKed only after its complete application payload is durably stored in PostgreSQL in the same transaction as the Inbox receipt. It is no longer dependent on broker redelivery. When the missing next version arrives, that transaction applies it and drains every contiguous buffered successor while holding the aggregate cursor lock.

The runtime result field `applied` still means the first Inbox handler transaction committed. The projection-specific outcome is stored under `domain_event_inbox.metadata.projection.status` as `applied`, `buffered`, or `stale`.

## Concurrency boundary

Every first event inserts a zero-version cursor row if absent. The handler then locks that `(consumer_name, position_id)` row with `FOR UPDATE`.

This gives one serialization lane per logical consumer and Inventory Position without globally serializing unrelated positions. Adjacent versions processed by different workers converge to the same cursor and quantities regardless of which transaction acquires the lock first.

## Conflict behavior

One aggregate version belongs to one semantic event. A different event ID claiming the current or already-buffered version is not treated as a harmless duplicate.

The transaction raises a uniqueness error, rolls back the new Inbox receipt, and leaves the broker message unacknowledged. Deployment `max_deliver`, advisories, and DLQ policy must make that conflict visible instead of retrying forever.

## Projection invariants

The materialized quantities preserve the authoritative Position constraints:

```text
physical_qty >= 0
reserved_qty >= 0
allocated_qty >= 0
reserved_qty <= physical_qty
allocated_qty <= physical_qty
```

An event that would violate them rolls back its Inbox receipt and projection changes. This catches missing history, corrupt deltas, and incompatible bootstrap state.

The Python handler also requires:

- event type `inventory.position.changed`;
- aggregate type `InventoryPosition`;
- canonical UUID aggregate and transaction identities;
- schema version 1;
- finite JSON numbers for all three deltas.

## Bootstrap and replay

Delta projection bootstrap must start from version 1 and replay every version, or begin from a separately verified snapshot with an explicit cursor.

This slice does not invent a snapshot from the first event it sees. If the first retained event is version 40, it remains buffered with expected version 1. Silently assuming versions 1 through 39 were zero would create a plausible but false quantity view.

Inbox receipts, pending events, and the projection cursor form one rebuild boundary. A restrictive foreign key blocks deletion of an Inbox receipt while its event is pending. Deleting only one part is unsafe. Destructive rebuild and snapshot-import tooling remain a separate operational slice.

## Gap observability

`inventory_position_projection_gaps` exposes:

```text
consumer_name
position_id
expected_version
first_pending_version
last_pending_version
pending_count
oldest_buffered_at
```

This makes unresolved holes queryable without interpreting broker state. Alert thresholds and automatic repair remain deployment policy.

## Executable invariants

The PostgreSQL and pinned NATS lab proves:

1. authoritative Position events use consecutive post-change versions;
2. sequential deltas apply exactly once;
3. an out-of-order event commits to the pending buffer before ACK;
4. missing versions drain the contiguous buffered tail;
5. late stale events are audited without changing quantities;
6. parallel adjacent versions serialize to the same final projection;
7. conflicting events for one version fail closed;
8. invalid or impossible deltas roll back Inbox and projection state;
9. real JetStream delivery can ACK version 2 from the durable buffer, then apply versions 1 and 2 when version 1 arrives.

## Consequences

### Positive

- projection correctness no longer depends on broker arrival order;
- a gap does not block unrelated aggregate delivery;
- ACKed gap events remain durable locally;
- per-position concurrency is serialized without a global lock;
- stale and buffered decisions remain attached to Inbox audit evidence;
- unresolved gaps are directly observable in PostgreSQL.

### Costs and limits

- pending payloads duplicate event data until their gaps close;
- a permanently missing version leaves later events buffered;
- delta replay requires complete history or a verified snapshot cursor;
- the projection covers Position quantities, not inventory identity dimensions, holds, eligibility, availability, or other read models;
- conflict quarantine, snapshot import, rebuild automation, gap alert thresholds, and pending-event retention remain later operational slices.
