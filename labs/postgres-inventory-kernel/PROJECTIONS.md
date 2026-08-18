# Version-Aware Projections

Executable companion to ADR 0029 and ADR 0030.

## Concrete projection

`InventoryPositionQuantityProjector` consumes `inventory.position.changed` schema version 1 and materializes ordered Position quantities:

```text
physical_qty
reserved_qty
allocated_qty
aggregate_version
```

It is a read model, not authoritative inventory state and not a complete availability projection.

## Runtime composition

Use the same stable logical identity for the runtime and projector:

```python
from consumer_runtime import InboxConsumerRuntime, PostgresInboxStore
from nats_consumer import NatsJetStreamPullSource
from position_projection import InventoryPositionQuantityProjector


consumer_name = "inventory-position-quantity-projector"

with NatsJetStreamPullSource(
    server_url="nats://127.0.0.1:4222",
    stream_name="WMS_EVENTS",
    durable_name="INVENTORY_POSITION_QUANTITY_PROJECTOR",
) as source:
    runtime = InboxConsumerRuntime(
        source=source,
        store=PostgresInboxStore(database_url),
        consumer_name=consumer_name,
        handler=InventoryPositionQuantityProjector(
            consumer_name=consumer_name,
        ),
    )
    runtime.run_once()
```

A different runtime and handler `consumer_name` fails the transaction because the projector requires its Inbox receipt in the same namespace.

## Storage model

| Object | Purpose |
|---|---|
| `inventory_position_quantity_projection` | current quantities and version cursor per consumer and Position |
| `inventory_position_projection_pending` | complete out-of-order delta events waiting for missing versions |
| `inventory_position_projection_gaps` | queryable unresolved-gap summary |
| `domain_event_inbox.metadata.projection` | per-delivery applied, buffered, or stale decision evidence |

Snapshot-seeded rows additionally preserve the bootstrap identity, exact seed quantities, artifact checksum, source/reference, operator, reason, and timestamps. PostgreSQL derives `cursor_source` as `empty`, `snapshot`, or `event`; a snapshot never receives a fake `last_event_id`.

## Processing policy

Assume the current projection version is 7:

| Incoming version | Result |
|---|---|
| 6 | commit an audited stale decision, no quantity mutation |
| 7 with another event ID | conflict, rollback, no ACK |
| 8 | apply, then drain buffered 9, 10, and later contiguous versions |
| 10 while 8 is missing | durably buffer version 10, then ACK |

The generic `ConsumeCycleResult.applied` field means a first Inbox handler transaction committed. For a gap, that committed handler effect is the durable buffer insert. Read projection-specific status from Inbox metadata or the projection tables.

## Gap inspection

```sql
SELECT
  consumer_name,
  position_id,
  expected_version,
  first_pending_version,
  last_pending_version,
  pending_count,
  oldest_buffered_at
FROM kernel_lab.inventory_position_projection_gaps
ORDER BY oldest_buffered_at;
```

No row means no currently buffered gap for that Position and consumer. It does not prove the consumer is caught up with the broker.

Use the read-only health interface for monitoring:

```bash
KERNEL_LAB_DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55432/kernel_lab \
  bin/inspect-kernel-projection-gaps \
  --consumer-name inventory-position-quantity-projector \
  --format prometheus --check
```

The default policy warns when the oldest gap reaches 300 seconds or pending events reach 100, and becomes critical at 1,800 seconds or 1,000 pending events. These are deployment starting points, not kernel SLOs.

## Inbox outcome inspection

```sql
SELECT
  event_id,
  first_seen_at,
  metadata -> 'projection' AS projection
FROM kernel_lab.domain_event_inbox
WHERE consumer_name = 'inventory-position-quantity-projector'
ORDER BY first_seen_at DESC;
```

Buffered metadata is advanced to `applied` when a later transaction drains that event. The original `buffered_at` and `expected_version` evidence remains, and `drained_by_event_id` identifies the event whose arrival closed the contiguous gap.

## Bootstrap rule

This is a delta projection. Start with one of two controlled modes:

1. empty cursor plus complete replay beginning at aggregate version 1;
2. verified quantity snapshot plus its exact aggregate version cursor.

The lab implements both modes. It intentionally does not create a fake zero baseline at an arbitrary retained version.

Snapshot bootstrap accepts exactly one verified JSON artifact:

```json
{
  "position_id": "018f0000-0000-7000-8000-000000000001",
  "aggregate_version": 40,
  "physical_qty": "100.000000",
  "reserved_qty": "20.000000",
  "allocated_qty": "10.000000",
  "source": "authoritative-inventory-export",
  "reference": "snapshot-2026-08-18T20:00:00Z",
  "recorded_at": "2026-08-18T20:00:00+00:00"
}
```

Import it with a stable bootstrap command identity:

```bash
KERNEL_LAB_DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55432/kernel_lab \
  bin/bootstrap-kernel-position-projection position-snapshot.json \
  --bootstrap-id 018f0000-0000-7000-8000-000000000002 \
  --consumer-name inventory-position-quantity-projector \
  --operator operator@example.com \
  --reason "initial projection seed"
```

The importer stores the SHA-256 of the exact file bytes. An exact retry returns `duplicate`; changed input under the same bootstrap ID fails. The command has no replace flag and rejects a cursor already created by either an event or another bootstrap.

After a version-40 snapshot, event version 40 is audited as stale, version 41 applies, and a higher version remains durably buffered until version 41 arrives.

## Failure behavior

### Invalid event contract

Wrong event type, aggregate type, UUID identity, schema version, or delta shape raises before projection SQL. Inbox receipt rolls back and the broker is not ACKed.

### Quantity invariant violation

Negative quantities or commitments exceeding physical quantity fail the PostgreSQL check constraint. Inbox, cursor, drained pending rows, and quantity changes all roll back together.

### Version conflict

A different event claiming the current or an already-buffered version raises a uniqueness error. It is a producer or replay-contract defect, not a stale fact to ignore.

### Permanent gap

Later versions remain in the pending table. Monitor `oldest_buffered_at` or the aggregate health command; do not delete them to make a dashboard green. Repair requires the missing event or a separately fenced and audited rebuild. Non-destructive bootstrap cannot overwrite the existing gap cursor.

## Retention boundary

Pending events depend on their Inbox receipts through a restrictive foreign key, so Inbox cleanup cannot silently delete an unresolved gap payload. Projection rebuild must treat these as one unit:

```text
Inbox receipts
+ pending gap events
+ projection cursor and quantities
```

Producer Outbox retention does not define when these consumer records are safe to remove.

## Executable coverage

`tests/test_position_projection.py` proves producer versions, sequential application, buffering, draining, stale audit, conflict rejection, invariant rollback, and concurrent adjacent versions.

`tests/test_nats_consumer.py` additionally proves the out-of-order flow through a real pinned JetStream server and server-confirmed ACKs.

`tests/test_projection_operations.py` proves verified snapshot import, payload-bound retry, post-snapshot ordering, bootstrap/event races, gap telemetry, and stable health alerts.
