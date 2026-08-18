# ADR 0030 - Verified Position Projection Snapshot Bootstrap and Gap Health

Status: Accepted

Date: 2026-08-18

## Context

ADR 0029 made `inventory.position.changed` a version-aware delta projection. Complete replay from aggregate version 1 is correct, but a retained stream may begin after earlier versions have expired. Treating the first retained delta as a complete state would create a plausible but false quantity view.

A separately verified authoritative snapshot can establish the quantity state and exact aggregate cursor without replaying every earlier delta. That operation must not pretend to be a Domain Event, must not create a fake Inbox receipt, and must never overwrite a cursor that event processing already initialized.

Version gaps are already queryable in PostgreSQL, but a raw row set is not enough for monitoring. Operators need one read-only bounded-cardinality health snapshot with stable alert codes.

## Decision

Add two operational contracts:

1. non-destructive bootstrap of one absent `(consumer_name, position_id)` projection cursor from a verified snapshot artifact;
2. read-only aggregate gap telemetry with configurable warning and critical thresholds.

Destructive replacement of an existing cursor, pending-event repair, and full projection rebuild remain a separate decision.

## Snapshot artifact

The import artifact contains exactly:

```text
position_id
aggregate_version
physical_qty
reserved_qty
allocated_qty
source
reference
recorded_at
```

Quantities are exact decimal strings or integers with at most six fractional digits. The importer records the SHA-256 of the exact artifact bytes. The checksum binds operator evidence to the file that was imported; it does not prove that the source system was truthful.

The operator must verify that the artifact represents the authoritative Position state at the stated aggregate version. Authentication and authorization of that operator remain deployment responsibilities.

## Provenance

The projection row preserves:

```text
bootstrap_id
bootstrap_version
bootstrap quantities
snapshot source and reference
snapshot artifact checksum
snapshot recorded_at
operator identity
operator reason
bootstrapped_at
```

Bootstrap quantities remain immutable provenance after later events advance the current quantities.

`cursor_source` is derived by PostgreSQL:

| Cursor state | Meaning |
|---|---|
| `empty` | version 0, no event and no snapshot |
| `snapshot` | current version and quantities came directly from the verified snapshot |
| `event` | at least one Domain Event advanced the current cursor |

A snapshot cursor has no `last_event_id`. Snapshot import is an administrative seed, not a transport delivery.

## Bootstrap identity and retry

`bootstrap_id` is a stable operator-command identity.

- first exact command inserts the snapshot and returns `bootstrapped`;
- an exact retry returns `duplicate`, even if later events already advanced the cursor;
- reuse of the same bootstrap identity with changed snapshot or operator fields fails closed;
- a different bootstrap against an initialized cursor fails closed;
- an event-created version-0 gap cursor is already initialized and cannot be overwritten.

The retained bootstrap quantities and provenance make retry payload comparison possible after the live projection has moved beyond the bootstrap version.

## Concurrency boundary

Bootstrap and event handling contend on the existing projection primary key.

If bootstrap commits first, the next contiguous event applies from the snapshot cursor. If an event creates the cursor first, bootstrap fails instead of replacing the event or pending-gap state. PostgreSQL uniqueness and row locking choose the winner without a global projection lock.

## Event behavior after snapshot

For snapshot cursor version `N`:

| Incoming event | Result |
|---|---|
| version `< N` | audited stale event |
| version `N` | audited stale event already represented by the snapshot |
| version `N + 1` | apply and change cursor source to `event` |
| version `> N + 1` | durably buffer and expose a gap |

The snapshot does not relax quantity constraints. A later event that would make physical, reserved, or allocated quantities invalid still rolls back with its Inbox receipt.

## Gap health

`read_inventory_position_projection_gap_telemetry()` aggregates the gap view into:

```text
gap_count
pending_event_count
max_pending_per_gap
oldest_gap_age_seconds
```

The snapshot may cover all consumers or one configured logical consumer. Prometheus labels contain only the optional consumer name plus stable alert code/severity. Position IDs, event IDs, payloads, and error text are excluded.

Default deployment starting points are:

| Signal | Warning | Critical |
|---|---:|---:|
| oldest unresolved gap | 300 seconds | 1,800 seconds |
| pending events | 100 | 1,000 |

These values are not kernel SLOs. A short-lived gap can be normal under at-least-once delivery; age distinguishes transient reordering from operational failure.

Telemetry is read-only. It cannot delete, skip, synthesize, or repair an event.

## Operator interfaces

`bin/bootstrap-kernel-position-projection` imports one artifact and emits JSON evidence. It has no replace flag.

`bin/inspect-kernel-projection-gaps` emits JSON or Prometheus exposition and can return monitoring exit codes: 0 for healthy, 1 for warning, and 2 for critical.

## Executable invariants

The PostgreSQL lab proves:

1. a verified snapshot creates exact quantities and cursor version without an Inbox receipt;
2. exact bootstrap retry is idempotent and payload-bound;
3. event version equal to the snapshot cursor is stale rather than a conflicting fake event identity;
4. version `N + 1` advances the snapshot cursor and preserves bootstrap provenance;
5. a post-snapshot gap buffers and drains through the existing ordered projection contract;
6. an existing event or gap cursor rejects snapshot overwrite;
7. a bootstrap/event race produces either a valid snapshot-first result or an untouched event-first gap;
8. gap telemetry aggregates multiple Positions without identity labels;
9. warning and critical policies are deterministic;
10. an empty gap set is healthy and emits no age sample.

## Consequences

### Positive

- retained event streams can seed a correct delta projection from trusted current state;
- snapshot provenance remains inspectable after later events;
- network retry cannot reset a progressed projection;
- event handling and bootstrap cannot silently overwrite one another;
- permanent gaps become independently monitorable without broker introspection;
- operator tooling remains broker-neutral.

### Costs and limits

- snapshot trust is an operational verification responsibility;
- exact artifact-byte checksums treat formatting changes as a different artifact;
- this slice cannot replace a live cursor or close an existing gap;
- automatic repair, consumer fencing, pending-event disposition, destructive rebuild, conflict quarantine, and Inbox cleanup remain later operational work;
- metric thresholds require deployment calibration.
