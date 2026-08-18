# ADR 0021 - Transactional Outbox and Domain Event Delivery

Status: Accepted

Date: 2026-08-18

## Context

WMS Kernel v0.1 freezes the semantic separation:

```text
InventoryTransaction != DomainEvent != EPCISEvent
```

ADR 0013 already requires Domain Events to be persisted atomically with authoritative inventory changes and delivered at least once. Until this ADR, that boundary was conceptual: the PostgreSQL kernel proved ledger, position, commitment, hold, Work, Golden Scenario, and contention behavior, but did not persist or deliver an executable outbox.

Publishing directly to a broker from a posting function would create a dual-write failure mode:

```text
DB COMMIT succeeds / broker publish fails
or
broker publish succeeds / DB COMMIT fails
```

Either outcome can make downstream systems disagree with the authoritative inventory kernel.

## Decision

Add a PostgreSQL Transactional Outbox as the first post-v0.1 / v0.2 kernel slice.

```text
Domain command
     |
     v
Inventory / Work transaction
     |
     +--> authoritative rows
     +--> InventoryTransaction + Legs
     +--> DomainEvent Outbox
     |
     v
  DB COMMIT
     |
     v
Publisher claim
     |
     v
Broker / transport
     |
     +--> ACK -> published_at
     |
     +--> NACK / lease expiry -> retry same event_id
```

The database transaction is the atomicity boundary. There is no network publish inside the authoritative posting transaction.

## Event production boundary

Inventory Domain Events are derived from durable kernel facts rather than duplicated application branching.

### InventoryTransaction trigger

Every inserted `InventoryTransaction` creates exactly one:

```text
inventory.transaction.posted
```

Selected transaction types also create semantic events from the existing registry:

```text
reservation          -> inventory.reservation.created
reservation_release  -> inventory.reservation.released
allocation           -> inventory.allocation.created
allocation_release   -> inventory.allocation.released
movement             -> inventory.movement.confirmed
count_reconciliation -> inventory.reconciliation.posted
```

`inventory.allocation.created` is emitted only for the first-class Allocation aggregate created by `allocate_inventory_commitment()`. The lower-level position allocation primitive does not pretend that a first-class Allocation object exists.

### InventoryTransactionLeg trigger

Every inserted transaction leg creates one:

```text
inventory.position.changed
```

The event carries the transaction identity, transaction type, position identity, and physical/reserved/allocated deltas. Its aggregate version is the current `InventoryPosition.version` after the accepted write.

### Why an AFTER INSERT trigger is still a committed fact

The trigger executes before the surrounding database transaction commits, but the outbox row is not externally visible as a committed event until that same transaction commits. If any later invariant or confirmation in the transaction fails, PostgreSQL rolls back both authoritative state and every outbox row.

Therefore `posted` means **committed visibility**, not "the trigger happened after every statement in the function".

## Event identity and deduplication

`event_id` is generated once with PostgreSQL 18 `uuidv7()` when the outbox row is first created.

Every event also has a tenant-scoped semantic `dedup_key`.

```text
UNIQUE (tenant_id, dedup_key)
```

An exact duplicate enqueue returns the existing `event_id`. Reuse of the same dedup key for a different semantic payload is rejected with a uniqueness error instead of silently accepting conflicting event content.

Posting-command idempotency and event idempotency are separate layers:

```text
command idempotency
  -> prevents duplicate authoritative posting

event deduplication
  -> prevents duplicate semantic outbox records
```

## Publisher lease protocol

Publishers use:

```text
claim_domain_events(worker, limit, lease_seconds)
ack_domain_event(event_id, claim_token)
nack_domain_event(event_id, claim_token, error, retry_delay)
```

Claiming uses:

```sql
FOR UPDATE SKIP LOCKED
```

Multiple publisher workers can therefore claim disjoint ready rows without a global publisher lock.

A claim records:

```text
claimed_by
claim_token
claimed_until
attempt_count
```

The claim token is a fencing token for that delivery attempt. An expired or superseded worker cannot ACK an event after another worker has reclaimed it.

`NACK` preserves the same stable `event_id`, records the last delivery error, clears the lease, and moves `available_at` according to retry delay.

An event is retained after successful publication with `published_at`; retention/compaction is a separate policy.

## Delivery guarantee

The outbox provides:

```text
DB side: exactly one durable semantic outbox record per dedup key
transport side: at-least-once delivery
```

It does **not** claim exactly-once message delivery across a broker and arbitrary consumers.

A crash after broker acceptance but before `ack_domain_event()` can cause the same `event_id` to be published again after lease expiry. Consumers must therefore be idempotent.

## Consumer Inbox contract

The lab adds:

```text
domain_event_inbox
PRIMARY KEY (consumer_name, event_id)
```

and:

```text
try_record_domain_event_receipt(consumer_name, event_id)
```

The intended consumer transaction is:

```text
BEGIN
  INSERT Inbox receipt
    conflict? -> already processed, skip side effect
  apply local side effect / projection
COMMIT
```

The Inbox receipt and the consumer's local side effect must share the consumer's own database transaction when exactly-once local effect is required. Recording a receipt in one transaction and applying the side effect later would reintroduce a crash gap.

The Inbox table intentionally has no foreign key to Spotwo's local outbox because consumers may receive events from another service or retained broker after the producer has compacted its delivery rows.

## Envelope

The publisher returns the ADR 0013 event envelope as JSON:

```text
event_id
type
source
subject
occurred_at
recorded_at
aggregate_type
aggregate_id
aggregate_version
causation_id?
correlation_id?
actor?
warehouse_id?
schema_version
data
```

CloudEvents remains a transport mapping candidate. The internal outbox is not itself an EPCIS event store.

## Concurrency and failure invariants

The executable lab must prove:

1. a failed posting leaves neither an InventoryTransaction nor an outbox event;
2. an exact posting retry does not duplicate Domain Events;
3. event dedup keys are payload-bound and return a stable event ID for exact retries;
4. a movement creates one movement event and position-delta events for both affected positions;
5. low-level position allocation does not falsely emit a first-class Allocation-created event;
6. parallel publishers claim disjoint rows;
7. a stale claim token cannot ACK a later claim;
8. NACK/retry preserves event identity and increments delivery attempts;
9. consumer Inbox identity is `(consumer_name, event_id)` so separate consumers remain independent.

## Deliberate non-goals

This slice does not yet define:

- Kafka, NATS, SQS, SNS, RabbitMQ, webhook, or another concrete transport adapter;
- a daemon/process implementation for the publisher loop;
- broker-specific partition keys;
- global event ordering;
- exactly-once transport delivery;
- event archive / replay retention policy;
- schema registry compatibility automation;
- PII classification and field-level redaction rules;
- CloudEvents wire encoding;
- EPCIS transformation and publication;
- Warehouse Work lifecycle event types beyond the existing inventory registry.

Those are higher layers over the transactional producer/delivery contract proven here.

## Consequences

Positive:

- authoritative state and event intent cannot diverge through a DB/broker dual write;
- existing posting code gains events from durable transaction/leg facts instead of duplicate branching;
- publisher workers scale horizontally with `SKIP LOCKED`;
- retry preserves event identity;
- consumer deduplication becomes executable rather than advisory prose;
- the same contract can later feed broker adapters, webhooks, internal projectors, analytics, and integration gateways.

Tradeoffs:

- every accepted inventory transaction now adds outbox write amplification;
- one InventoryTransaction may produce multiple Domain Events;
- published delivery rows need retention/compaction policy;
- at-least-once semantics move idempotency responsibility to consumers;
- transaction/leg triggers are an infrastructure coupling that must remain intentionally small and side-effect-free beyond local durable writes.

## Evolution

This ADR does not change a frozen v0.1 semantic boundary. It makes the already-frozen boundary `InventoryTransaction != DomainEvent` executable.

A later WMS Kernel v0.2 freeze may include this contract after transport, retention, replay, and broader Work-event questions are resolved.
