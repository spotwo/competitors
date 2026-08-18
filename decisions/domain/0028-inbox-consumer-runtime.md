# ADR 0028 - Inbox Consumer Runtime and ACK Boundary

Status: Accepted

Date: 2026-08-18

## Context

ADR 0021 defined the consumer Inbox schema and required receipt plus local side effect to share one transaction. ADR 0024 proved JetStream pull delivery and redelivery, but only at the transport level. No runtime yet connected those two contracts.

That missing boundary is where at-least-once systems usually duplicate effects:

```text
message received
  -> side effect commits
  -> worker crashes before broker ACK
  -> message redelivers
  -> side effect runs again
```

The producer's stable `event_id` is necessary but insufficient. A real consumer must make the Inbox receipt and application handler atomic, then acknowledge the broker only after that database transaction commits.

## Decision

Add a broker-neutral `InboxConsumerRuntime`, a PostgreSQL `PostgresInboxStore`, and a real `NatsJetStreamPullSource`.

The runtime processes one delivery per call:

```text
fetch one delivery
  -> validate canonical envelope
  -> BEGIN PostgreSQL transaction
       insert (consumer_name, event_id) Inbox receipt
       first receipt? run handler in the same transaction
       duplicate? skip handler
     COMMIT
  -> JetStream ACK sync
```

One-message scope is intentional. It isolates failures, keeps the database transaction bounded, and makes the ACK boundary explicit. Horizontal throughput comes from multiple workers sharing one durable consumer and one logical `consumer_name`.

## Stable consumer identity

The Inbox primary key is:

```text
(consumer_name, event_id)
```

`consumer_name` is the logical idempotency namespace. It must remain stable across deployments and worker replicas. Renaming it creates a new logical consumer and allows every retained event to apply again.

The NATS durable name and `consumer_name` can differ, but deployment configuration should map them explicitly. Multiple process instances that share one durable NATS consumer must also share one `consumer_name` when they implement the same projection or side effect.

## PostgreSQL transaction contract

`PostgresInboxStore.process_once()` owns one connection and transaction. It calls `try_record_domain_event_receipt()` with bounded transport metadata. If the receipt is new, it invokes the handler with that same transaction object. Returning from the store means the context manager committed successfully.

Concurrent attempts for the same `(consumer_name, event_id)` serialize on the Inbox primary key. Exactly one transaction receives `true` and runs the handler. If that handler rolls back, another waiting transaction can insert the receipt and apply the event.

The handler may:

- update a local projection;
- append local audit state;
- append a downstream transactional Outbox row.

The handler must not treat an external HTTP call, email send, device command, or another nontransactional effect as exactly-once. If an external action is required, the handler writes a local Outbox command in the same transaction and a separate publisher performs the external call.

## Broker ACK boundary

The runtime calls `delivery.ack()` only after `PostgresInboxStore` returns from a committed transaction.

The NATS adapter uses `ack_sync`, which waits for the server to process the ACK. A successful runtime result therefore means both the database transaction and JetStream ACK were confirmed.

Failure behavior without the optional ADR 0032 durable failure lane is asymmetric by design:

| Failure point | Database state | Broker ACK | Next delivery |
|---|---|---|---|
| envelope validation | unchanged | absent | redelivery / broker poison policy |
| handler or database before commit | rolled back | absent | handler may run on redelivery |
| commit succeeds, ACK confirmation fails | receipt and effect committed | uncertain | duplicate receipt skips handler, then ACKs |
| duplicate receipt | unchanged except prior committed state | confirmed after skip | complete |

The base runtime does not NAK after a handler exception. Leaving the message unacknowledged preserves the durable consumer's configured `ack_wait`, backoff, `max_deliver`, and advisory/DLQ policy.

ADR 0032 adds an explicit optional path for a valid event: after the handler transaction rolls back, the complete envelope can be committed to a PostgreSQL `deferred` or `quarantined` row before broker ACK. If that handoff fails, no ACK occurs. Malformed envelopes remain outside that path because no trusted event identity exists yet.

## Canonical envelope validation

Before opening the consumer transaction, the runtime requires:

- UUID `event_id`;
- nonblank type, source, subject, aggregate type, and aggregate ID;
- timezone-aware occurred and recorded timestamps;
- positive aggregate and schema versions;
- presence of event data.

The NATS adapter additionally requires the canonical publication headers and verifies that message ID, event type, aggregate identity, and aggregate version match the JSON envelope. Invalid or mismatched messages are never ACKed.

Unknown additional envelope fields remain allowed for forward-compatible metadata evolution.

## Durable consumer ownership

`NatsJetStreamPullSource` binds only to a pre-provisioned durable consumer. It first calls `consumer_info()` and refuses to create a missing consumer.

Stream retention, filter subjects, explicit ACK policy, ACK wait, backoff, maximum deliveries, replicas, storage, and advisory/DLQ handling are deployment policy. Silently creating a consumer from application defaults would make correctness depend on which worker started first.

## Ordering boundary

Inbox deduplication prevents duplicate local effect for one event ID. It does not solve out-of-order aggregate delivery.

Handlers still use:

```text
aggregate_type
+ aggregate_id
+ aggregate_version
```

to reject stale versions, detect gaps, or serialize an aggregate-specific projection. Strict per-aggregate lanes and gap recovery remain a separate executable slice.

## Inbox retention

Inbox receipt retention is consumer-owned and independent of producer Outbox retention. A receipt must not be deleted while the same logical consumer can still receive or administratively replay that event. Deleting receipts earlier re-enables side effects by design.

## Executable invariants

The PostgreSQL and pinned NATS lab proves:

1. transport ACK happens only after the store reports commit;
2. base-runtime store or handler failure produces no ACK;
3. Inbox receipt and handler SQL commit together;
4. handler failure rolls back both receipt and partial SQL effect;
5. concurrent duplicate receipts run one handler transaction;
6. ACK-confirmation failure after commit causes JetStream redelivery without reapplying the handler;
7. base-runtime handler failure causes JetStream redelivery and a later successful transaction;
8. canonical NATS headers match the received envelope;
9. the source refuses to create a missing durable consumer;
10. invalid timestamps, versions, and delivery counts fail closed.

## Consequences

### Positive

- consumer-side retry safety is executable end to end;
- PostgreSQL commit is the single local-effect boundary;
- ACK uncertainty is safe because the durable receipt already exists;
- transport adapter and handler remain independently replaceable;
- NATS consumer policy remains explicit infrastructure configuration;
- multiple workers can share one consumer identity safely.

### Costs and limits

- handler code must stay inside the supplied local transaction boundary;
- slow handlers hold database transactions and must be optimized or decomposed;
- external effects require another local Outbox rather than direct calls;
- ADR 0032 failure deferral is opt-in and requires a separately operated retry worker;
- malformed-message termination, aggregate gap handling, strict ordering lanes, and Inbox cleanup remain later policies;
- the lab provides a runtime library rather than a misleading standalone process with a no-op handler.
