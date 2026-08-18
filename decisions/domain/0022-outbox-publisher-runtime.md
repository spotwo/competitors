# ADR 0022 - Outbox Publisher Runtime and Transport Boundary

Status: Accepted

Date: 2026-08-18

## Context

ADR 0021 made Domain Event persistence and delivery state durable inside PostgreSQL. The next boundary is the process that moves committed Outbox rows to an external transport.

The publisher must preserve the transactional guarantees already established by the kernel without coupling inventory posting code to Kafka, NATS, SQS, Cloudflare Queues, webhooks, or another broker.

A dangerous implementation would claim rows inside a database transaction and keep that transaction open while performing network I/O. Broker latency or outage would then extend database row-lock lifetime and couple warehouse transaction health to transport health.

Another dangerous implementation would treat transport success as exactly-once delivery. A publish may be accepted by the transport while the publisher crashes or loses its database ACK, which necessarily permits redelivery.

## Decision

Introduce a broker-neutral Publisher Runtime with a narrow Transport adapter contract.

```text
PostgreSQL Outbox
      |
      v
claim + lease
      |
    COMMIT
      |
      v
Transport.publish(event)
   /             \
success         failure
  |                |
 ACK              NACK
  |                |
COMMIT           COMMIT
```

### 1. Claim is committed before network I/O

The runtime MUST finish the claim transaction before calling a Transport adapter.

The publisher therefore never intentionally holds an Outbox row lock while waiting for an external broker or endpoint.

### 2. Transport is an adapter boundary

The runtime depends on this semantic operation:

```text
publish(ClaimedEvent) -> confirmed transport acceptance OR exception
```

A Transport adapter MUST return success only when it has enough evidence, according to that transport's contract, to treat the message as accepted for delivery.

Transport-specific configuration, authentication, batching, partitioning, acknowledgement modes, and retry semantics stay outside inventory posting code.

### 3. Delivery remains at-least-once

A successful transport publish followed by publisher crash, database outage, ACK failure, or lease expiry can cause the same `event_id` to be published again.

This is expected behavior, not an error in the transactional outbox model.

```text
stable event_id + consumer Inbox dedupe = retry-safe delivery effects
```

The runtime MUST NOT generate a new Domain Event identity for a retry.

### 4. Lease token fences stale workers

ACK and NACK continue to require the claim token created by the current lease.

A worker whose lease expired MUST NOT be able to mutate a later worker's claim.

If publish succeeded but ACK is stale, the runtime records that condition operationally and leaves the row eligible for later redelivery.

### 5. No global event-order guarantee

Outbox claim order is a scheduling preference, not a global delivery-order contract.

Multiple workers can claim disjoint rows and transports can complete publishes at different times.

Consumers MUST NOT depend on one global order.

The envelope already carries:

```text
event_id
aggregate_type
aggregate_id
aggregate_version
```

Consumers that maintain aggregate projections should use `aggregate_version` to detect stale, duplicate, or out-of-order facts.

A future transport adapter may additionally map aggregate identity to a broker partition/key when the transport supports ordered streams.

### 6. Bounded batches provide backpressure

Each runtime cycle claims at most a configured batch size. The publisher does not load the entire Outbox into memory and does not create an unbounded in-process queue.

Horizontal scaling is achieved with multiple workers and PostgreSQL `FOR UPDATE SKIP LOCKED` claim semantics.

### 7. Runtime failure policy

For the current lab runtime:

- confirmed publish -> ACK;
- publish exception -> NACK with a bounded configured retry delay;
- ACK database failure is not treated as publish failure and may cause later redelivery;
- NACK database failure is surfaced rather than silently discarding the event;
- one event publish failure does not prevent the runtime from attempting the remainder of the already-claimed batch.

Exponential backoff, jitter, poison-message thresholds, dead-letter state, retention, and broker-specific bulk publish are future policy layers rather than hidden assumptions in this slice.

### 8. Development transport

The executable lab provides:

- `InMemoryTransport` for deterministic contract tests;
- JSON-lines stdout transport through `bin/run-kernel-outbox-publisher` for manual inspection and process-level experiments.

The stdout adapter is development evidence, not a production broker recommendation.

## Executable invariants

The PostgreSQL kernel lab must prove:

1. a claimed batch is published and ACKed;
2. a transport exception NACKs the row and retries the same `event_id`;
3. the claim transaction is committed before `Transport.publish()` starts;
4. successful publish plus expired lease produces a stale ACK and safe redelivery;
5. two publisher workers claim disjoint batches;
6. the Transport receives the stable canonical Domain Event envelope.

## Consequences

### Positive

- broker choice stays replaceable;
- warehouse posting latency is not coupled to broker network latency;
- multiple publisher workers can scale horizontally;
- failure semantics are explicit and executable;
- transport retries preserve Domain Event identity;
- the same runtime contract can support NATS, Kafka/Redpanda, SQS/SNS, Cloudflare Queues, HTTP/webhook delivery, or another adapter.

### Costs

- transport delivery is at-least-once, so consumers require idempotency/version handling;
- durable ACK/NACK writes add database traffic;
- strict global ordering is intentionally not provided;
- poison-message/dead-letter and retention policy remain separate work.

## Follow-up candidates

1. Compare concrete transport adapters for the WMS workload.
2. Add exponential backoff + jitter and poison-message/dead-letter policy.
3. Add Outbox retention/archival and operational backlog metrics.
4. Add OpenTelemetry spans/metrics around claim, publish, ACK/NACK, retry age, and backlog depth.
5. Add broker-specific aggregate partition/key mapping where useful.
