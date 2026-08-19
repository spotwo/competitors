# ADR 0064: Prove durable external webhook delivery

Status: Accepted
Date: 2026-08-19

## Context

ADR 0063 narrowed the public asynchronous integration trial to CloudEvents 1.0 structured JSON delivered through signed HTTPS webhooks and described with AsyncAPI 3.1.0. It deliberately left the architecture boundary unresolved because a static contract cannot prove delivery durability, crash recovery, replay, or worker coordination.

The next question is whether webhook delivery can be operated as a durable subsystem without leaking internal NATS JetStream topology into the customer contract and without placing customer availability inside a warehouse domain transaction.

## Decision

Implement a PostgreSQL-backed delivery journal in the existing kernel lab with three durable records:

1. external event subscriptions;
2. external event delivery generations;
3. immutable-in-order delivery attempt history.

The public CloudEvents `id` remains stable across ordinary retries and explicit replay. A separate `delivery_id` identifies one operational delivery generation. `(subscription_id, event_id, generation)` is unique.

The initial generation is idempotent. Attempting to reuse an existing public event identity with a different envelope is rejected as an identity conflict.

## Worker coordination

Workers claim eligible rows using PostgreSQL `FOR UPDATE SKIP LOCKED` and assign a bounded lease in the same transaction that increments the attempt number and inserts the attempt journal row.

A worker may only complete the delivery while it owns the current unexpired lease and current attempt number. If the lease expires, another worker can recover the same durable delivery. Recovery closes the abandoned attempt as `lease_expired` before creating the next attempt. The stale worker is fenced from later completion.

This coordination prevents two active workers from owning the same delivery attempt while preserving the public at-least-once contract. It does not claim exactly-once HTTP delivery.

## Retry and dead-letter policy

The lab keeps the HTTP classifier from ADR 0063:

- 2xx is success;
- 408, 425, 429, and 5xx are retryable;
- other 4xx responses are terminal;
- network failures without an HTTP status are retryable.

Retry scheduling is deterministic and bounded. Each subscription has a maximum attempt count. A retryable failure at the final attempt becomes `dead_letter` rather than retrying forever.

Dead-letter ownership remains with Spotwo. Failed deliveries are not silently discarded and customer endpoints are not treated as the owner of Spotwo delivery diagnostics.

## Replay

Replay is explicit and operator initiated. Only terminal deliveries may be replayed.

A replay preserves the same public CloudEvents id and event envelope while creating a new delivery generation linked to the terminal generation it was replayed from. This separates the immutable public fact from the operational act of sending it again.

## Secret rotation

The delivery database stores an opaque secret-manager reference and version, never secret bytes.

A delivery generation snapshots the subscription secret reference/version at creation. Rotating a subscription does not silently rewrite an already queued generation. A later explicit replay snapshots the then-current secret version.

## Isolation boundary

This journal is a public-integration delivery subsystem, not an external view of the internal event broker.

It must not expose or derive customer-visible semantics from NATS subjects, stream names, durable consumer names, broker retention, or topology migrations. A domain fact can be projected into this journal, but customer delivery remains independently durable and independently versioned.

## Evidence required from the lab

The PostgreSQL tests must prove at minimum:

- idempotent initial enqueue;
- event identity conflict rejection;
- two-worker claim exclusion;
- expired-lease recovery;
- stale-worker fencing;
- deterministic retries;
- retry-budget dead letter;
- explicit replay generations;
- subscription pause behavior;
- secret-version snapshotting.

## What remains before promotion

Passing this lab is enough runtime evidence to justify a subsequent scoped technology decision and candidate architecture boundary, but promotion should occur in a separate change after the executable evidence is green.

Production adoption still requires real HTTP/TLS failure tests, endpoint ownership verification, authorization, rate limiting, secret-manager integration, diagnostics/operator APIs, retention policy, observability/SLOs, and a decision on durable pull-feed recovery.

## Consequences

External customer downtime no longer needs to block the domain transaction in the architecture under trial. Delivery durability and retry state become explicit Spotwo-owned records, workers are crash recoverable, and replay becomes auditable.

The next decision slice can use this executable evidence to move `external-async-integration` from `unresolved` to `candidate` and register signed CloudEvents webhooks as a `trial`, without claiming production adoption.
