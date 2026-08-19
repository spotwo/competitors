# Durable external webhook delivery lab

This lab proves the storage and worker semantics required before the public external asynchronous boundary can move beyond a static webhook contract.

It is deliberately separate from the internal NATS JetStream event pipeline. Spotwo domain facts may be projected into this delivery journal, but customer integrations never receive NATS subjects, stream names, durable consumer identities, or retention settings.

## State model

```text
pending
  |
  | claim with lease
  v
in_flight
  |\
  | \  lease expires
  |  \____________________
  |                       |
  | 2xx                   | another worker recovers
  v                       v
delivered              in_flight

in_flight
  |
  | retryable failure and budget remains
  v
pending(next_attempt_at)

in_flight
  |
  | terminal 4xx or retry budget exhausted
  v
dead_letter
```

A delivery is selected with `FOR UPDATE SKIP LOCKED`. Claiming it, assigning the lease, incrementing the attempt number, and inserting the attempt journal row happen in one PostgreSQL transaction.

## Durable identities

- `subscription_id` identifies the external subscription.
- CloudEvents `id` is the stable public event identity and remains unchanged across ordinary retries and explicit replays.
- `delivery_id` identifies one delivery generation.
- `(subscription_id, event_id, generation)` is unique.
- generation `0` is the initial delivery.
- an explicit replay creates generation `N+1` and records the terminal delivery it was replayed from.

Reusing the same CloudEvents id with a different envelope is rejected. This prevents accidental mutation of an already published public fact.

## Lease and crash recovery

A worker claims an eligible delivery for a bounded lease. While that lease is valid, another worker cannot claim the same delivery.

If the worker crashes, the row remains `in_flight`. After `lease_expires_at`, another worker may claim it. The previous unfinished attempt is closed as `lease_expired`, and a new attempt is created. A stale worker cannot later complete the recovered delivery because completion requires the current lease owner and current attempt number.

The lease is coordination state, not a delivery guarantee. Customer endpoints can process a request and lose the HTTP response. Therefore the public contract remains at-least-once and consumers must deduplicate by stable event identity.

## Retry policy

The bounded lab schedule is deterministic:

```text
attempt 1  -> 5 seconds
attempt 2  -> 30 seconds
attempt 3  -> 120 seconds
attempt 4  -> 600 seconds
attempt 5  -> 1800 seconds
attempt 6  -> 3600 seconds
attempt 7  -> 7200 seconds
attempt 8+ -> 21600 seconds
```

Classification matches the static external async contract lab:

- `2xx` -> success;
- `408`, `425`, `429`, and `5xx` -> retry;
- other `4xx` -> terminal dead letter;
- connection and timeout failures without an HTTP status -> retry.

Each subscription has an explicit `max_attempts`. A retryable failure at the final attempt becomes `dead_letter` with `max_attempts_exhausted`.

## Dead letter and replay ownership

Dead-letter state is owned by Spotwo. It is never silently discarded and it is not delegated to the customer's endpoint.

Replay is an explicit operator action. Only a terminal delivery can be replayed. The replay keeps the same public event id and envelope but creates a new delivery generation. That keeps the public fact stable while making operational recovery auditable.

## Secret rotation

The database stores only an opaque secret reference and version, never the secret bytes.

A delivery snapshots the subscription's secret reference/version when the delivery generation is created. Rotating the subscription does not rewrite already queued generations. A later explicit replay snapshots the current secret version.

The actual secret value is resolved by the delivery runtime from the secret manager when signing the exact raw request body according to the external async contract lab.

## Subscription state

Subscriptions can be:

- `active`: enqueue and claim are permitted;
- `paused`: existing durable state is retained but no new claim is issued;
- `revoked`: the subscription is no longer eligible for delivery or replay.

Pausing or revoking does not delete delivery history.

## What this lab proves

The PostgreSQL tests exercise:

- idempotent initial enqueue;
- public event identity conflict rejection;
- two-worker claim exclusion;
- expired-lease recovery;
- stale-worker fencing;
- success, retry, network failure, dead-letter, and retry-budget transitions;
- deterministic retry timing;
- subscription pause behavior;
- explicit replay generation;
- secret-version snapshotting across rotation.

## What it does not prove

This is still a lab, not a production webhook service. It does not yet prove:

- real HTTP client timeout/TLS/proxy behavior;
- per-tenant rate limiting and backpressure;
- endpoint ownership verification;
- subscription authorization policy;
- secret-manager integration and dual-key rotation windows;
- operator UI/API for dead-letter inspection and replay;
- long-window retention/archival;
- a durable pull recovery feed;
- production SLOs and observability.

Those remain required before the public asynchronous contract can be adopted as a production default.
