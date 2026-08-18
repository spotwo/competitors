# ADR 0025 - Outbox Retry and Poison-Event Quarantine

Status: Accepted

Date: 2026-08-18

## Context

ADR 0022 established the broker-neutral Publisher Runtime and initially used one fixed retry delay. ADR 0024 then proved the first real transport against NATS JetStream.

A fixed delay is not an adequate operational policy. A broker outage can create synchronized retry storms, and an event that fails permanently because of invalid data or incompatible configuration can consume publisher capacity forever. Broker dead-letter facilities cannot solve this producer-side boundary because a publish may fail before a broker accepts the message.

The policy must remain independent of NATS, Kafka, SQS, Cloudflare Queues, or any other adapter. PostgreSQL is authoritative for Outbox publication state.

## Decision

Add a broker-neutral `RetryPolicy` to the Publisher Runtime and durable quarantine state to the PostgreSQL Outbox.

For each transport exception, the runtime uses the claimed row's incremented `attempt_count` to choose exactly one action:

```text
attempt_count < max_attempts
  -> NACK with bounded exponential delay and deterministic jitter

attempt_count >= max_attempts
  -> quarantine with the current lease token
```

The defaults are:

| Parameter | Default |
|---|---:|
| base delay | 5 seconds |
| maximum delay | 300 seconds |
| jitter ratio | 20% |
| maximum attempts | 10 |

All values are process configuration. They are not embedded in a transport adapter.

## Retry calculation

Before jitter, delay grows as:

```text
min(max_delay, base_delay * 2^(attempt_count - 1))
```

Jitter is a stable value derived from SHA-256 of `event_id + attempt_count`. It spreads different events across the window without random test behavior or worker-local state. The final delay is clamped to the inclusive range from one second through `max_delay`.

`nack_domain_event()` persists:

- the next `available_at`;
- a bounded `last_error`;
- `last_failed_at`;
- release of the claim triplet.

The NACK remains lease-fenced. A stale worker cannot reschedule a row after its claim expires or another worker reclaims it.

## Quarantine boundary

At the configured attempt limit, `quarantine_domain_event()` atomically:

- verifies the live `claim_token` and lease;
- records `last_error` and `last_failed_at`;
- records `quarantined_at` and a machine-produced `quarantine_reason`;
- releases the claim triplet.

`claim_domain_events()` excludes quarantined rows, and the partial ready index excludes them as well. A permanently failing row therefore stops creating hot loops and no longer consumes claim batches.

Quarantine is distinct from a broker DLQ. It represents a producer Outbox record that has not reached a confirmed transport boundary.

## Operator replay

Automatic unquarantine is forbidden.

`replay_quarantined_domain_event(event_id, operator_id, reason)` requires nonblank operator identity and reason. In one transaction it locks the quarantined event, appends an immutable `domain_event_outbox_operator_actions` replay record with the previous attempt count/error/quarantine reason, clears the current failure state, resets `attempt_count` to zero, and makes the event immediately claimable.

Resetting the attempt count gives a repaired event a fresh configured retry budget. The audit row preserves the history that was cleared from the active delivery row.

Requests for unknown, published, or non-quarantined events return `false` and do not append an audit action.

## Publication uncertainty remains unchanged

Retry and quarantine do not create exactly-once delivery. If a transport accepts an event but PostgreSQL ACK persistence fails, the event can be published again and can eventually reach quarantine after repeated ACK uncertainty or publish failures.

The stable contract remains:

```text
stable event_id
+ at-least-once transport
+ consumer Inbox dedupe
+ aggregate_version handling
```

JetStream `Nats-Msg-Id` deduplication remains a transport optimization within its duplicate window, not a replacement for this contract.

## Executable invariants

The kernel lab proves:

1. attempt number is returned by the lease claim and increments once per claim;
2. exponential delay grows and never exceeds the configured cap;
3. jitter is deterministic for one `event_id + attempt_count` pair;
4. NACK and quarantine both reject stale claim tokens;
5. an event is quarantined on the configured failed attempt;
6. quarantined events cannot be claimed by any publisher worker;
7. replay requires operator identity and reason;
8. replay appends the previous failure state to an audit table;
9. replay resets the attempt budget and makes the same stable `event_id` claimable again.

## Consequences

### Positive

- broker outages no longer synchronize every failed event onto one fixed retry cadence;
- poison events stop consuming publisher batches after a bounded number of failures;
- policy remains identical across every transport adapter;
- operator intervention is explicit and auditable;
- failure history survives resetting the active row for replay.

### Costs and limits

- operators need alerts and a review process for quarantined rows;
- deterministic jitter is load spreading, not a fairness or security mechanism;
- the current lab exposes SQL and store APIs rather than a complete quarantine UI;
- bulk replay, quarantine retention, archival, and authorization policy remain later operational slices.
