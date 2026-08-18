# ADR 0032 - Consumer Failure Deferral and Quarantine

Status: Accepted

Date: 2026-08-18

## Context

ADR 0028 rolls back a failed handler transaction and leaves its broker delivery unacknowledged. That is safe for short transient failures, but it makes broker delivery limits carry application retry policy. A long Position rebuild fence can consume every JetStream `max_deliver` attempt before an operator completes or cancels the rebuild. A terminal version conflict can instead redeliver forever when the broker limit is unbounded.

The consumer needs to separate three facts:

1. the broker delivered a valid canonical Domain Event;
2. the local handler did not commit;
3. the event is either retryable later or requires operator review.

The Inbox receipt cannot represent a failed attempt. Its presence continues to mean that the handler effect committed in the same transaction.

## Decision

Add an optional broker-neutral durable failure lane for valid parsed Domain Events.

On handler or handler-database failure:

```text
Inbox + handler transaction rolls back
  -> classify bounded failure code
  -> persist complete envelope and delivery evidence in PostgreSQL
  -> commit deferred or quarantined state
  -> acknowledge broker delivery
```

If durable capture fails, the broker is not acknowledged. Transport ownership moves to PostgreSQL only after the complete event is committed there.

The lane is optional so ADR 0028 behavior remains compatible. Without it, a handler failure still rolls back and leaves the delivery unacknowledged.

## Failure states

`domain_event_consumer_failures` is keyed by `(consumer_name, event_id)` and binds that identity to one exact canonical envelope.

```text
handler failure -> deferred -> resolved
                -> quarantined

quarantined -> audited replay -> deferred
```

The row retains the full envelope, original delivery metadata, stable failure code, bounded error text, failure count, lease state, quarantine reason, and resolution evidence.

| State | Meaning |
|---|---|
| `deferred` | complete event is durable and eligible for a local leased retry after `available_at` |
| `quarantined` | no automatic retry; complete event remains available for inspection and audited replay |
| `resolved` | the normal Inbox transaction later applied the handler or found its receipt already committed |

An exact broker redelivery after ACK uncertainty finds the existing payload-bound row and ACKs without incrementing the local attempt count or bypassing quarantine.

## Classification

Classification uses bounded codes suitable for operational aggregation. Raw exception text is retained only as bounded audit data.

| Failure | Code | Initial decision |
|---|---|---|
| active Position rebuild fence, SQLSTATE `55000` | `projection_rebuild_fence_active` | retryable |
| serialization, deadlock, lock timeout, statement cancellation, connection failure | bounded database code | retryable |
| Position version ownership conflict, SQLSTATE `23505` | `projection_version_conflict` | terminal |
| handler constraint or data violation | bounded contract code | terminal |
| handler `ValueError` | `invalid_event_contract` | terminal |
| unknown handler exception | `handler_failure` | retryable with a bounded attempt budget |

Classification is policy, not proof that the underlying condition is safe. New handlers must extend and test their own terminal codes rather than use unbounded exception strings as labels.

## Local retry transaction

Deferred rows are claimed with `FOR UPDATE SKIP LOCKED` and a bounded lease. Broker I/O is not part of this path.

For each valid claim, one PostgreSQL transaction:

1. locks the exact failure row and verifies its claim token;
2. inserts the normal Inbox receipt;
3. runs the original handler when the receipt is new;
4. marks the failure row `resolved` as `applied` or `duplicate`;
5. commits all changes together.

If the handler fails, that transaction rolls back. A separate short transaction records the next bounded delay or quarantine at the attempt limit. Exponential delay uses deterministic event and attempt jitter so a restarted worker computes the same schedule.

If a handler transaction committed but its client observed an ambiguous error, a later retry sees the existing Inbox receipt, skips the handler, and resolves the failure row as `duplicate`. This preserves the ADR 0028 exactly-once local effect boundary.

## Operator replay

Terminal and exhausted rows remain quarantined until an operator supplies:

- a stable `replay_id`;
- exact consumer and event identity;
- operator identity;
- a nonblank reason.

`domain_event_consumer_failure_actions` stores the prior attempt count, failure code, retryability, error, and quarantine reason. Exact replay retries return `duplicate`; reuse of one replay ID for changed input fails closed. Replay resets the live attempt count for one fresh bounded retry budget and returns the row to `deferred`, while the action row preserves the prior budget evidence. It does not insert an Inbox receipt, bypass the handler, or declare the event successful.

## Transport boundary

This contract does not require JetStream NAK delay, termination, or broker-specific DLQ APIs. NATS only supplies and acknowledges the original delivery. PostgreSQL becomes the retry source for a valid event after handoff, so a temporary rebuild fence does not consume `max_deliver`.

Malformed bytes, invalid canonical envelopes, and NATS header mismatches occur before a trusted event identity exists. They remain unacknowledged under the adapter-level poison policy and are outside this slice.

## Executable invariants

The PostgreSQL and pinned NATS lab proves:

1. handler rollback precedes durable failure capture and broker ACK;
2. capture failure produces no broker ACK;
3. Inbox remains absent while an event is deferred or quarantined;
4. full envelope identity is payload-bound across duplicate delivery and replay;
5. an active rebuild fence is deferred with a stable retryable code;
6. a Position version conflict is terminal and quarantined;
7. local retry atomically commits Inbox effect and `resolved` state;
8. repeated retryable failure quarantines at the local attempt budget;
9. only one worker can claim a due row at a time;
10. operator replay is stable, audited, and still uses the normal Inbox handler path;
11. a real JetStream consumer with `max_deliver=1` ACKs after durable fence handoff and later resolves the event locally without broker redelivery.

## Consequences

### Positive

- broker delivery limits no longer double as application retry budgets for valid events;
- temporary and terminal failures have explicit, queryable states;
- quarantine retains the complete event instead of deleting or silently skipping it;
- Inbox continues to prove committed handler effect only;
- the runtime stays transport-neutral;
- audited replay cannot bypass domain handling.

### Costs and limits

- deployments must run and monitor the local retry worker;
- failure rows and action audit require an explicit retention policy;
- classifier policy must evolve with each concrete handler;
- malformed transport messages still need an adapter-level poison policy;
- operator authentication, authorization, approvals, alerts, and UI remain deployment concerns;
- Inbox cleanup, failure-row cleanup, bulk replay, and generic multi-handler routing remain later slices.
