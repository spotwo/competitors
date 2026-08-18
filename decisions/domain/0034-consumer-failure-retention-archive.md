# ADR 0034 - Consumer Failure Retention Archive

Status: Accepted

Date: 2026-08-18

## Context

ADR 0032 persists valid failed Domain Events until the normal Inbox transaction resolves them or an operator leaves them quarantined. ADR 0033 intentionally excludes `resolved` rows from operational backlog health. The resolved rows and their replay actions still grow without bound in the live failure tables.

A retention process must reduce live-table pressure without weakening these contracts:

1. deferred and quarantined events remain available for retry or operator review;
2. a `resolved` row remains credible only when the matching Inbox receipt proves the local effect committed;
3. replay command identity remains payload-bound after its live audit row moves;
4. different logical consumers can retain different replay windows;
5. a partial copy or identity conflict never deletes live evidence.

## Decision

Add a consumer-scoped transactional archive for old resolved failure history.

An event is eligible only when all three predicates hold:

```text
status = resolved
+ resolved_at older than explicit cutoff
+ matching (consumer_name, event_id) Inbox receipt exists
= archive candidate
```

The caller must provide one exact `consumer_name`, a timezone-aware cutoff, and a batch limit from 1 through 1,000. The default runtime policy is a 30-day live grace period and a 1,000-failure batch. These are operational starting points, not kernel SLOs.

Recent resolved rows, every deferred or quarantined row, and any resolved row without Inbox evidence remain live. The archive function never infers one consumer's retention window from another consumer or from producer Outbox age.

## Archived evidence

`domain_event_consumer_failure_archive` preserves the complete failure row plus `archived_at` and `archive_run_id`. It accepts only resolved, unclaimed, non-quarantined state and has a foreign key to the matching Inbox receipt.

`domain_event_consumer_failure_action_archive` preserves every replay action for an archived failure and binds it to the same archive run. The failure copy, all associated action copies, live action deletes, and live failure deletes occur in one PostgreSQL transaction.

Copy and delete counts must reconcile. Archive identity collisions fail closed and roll back the entire batch. The implementation does not use `ON CONFLICT DO NOTHING` to hide inconsistent history.

## Concurrency

Candidates are selected oldest first with `FOR UPDATE SKIP LOCKED`. Concurrent schedulers therefore receive disjoint failure batches without a global retention lock. Existing retry and capture paths also lock the failure row, so resolution, duplicate delivery handling, and archival serialize on the same database identity.

The parent failure count is explicitly bounded. All replay actions belonging to each selected failure move atomically with it, so unusually large per-event audit histories can still increase one batch's work and must be observed operationally.

## Replay identity after archival

Archiving action rows must not free their `replay_id` values for changed commands.

`replay_domain_event_consumer_failure()` takes the existing transaction advisory lock for the replay ID and checks both the live and archive action tables:

- an exact retry returns `duplicate`;
- the same replay ID with changed consumer, event, operator, or reason raises a uniqueness error;
- a new replay command against archived resolved history returns `not_quarantined`.

This preserves command idempotency without moving an archived failure back into the live retry lane.

## Inbox boundary

The archive process reads but never deletes or rewrites `domain_event_inbox`. The archive foreign key keeps the receipt present while archived failure history depends on it.

Inbox retention is a separate consumer-specific destructive decision. It must cover broker replay, projection rebuild, restoration, and audit requirements before any receipt can be removed. Producer Outbox retention does not define that window.

## Executable invariants

The PostgreSQL kernel lab proves:

1. only old resolved failures with matching Inbox evidence move;
2. recent resolved, deferred, quarantined, and resolved-without-Inbox rows remain live;
3. one consumer-scoped run cannot archive another consumer's rows;
4. the complete failure and replay-action records survive the move;
5. the Inbox receipt and its metadata remain unchanged;
6. exact archived replay retries remain duplicates and changed reuse fails closed;
7. concurrent bounded archive calls select disjoint batches;
8. any archive copy conflict rolls back all live deletes;
9. invalid scope, cutoff, and batch inputs fail before mutation.

## Consequences

### Positive

- operational failure queries stay focused on unresolved and recent history;
- retry and quarantine safety are unchanged;
- archived evidence remains joined to its committed Inbox proof;
- replay command idempotency survives live-table cleanup;
- one bounded command supports independent schedules per logical consumer;
- corruption evidence causes rollback instead of silent data loss.

### Costs and limits

- deployments must choose and schedule a retention window for every stable consumer identity;
- archive tables and Inbox receipts continue to grow;
- large replay histories can make one otherwise bounded failure batch heavier;
- physical partitioning, cold-object export, restore verification, legal retention, and destructive archive purge remain later decisions;
- Inbox cleanup, bulk DLQ review, malformed-message adapter quarantine, and generic multi-handler routing remain outside this slice.
