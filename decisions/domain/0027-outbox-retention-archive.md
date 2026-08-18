# ADR 0027 - Transactional Outbox Retention and Archive

Status: Accepted

Date: 2026-08-18

## Context

ADR 0021 made PostgreSQL Outbox rows durable until transport publication is acknowledged. ADR 0025 added replay audit rows, and ADR 0026 made every unpublished delivery state observable.

The live Outbox cannot grow forever. It is a delivery buffer, not the system's long-term event store. A naive age-based delete is still unsafe for three independent reasons:

1. age alone does not prove transport acceptance;
2. deleting the live `(tenant_id, dedup_key)` uniqueness record would let an old producer retry create a second event;
3. a successfully replayed event can have operator audit rows that must survive live-row cleanup.

Retention must bound each database transaction, coexist with publisher workers, and fail without leaving a partially copied batch.

## Decision

Add three durable structures and one bounded archive function:

| Structure | Responsibility |
|---|---|
| `domain_event_idempotency_keys` | permanent producer identity and payload binding after delivery retention |
| `domain_event_outbox_archive` | published event envelope plus final delivery history |
| `domain_event_outbox_operator_action_archive` | operator replay audit associated with the archived event |
| `archive_published_domain_events(before, limit)` | transactional copy-and-remove operation |

The default operator policy keeps published rows live for 30 days and archives at most 1,000 events per invocation. Both values are deployment inputs, not kernel SLOs.

## Eligibility boundary

An event is eligible only when:

```sql
published_at IS NOT NULL
AND published_at < explicit_cutoff
```

The function does not infer safety from `recorded_at`, `available_at`, attempt count, lease age, or quarantine age.

Therefore these rows are never eligible:

- ready unpublished events;
- intentionally delayed events;
- live or expired publisher leases that remain unpublished;
- quarantined poison events;
- published events inside the configured grace period.

A future cutoff is rejected. The batch limit is constrained to the inclusive range 1 through 1,000 in both Python and PostgreSQL.

## Durable idempotency identity

Before this decision, Outbox uniqueness itself bound one producer `dedup_key` to one event payload. Archiving that row would remove the binding.

`domain_event_idempotency_keys` now owns the durable mapping:

```text
(tenant_id, dedup_key)
  -> stable event_id
  -> immutable event identity and payload
```

`enqueue_domain_event()` inserts the idempotency key and live Outbox row in the same producer transaction. An exact retry returns the original `event_id`, including after the live row has been archived. A changed event identity or payload under the same key still raises a uniqueness violation and never recreates an Outbox row.

Existing live rows are backfilled into the registry before the new foreign key is installed. The registry has no tenant or warehouse lifecycle foreign key because its purpose is to outlive delivery-buffer retention. Registry purge policy is explicitly outside this slice.

## Transaction and concurrency model

One archive call:

1. selects the oldest eligible live rows with `FOR UPDATE SKIP LOCKED`;
2. copies their complete envelopes and delivery history into the event archive;
3. copies associated replay audit rows into the action archive;
4. deletes the copied live audit rows;
5. deletes the copied live Outbox rows;
6. returns one archive run ID, timestamp, and both row counts.

All steps run in one PostgreSQL transaction. A copy conflict, constraint failure, count mismatch, or caller rollback preserves every live source row. Plain inserts intentionally fail on archive identity collisions instead of silently treating corruption as success.

Row locks remain held until commit. Concurrent archive workers skip one another's locked candidates and receive disjoint batches. Publisher claims cannot overlap because eligible rows are already published. Newly ACKed rows become eligible only in a later call and only when their `published_at` is older than the explicit cutoff.

## Audit and ACK behavior

Replay audit rows move with the event and retain their original action ID, operator identity, reason, previous attempt state, and action timestamp. Both archive rows share the same archive run ID.

`ack_domain_event()` treats an archived event as already ACKed. This preserves idempotent ACK semantics if an old ACK request arrives after retention moved the live row.

Archive does not republish an event and does not remove consumer Inbox receipts. Consumer Inbox retention is an independent consumer-owned policy.

## Operator interface

`bin/archive-kernel-outbox` performs exactly one bounded batch and emits JSON evidence. By default it calculates a cutoff 30 days before invocation:

```bash
KERNEL_LAB_DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55432/kernel_lab \
  python bin/archive-kernel-outbox --retention-days 30 --batch-size 1000
```

An operator may instead provide an explicit timezone-aware `--before` timestamp. A scheduler can invoke the command repeatedly; the command itself has no unbounded drain mode.

## Executable invariants

The PostgreSQL kernel lab proves:

1. only events published before the cutoff move to archive;
2. recent published, ready, delayed, leased, and quarantined rows remain live;
3. the complete event envelope and final delivery fields round-trip unchanged;
4. replay audit rows move with the same archive run ID;
5. an exact enqueue retry after archive returns the stable event ID without recreating delivery work;
6. changed payload under an archived dedup key is rejected;
7. ACK remains idempotent after archive;
8. parallel workers archive disjoint bounded batches;
9. an empty batch returns zero counts and no run ID;
10. an archive-copy failure rolls back before any live deletion;
11. future cutoffs and limits above 1,000 are rejected.

## Consequences

### Positive

- the hot Outbox delivery table can remain bounded without risking unpublished facts;
- producer idempotency survives live delivery retention;
- operator replay evidence remains queryable after cleanup;
- one failed or slow archive worker does not block all other archive workers;
- each invocation has bounded row and transaction scope;
- the contract remains transport-neutral.

### Costs and limits

- the idempotency registry is intentionally append-only in this slice;
- the PostgreSQL archive still grows and needs later partition, cold export, and verified purge policy;
- the 30-day default requires deployment-specific capacity and recovery analysis;
- archived publication is not a general event-store replay API;
- authorization, scheduler deployment, legal retention periods, and consumer Inbox cleanup remain deployment concerns.
