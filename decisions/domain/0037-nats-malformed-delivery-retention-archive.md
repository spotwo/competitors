# ADR 0037: Archive inactive malformed JetStream delivery evidence

- Status: accepted
- Date: 2026-08-19
- Scope: PostgreSQL inventory kernel consumer path
- Depends on: ADR 0035, ADR 0036

## Context

ADR 0035 makes malformed JetStream delivery quarantine durable before ACK, and ADR 0036 separates recent poison activity from retained forensic inventory. The live poison table therefore accumulates evidence indefinitely even after an incident is no longer active.

Malformed deliveries do not have a trusted Domain Event `event_id`, Inbox receipt, resolved status, or replay lifecycle. Their stable identity is transport-owned:

```text
consumer_name
+ JetStream stream
+ JetStream durable consumer
+ JetStream stream sequence
```

Retention must not weaken that identity boundary or allow an archived stream sequence to return later with different evidence.

## Decision

Archive inactive malformed delivery records in explicit, consumer-scoped, bounded PostgreSQL transactions.

Eligibility is:

```text
consumer_name = requested scope
AND last_seen_at < explicit cutoff
```

`last_seen_at` is used rather than `first_seen_at` or `quarantined_at` so a recently reobserved poison delivery remains live even when it was first quarantined long ago.

The archive command performs one batch only. It never loops until exhaustion, never runs from telemetry, and never runs from the consumer ACK path.

## Archive transaction

For one stable logical consumer:

```text
select at most N eligible live poison rows
  -> FOR UPDATE SKIP LOCKED
  -> copy complete forensic evidence to archive
  -> verify copied count
  -> delete exactly those live rows
  -> verify deleted count
  -> commit
```

The default maximum batch is 1,000 rows. Multiple archive workers may run concurrently because locked rows are skipped and each archive run has its own `archive_run_id`.

The archive retains the full live record plus:

- `archived_at`
- `archive_run_id`

No archive purge is introduced by this decision.

## Archived identity reobservation

Archiving must not reset transport identity.

If a delivery with an archived JetStream identity is observed again, capture checks the newest archived snapshot before creating a live row.

For identical evidence:

```text
archived identity + same evidence
  -> reactivate live poison record
  -> preserve original first-seen/quarantine evidence
  -> increment cumulative observation_count
  -> record current last delivery metadata
  -> ACK remains allowed only after the live capture commits
```

The reactivated insert returns `created = false` because the transport identity already existed.

For conflicting evidence:

```text
archived identity + different subject/payload/headers
  -> fail closed
  -> no live poison row
  -> no ACK
```

The immutable archive snapshot remains available even after reactivation. A later retention cycle may create another archive snapshot for the same transport identity and a different `archive_run_id`; the newest snapshot carries the highest cumulative observation count.

## Cutoff policy

The kernel default is a 30-day inactive-evidence window, but this is only a starting point. Deployments should choose a window that covers their incident-response and broker replay requirements.

Operators may supply an exact timezone-aware cutoff instead of retention days. Future cutoffs are rejected.

## Telemetry interaction

Operational malformed-delivery telemetry reads only the live poison table. After this ADR, its retained count means evidence still inside the live retention horizon, not total evidence across live plus archive.

This is intentional: monitoring must stay focused on recent operational state and must not scan an indefinitely growing forensic archive on every scrape.

Archive size and long-term purge policy are separate operational concerns.

## Consequences

- old inactive poison evidence no longer grows the live operational table without bound
- recent reobservations remain live because eligibility is based on `last_seen_at`
- complete forensic evidence remains queryable after archival
- archive execution is explicit, bounded, transactional, and consumer-scoped
- parallel archive workers are safe through row locking and `SKIP LOCKED`
- an archived JetStream identity cannot silently return as a new unrelated poison record
- no automatic purge, replay, repair, Inbox receipt, or synthetic `event_id` is introduced

## Non-goals

This decision does not define archive deletion, object-storage export, legal retention duration, cross-region replication, automatic malformed-message repair, or replay of malformed bytes as Domain Events.
