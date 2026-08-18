# Outbox Retention Runbook

Executable companion to ADR 0027.

## Safety boundary

The live Outbox is a transport delivery buffer. Archive eligibility is based only on confirmed publication:

```text
published_at present
+ published_at older than explicit cutoff
= archive candidate
```

Ready, delayed, leased, and quarantined events are unpublished and cannot be selected. Never replace the archive function with a direct age-based `DELETE`.

The permanent `domain_event_idempotency_keys` registry keeps each producer `dedup_key` bound to its original event ID and payload after the live delivery row moves. The event archive preserves the complete canonical envelope and final delivery fields. The operator-action archive preserves replay evidence.

## Run one batch

The default policy keeps published rows in the live table for 30 days and caps one transaction at 1,000 events:

```bash
KERNEL_LAB_DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55432/kernel_lab \
  python bin/archive-kernel-outbox --pretty
```

Set the policy explicitly when deployment requirements differ:

```bash
python bin/archive-kernel-outbox \
  --database-url postgresql://postgres:postgres@127.0.0.1:55432/kernel_lab \
  --retention-days 45 \
  --batch-size 500 \
  --pretty
```

For a controlled backfill or incident, use an explicit timezone-aware cutoff:

```bash
python bin/archive-kernel-outbox \
  --database-url postgresql://postgres:postgres@127.0.0.1:55432/kernel_lab \
  --before 2026-07-01T00:00:00Z \
  --batch-size 250
```

The command performs one batch only. A successful nonempty result resembles:

```json
{
  "archive_run_id": "019c0000-0000-7000-8000-000000000001",
  "archived": {
    "events": 250,
    "operator_actions": 3
  },
  "archived_at": "2026-08-18T18:00:00+00:00",
  "cutoff": "2026-07-01T00:00:00+00:00"
}
```

Zero events is a normal successful result. It returns `archive_run_id: null` and both counts as zero.

## Scheduling

Run one bounded invocation per scheduler tick. Multiple schedulers are safe: `FOR UPDATE SKIP LOCKED` assigns disjoint batches. Choose cadence and batch size from measured publish volume and database capacity.

Do not add an unbounded loop inside the publisher process. Separate scheduling keeps retention failure independent from event publication and gives every transaction an explicit maximum size.

## Failure behavior

Archive copy, replay-audit copy, and live deletes share one PostgreSQL transaction. Any exception rolls the whole batch back. Retry the same command after investigating the database error.

Archive identity conflicts are treated as corruption evidence and fail closed. Do not work around them with `ON CONFLICT DO NOTHING` or direct deletion.

## Verification queries

Inspect recent archive runs:

```sql
SELECT
  archive_run_id,
  min(archived_at) AS archived_at,
  count(*) AS archived_events,
  min(published_at) AS oldest_publication,
  max(published_at) AS newest_publication
FROM kernel_lab.domain_event_outbox_archive
GROUP BY archive_run_id
ORDER BY archived_at DESC
LIMIT 20;
```

Verify that no unpublished row entered the archive. The archive schema also enforces this through non-null `published_at`:

```sql
SELECT count(*)
FROM kernel_lab.domain_event_outbox_archive
WHERE published_at IS NULL;
```

Compare archived replay-action counts for one run:

```sql
SELECT count(*)
FROM kernel_lab.domain_event_outbox_operator_action_archive
WHERE archive_run_id = '019c0000-0000-7000-8000-000000000001';
```

## Deferred lifecycle

This slice does not purge the archive or the idempotency registry. PostgreSQL partitioning, cold-object export, restore verification, legal retention, and destructive purge require a separate decision and executable proof.
