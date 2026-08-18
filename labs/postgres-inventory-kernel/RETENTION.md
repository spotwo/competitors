# Operational Retention Runbook

Executable companion to ADR 0027 and ADR 0034.

## Outbox retention

### Safety boundary

The live Outbox is a transport delivery buffer. Archive eligibility is based only on confirmed publication:

```text
published_at present
+ published_at older than explicit cutoff
= archive candidate
```

Ready, delayed, leased, and quarantined events are unpublished and cannot be selected. Never replace the archive function with a direct age-based `DELETE`.

The permanent `domain_event_idempotency_keys` registry keeps each producer `dedup_key` bound to its original event ID and payload after the live delivery row moves. The event archive preserves the complete canonical envelope and final delivery fields. The operator-action archive preserves replay evidence.

### Run one batch

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

### Scheduling

Run one bounded invocation per scheduler tick. Multiple schedulers are safe: `FOR UPDATE SKIP LOCKED` assigns disjoint batches. Choose cadence and batch size from measured publish volume and database capacity.

Do not add an unbounded loop inside the publisher process. Separate scheduling keeps retention failure independent from event publication and gives every transaction an explicit maximum size.

### Failure behavior

Archive copy, replay-audit copy, and live deletes share one PostgreSQL transaction. Any exception rolls the whole batch back. Retry the same command after investigating the database error.

Archive identity conflicts are treated as corruption evidence and fail closed. Do not work around them with `ON CONFLICT DO NOTHING` or direct deletion.

### Verification queries

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

### Deferred lifecycle

This slice does not purge the archive or the idempotency registry. PostgreSQL partitioning, cold-object export, restore verification, legal retention, and destructive purge require a separate decision and executable proof.

## Consumer failure retention

### Safety boundary

Archive eligibility is consumer-scoped and requires committed-effect evidence:

```text
one explicit consumer_name
+ status = resolved
+ resolved_at older than explicit cutoff
+ matching Inbox receipt exists
= archive candidate
```

Deferred and quarantined rows still belong to retry or operator workflows and cannot be selected. Recent resolved rows remain live. A resolved row without matching Inbox evidence also remains live so retention cannot hide an inconsistent success claim.

The archive preserves the complete failure row and all of its replay actions. Its foreign key keeps the matching Inbox receipt present. This command never deletes or changes Inbox data.

### Run one batch

The default policy keeps resolved rows live for 30 days and caps one transaction at 1,000 failures:

```bash
KERNEL_LAB_DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55432/kernel_lab \
  bin/archive-kernel-consumer-failures \
  --consumer-name inventory-position-quantity-projector \
  --pretty
```

Choose the logical consumer and policy explicitly when deployment requirements differ:

```bash
bin/archive-kernel-consumer-failures \
  --database-url postgresql://postgres:postgres@127.0.0.1:55432/kernel_lab \
  --consumer-name availability-projector \
  --retention-days 45 \
  --batch-size 500 \
  --pretty
```

For a controlled backfill or incident, use a timezone-aware cutoff:

```bash
bin/archive-kernel-consumer-failures \
  --database-url postgresql://postgres:postgres@127.0.0.1:55432/kernel_lab \
  --consumer-name inventory-position-quantity-projector \
  --before 2026-07-01T00:00:00Z \
  --batch-size 250
```

A successful nonempty result resembles:

```json
{
  "archive_run_id": "019c0000-0000-7000-8000-000000000002",
  "archived": {
    "failures": 250,
    "replay_actions": 4
  },
  "archived_at": "2026-08-18T18:00:00+00:00",
  "consumer_name": "inventory-position-quantity-projector",
  "cutoff": "2026-07-01T00:00:00+00:00"
}
```

The command performs one batch only. Zero failures is a normal success with a null run ID and zero counts.

### Scheduling and failure behavior

Schedule each stable `consumer_name` independently. Multiple invocations for the same consumer are safe because `FOR UPDATE SKIP LOCKED` assigns disjoint batches. Do not place an unbounded retention loop inside the retry worker.

Failure copy, replay-action copy, and both live deletes share one transaction. Count mismatches, archive identity collisions, Inbox evidence loss, and database errors roll the whole batch back. Investigate the error and retry the same command. Never bypass a conflict with direct deletion or `ON CONFLICT DO NOTHING`.

The batch limit bounds parent failure rows. Every replay action for a selected failure moves with it, so monitor transaction duration and action volume when an event has unusually long replay history.

### Verification queries

Inspect recent runs for one consumer:

```sql
SELECT
  archive_run_id,
  min(archived_at) AS archived_at,
  count(*) AS archived_failures,
  min(resolved_at) AS oldest_resolution,
  max(resolved_at) AS newest_resolution
FROM kernel_lab.domain_event_consumer_failure_archive
WHERE consumer_name = 'inventory-position-quantity-projector'
GROUP BY archive_run_id
ORDER BY archived_at DESC
LIMIT 20;
```

Compare replay actions for one run:

```sql
SELECT count(*)
FROM kernel_lab.domain_event_consumer_failure_action_archive
WHERE archive_run_id = '019c0000-0000-7000-8000-000000000002';
```

The archive schema and foreign key should make both diagnostic counts zero:

```sql
SELECT count(*)
FROM kernel_lab.domain_event_consumer_failure_archive
WHERE status <> 'resolved';

SELECT count(*)
FROM kernel_lab.domain_event_consumer_failure_archive AS a
LEFT JOIN kernel_lab.domain_event_inbox AS i
  USING (consumer_name, event_id)
WHERE i.event_id IS NULL;
```

### Deferred lifecycle

This slice does not purge consumer failure archives or Inbox receipts. Inbox cleanup has a separate replay and exactly-once safety boundary. Archive partitioning, cold export, restoration drills, legal retention, and destructive purge require another explicit decision and executable proof.
