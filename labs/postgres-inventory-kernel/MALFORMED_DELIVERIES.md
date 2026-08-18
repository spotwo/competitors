# Malformed JetStream Delivery Operations

Executable companion to ADR 0035, ADR 0036, and ADR 0037.

## Boundary

Malformed delivery handling happens before a Domain Event `event_id` can be trusted:

```text
JetStream delivery
  -> capture trusted stream / durable consumer / stream sequence
  -> decode and validate payload, envelope, and headers
  -> malformed: persist poison evidence in PostgreSQL
  -> commit
  -> ACK
```

No synthetic `event_id` is created and no Inbox receipt is written for this path.

## Inspect operational health

Inspect live poison evidence inside the retention horizon:

```bash
KERNEL_LAB_DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55432/kernel_lab \
  python bin/inspect-kernel-malformed-deliveries --pretty
```

Scope to one logical consumer:

```bash
KERNEL_LAB_DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55432/kernel_lab \
  python bin/inspect-kernel-malformed-deliveries \
  --consumer-name inventory-position-quantity-projector \
  --lookback-seconds 300 \
  --pretty
```

Use `--format prometheus` for scraping. Add `--check` for monitoring exit codes:

```text
0  ok
1  warning
2  critical
```

## What is an incident

Live retained poison evidence is not a retry backlog. An old record can remain useful after the transport problem is gone, so retained count alone does not alert.

Operational alerts use recent activity inside the explicit lookback:

| Signal | Warning | Critical |
|---|---:|---:|
| records first quarantined in lookback | 1 | 10 |
| reobserved records with latest observation in lookback | 1 | 5 |

Override thresholds when the deployment has a known higher malformed-message rate:

```bash
python bin/inspect-kernel-malformed-deliveries \
  --lookback-seconds 600 \
  --warning-recent-count 2 \
  --critical-recent-count 20 \
  --warning-reobserved-count 2 \
  --critical-reobserved-count 10
```

These defaults are operational starting points, not kernel SLOs.

## Snapshot contract

The read-only operational snapshot reports only the live poison table:

- total live retained poison records;
- records first seen inside the lookback;
- recently reobserved records;
- live retained observation total;
- maximum observation count on one live record;
- bounded failure kinds: `invalid_encoding`, `invalid_json`, `invalid_envelope`, `invalid_headers`, `other`;
- oldest and newest live-record age;
- age of the latest live observation.

A recently reobserved record means `observation_count > 1` and `last_seen_at` is inside the lookback. It does not claim an exact count of recent redeliveries because the quarantine table intentionally keeps cumulative evidence rather than an unbounded observation ledger.

Archived evidence is intentionally excluded from operational scrapes so an indefinitely growing forensic archive cannot turn into an expensive monitoring query.

## Metric cardinality

Prometheus labels may contain only bounded telemetry dimensions plus optional deployment-controlled `consumer` identity.

Do not export these as labels:

- JetStream stream or durable consumer names;
- subjects or stream sequences;
- payload digests or previews;
- raw failure codes;
- headers or error text;
- event IDs, because malformed deliveries have no trusted event identity.

Detailed evidence stays in PostgreSQL for bounded operator queries.

## Operator response

For newly quarantined records:

1. Inspect `failure_code`, subject, bounded preview, headers, and trusted JetStream coordinates in PostgreSQL.
2. Decide whether the producer emitted invalid bytes, invalid JSON, a bad Domain Event envelope, or inconsistent transport headers.
3. Repair the producer or transport adapter before considering any manual data recovery.
4. Do not fabricate an `event_id` to force malformed bytes through Inbox processing.

For recent reobservations:

1. Check application logs for ACK confirmation failures or database availability around `last_seen_at`.
2. Verify JetStream consumer ACK policy and transport connectivity.
3. Confirm the poison row is idempotently updating rather than multiplying records for one stream sequence.

## Forensic query

Use a bounded query for the newest live records:

```sql
SELECT
  consumer_name,
  stream,
  durable_consumer,
  stream_sequence,
  subject,
  failure_code,
  payload_sha256,
  payload_size,
  payload_truncated,
  observation_count,
  first_seen_at,
  last_seen_at,
  last_error
FROM kernel_lab.nats_jetstream_consumer_poison_deliveries
ORDER BY last_seen_at DESC
LIMIT 100;
```

The telemetry command never ACKs, replays, deletes, or mutates a poison record.

## Archive inactive poison evidence

Archive one bounded batch for one stable logical consumer:

```bash
KERNEL_LAB_DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55432/kernel_lab \
  bin/archive-kernel-malformed-deliveries \
  --consumer-name inventory-position-quantity-projector \
  --pretty
```

The default inactive-evidence window is 30 days. Use an exact timezone-aware cutoff when an operator needs deterministic control:

```bash
bin/archive-kernel-malformed-deliveries \
  --consumer-name inventory-position-quantity-projector \
  --before 2026-07-20T00:00:00Z \
  --batch-size 500 \
  --pretty
```

Eligibility is deliberately based on the latest observation:

```text
consumer_name = requested scope
AND last_seen_at < cutoff
```

A poison record first seen months ago but reobserved today stays live. The archive command uses `FOR UPDATE SKIP LOCKED`, copies complete forensic evidence, verifies the copy count, deletes exactly the copied live rows, verifies the delete count, and commits once.

The command runs one batch only. Scheduling and repeated invocation belong to deployment operations, not the consumer runtime.

## Archive contents

`nats_jetstream_consumer_poison_delivery_archive` retains every field from the live poison record plus:

- `archived_at`;
- `archive_run_id`.

The archive keeps subject, trusted JetStream coordinates, full-payload SHA-256, bounded payload preview, headers, first/latest delivery metadata, failure code, bounded error text, cumulative observation count, and first/latest timestamps.

There is no automatic purge of archive rows in this slice.

Use a bounded archive query when historical evidence is required:

```sql
SELECT
  consumer_name,
  stream,
  durable_consumer,
  stream_sequence,
  failure_code,
  observation_count,
  first_seen_at,
  last_seen_at,
  archived_at,
  archive_run_id
FROM kernel_lab.nats_jetstream_consumer_poison_delivery_archive
WHERE consumer_name = 'inventory-position-quantity-projector'
ORDER BY archived_at DESC, stream_sequence DESC
LIMIT 100;
```

## Reobservation after archival

Archive is not an identity reset.

If JetStream later presents the same transport identity with the same poison evidence, capture reactivates a live record while preserving the original first-seen and quarantine timestamps and the first delivery metadata. The cumulative `observation_count` continues from the newest archived snapshot.

```text
archived identity + matching evidence
  -> live reactivation
  -> observation_count + 1
  -> durable commit
  -> ACK
```

The capture result reports `created = false` because the transport identity already existed.

If the archived identity appears with a different subject, payload digest/size/preview, truncation state, or headers, capture fails closed and the delivery is not ACKed:

```text
archived identity + conflicting evidence
  -> capture failure
  -> no new live poison row
  -> no ACK
```

The historical archive snapshot remains immutable. If a reactivated record later becomes inactive again, a new archive run may create another snapshot for the same transport identity with a different `archive_run_id` and a higher cumulative observation count.

## Retention policy boundary

Thirty days is a kernel starting point, not a compliance rule. Deployments should select a window that covers incident response, expected JetStream replay horizons, audit needs, and storage cost.

Archive purge, object-storage export, legal hold, and legal retention duration are separate future policies. They must not be hidden inside telemetry, consumer processing, or this bounded archive command.
