# Malformed JetStream Delivery Operations

Executable companion to ADR 0035 and ADR 0036.

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

Inspect all retained poison evidence:

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

Retained poison evidence is not a retry backlog. An old record can remain useful after the transport problem is gone, so retained count alone does not alert.

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

The read-only snapshot reports:

- total retained poison records;
- records first seen inside the lookback;
- recently reobserved records;
- retained all-time observation total;
- maximum observation count on one record;
- bounded failure kinds: `invalid_encoding`, `invalid_json`, `invalid_envelope`, `invalid_headers`, `other`;
- oldest and newest retained-record age;
- age of the latest observation.

A recently reobserved record means `observation_count > 1` and `last_seen_at` is inside the lookback. It does not claim an exact count of recent redeliveries because the quarantine table intentionally keeps cumulative evidence rather than an unbounded observation ledger.

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

Use a bounded query for the newest records:

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

## Retention boundary

Poison evidence currently remains live until an explicit retention policy is introduced. Retention must be a separate bounded operation with its own eligibility, concurrency, and audit contract. Do not hide deletion inside telemetry or consumer runtime code.
