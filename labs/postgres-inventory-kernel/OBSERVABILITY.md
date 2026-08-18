# Operational Telemetry

Executable companion to ADR 0026 and ADR 0033.

## Outbox telemetry

### Purpose

Publisher cycle counters cannot detect a publisher process that is stopped. The Outbox therefore exposes an independent, read-only PostgreSQL snapshot that can be polled without claiming, delaying, ACKing, or replaying any event.

The snapshot is broker-neutral. It reports PostgreSQL publication state whether the active Transport is NATS JetStream, Kafka/Redpanda, SQS/SNS, Cloudflare Queues, or another adapter.

### Backlog classification

Every unpublished event belongs to exactly one state at the snapshot observation time:

| State | Definition | Operator meaning |
|---|---|---|
| `ready` | not quarantined, no live lease, `available_at <= observed_at` | a publisher can claim it now |
| `delayed` | not quarantined, no live lease, `available_at > observed_at` | waiting for retry backoff |
| `leased` | not quarantined, `claimed_until > observed_at` | currently owned by a publisher attempt |
| `quarantined` | `quarantined_at IS NOT NULL` | stopped pending operator repair/replay |

The machine invariant is:

```text
backlog = ready + delayed + leased + quarantined
```

Expired leases become `ready` once `available_at` has passed. Published rows are not backlog and are excluded.

### Snapshot fields

`read_domain_event_outbox_telemetry()` returns:

- backlog count and the four state counts;
- attempt buckets `0`, `1`, `2_to_4`, and `5_plus`;
- maximum current attempt count;
- age of the oldest backlog event;
- age of the oldest ready event;
- age of the oldest quarantine entry;
- one shared `observed_at` timestamp for every classification and age calculation.

The state counts and attempt buckets are each required to sum to the backlog count. `OutboxTelemetrySnapshot` rejects malformed results rather than exporting inconsistent telemetry.

### JSON inspection

With the lab database running:

```bash
KERNEL_LAB_DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55432/kernel_lab \
  python bin/inspect-kernel-outbox --pretty
```

The stable top-level shape is:

```json
{
  "status": "critical",
  "alerts": [
    {
      "code": "outbox_quarantined_events",
      "severity": "critical",
      "observed_value": 2,
      "threshold": 0
    }
  ],
  "telemetry": {
    "observed_at": "2026-08-18T18:00:00+00:00",
    "backlog": {
      "total": 1200,
      "ready": 1197,
      "delayed": 1,
      "leased": 0,
      "quarantined": 2
    }
  }
}
```

The full telemetry object also contains attempt buckets and oldest-age gauges.

### Prometheus exposition

Render a scrape-compatible snapshot:

```bash
KERNEL_LAB_DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55432/kernel_lab \
  python bin/inspect-kernel-outbox --format prometheus
```

Metric families:

| Metric | Labels | Meaning |
|---|---|---|
| `spotwo_wms_outbox_backlog_events` | none | total unpublished backlog |
| `spotwo_wms_outbox_events` | `state` | mutually exclusive ready/delayed/leased/quarantined gauges |
| `spotwo_wms_outbox_attempts` | `bucket` | current attempt distribution |
| `spotwo_wms_outbox_attempts_max` | none | highest attempt count in backlog |
| `spotwo_wms_outbox_oldest_age_seconds` | `state` | oldest backlog/ready/quarantined age |
| `spotwo_wms_outbox_health_status` | none | `0` ok, `1` warning, `2` critical |
| `spotwo_wms_outbox_alert` | `code`, `severity` | active stable alert code |
| `spotwo_wms_outbox_snapshot_timestamp_seconds` | none | observation timestamp |

Event IDs, error messages, event types, aggregate IDs, and worker IDs are deliberately absent from labels. Detailed diagnosis remains a bounded database query, not an unbounded metrics-cardinality channel.

### Alert policy

Default inspection thresholds are:

| Signal | Warning | Critical |
|---|---:|---:|
| oldest ready age | 60 seconds | 300 seconds |
| backlog count | 1,000 | 10,000 |
| quarantined count | n/a | any value greater than zero |

These defaults are operational starting points, not kernel SLOs. Deployments must calibrate them to warehouse volume, publisher capacity, and the expected broker recovery window.

Override thresholds explicitly:

```bash
python bin/inspect-kernel-outbox \
  --warning-ready-age-seconds 30 \
  --critical-ready-age-seconds 120 \
  --warning-backlog-count 500 \
  --critical-backlog-count 5000
```

Normal inspection exits `0` and carries status in the payload. Monitoring probes can add `--check` to return:

```text
0  ok
1  warning
2  critical
```

### Operator response

| Signal | First response |
|---|---|
| ready age rising | verify publisher processes, database connectivity, and transport availability |
| delayed count rising | inspect recent `last_error` values and expected retry window |
| leased count stuck | verify publisher liveness and wait for lease expiry before intervention |
| quarantined count nonzero | inspect the bounded poison-event set, repair root cause, then use audited replay |

Telemetry never automatically replays or deletes events.

### Cost boundary

The snapshot is one statement-level, read-only aggregate over unpublished Outbox rows. It takes no explicit row locks and cannot interfere with lease ownership semantics.

At larger retention volumes, the query plan and scrape interval must be measured. Materialized counters, partition-aware aggregation, and exporter caching are later scale options, not hidden behavior in this lab.

## Consumer failure telemetry

Once ADR 0032 durably captures a valid failed event and ACKs its broker delivery, PostgreSQL owns the retry. `read_domain_event_consumer_failure_telemetry()` makes that local backlog independently observable even when every retry worker is stopped.

Resolved rows are retained history and are excluded. Every unresolved failure belongs to exactly one state at the shared observation time:

| State | Definition | Operator meaning |
|---|---|---|
| `ready` | deferred, no live lease, `available_at <= observed_at` | retry worker can claim it now |
| `delayed` | deferred, no live lease, `available_at > observed_at` | waiting for local backoff |
| `leased` | deferred, `claimed_until > observed_at` | currently owned by a retry attempt |
| `quarantined` | status is quarantined | stopped pending operator repair/replay |

```text
unresolved backlog = ready + delayed + leased + quarantined
```

Quarantine is independently partitioned into `terminal` and `attempt_limit`. The full backlog is independently partitioned into attempt buckets `0`, `1`, `2_to_4`, and `5_plus`; zero is valid immediately after audited replay resets the local budget. The typed snapshot rejects any partition that does not reconcile.

The age gauges use `first_failed_at` for backlog and ready age, and `quarantined_at` for quarantine age. Expired leases and elapsed delays become ready without any telemetry mutation. An empty state has no age sample.

Inspect all consumers or one stable logical consumer:

```bash
KERNEL_LAB_DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55432/kernel_lab \
  bin/inspect-kernel-consumer-failures \
  --consumer-name inventory-position-quantity-projector \
  --pretty
```

```bash
KERNEL_LAB_DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55432/kernel_lab \
  bin/inspect-kernel-consumer-failures \
  --consumer-name inventory-position-quantity-projector \
  --format prometheus
```

Metric families are:

| Metric | Bounded labels | Meaning |
|---|---|---|
| `spotwo_wms_consumer_failure_backlog_events` | optional `consumer` | total unresolved backlog |
| `spotwo_wms_consumer_failure_events` | `state`, optional `consumer` | exclusive ready/delayed/leased/quarantined gauges |
| `spotwo_wms_consumer_failure_quarantined_events` | `kind`, optional `consumer` | terminal versus attempt-limit quarantine |
| `spotwo_wms_consumer_failure_attempts` | `bucket`, optional `consumer` | unresolved attempt distribution |
| `spotwo_wms_consumer_failure_attempts_max` | optional `consumer` | highest current attempt count |
| `spotwo_wms_consumer_failure_oldest_age_seconds` | `state`, optional `consumer` | oldest backlog/ready/quarantined age |
| `spotwo_wms_consumer_failure_health_status` | optional `consumer` | `0` ok, `1` warning, `2` critical |
| `spotwo_wms_consumer_failure_alert` | `code`, `severity`, optional `consumer` | active stable alert |
| `spotwo_wms_consumer_failure_snapshot_timestamp_seconds` | optional `consumer` | observation timestamp |

`consumer` is a deployment-controlled logical identity. Event IDs, aggregate and Position IDs, worker IDs, envelopes, failure codes, delivery metadata, and exception text are excluded from every metric label.

Default policy starting points are:

| Signal | Warning | Critical |
|---|---:|---:|
| oldest ready age | 300 seconds | 1,800 seconds |
| unresolved backlog | 100 | 1,000 |
| quarantine count | n/a | any value greater than zero |

Override them with `--warning-ready-age-seconds`, `--critical-ready-age-seconds`, `--warning-backlog-count`, and `--critical-backlog-count`. `--check` uses the same process exit contract as Outbox telemetry.

| Signal | First response |
|---|---|
| ready age rising | verify retry worker liveness, database availability, and any active projection rebuild fence |
| delayed count rising | inspect bounded failure rows and confirm the expected local retry window |
| leased count stuck | verify retry worker liveness and allow the claim lease to expire |
| terminal quarantine | repair or explicitly reject the handler contract before audited replay |
| attempt-limit quarantine | repair the repeated transient cause and confirm retry capacity before audited replay |

The consumer snapshot is one statement-level, read-only aggregate. It never claims, delays, resolves, quarantines, replays, ACKs, or deletes an event.
