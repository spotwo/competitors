# ADR 0033 - Consumer Failure Operational Telemetry

Status: Accepted

Date: 2026-08-18

## Context

ADR 0032 moves a valid failed Domain Event from broker delivery into a durable PostgreSQL failure lane. That prevents a temporary handler condition from consuming the broker delivery budget, but it also creates a new operational responsibility: a stopped local retry worker or an unattended quarantine must be visible without relying on worker logs.

Cycle counters are insufficient because a stopped worker emits no new cycle. JetStream state is also insufficient after the broker delivery has been acknowledged and PostgreSQL owns the retry. The authoritative health source is therefore `domain_event_consumer_failures`.

The telemetry contract must expose current health without exporting event identity, Position identity, exception text, worker identity, or arbitrary failure codes as metric labels.

## Decision

Add one read-only PostgreSQL snapshot function, a typed Python health model, and JSON/Prometheus inspection through `bin/inspect-kernel-consumer-failures`.

The snapshot can aggregate all logical consumers or one explicit `consumer_name`. It uses one observation timestamp so all state transitions and ages describe the same instant.

## Operational backlog

Rows with `status = 'resolved'` are retained history and are excluded from current backlog, just as published Outbox rows are excluded from publication backlog.

Every unresolved row is classified in precedence order:

```text
status quarantined
  -> quarantined

live claimed_until
  -> leased

future available_at
  -> delayed

otherwise
  -> ready
```

This creates the invariant:

```text
backlog_count
  = ready_count
  + delayed_count
  + leased_count
  + quarantined_count
```

An expired lease becomes ready when its retry delay has also elapsed. Telemetry never extends or clears a lease.

## Quarantine and attempt partitions

The quarantine set is split into two bounded operational kinds:

| Kind | Meaning |
|---|---|
| `terminal` | classifier rejected automatic retry |
| `attempt_limit` | retryable failure exhausted its local attempt budget |

Their counts must sum to the total quarantine count. Failure codes remain available in bounded database inspection but are not Prometheus labels.

The unresolved backlog is independently partitioned into attempt buckets `0`, `1`, `2_to_4`, and `5_plus`. Attempt `0` is valid after an audited replay resets the local budget. The typed snapshot rejects any state, quarantine-kind, or attempt partition that does not reconcile.

## Age gauges

The snapshot reports nonnegative seconds for:

- the oldest unresolved failure by `first_failed_at`;
- the oldest currently ready failure by `first_failed_at`;
- the oldest quarantine entry by `quarantined_at`.

An empty state returns `NULL`, not zero. Ready age is the primary retry-worker lag signal. Backlog age includes intentionally delayed and quarantined rows and is diagnostic context.

## Alert evaluation

The default policy emits stable alert codes for:

- any quarantined event, critical;
- backlog count at configurable warning/critical thresholds;
- oldest ready age at configurable warning/critical thresholds.

Defaults are 100/1,000 unresolved events and 300/1,800 seconds of ready age. They are deployment starting points, not WMS kernel SLOs. `--check` maps `ok`, `warning`, and `critical` to exit codes `0`, `1`, and `2`.

## Export cardinality

Prometheus exposition uses only:

- optional configured logical `consumer` scope;
- bounded state, quarantine kind, and attempt bucket;
- stable alert code and severity.

Event IDs, aggregate IDs, Position IDs, delivery metadata, worker IDs, raw errors, envelopes, and failure codes are forbidden as labels. Detailed diagnosis remains an explicit database or operator-tool query.

JSON preserves the same typed counts, age values, scope, and alert evidence.

## Read-only boundary

`read_domain_event_consumer_failure_telemetry()` is one statement-level aggregate. It takes no explicit row locks and never claims, delays, resolves, quarantines, replays, ACKs, or deletes an event.

Telemetry cannot repair state. The retry runtime and audited replay command from ADR 0032 remain the only mutation paths in this slice.

## Executable invariants

The PostgreSQL kernel lab proves:

1. ready, delayed, leased, and quarantined counts are mutually exclusive and exhaustive;
2. resolved rows are excluded from operational backlog;
3. consumer-scoped and global snapshots return the expected sets;
4. elapsed delays and expired leases become ready at a later observation time;
5. terminal and attempt-limit quarantine counts partition quarantine exactly;
6. attempt buckets partition unresolved backlog and expose the maximum attempt count;
7. oldest ages use one shared observation timestamp and return `NULL` for empty states;
8. quarantine produces a critical stable alert;
9. backlog and ready-age thresholds produce deterministic warning/critical alerts;
10. JSON and Prometheus expose aggregate evidence without event identity, failure code, worker, or error labels.

## Consequences

### Positive

- a stopped retry worker becomes visible through growing ready age and backlog;
- an unattended terminal or exhausted event produces an immediate critical signal;
- health remains broker-neutral after PostgreSQL accepts retry ownership;
- reconciliation invariants fail closed before inconsistent metrics are exported;
- one command supports human JSON inspection, Prometheus scraping, and monitoring exit codes.

### Costs and limits

- the aggregate reads current unresolved failure rows on every poll;
- optional consumer labels require a bounded deployment-controlled consumer set;
- default thresholds require deployment calibration;
- metrics identify the operational class, not the individual failed event;
- dashboards, hosted exporters, notification routing, bulk review, and malformed-message adapter quarantine remain later slices;
- resolved history leaves the live table only through the ADR 0034 consumer-scoped archive.
