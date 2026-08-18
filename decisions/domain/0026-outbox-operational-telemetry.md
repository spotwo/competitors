# ADR 0026 - Outbox Operational Telemetry and Alerting

Status: Accepted

Date: 2026-08-18

## Context

ADR 0025 bounded publication retries and introduced poison-event quarantine. The kernel can now stop a permanently failing event, but an operator still needs to know that backlog or quarantine exists.

Publisher cycle logs are insufficient. If every publisher process is down, no cycle runs and no cycle metric reports the growing backlog. Broker dashboards are also insufficient because an event may be delayed or quarantined before the broker confirms it.

The observability boundary must therefore start from PostgreSQL, which remains authoritative for Outbox publication state, and it must remain independent of the selected Transport.

## Decision

Add one read-only PostgreSQL snapshot function, a typed Python health model, and JSON/Prometheus inspection through `bin/inspect-kernel-outbox`.

The snapshot uses one explicit observation timestamp so counts and ages describe the same instant.

## Mutually exclusive delivery states

Every row with `published_at IS NULL` is classified in precedence order:

```text
quarantined_at present
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

The same backlog is independently partitioned into attempt-count buckets `0`, `1`, `2_to_4`, and `5_plus`. The typed snapshot rejects either partition if it does not sum to total backlog.

## Age gauges

The snapshot reports nonnegative seconds for:

- the oldest unpublished event by `recorded_at`;
- the oldest currently ready event by `recorded_at`;
- the oldest quarantine entry by `quarantined_at`.

An empty state returns `NULL`, not zero, because zero would incorrectly imply that an event exists and is new.

Ready age is the primary publisher-lag signal. Backlog age includes intentionally delayed and quarantined rows, so it is diagnostic context rather than an availability signal by itself.

## Alert evaluation

The default health policy emits stable alert codes for:

- any quarantined event, critical;
- backlog count at configurable warning/critical thresholds;
- oldest ready age at configurable warning/critical thresholds.

Default thresholds are 1,000/10,000 backlog events and 60/300 seconds of ready age. These values are deployment starting points and are explicitly not WMS kernel SLOs.

Overall status is the highest active severity: `ok`, `warning`, or `critical`. `--check` maps those states to process exit codes `0`, `1`, and `2` for monitoring systems.

## Export formats

JSON preserves the typed nested snapshot and alert evidence.

Prometheus exposition uses bounded labels only:

- delivery `state`;
- attempt `bucket`;
- stable alert `code` and `severity`.

Event IDs, aggregate IDs, worker IDs, event types, and error messages are forbidden as metric labels. They would create unbounded cardinality and could leak operational payload context.

This slice does not require an OpenTelemetry SDK or a running Prometheus server. The stable snapshot and exposition format can later be hosted by a sidecar, service endpoint, or collector integration.

## Read-only boundary

`read_domain_event_outbox_telemetry()` is one statement-level aggregate. It takes no explicit row locks and never calls claim, ACK, NACK, quarantine, or replay functions.

Telemetry cannot repair state. Quarantine replay remains the explicit audited operator action from ADR 0025.

## Executable invariants

The PostgreSQL kernel lab proves:

1. ready, delayed, leased, and quarantined counts are mutually exclusive and exhaustive;
2. published rows are excluded from backlog;
3. expired leases and elapsed delays become ready at a later observation time;
4. attempt buckets are exhaustive and expose the maximum attempt count;
5. oldest ages use the shared observation timestamp and return `NULL` for empty states;
6. any quarantine entry creates a critical stable alert;
7. backlog and ready-age thresholds produce deterministic warning/critical alerts;
8. JSON and Prometheus outputs contain the same state values;
9. Prometheus labels remain bounded and contain no event identity or error text.

## Consequences

### Positive

- a stopped publisher becomes observable from growing ready age/backlog;
- poison events create an immediate machine-readable critical state;
- monitoring is transport-independent and still works before broker acceptance;
- snapshot consistency is machine-validated before export;
- the same command supports human JSON inspection and Prometheus scraping.

### Costs and limits

- the aggregate reads the current unpublished set on every poll;
- default thresholds require deployment calibration;
- metrics identify the class of failure, not the individual event;
- hosted exporters, OpenTelemetry spans, dashboards, notification routing, retention, and materialized counters remain later operational slices.
