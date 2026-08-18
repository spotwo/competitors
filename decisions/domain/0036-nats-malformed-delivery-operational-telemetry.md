# ADR 0036: Observe malformed NATS delivery quarantine as recent activity plus retained evidence

- Status: accepted
- Date: 2026-08-19
- Scope: PostgreSQL inventory kernel consumer path
- Depends on: ADR 0035

## Context

ADR 0035 makes malformed JetStream delivery safe: trusted broker identity is persisted in PostgreSQL before ACK and no synthetic `event_id` enters Inbox state.

That durability creates a new operational boundary. A stopped or broken application may now accumulate poison evidence without any normal Domain Event identity, while old poison records remain useful forensic history.

Treating every retained poison record as an active backlog would be wrong. Malformed deliveries are terminal transport poison, not retry work. Once one record exists, a lifetime `count > 0` alert would remain permanently critical until retention deletes evidence.

## Decision

Expose one read-only PostgreSQL snapshot with two separate views of the same quarantine table:

```text
retained evidence
  -> inventory / forensic / retention signal

recent activity within an explicit lookback
  -> operational incident signal
```

The default lookback is 300 seconds and is an explicit query input bounded to seven days.

The snapshot may be global or scoped to one stable logical `consumer_name` and reports:

- retained poison record count;
- records first quarantined inside the lookback;
- records with `observation_count > 1` whose latest observation is inside the lookback;
- retained all-time observation total and maximum observations on one record;
- failure-kind counts for the four bounded ADR 0035 classes plus `other`;
- age of the oldest and newest retained record;
- age of the latest recorded observation;
- one shared observation timestamp and the configured lookback.

## Failure-kind cardinality

Prometheus never exports raw `failure_code` as a label. The SQL snapshot maps records to exactly five bounded kinds:

```text
invalid_encoding
invalid_json
invalid_envelope
invalid_headers
other
```

`other` keeps the partition total correct if a future runtime introduces another stable database code before telemetry is updated.

Stream names, durable consumer names, subjects, stream sequences, payload hashes, headers, previews, error text, and any invented event identity are excluded from metric labels.

The optional `consumer` label is deployment-controlled persisted application identity, the same cardinality boundary already used by consumer-failure telemetry.

## Alert policy

Default starting points are:

| Signal | Warning | Critical |
|---|---:|---:|
| newly quarantined records in lookback | 1 | 10 |
| recently reobserved records in lookback | 1 | 5 |

A retained historical record outside the lookback does not alert by itself.

Recent reobservation is deliberately a record count, not an exact recent-delivery count. ADR 0035 stores `first_seen_at`, `last_seen_at`, and cumulative `observation_count`, not an unbounded observation ledger. A record with a recent `last_seen_at` and count greater than one is therefore evidence of recent redelivery or ACK uncertainty without pretending to reconstruct every observation timestamp.

## Metrics

The snapshot exports bounded JSON and these Prometheus families:

- `spotwo_wms_malformed_delivery_retained_records`;
- `spotwo_wms_malformed_delivery_recent_records`;
- `spotwo_wms_malformed_delivery_recent_reobserved_records`;
- `spotwo_wms_malformed_delivery_observations`;
- `spotwo_wms_malformed_delivery_observations_max`;
- `spotwo_wms_malformed_delivery_failure_records{kind=...}`;
- `spotwo_wms_malformed_delivery_age_seconds{kind=...}`;
- `spotwo_wms_malformed_delivery_lookback_seconds`;
- `spotwo_wms_malformed_delivery_health_status`;
- `spotwo_wms_malformed_delivery_alert{code=...,severity=...}`;
- `spotwo_wms_malformed_delivery_snapshot_timestamp_seconds`.

## Safety boundary

The telemetry function is `STABLE` and read-only. Inspection cannot ACK, replay, delete, mutate, or synthesize a poison delivery.

The typed Python snapshot fails closed if failure-kind counts do not reconcile to retained count, cumulative observations are lower than retained records, recent counts exceed retained inventory, or age ordering is inconsistent.

## Consequences

- malformed-delivery incidents remain visible even after JetStream ACK removes broker pressure;
- historical forensic evidence does not create a permanent false alarm;
- repeated poison observations surface broker ACK uncertainty or consumer instability;
- failure metrics remain bounded even if stored evidence contains arbitrary subjects, headers, or error text;
- retention remains an independent lifecycle decision rather than hidden inside telemetry.

## Non-goals

This slice does not delete poison records, replay malformed payloads, repair envelopes, add a generic broker DLQ abstraction, or claim exact per-window observation counts. Poison retention and archive policy is the next independent lifecycle slice.
