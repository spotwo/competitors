# ADR 0051: Observe topology provisioning execution health

- Status: accepted
- Date: 2026-08-19

## Context

ADR 0050 made event-pipeline topology provisioning crash-safe by persisting an execution journal before the first JetStream mutation and protecting each active run with a single-writer lease.

That solves recovery correctness, but it creates a new operational object that can itself become unhealthy. A process can disappear while a run remains in `prepared`, `applying`, `applied`, a verification state, or a rollback state. Operators and future unattended reconciliation need a bounded, read-only way to distinguish a healthy in-flight rollout from an expired lease or an execution that has stopped progressing.

Using `updated_at` alone is not sufficient because lease renewal and other metadata updates can move that timestamp without changing the durable state machine. We need explicit state-entry age.

## Decision

Add `state_changed_at` to `event_pipeline_topology_provisioning_runs` and maintain it with a PostgreSQL trigger whenever `state` changes.

Add a read-only provisioning-health collector and policy that expose:

- the active non-terminal execution, if any;
- its bounded state and affected resource;
- time spent in the current durable state;
- lease remaining time and whether the lease is expired;
- durable attempt and recovery counts;
- the latest terminal execution;
- bounded state counts across stored execution history.

The default policy classifies current execution health as:

- `ok` when there is no active execution or its state age is below the warning threshold and its lease is live;
- `warning` when the active execution remains in one durable state beyond the warning threshold;
- `critical` when its lease has expired or its state age exceeds the critical threshold.

The standalone inspector also surfaces the latest terminal execution as operational attention:

- latest `completed` -> `ok`;
- latest `rolled_back` -> `warning`;
- latest `failed` -> `critical`;
- latest `manual_intervention` -> `critical`.

Historical terminal attention does **not** participate in deployment readiness. Readiness consumes only the current active-execution health. Otherwise an old failed or rolled-back migration could permanently prevent a later reviewed recovery migration from becoming ready.

## Read-only boundary

The health collector must not:

- claim or renew a provisioning lease;
- change journal state;
- mutate JetStream;
- run a canary;
- alter the deployment registry;
- perform reconciliation or rollback.

It may only read the journal table and derive bounded health signals.

## Metrics cardinality

Prometheus labels are limited to bounded dimensions such as pipeline, durable state, resource, status, severity, and bounded alert code.

Do not label metrics with:

- run IDs;
- migration IDs;
- lease-owner IDs;
- arbitrary state-code text;
- exception messages.

Those identities remain available in JSON inspection output for operator correlation.

## Consequences

A future unattended reconciler can use the same journal while monitoring independently detects expired or stuck executions. Recovery remains an explicit mutation path; observation never becomes auto-remediation by accident.

The first implementation uses generic state-age thresholds. State-specific SLOs can be introduced later if operational evidence shows materially different expected durations by state.