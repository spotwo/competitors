# Event pipeline topology provisioning health

This runbook observes the crash-safe topology provisioning journal introduced by ADR 0050. It is deliberately read-only.

## Inspect one pipeline

```bash
bin/inspect-kernel-event-pipeline-topology-provisioning \
  --pipeline inventory-position-projection \
  --pretty \
  --check
```

The command resolves PostgreSQL through the environment variable named by the deployment registry. `--database-url` exists only as a lab/operator override.

Exit codes:

- `0` - healthy;
- `1` - warning;
- `2` - critical.

Prometheus output:

```bash
bin/inspect-kernel-event-pipeline-topology-provisioning \
  --pipeline inventory-position-projection \
  --format prometheus
```

## Current execution health

The inspector evaluates the active non-terminal run, if one exists.

Default thresholds:

```text
state age < 60 s       OK
state age >= 60 s      WARNING
state age >= 180 s     CRITICAL
expired lease          CRITICAL
```

Thresholds can be overridden for a lab invocation with `--warning-state-age-seconds` and `--critical-state-age-seconds`.

An expired lease is critical regardless of state age because no currently authorized writer owns progress anymore. The observer does not claim the lease or reconcile the run.

## Historical attention

The latest terminal execution remains visible:

```text
completed             OK
rolled_back           WARNING
failed                CRITICAL
manual_intervention   CRITICAL
```

This is useful for dashboards and operator follow-up. It does not block deployment readiness by itself. Readiness consumes only current active-execution health, so an old failure cannot deadlock a later reviewed recovery rollout.

## State age semantics

`state_changed_at` is maintained in PostgreSQL when the durable `state` field changes. Lease renewal, `updated_at`, attempt count, and recovery metadata do not reset state age.

This prevents a stuck execution from appearing fresh merely because some unrelated journal metadata was touched.

## What to do on alerts

For `topology_provisioning_execution_slow`, inspect the active state, lease remaining time, and the process responsible for the rollout. Do not start a second migration.

For `topology_provisioning_execution_stuck` or `topology_provisioning_lease_expired`, use the crash-recovery procedure in `EVENT_PIPELINE_TOPOLOGY_CRASH_RECOVERY.md`. Re-run the authorized apply command with the same reviewed migration ID after the old lease is reclaimable.

For `topology_provisioning_manual_intervention`, do not auto-repair. Compare the deployment registry, journal source snapshot, and live JetStream topology before deciding the corrective action.

For latest historical `failed` or `rolled_back`, determine whether a successful later migration superseded the incident. A later `completed` execution naturally clears latest-terminal attention.

## Safety boundary

The inspector performs only PostgreSQL reads. It never:

- acquires or renews a provisioning lease;
- transitions the state machine;
- creates, updates, or deletes JetStream resources;
- publishes, fetches, ACKs, or purges messages;
- runs a canary;
- performs rollback or reconciliation;
- edits the deployment registry.

Prometheus output excludes run IDs, migration IDs, lease-owner IDs, and arbitrary error strings from labels.