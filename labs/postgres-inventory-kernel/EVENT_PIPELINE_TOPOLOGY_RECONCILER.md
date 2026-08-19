# Event pipeline topology reconciliation controller

## Purpose

The crash-safe provisioner can recover an interrupted JetStream topology rollout when the same apply command is run again after its PostgreSQL lease expires. This controller turns that recovery path into an unattended control loop without granting the loop authority to start new migrations.

The controller is deliberately **recovery-only**:

```text
journal has no active run
        -> IDLE

active run has a live lease
        -> DEFERRED

active run lease expired
        +
reviewed topology_migration still matches journal migration_id
        -> claim expired run
        -> inspect live JetStream
        -> deterministic crash recovery
```

It never interprets registry drift as permission to begin a rollout.

## Safety contract

The controller may claim only an already-existing non-terminal provisioning journal row whose lease has expired. It does not call the normal provisioner entry point that can create a new `prepared` run.

Before claiming an expired execution it requires the current reviewed `topology_migration.id` in `operations/event-pipelines/registry.yml` to equal the journaled migration ID. If the migration declaration is missing, invalid, or different, the controller stops before lease acquisition.

This creates an operational rule:

> Do not remove `topology_migration` from the registry until the corresponding provisioning journal execution is terminal.

A live lease is never stolen. If the previous owner renews between the health observation and the claim transaction, PostgreSQL lease arbitration wins and the controller defers.

## Recovery decisions

After claiming the expired run, recovery remains the same deterministic state machine introduced by ADR 0050:

```text
live topology == SOURCE
        -> finalize source restoration
        -> ROLLED_BACK

live topology == TARGET
        -> continue missing target verification
        -> fresh synthetic canary
        -> deployment readiness
        -> COMPLETED

state == ROLLBACK_STARTED and TARGET is still live
        -> restore persisted source values
        -> verify exact SOURCE
        -> ROLLED_BACK

live topology is neither SOURCE nor TARGET
        -> MANUAL_INTERVENTION
```

The controller does not guess, merge arbitrary broker configuration, or repair third-party topology drift.

## Run continuously

```bash
bin/run-kernel-event-pipeline-topology-reconciler \
  --pipeline inventory-position-projection \
  --tenant-id "$KERNEL_LAB_CANARY_TENANT_ID"
```

The default poll interval is 30 seconds. Runtime endpoints still come from the environment variable names declared by the deployment registry.

The process uses one stable owner ID for its lifetime. Set `KERNEL_TOPOLOGY_RECONCILER_OWNER` when the deployment platform already provides a stable instance identity.

Each tick writes one JSON record to stdout.

## One-shot mode

Schedulers, Kubernetes CronJobs, systemd timers, and operational tests may run one deterministic tick:

```bash
bin/run-kernel-event-pipeline-topology-reconciler \
  --pipeline inventory-position-projection \
  --tenant-id "$KERNEL_LAB_CANARY_TENANT_ID" \
  --once \
  --pretty
```

One-shot exit codes:

- `0` - idle, deferred because another live lease exists, or successfully reconciled;
- `2` - recovery is blocked or failed.

`DEFERRED` is not an error. It means another process still owns the valid single-writer lease.

## Fresh verification inputs

When recovery reaches the target path, the controller runs the same mandatory post-apply gates as manual recovery:

1. exact target topology inspection;
2. a fresh tracked synthetic canary;
3. full topology-aware deployment readiness.

The watchdog state file is re-read when readiness is evaluated instead of being cached for the lifetime of the controller process.

## Lease behavior

The recovery lease defaults to the greater of 120 seconds or canary timeout plus 60 seconds. The configured lease must exceed the canary timeout.

The controller first observes journal health read-only. Only an expired execution whose reviewed migration contract still matches can advance to `claim_active()`. The claim itself is transactional and re-checks lease ownership, so the initial observation is not treated as an authorization token.

## Observability

Use the read-only provisioning health inspector from ADR 0051 alongside the controller:

```bash
bin/inspect-kernel-event-pipeline-topology-provisioning \
  --pipeline inventory-position-projection \
  --check
```

That inspector exposes active state age, lease expiry, attempts, recovery handoffs, terminal history, and bounded Prometheus alerts. The controller output is an execution log, not a replacement for those metrics.

## Explicit non-goals

This controller does not:

- start a new topology migration;
- infer migration authorization from desired-state drift;
- remove or edit `topology_migration` in Git;
- bypass the single-writer lease;
- suppress `manual_intervention`;
- retry more frequently than the configured control-loop interval and lease arbitration permit;
- act as a deployment scheduler for normal topology changes.

New rollouts must still begin through the reviewed migration contract, dry-run preflight, and explicit authorized provisioning path.
