# ADR 0050: Crash-safe event pipeline topology provisioning

## Status

Accepted.

## Context

ADR 0049 introduced an explicitly authorized provisioner for one reviewed, in-place JetStream topology mutation. It verifies preflight safety, deployment readiness, the exact target, a fresh synthetic canary, and post-apply readiness, and it restores the captured source configuration when a post-apply gate fails.

That protocol was process-safe but not yet crash-safe. A process can disappear after JetStream accepts the mutation and before the next verification or rollback step is recorded. A second process must not guess whether the previous process mutated the broker, and two provisioners must never concurrently act on one pipeline.

The broker is the source of truth for live topology, while PostgreSQL is already the durable operational data store available to the deployment command. We need enough durable execution intent to reconcile a crash without turning the journal itself into a second topology authority.

## Decision

Persist each mutating topology rollout in `kernel_lab.event_pipeline_topology_provisioning_runs` before the first broker write.

The durable execution state machine is:

```text
PREPARED
   ↓
APPLYING
   ↓
APPLIED
   ↓
TARGET_VERIFIED
   ↓
CANARY_VERIFIED
   ↓
READINESS_VERIFIED
   ↓
COMPLETED
```

Rollback uses:

```text
ROLLBACK_STARTED
   ↓
SOURCE_VERIFIED
   ↓
ROLLED_BACK
```

Exceptional terminal states are `FAILED` and `MANUAL_INTERVENTION`.

The journal stores only the reviewed execution identity and recovery material:

- pipeline ID;
- exact migration ID;
- affected resource;
- the source topology snapshot;
- the bounded source-to-target changes;
- a SHA-256 intent fingerprint;
- state and bounded state code;
- lease owner and expiry;
- attempt and recovery counts;
- timestamps.

It does not store NATS credentials, database credentials, canary payloads, or arbitrary exception strings.

## Lease and single-writer rule

At most one non-terminal journal row may exist per pipeline. A partial unique index enforces this in PostgreSQL.

Each active row has a lease owner and lease expiry. A second provisioner:

- is blocked while another owner's lease is live;
- may reclaim the same run after the lease expires;
- may not replace an active run with a different migration ID;
- continues the same run ID rather than creating a second history row.

Lease loss is fail-stop. A process that no longer owns a valid lease must not perform a compensating broker write, because another process may already be reconciling the run.

The command chooses a lease TTL longer than the configured synthetic-canary timeout.

## Recovery is based on live topology, not the last journal state alone

After reclaiming an expired run, the provisioner re-inspects JetStream and compares the complete deployment contract.

If the live deployment matches the **source**, the run is closed through `ROLLBACK_STARTED -> SOURCE_VERIFIED -> ROLLED_BACK`. This is safe whether the original process crashed before mutation or after a rollback that it did not manage to record.

If the live deployment matches the **target**, the provisioner resumes forward verification from the durable state:

- a run before `TARGET_VERIFIED` advances to target verification;
- after target verification, a fresh canary is required unless the journal already records `CANARY_VERIFIED`;
- after canary verification, deployment readiness is required unless the journal already records `READINESS_VERIFIED`;
- `READINESS_VERIFIED` may finalize as `COMPLETED` after the target is re-confirmed live.

If recovery had already entered `ROLLBACK_STARTED` while the target is still live, the provisioner restores the source values from the durable source snapshot and verifies the complete source contract.

If live topology matches neither exact source nor exact target, the run becomes `MANUAL_INTERVENTION`. The provisioner does not guess, merge, delete/recreate, or auto-repair a third topology.

## Snapshot-based recovery rollback

The crash journal does not serialize the complete nats-py configuration object. The source topology snapshot is sufficient because ADR 0049 allows only a narrow set of in-place fields and requires exact source comparison immediately before mutation.

On recovery rollback, the provisioner:

1. proves the whole deployment currently matches the reviewed target;
2. re-reads the current resource configuration;
3. changes only the allowlisted fields back to their persisted source values;
4. writes the resource in place;
5. verifies that the whole deployment matches the source contract.

This preserves any configuration fields that the provisioner never owned while still providing deterministic recovery for the fields it did own.

## Preflight versus recovery

A fresh rollout still requires `preflight = safe_to_apply` and pre-apply readiness `READY`.

Recovery is checked **before** the fresh-rollout preflight decision is enforced. This is necessary because a crashed rollout can leave the broker already at the target, in which case a new dry-run correctly reports `no_changes`. The active journal row is what proves that the target state belongs to an unfinished authorized execution and may be reconciled.

The caller must still provide the exact current `topology_migration.id`; the journal never authorizes a different migration.

## Consequences

A process crash no longer leaves an ambiguous topology rollout with no durable owner or recovery path. Re-running the same authorized apply command after lease expiry either completes the verified target, confirms/restores the source, or stops at manual intervention.

The design intentionally does not attempt distributed consensus with JetStream. PostgreSQL serializes provisioner ownership, and JetStream remains authoritative for observed live topology.

The execution journal is operational history, not a desired-state registry. Successful completion still requires removing the now-obsolete `topology_migration` declaration through the normal reviewed Git workflow.

## Not included

This decision does not add automatic scheduler retries, a deployment controller loop, automatic Git changes, multi-resource migrations, delete/recreate migrations, or arbitrary drift repair. Those are separate reliability and orchestration decisions.
