# Event pipeline topology provisioning crash recovery

This runbook covers the crash-safe execution journal and reconciliation path used by `bin/apply-kernel-event-pipeline-topology`.

## Normal execution

Run the same explicitly authorized command introduced by the topology provisioner:

```bash
bin/apply-kernel-event-pipeline-topology \
  --pipeline inventory-position-projection \
  --apply-migration <exact-reviewed-migration-id> \
  --tenant-id <tenant-uuid> \
  --pretty
```

The command now creates a PostgreSQL execution-journal row before the first JetStream mutation. JSON output includes `execution_journal` with the run ID, durable state, attempt count, recovery count, and lease metadata.

The default lease TTL is the larger of 120 seconds or the configured canary timeout plus 60 seconds. A custom value may be supplied with `--lease-seconds`, but it must exceed the canary timeout.

## State machine

Forward progress:

```text
prepared
  ↓
applying
  ↓
applied
  ↓
target_verified
  ↓
canary_verified
  ↓
readiness_verified
  ↓
completed
```

Rollback:

```text
rollback_started
  ↓
source_verified
  ↓
rolled_back
```

`failed` and `manual_intervention` are terminal.

## What to do after the process dies

Do not create a new migration ID and do not manually delete the journal row.

After the existing lease expires, run the **same** command again with the **same** reviewed migration ID. A new process owner claims the existing run ID and compares live JetStream topology with the reviewed source and target.

The possible recovery outcomes are:

### Live topology is source

The run is finalized as `rolled_back` with code `topology_recovery_source_live`.

This covers both cases that are intentionally indistinguishable after a crash:

- the process died before the broker mutation happened;
- the process restored source but died before recording rollback completion.

No broker mutation is needed during this reconciliation.

### Live topology is target

The provisioner resumes forward verification.

It does not repeat already durable verification stages. For example, if the journal already reached `canary_verified`, recovery proceeds to readiness rather than generating an unnecessary second canary.

A run recovered before `target_verified` first records that the reviewed target is live. It then runs the required fresh canary and deployment-readiness checks before `completed`.

### Journal says rollback started and target is still live

The provisioner reconstructs the rollback from the persisted source topology snapshot, changes only the allowlisted fields owned by the migration, and then verifies the complete source contract.

### Live topology is neither source nor target

The run terminates as `manual_intervention` with code:

```text
topology_recovery_manual_intervention_required
```

Do not re-run automatic mutation until the third topology is understood. The provisioner intentionally refuses to guess which fields should win.

## Concurrent provisioners

Only one active journal row is allowed per pipeline.

If another process owns a live lease, the command returns:

```text
topology_provisioning_lease_held
```

Wait for that process to finish or for its lease to expire. Do not shorten the lease in PostgreSQL during normal operation.

If a different migration ID is supplied while an earlier run remains active, the command returns:

```text
topology_provisioning_active_migration_conflict
```

Finish or reconcile the original run first.

## Lease loss during execution

Lease loss is fail-stop. Once a process no longer owns a valid lease, it must not perform rollback or another broker mutation because a different provisioner may already have reclaimed the run.

The old process returns a provisioning-journal error. The new owner performs reconciliation after comparing live topology.

## Inspecting execution history

The journal is operational data in PostgreSQL:

```sql
SELECT
  run_id,
  pipeline_id,
  migration_id,
  resource,
  state,
  state_code,
  attempt_count,
  recovery_count,
  lease_owner,
  lease_expires_at,
  started_at,
  updated_at
FROM kernel_lab.event_pipeline_topology_provisioning_runs
WHERE pipeline_id = 'inventory-position-projection'
ORDER BY started_at DESC;
```

Do not use the journal as desired-state configuration. The deployment registry and its `topology` / `topology_migration` contract remain the reviewed source of desired topology.

## Successful completion

A successful normal or recovered run still requires the usual cleanup: remove the completed `topology_migration` declaration through a reviewed Git change after the target is live and readiness is healthy.

The provisioner does not edit the registry automatically.
