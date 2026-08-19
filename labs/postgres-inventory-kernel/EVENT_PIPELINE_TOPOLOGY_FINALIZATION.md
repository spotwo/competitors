# Event pipeline topology migration finalization

This runbook closes the bounded topology migration lifecycle after the reviewed target topology is live.

The finalization gate is read-only. It never edits `operations/event-pipelines/registry.yml`, never mutates JetStream, and never rewrites provisioning or intervention history.

## What is being finalized

During a reviewed topology rollout the deployment registry may temporarily contain both:

- the steady-state target under `topology`
- a bounded compatibility contract under `topology_migration.from_overrides`

The compatibility stanza allows the source topology to remain accepted while the migration is in progress. Once the target has been reached and verified, keeping that stanza forever would unnecessarily widen the accepted topology set.

Finalization answers one narrow question:

> Is it safe to remove `topology_migration` from the registry now?

## Finalization states

The inspector returns one of three bounded states:

- `finalized` - the registry already has no `topology_migration` stanza
- `ready` - the stanza exists and all evidence says it is safe to remove
- `blocked` - the stanza must remain until the reported blocker is resolved

`safe_to_remove_topology_migration` is the machine-readable boolean used by automation or a human reviewer.

## Required proof

A migration is `ready` for finalization only when all of these conditions hold:

1. the registry still contains a valid `topology_migration`
2. no topology provisioning execution is active
3. the latest terminal provisioning run belongs to the same migration ID
4. the rollout lineage is either:
   - a normal provisioning run in `completed`, or
   - a terminal `manual_intervention` run whose separate intervention record ended in `resolved_target`
5. live JetStream topology exactly matches the reviewed steady-state target
6. deployment readiness is `READY` when evaluated as the post-removal steady state, with `topology_migration=None`

The last rule is intentional. An expired migration contract can make current readiness degraded even when the exact target is already live. The finalization gate therefore evaluates the state that will exist after the compatibility stanza is removed, preventing the expired contract from blocking its own cleanup.

## What blocks finalization

Examples of bounded blocker codes include:

- `topology_migration_finalization_runtime_missing`
- `topology_migration_provisioning_active`
- `topology_migration_finalization_history_missing`
- `topology_migration_finalization_history_mismatch`
- `topology_migration_finalization_history_not_successful`
- `topology_migration_manual_intervention_resolution_missing`
- `topology_migration_manual_intervention_not_target`
- `topology_migration_source_still_live`
- `topology_migration_live_topology_not_target`
- `topology_migration_finalization_topology_unavailable`
- `topology_migration_finalization_readiness_unavailable`
- `topology_migration_finalization_readiness_not_ready`

A source topology that is still live is never treated as cleanup-ready. A rolled-back, failed, or source-resolved rollout cannot be finalized merely because someone later changed the broker to look like the target.

## Inspect JSON

```bash
bin/inspect-kernel-event-pipeline-topology-finalization \
  --pipeline inventory-position-projection \
  --database-url "$KERNEL_LAB_DATABASE_URL" \
  --nats-url "$KERNEL_LAB_NATS_URL" \
  --watchdog-state "$KERNEL_LAB_CANARY_WATCHDOG_STATE_PATH" \
  --pretty
```

Example successful result:

```json
{
  "status": "ready",
  "code": "topology_migration_finalization_ready",
  "safe_to_remove_topology_migration": true,
  "lineage": "completed",
  "live_match": "target",
  "readiness_status": "ready",
  "registry_change": {
    "operation": "remove_topology_migration",
    "pipeline": "inventory-position-projection",
    "safe": true
  }
}
```

The JSON describes the permitted Git change but does not perform it.

## Use as a gate

```bash
bin/inspect-kernel-event-pipeline-topology-finalization \
  --pipeline inventory-position-projection \
  --database-url "$KERNEL_LAB_DATABASE_URL" \
  --nats-url "$KERNEL_LAB_NATS_URL" \
  --watchdog-state "$KERNEL_LAB_CANARY_WATCHDOG_STATE_PATH" \
  --check
```

Exit codes:

- `0` - already finalized or safe to remove the stanza
- `2` - blocked

The current lab registry has no migration stanza, so the command returns `finalized` without needing PostgreSQL or NATS. That makes the steady state idempotent.

## Prometheus

```bash
bin/inspect-kernel-event-pipeline-topology-finalization \
  --pipeline inventory-position-projection \
  --format prometheus
```

Metrics are deliberately bounded:

- `spotwo_wms_event_pipeline_topology_migration_removal_safe{pipeline}`
- `spotwo_wms_event_pipeline_topology_migration_finalization_state{pipeline,state}`
- `spotwo_wms_event_pipeline_topology_migration_finalization_blocked{pipeline,code}`

Migration IDs, run IDs, intervention IDs, operator identities, runtime URLs, and exception text are not labels.

## Git finalization workflow

When the gate returns `ready`:

1. remove only the reviewed pipeline's `topology_migration` stanza from `operations/event-pipelines/registry.yml`
2. do not change the steady-state `topology` values in the same cleanup unless a separate reviewed change requires it
3. run the registry validator and kernel lab CI
4. after merge, run deployment readiness again without the migration compatibility contract

When the gate returns `blocked`, keep the stanza and resolve the reported operational state first.

## Safety boundaries

This slice does not:

- edit Git or create a pull request automatically
- mutate JetStream
- reopen terminal provisioning journal rows
- reinterpret `resolved_source` as success
- infer success from live topology without matching governed provisioning lineage
- bypass canary, watchdog, SLI, or readiness gates
- delete historical provisioning or manual-intervention evidence
