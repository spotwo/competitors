# Event pipeline topology rollout preflight

Use this runbook before an external provisioner changes the JetStream topology declared in `operations/event-pipelines/registry.yml`.

The preflight is a dry run. It reads registry state and JetStream metadata, builds a deterministic plan, and never changes the broker.

## Command

```bash
bin/plan-kernel-event-pipeline-topology \
  --pipeline inventory-position-projection \
  --pretty \
  --check
```

The NATS URL comes from the environment variable named by `runtime.nats_url_env`. For a lab, `--nats-url` may override it.

Exit codes with `--check`:

```text
0  preflight status ok
1  warning - topology apply is not required, but cleanup is required
2  rollout blocked
```

## Decision model

The JSON result has both `status` and `decision`.

```text
safe_to_apply
  An active reviewed migration exists.
  Live topology exactly matches the declared source.
  Required changes affect exactly one JetStream resource.

no_changes
  The target is already deployed.
  There may still be verification or migration-contract cleanup steps.

blocked
  Do not begin topology mutation from this state.
```

The result also declares:

```json
{
  "execution": {
    "dry_run": true,
    "mutates": false,
    "external_provisioner_required": true
  }
}
```

`external_provisioner_required` is true only when the decision is `safe_to_apply`.

## Expected rollout shape

Example migration:

```yaml
topology:
  stream:
    subjects:
      - spotwo.wms.events.>
    storage: file
    retention: limits
    replicas: 3
    duplicate_window_seconds: 120
  business_consumer:
    delivery_mode: pull
    deliver_policy: all
    ack_policy: explicit
    filter_subject: spotwo.wms.events.inventory.position.changed
    max_ack_pending: 10
    max_deliver: 3
  canary_consumer:
    delivery_mode: pull
    deliver_policy: all
    ack_policy: explicit
    filter_subject: spotwo.wms.events.health_check.ping
    max_ack_pending: 10
    max_deliver: 3

topology_migration:
  id: replicas-1-to-3
  valid_until: "2026-08-19T12:00:00Z"
  from_overrides:
    stream:
      replicas: 1
```

When the live stream has one replica and the deadline is active, preflight returns a change like:

```json
{
  "resource": "stream",
  "field": "replicas",
  "current": 1,
  "target": 3
}
```

and the ordered steps are:

```text
1. apply_target_resource
2. verify_exact_target
3. verify_event_pipeline_readiness
4. remove_topology_migration_contract
```

`apply_target_resource` has `requires_atomic_resource_update: true`. That means all declared field changes for that resource should be applied as one provisioner operation from the control-plane perspective. The planner does not mutate the resource itself and does not claim that every JetStream field supports an in-place update.

## Multi-resource changes

Do not use one migration for changes to more than one of:

```text
stream
business_consumer
canary_consumer
```

Preflight blocks such a plan with:

```text
topology_migration_requires_single_resource
```

Why: ADR 0047 accepts only the exact source or exact target. A sequential change across two resources would produce a hybrid intermediate topology and the readiness gate would correctly report it as drift.

Split the desired change into multiple migrations. Finish, verify, and clean up the first migration before declaring the next.

## Blocked states

`topology_migration_required`

The live topology differs from target but no reviewed migration contract exists. The JSON still includes the target delta so the change can be reviewed and declared.

`topology_migration_expired`

The migration deadline passed while the source topology is still deployed. Extend or replace the migration only through normal review. Do not treat the old declaration as authorization.

`topology_live_state_unexpected`

The broker matches neither the declared source nor target. Investigate drift before applying anything else.

`topology_migration_requires_single_resource`

The source-to-target delta spans multiple JetStream resources. Split the rollout.

## Target already reached

If the target is already deployed while the migration is active, preflight returns:

```text
decision = no_changes
code = topology_migration_target_reached
```

Only these steps remain:

```text
verify_exact_target
verify_event_pipeline_readiness
remove_topology_migration_contract
```

If the target is already deployed but the migration deadline has expired, the result is warning-level:

```text
code = topology_migration_cleanup_required
```

The broker does not need another topology change. The stale migration declaration needs to be removed after verification.

## Verification

After an external provisioner applies a `safe_to_apply` plan, run:

```bash
bin/inspect-kernel-event-pipeline-topology \
  --pipeline inventory-position-projection \
  --pretty \
  --check
```

Then run the full deployment readiness gate:

```bash
bin/inspect-kernel-event-pipeline-readiness \
  --pipeline inventory-position-projection \
  --pretty \
  --check
```

Only after exact target topology and operational readiness are healthy should the migration declaration be removed.

## Safety boundary

The preflight does not create, update, delete, publish, fetch, ACK, purge, edit registry files, or perform rollback. It is a control-plane planning and blocking decision only.
