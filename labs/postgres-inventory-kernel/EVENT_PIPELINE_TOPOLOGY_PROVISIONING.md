# Authorized event-pipeline topology provisioning

This runbook executes one reviewed, single-resource JetStream topology migration after the dry-run preflight has proven the live broker is exactly at the declared source state.

Unlike the topology inspector and preflight planner, this command is intentionally **mutating**. It is narrow by design and is not a general JetStream administration tool.

## Prerequisites

Before applying anything:

1. Merge the reviewed steady-state `topology` target and bounded `topology_migration` into the deployment registry.
2. Keep the normal Outbox publisher runtime running.
3. Keep the dedicated event-pipeline canary consumer running.
4. Provide the registry-named PostgreSQL and NATS runtime endpoints.
5. Provide the external canary watchdog state file required by deployment readiness.
6. Provide a tenant UUID for the mandatory fresh synthetic canary.
7. Confirm the migration deadline is still active.
8. Run the dry-run planner and require `safe_to_apply`.

The provisioner does not create missing streams or consumers, does not repair arbitrary drift, and does not infer destructive recreate strategies.

## Step 1 - Run preflight

```bash
bin/plan-kernel-event-pipeline-topology \
  --pipeline inventory-position-projection \
  --pretty \
  --check
```

Proceed only when the decision is:

```text
safe_to_apply
```

A blocked preflight means no broker mutation should be attempted.

## Step 2 - Apply the exact reviewed migration

The apply command requires the exact `topology_migration.id` as an explicit authorization token:

```bash
bin/apply-kernel-event-pipeline-topology \
  --pipeline inventory-position-projection \
  --apply-migration business-ack-window-10-to-20 \
  --tenant-id 00000000-0000-0000-0000-000000000001 \
  --pretty
```

For production use, prefer the runtime environment variables named by `operations/event-pipelines/registry.yml` instead of URL flags.

The supplied migration ID must exactly equal the currently reviewed registry value. A stale operator command cannot silently authorize a newer migration.

## Execution sequence

The provisioner performs the following gates in order:

```text
exact migration-id authorization
        ↓
preflight = safe_to_apply
        ↓
current deployment readiness = READY
        ↓
re-read source resource and compare with preflight snapshot
        ↓
one in-place resource update
        ↓
exact target topology verification
        ↓
fresh synthetic canary = OK
        ↓
full deployment readiness = READY
        ↓
SUCCESS
```

The compare-before-write check closes the gap between preflight and mutation. If the bounded source resource changed after preflight, the provisioner stops before writing.

## Initial supported in-place fields

| Resource | Fields |
| --- | --- |
| `stream` | `replicas`, `duplicate_window_seconds` |
| `business_consumer` | `max_ack_pending`, `max_deliver` |
| `canary_consumer` | `max_ack_pending`, `max_deliver` |

All changed fields for one supported resource are applied together using a copy of the complete current JetStream config object.

The following are intentionally **not** automatically applied in this slice:

- stream subjects;
- stream storage;
- stream retention;
- consumer pull/push mode;
- consumer delivery policy;
- ACK policy;
- filter subjects;
- any delete/recreate transition.

If preflight identifies one of those fields, the provisioner returns `topology_mutation_not_supported` without mutation.

## Fresh canary requirement

After exact target verification the provisioner starts a new tracked synthetic canary using the canonical pipeline identities.

The canary requires the normal Outbox publisher and dedicated canary consumer runtimes to be active. The provisioner does not start those long-running runtimes itself.

A canary status other than `ok` is a failed post-apply gate and triggers rollback.

## Rollback

Immediately before changing the resource, the provisioner keeps the complete live JetStream configuration in memory.

If target verification, the fresh canary, or post-apply readiness fails after mutation begins:

```text
post-apply gate fails
        ↓
restore captured source resource config
        ↓
inspect live topology again
        ↓
require migration contract matched = source
```

A verified restore returns terminal status `rolled_back`.

If rollback itself fails, or source topology cannot be proven after rollback, terminal status is `failed`. Stop automation and inspect live topology manually. Do not blindly retry the apply command because the broker state is no longer safely proven.

## Terminal outcomes

- `succeeded` - target is exact, a fresh canary is OK, and full readiness is READY.
- `blocked` - authorization/preflight/capability/pre-readiness stopped execution before mutation.
- `rolled_back` - mutation occurred, a post-apply gate failed, and the exact source state was restored and verified.
- `failed` - the final broker state cannot be safely proven by the provisioner.

The CLI exits `0` only for `succeeded`; all other terminal states exit `2`.

## Important bounded failure codes

Common codes include:

- `topology_migration_required`;
- `topology_migration_authorization_mismatch`;
- `topology_mutation_not_supported`;
- `topology_preapply_readiness_not_ready`;
- `topology_source_changed_before_apply`;
- `topology_source_resource_missing`;
- `topology_mutation_failed`;
- `topology_target_verification_failed`;
- `topology_postapply_canary_not_ok`;
- `topology_postapply_readiness_not_ready`;
- `topology_rollback_failed`;
- `topology_rollback_verification_failed`.

Raw exception text is not part of the machine-readable outcome contract.

## Registry cleanup after success

A successful report sets:

```json
{
  "cleanup": {
    "remove_topology_migration_contract": true
  }
}
```

The provisioner deliberately does **not** edit Git or the deployment registry. Remove `topology_migration` in a normal reviewed repository change after success. The steady-state topology inspector will then return to strict target-only matching.

## Crash boundary

JetStream mutation, canary execution, readiness evaluation, and rollback cannot be one distributed transaction.

If the provisioner process or host dies after the broker mutation, inspect live topology against the still-reviewed source/target migration contract before doing anything else:

- exact source - the change did not complete or was restored;
- exact target - continue target verification before registry cleanup;
- neither - stop automated rollout and investigate.

This slice does not yet provide a crash-safe external execution journal or lease. That is the next reliability boundary for an unattended topology controller.
