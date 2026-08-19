# ADR 0049: Authorized event-pipeline topology provisioning

Status: Accepted

## Context

ADR 0046 made deployed JetStream topology observable against a canonical registry. ADR 0047 added a bounded source-to-target migration contract. ADR 0048 added a deterministic dry-run preflight that can identify a single-resource transition as `safe_to_apply`.

The remaining gap is execution. An operator or agent could still translate a correct preflight into an unsafe broker change, apply a different migration than the one reviewed, overwrite unrelated JetStream configuration, continue when the pipeline is already degraded, or leave a failed target deployed without a deterministic rollback attempt.

We need a mutating path, but it must be narrower than a general JetStream administrator.

## Decision

Add an authorized topology provisioner that may perform exactly one in-place JetStream resource update only when all of the following are true:

1. the registry contains a `topology_migration`;
2. the caller explicitly supplies the exact reviewed `topology_migration.id`;
3. rollout preflight returns `safe_to_apply`;
4. the transition affects exactly one JetStream resource;
5. every changed field is in the provisioner's explicit in-place capability allowlist;
6. full event-pipeline readiness is `READY` before mutation;
7. a fresh read immediately before mutation still matches the source resource snapshot observed by preflight.

The initial in-place capability allowlist is deliberately small:

| Resource | Supported fields |
| --- | --- |
| stream | `replicas`, `duplicate_window_seconds` |
| business consumer | `max_ack_pending`, `max_deliver` |
| canary consumer | `max_ack_pending`, `max_deliver` |

Changes to stream subjects, storage, retention, consumer delivery mode, delivery policy, ACK policy, or filter subjects are not automatically recreated or emulated. They remain blocked until a separate migration design proves a safe transition.

## Mutation method

The provisioner reads the live JetStream resource immediately before mutation and compares its complete bounded topology snapshot with the source snapshot from preflight. If it changed, execution stops with `topology_source_changed_before_apply`.

For an authorized update, the provisioner deep-copies the complete live JetStream config object, changes only the allowed target fields, and calls the native in-place update operation for that resource. It does not synthesize a new config from only the registry fields. This preserves unrelated server/client configuration that is outside the current topology contract.

The original full live config object is retained in memory as the rollback target for the duration of the execution.

## Post-apply verification

A successful broker API response is not sufficient. After mutation, the provisioner requires this sequence:

```text
exact target topology
        ↓
fresh synthetic canary = OK
        ↓
full event-pipeline readiness = READY
        ↓
SUCCESS
```

The fresh canary uses the existing tracked event-pipeline canary path and therefore exercises Outbox -> JetStream -> dedicated canary consumer -> Inbox/projection -> ACK confirmation. The normal publisher and canary-consumer runtimes must already be running.

The readiness check uses the canonical deployment registry, current topology contract, PostgreSQL signals, external watchdog state, and canary SLI state.

## Rollback

If mutation has started and target verification, fresh canary, or post-apply readiness fails, the provisioner attempts an in-place rollback of the same resource using the exact full live config captured immediately before mutation.

Rollback is not considered complete until a fresh topology inspection reports `matched = source` under the still-active migration contract.

Possible terminal outcomes are:

- `succeeded` - target applied, exact target verified, fresh canary OK, readiness READY;
- `blocked` - no mutation started because authorization, preflight, capability, or pre-readiness failed;
- `rolled_back` - mutation started, a post-apply gate failed, source config was restored and verified;
- `failed` - mutation/rollback state is not safely proven and operator intervention is required.

No raw exception text is part of the machine contract. Failures use bounded codes.

## No automatic recreate

The provisioner never falls back from an unsupported or rejected update to delete/recreate. Deleting and recreating a stream or durable consumer can alter message retention, consumer position, pending delivery state, ACK floors, or delivery semantics. Such migrations need a separate explicit protocol and evidence.

## No automatic Git cleanup

A successful run reports that `topology_migration` cleanup is required, but it does not edit the registry or Git history. The migration declaration is removed through a normal reviewed repository change after the target is proven healthy.

## Failure boundary

JetStream configuration updates and the surrounding health checks are not one distributed transaction. A process or host failure can occur after the broker mutation and before rollback completes. The operator must then inspect live topology against the migration contract before any further action.

This ADR intentionally does not claim crash-safe execution journaling. A future slice may add a durable external execution journal/lease so another controller can detect and resume or safely reconcile an interrupted provisioning attempt.

## Consequences

The system now has a narrow mutation path from reviewed desired state to broker state while keeping exact-source/exact-target migration semantics, pre/post health gates, fresh synthetic verification, source compare-before-write, and deterministic rollback behavior.

The tradeoff is that some legitimate JetStream changes remain intentionally manual because the provisioner refuses to infer destructive recreate strategies.