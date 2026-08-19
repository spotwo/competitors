# ADR 0054: Gate topology migration finalization on governed target evidence

- Status: Accepted
- Date: 2026-08-19

## Context

ADR 0048 introduced bounded topology migration contracts so source and target JetStream configurations could both be accepted during a reviewed rollout. ADRs 0049-0052 added guarded provisioning, crash-safe journaling, provisioning health, and unattended recovery. ADR 0053 added an explicit two-person workflow for terminal states that require manual intervention.

Those decisions define how to move from source to target safely, but they leave one lifecycle step informal: deciding when the temporary `topology_migration` compatibility stanza can be removed from the deployment registry.

Removing it too early can make the registry reject a source topology that is still legitimately live. Leaving it forever weakens desired-state enforcement because two topologies remain accepted after the rollout has ended.

## Decision

Add a read-only topology migration finalization gate.

The gate returns:

- `finalized` when no migration stanza exists
- `ready` when the migration stanza exists and is safe to remove
- `blocked` otherwise

`safe_to_remove_topology_migration` is the canonical machine-readable cleanup decision.

A migration is `ready` only when all of the following are true:

1. no provisioning execution is active
2. the latest terminal provisioning run belongs to the migration currently declared in the registry
3. governed rollout lineage proves target completion through either:
   - provisioning state `completed`, or
   - provisioning state `manual_intervention` with a separate intervention ending in `resolved_target`
4. live JetStream topology exactly matches the reviewed target
5. deployment readiness is `READY` when evaluated against the post-removal steady state, with the migration compatibility contract omitted

The gate must fail closed when runtime evidence is missing or unavailable.

## Post-removal readiness

The final readiness check intentionally evaluates `topology_migration=None`.

An expired compatibility contract can itself produce a topology warning even after the exact target is live. Requiring readiness with the expired contract still installed would create a cleanup deadlock. The finalization gate therefore evaluates the state that will exist after removal, but only after independently proving that live topology exactly matches the target.

## Governed lineage

Live topology alone is insufficient evidence for finalization.

A `rolled_back`, `failed`, or `resolved_source` rollout remains blocked even if the broker is later changed out of band to resemble the target. A new governed rollout or explicit reviewed recovery is required to establish valid target lineage.

This preserves the distinction between observed configuration and authorized migration history.

## Manual intervention

A provisioning run in terminal state `manual_intervention` can be finalized only if its separate intervention record ended in `resolved_target`.

The following states do not authorize removal:

- `detected`
- `proposed`
- `confirmed`
- `executing`
- `resolved_source`
- `aborted`
- `failed`

The terminal provisioning journal is never reopened or rewritten.

## Registry mutation boundary

The finalization gate does not edit the registry. It emits a machine-readable recommendation:

```json
{
  "registry_change": {
    "operation": "remove_topology_migration",
    "pipeline": "inventory-position-projection",
    "safe": true
  }
}
```

The actual Git change remains a reviewed change subject to normal schema validation and CI.

## Observability

Prometheus output uses only bounded labels for pipeline, finalization state, and blocker code. It must not expose migration IDs, provisioning run IDs, intervention IDs, operator identities, runtime URLs, credentials, or arbitrary exception strings as labels.

## Consequences

Positive:

- the migration lifecycle now has a machine-verifiable end condition
- stale compatibility contracts can be removed without guesswork
- exact target topology is not enough without governed rollout provenance
- manual `resolved_target` recovery composes cleanly with the normal completion path
- absence of a migration stanza is idempotently treated as already finalized

Trade-offs:

- finalization depends on PostgreSQL journal history, live NATS topology, and deployment readiness while a migration is present
- a lost or mismatched journal blocks cleanup even if live topology looks correct
- operators may need a new reviewed migration rather than manually blessing unexplained live state

## Non-goals

This decision does not:

- automatically edit Git
- automatically remove `topology_migration`
- mutate JetStream
- delete migration history
- reconcile unknown topology
- expand the provisioner mutation allowlist
- replace the external watchdog or existing deployment readiness gate
