# ADR 0047: Bound event pipeline topology migrations in time

- Status: Accepted
- Date: 2026-08-19
- Scope: WMS event pipeline deployment rollout safety

## Context

ADR 0046 makes the live JetStream stream and durable-consumer configuration an exact deployment-readiness contract. Exact matching is the correct steady-state behavior, but an intentional topology change cannot usually become atomic across Git review, provisioning, and all runtime instances.

For example, changing stream replicas from one to three creates a short interval in which either the reviewed old topology or the reviewed new topology may legitimately be live. Treating that interval as arbitrary drift blocks a safe rollout. Allowing multiple topologies indefinitely weakens drift detection.

## Decision

The canonical `topology` object remains the single steady-state target.

A pipeline MAY additionally declare one optional `topology_migration`:

```yaml
topology_migration:
  id: replicas-1-to-3
  valid_until: "2026-08-19T12:00:00Z"
  from_overrides:
    stream:
      replicas: 1
```

`from_overrides` is applied to the target topology to reconstruct one complete reviewed source topology. Only topology fields may be overridden. Stream and durable identities remain owned by the normal deployment registry and cannot be migrated through this mechanism.

The migration object contains no mutable `phase` field. Phase is derived from the observation clock so stale state cannot claim that an expired rollout is still active.

## Runtime semantics

While `observed_at <= valid_until`:

- exact target topology is accepted;
- exact reconstructed source topology is accepted;
- any third topology is critical drift.

After `valid_until`:

- only exact target topology is accepted as the deployed state;
- a source topology that remains deployed is critical with `topology_migration_expired`;
- any other topology is normal critical drift;
- an exact target with an expired migration declaration is warning until the stale migration contract is removed.

The warning on an expired-but-completed declaration is intentional. It preserves service readiness while making contract cleanup visible.

## Validation

Static validation SHALL require:

- a normalized migration ID;
- a timezone-bearing RFC3339 `valid_until`;
- at least one supported override;
- the reconstructed source topology to satisfy all normal topology invariants;
- the reconstructed source to differ from the steady-state target.

The migration cannot relax the rule that business consumers exclude synthetic canary traffic or the rule that both consumers use explicit ACK pull mode.

## Read-only boundary

Migration evaluation reuses one read-only live topology snapshot. It does not provision, update, delete, publish, fetch, ACK, purge, or repair JetStream resources.

The source topology is compared to the same observed stream/consumer metadata used for the target comparison, avoiding a second broker read that could create an artificial cross-read race.

## Readiness

The topology readiness signal becomes:

```text
no migration
  target match                         -> ok
  target mismatch                      -> critical

active migration
  target match                         -> ok
  source match                         -> ok
  neither                              -> critical

expired migration
  target match                         -> warning (remove stale contract)
  source match                         -> critical
  neither                              -> critical
```

This preserves strict steady-state drift detection while allowing one explicit, reviewable, time-bounded transition.

## Observability cardinality

Prometheus exports only bounded deployment dimensions:

- pipeline ID;
- contract status;
- migration active flag;
- match state: `target`, `source`, or `neither`.

Migration IDs, timestamps, stream names, durable names, subjects, sequence IDs, peer names, and raw errors are not metric labels.

## Operational sequence

1. Change `topology` to the intended steady-state target.
2. Add `topology_migration.from_overrides` describing the currently deployed source and set a short `valid_until`.
3. Merge the reviewed contract before changing the broker.
4. Provision the target topology during the active window.
5. Verify the inspector reports `matched: target`.
6. Remove `topology_migration` in a follow-up change.

If the deadline is missed, the gate fails closed rather than silently extending the rollout.

## Consequences

### Positive

- Intentional rollout state is distinguished from accidental drift.
- The old topology cannot remain accepted forever.
- The steady-state target remains singular and obvious.
- Rollout authorization is Git-reviewed and machine-readable.
- No mutable deployment phase has to be synchronized with the broker.

### Trade-offs

- Operators must choose and review a real deadline.
- A completed migration left in Git becomes a warning after expiry until cleaned up.
- This mechanism supports one source and one target only. Multi-hop migrations must be split into separate reviewed migrations.

## Follow-up

A later slice may add deployment automation that consumes the same target/migration contract and performs preflight checks before applying infrastructure changes. Automatic repair remains outside the topology inspector itself.
