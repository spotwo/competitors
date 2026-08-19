# ADR 0048: Event pipeline topology rollout preflight

- Status: accepted
- Date: 2026-08-19
- Scope: WMS event-pipeline operational control plane

## Context

ADR 0046 made the reviewed deployment registry the desired JetStream topology. ADR 0047 added a bounded migration contract that may temporarily accept either the declared source topology or the steady-state target topology.

That contract answers whether a live broker state is currently acceptable, but it does not answer whether an operator should begin a topology change. A rollout can still be unsafe when:

- the live broker does not exactly match the declared source;
- the migration deadline has expired;
- there is no reviewed migration contract for the observed drift;
- a proposed migration changes multiple JetStream resources and therefore necessarily passes through a hybrid state that is neither the exact source nor the exact target;
- the target has already been reached and the only remaining work is verification and contract cleanup.

The repository needs a deterministic dry-run plan before any provisioner is allowed to mutate JetStream.

## Decision

Add a read-only topology rollout preflight planner.

The planner combines the canonical registry, optional `topology_migration`, and one live JetStream topology snapshot. It returns one of three decisions:

- `safe_to_apply` - a bounded active migration exists, live topology exactly matches its source, and all required changes belong to one JetStream resource;
- `no_changes` - the steady-state target is already deployed;
- `blocked` - rollout preconditions are not satisfied.

The planner never mutates JetStream.

## One-resource transition rule

A single migration may describe several field changes, but preflight permits those changes only when they belong to one resource:

- stream;
- business consumer;
- canary consumer.

This is required by the exact-source-or-exact-target contract from ADR 0047. If one migration changed both the stream and a consumer, any ordinary sequential rollout would create an intermediate hybrid state. That state would be neither the declared source nor the target and the existing readiness gate would correctly report critical drift.

Such changes must therefore be split into multiple reviewed migrations, one resource transition at a time.

Multiple fields on one resource are represented as one atomic resource-update step for the external provisioner. The planner does not claim that every JetStream field is mutable in place. Provisioner compatibility remains a separate execution concern.

## Plan semantics

For an active migration with live topology matching the source, the plan is:

```text
apply target configuration for the one changed resource
        ↓
verify exact target topology
        ↓
verify event-pipeline operational readiness
        ↓
remove topology_migration contract
```

The first step is descriptive only. This repository does not execute it.

If the live topology already matches the target, the apply step is omitted.

If the migration has expired while the source is still deployed, preflight is blocked. If the target is already deployed after expiry, preflight returns warning-level `no_changes` and asks only for verification and stale migration-contract cleanup.

If live topology matches neither source nor target, preflight is blocked even while the deadline is active.

If drift exists without a reviewed migration contract, preflight is blocked with `topology_migration_required` while still returning the observed target delta for review.

## Read-only boundary

Preflight may call the same metadata inspection path used by topology drift detection. It does not:

- create, update, or delete streams;
- create, update, or delete consumers;
- publish, fetch, or ACK messages;
- purge data;
- edit the registry;
- run automatic remediation.

The JSON result explicitly records `dry_run: true` and `mutates: false`.

## Change representation

Each required field change records bounded resource and field identity plus current and target values. The plan groups application into one resource-level step and then emits verification/cleanup steps.

The output is intended for humans and future provisioners. It is not a shell-script generator and is not an authorization to bypass deployment review.

## Consequences

Positive:

- rollout starts only from a reviewed and observed source state;
- expired migrations cannot be used as a stale authorization window;
- hybrid multi-resource rollouts are caught before mutation;
- operators can see the exact delta before applying it;
- completed migrations naturally collapse to verification and cleanup;
- the planner reuses the same topology contract and live snapshot semantics as readiness.

Trade-offs:

- multi-resource desired changes require multiple migrations;
- the planner deliberately does not decide whether a JetStream field can be changed in place;
- actual mutation remains external to this repository.

## Follow-up

A later provisioner may consume only `safe_to_apply` plans and execute the single resource-level change with its own authorization, rollback, and JetStream API compatibility checks. That mutation capability is explicitly outside this ADR.
