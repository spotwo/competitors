# ADR 0053: Resolve topology manual interventions explicitly

Status: Accepted

## Context

The crash-safe topology provisioner and unattended reconciler deliberately stop in `manual_intervention` when the live JetStream topology is neither the reviewed migration source nor its target, or when durable execution state contradicts the live broker state.

That stop is a safety property. Automatically guessing how to repair an unknown topology would turn reconciliation into an unreviewed configuration authority.

A terminal provisioning run must also remain immutable. Reopening the original run would weaken the audit trail and make it unclear which actor approved an emergency action.

## Decision

Introduce a separate durable manual-intervention workflow linked to the terminal provisioning run.

An intervention moves through:

```text
detected
  -> proposed
  -> confirmed
  -> executing
  -> resolved_source | resolved_target | aborted | failed
```

Only one non-terminal intervention attempt may exist for one provisioning run at a time. A failed attempt may be followed by a new detected attempt with fresh evidence. A successfully resolved or explicitly aborted attempt closes that provisioning incident.

### Evidence

Detection captures a machine-readable snapshot containing the immutable provisioning intent, source snapshot, reviewed target topology, reviewed migration contract, and one live JetStream topology snapshot. A canonical SHA-256 fingerprint excludes observation timestamps but includes the operationally relevant configuration.

Execution must re-read the provisioning run, registry contract, and live JetStream topology. If the current evidence fingerprint differs from the detected fingerprint, the attempt fails before any topology mutation with `topology_manual_intervention_evidence_stale`.

### Human authorization

The first operator proposes exactly one bounded action:

- `restore_source`
- `accept_target`
- `abort`

A different second operator must confirm the proposal. The proposer cannot confirm their own action.

Confirmation authorizes only the exact intervention ID and action already stored in PostgreSQL. It does not authorize arbitrary JetStream changes.

### Action semantics

`restore_source` uses the existing guarded snapshot rollback on only the resource owned by the reviewed migration. The result must then exactly match the reviewed source contract, pass a fresh synthetic canary, and pass deployment readiness.

`accept_target` never mutates JetStream. It is permitted only when the live topology already exactly matches the reviewed target, followed by a fresh canary and deployment readiness.

`abort` performs no JetStream mutation and closes only the intervention workflow. It does not declare the deployment healthy.

### Explicit non-goals

This decision does not:

- infer an action from observed drift;
- accept arbitrary observed topology;
- delete or recreate streams or consumers;
- expand the provisioner's mutation allowlist;
- edit the registry or Git automatically;
- reopen or rewrite the terminal provisioning journal run;
- automatically retry a failed intervention attempt.

## Consequences

Emergency topology resolution becomes auditable and deterministic without converting unknown drift into auto-repair.

The registry migration contract must remain available until the intervention is terminal, because both detection and execution validate the journaled migration ID against the reviewed contract.

Resolved source or target outcomes remain visible as operator attention until the reviewed migration contract and operational incident are cleaned up through the normal repository workflow.
