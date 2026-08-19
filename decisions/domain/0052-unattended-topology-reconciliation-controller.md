# ADR 0052: Unattended topology reconciliation controller

- Status: Accepted
- Date: 2026-08-19

## Context

ADR 0050 made authorized JetStream topology provisioning crash-safe by persisting rollout intent and execution state in PostgreSQL under a single-writer lease. A process restart can recover deterministically from the journal, but recovery still requires an operator or external scheduler to invoke the apply command again after lease expiry.

ADR 0051 made stuck and expired executions observable, but observation alone does not restore service after an unattended provisioner process dies.

The remaining requirement is to let an agent recover an interrupted rollout automatically without accidentally turning desired-state drift into authorization for a new topology change.

## Decision

Add a dedicated unattended reconciliation controller that is **recovery-only**.

The controller may act only when all of the following are true:

1. a non-terminal provisioning journal execution already exists for the configured pipeline;
2. its lease is expired;
3. the deployment registry still contains a valid `topology_migration` declaration;
4. the reviewed `topology_migration.id` exactly matches the journaled migration ID;
5. PostgreSQL atomically grants the controller the expired execution lease.

The controller then invokes the existing deterministic crash-recovery state machine for that already-claimed run.

The controller does not use the normal provisioning entry point that may create a new `prepared` execution. If there is no active journal row, the controller is idle even when live JetStream differs from the desired registry target.

## Lease race handling

The health observation that identifies an expired lease is advisory only. Lease ownership is decided again transactionally by `claim_active()`.

If the previous owner renews after the observation but before the claim, the controller defers. A live lease is never stolen.

## Migration contract lifetime

The reviewed `topology_migration` declaration must remain present until its provisioning execution becomes terminal.

If the declaration is removed, invalid, or replaced with a different migration ID while an expired execution remains active, unattended recovery is blocked before lease acquisition. Operators must restore the matching reviewed contract or intervene manually.

This deliberately prefers fail-closed recovery over reconstructing deployment authorization from journal data alone.

## Recovery semantics

After a successful claim, existing ADR 0050 semantics remain authoritative:

- exact source live -> finalize as rolled back/source restored;
- exact target live -> continue missing target, canary, and readiness verification;
- rollback started while target remains live -> restore persisted source values and verify source;
- neither exact source nor exact target -> transition to `manual_intervention`.

The controller does not invent a fourth topology state and does not auto-repair unrelated drift.

## Runtime model

Provide a long-running command with a bounded poll interval and a one-shot mode suitable for external schedulers.

A controller process uses one stable lease owner identity for its lifetime. Readiness re-reads external watchdog state when evaluated so a long-running process does not rely on a startup-time snapshot.

## Observability

Controller decisions are emitted as structured JSON records with bounded status values:

- `idle`;
- `deferred`;
- `reconciled`;
- `blocked`;
- `failed`.

ADR 0051 provisioning-health metrics remain the canonical Prometheus view for state age, lease expiry, attempts, recovery handoffs, and terminal attention. The controller does not create a second competing metric model in this slice.

## Consequences

### Positive

- interrupted authorized rollouts can recover without human invocation;
- unattended recovery cannot start a new migration;
- single-writer semantics are preserved across observation/claim races;
- reviewed Git intent remains part of recovery authorization;
- existing canary and readiness gates remain mandatory after target recovery.

### Trade-offs

- removing a migration contract before its journal run is terminal blocks unattended recovery;
- controller availability becomes part of recovery time, so it must be supervised or scheduled externally;
- recovery still depends on PostgreSQL journal availability and the same NATS/canary/readiness dependencies as manual reconciliation.

## Non-goals

This decision does not authorize automatic steady-state topology rollout, Git mutation, migration creation, deployment scheduling, arbitrary drift repair, or automatic resolution of `manual_intervention`.
