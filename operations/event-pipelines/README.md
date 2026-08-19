# Event pipeline deployment registry

This directory is the canonical machine-readable deployment contract for operational event pipelines.

The registry deliberately stores **identities, desired topology, bounded migration intent, policy, and environment-variable names**, not database URLs, NATS credentials, tokens, or other secrets. Runtime endpoints remain deployment inputs supplied through the named environment variables.

Each pipeline binds five operational views:

```text
static deployment configuration
        +
JetStream topology contract
        +
current component health
        +
external canary execution watchdog
        +
canary SLI and error-budget burn
        ↓
READY / DEGRADED / NOT_READY
```

## Why this exists

Before this registry, stream names, durable consumer names, PostgreSQL Inbox consumer identities, canary identities, cadence, SLO targets, watchdog thresholds, and broker provisioning could drift independently between commands and deployment manifests.

The registry makes those values one reviewed contract and gives the readiness gate one source of truth.

## Boundaries

- `transport.stream` is the JetStream stream carrying the business pipeline.
- `transport.subject_prefix` is the publisher routing prefix.
- `consumer.durable` is the durable JetStream consumer for the real pipeline.
- `consumer.inbox_consumer_name` is the logical PostgreSQL Inbox/projection consumer identity. It is intentionally separate from the NATS durable name.
- `canary.durable` and `canary.consumer_name` identify the dedicated synthetic canary path.
- `canary.cadence_seconds` is defined once and is also the expected watchdog cadence.
- `topology` declares the singular steady-state JetStream target.
- optional `topology_migration` authorizes one reviewed source topology only until a deadline.
- `runtime.*_env` values name environment variables; they do not contain secrets.

## Topology contract

The topology section declares:

### Stream

- exact subject set;
- storage type;
- retention policy;
- replica count;
- duplicate window.

### Business and canary durable consumers

- pull versus push mode;
- delivery policy;
- ACK policy;
- exact filter subject;
- `max_ack_pending`;
- `max_deliver`.

Both durables must remain explicit-ACK pull consumers. The business filter must stay within the publisher subject prefix **and must not match the synthetic canary subject**. For the Position projector the canonical business filter is `spotwo.wms.events.inventory.position.changed`. The canary durable is filtered only to `spotwo.wms.events.health_check.ping`.

This isolation matters because the Position projector accepts only `inventory.position.changed`; a broad business wildcard would also deliver synthetic canary traffic to the domain projector.

This is desired-state configuration, not a provisioning mechanism. The registry does not create, update, or repair JetStream resources.

## Bounded topology migration

For an intentional rollout, keep `topology` as the new steady-state target and describe only the old fields that differ:

```yaml
topology_migration:
  id: replicas-1-to-3
  valid_until: "2026-08-19T12:00:00Z"
  from_overrides:
    stream:
      replicas: 1
```

The runtime reconstructs and validates the complete source topology from those overrides.

While the deadline is active, exact source or exact target are acceptable. A third state is critical drift. After the deadline only target is accepted; a source still deployed is critical. If target is already live but the expired migration declaration remains in Git, topology becomes warning until the stale declaration is removed.

There is deliberately no stored `phase` value. Active versus expired is derived from `observed_at` and `valid_until`, so a stale string cannot extend a rollout indefinitely.

See `labs/postgres-inventory-kernel/EVENT_PIPELINE_TOPOLOGY_MIGRATION.md` for the rollout runbook.

## Validation

Run:

```bash
python scripts/validate_event_pipeline_deployments.py
```

or the full repository gate:

```bash
bin/check
```

Validation includes JSON Schema shape checks plus cross-field invariants such as unique pipeline IDs, warning/critical ordering, SLI freshness versus canary cadence, watchdog timeout versus canary timeout, stream subject coverage, business/canary subject isolation, pull-consumer mode, explicit ACK, and validity of any reconstructed migration source topology.

## Topology inspection

Compare the live broker with the steady-state target and any active migration contract:

```bash
bin/inspect-kernel-event-pipeline-topology \
  --pipeline inventory-position-projection \
  --pretty \
  --check
```

Exit codes are `0` ok, `1` warning, and `2` critical.

The inspector is read-only. It calls JetStream metadata APIs only and never creates, updates, deletes, publishes, fetches, ACKs, purges, or repairs resources.

Missing required streams or consumers are drift. Broker unavailability is reported as inspection unavailable rather than as a false match.

## Rollout preflight

Before an external provisioner mutates JetStream, build a deterministic dry-run plan:

```bash
bin/plan-kernel-event-pipeline-topology \
  --pipeline inventory-position-projection \
  --pretty \
  --check
```

The planner combines the reviewed target, optional migration contract, and one live topology snapshot. Its decision is one of:

- `safe_to_apply` - the migration is active, live topology exactly matches the source, and the source-to-target delta affects exactly one JetStream resource;
- `no_changes` - the target is already deployed;
- `blocked` - do not begin the rollout from the observed state.

A `safe_to_apply` plan groups all changed fields for one resource into one `apply_target_resource` step, followed by exact-target verification, full event-pipeline readiness verification, and migration-contract cleanup. The planner itself is always dry-run and never performs that resource update.

If one migration changes more than one of `stream`, `business_consumer`, and `canary_consumer`, preflight blocks it. A sequential multi-resource rollout would create a hybrid topology that is neither the exact source nor the exact target accepted by the bounded migration contract. Split such work into separate reviewed migrations.

See `labs/postgres-inventory-kernel/EVENT_PIPELINE_TOPOLOGY_PREFLIGHT.md` for the preflight runbook.

## Operational readiness

For one configured pipeline:

```bash
bin/inspect-kernel-event-pipeline-readiness \
  --pipeline inventory-position-projection \
  --pretty \
  --check
```

The command resolves runtime endpoints from the environment-variable names in this registry and resolves the external watchdog JSON file from `runtime.watchdog_state_path_env`. Explicit CLI endpoint overrides exist for labs, but the logical identities, topology, migration intent, and policy still come only from this registry.

The readiness result is fail-closed:

- `READY` - configuration is enabled, topology contract is ok, and all required runtime signals are available and healthy.
- `DEGRADED` - at least one signal is warning and none is critical.
- `NOT_READY` - configuration is disabled/invalid, topology is unauthorized, a required signal is unavailable, or any signal is critical.

The readiness gate is an operational deployment decision. It does not mutate the WMS database, NATS stream, consumer, Inbox, projection, SLI history, or external watchdog state.

## Intentional changes

Change the registry and provisioning as one reviewed rollout. For a topology transition, merge the target plus a short migration deadline before changing the broker. Run rollout preflight before mutation. Once live topology matches target and event-pipeline readiness is healthy, remove `topology_migration` so steady-state exact matching is again the only accepted state.
