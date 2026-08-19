# Event pipeline deployment registry

This directory is the canonical machine-readable deployment contract for operational event pipelines.

The registry deliberately stores **identities, desired topology, policy, and environment-variable names**, not database URLs, NATS credentials, tokens, or other secrets. Runtime endpoints remain deployment inputs supplied through the named environment variables.

Each pipeline now binds five operational views:

```text
static deployment configuration
        +
JetStream topology match
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
- `topology` declares the reviewed live JetStream configuration that must exist for the deployment to be ready.
- `runtime.*_env` values name environment variables; they do not contain secrets.

## Topology contract

The topology section currently declares:

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

The business durable must remain an explicit-ACK pull consumer filtered to `<subject_prefix>.>`. The canary durable must remain an explicit-ACK pull consumer filtered only to `<subject_prefix>.health_check.ping`.

This is desired-state configuration, not a provisioning mechanism. The registry does not create, update, or repair JetStream resources.

## Validation

Run:

```bash
python scripts/validate_event_pipeline_deployments.py
```

or the full repository gate:

```bash
bin/check
```

Validation includes JSON Schema shape checks plus cross-field invariants such as unique pipeline IDs, warning/critical ordering, SLI freshness versus canary cadence, watchdog timeout versus canary timeout, stream subject coverage, exact consumer filters, pull-consumer mode, and explicit ACK.

## Topology drift inspection

Compare the live broker with the registry:

```bash
bin/inspect-kernel-event-pipeline-topology \
  --pipeline inventory-position-projection \
  --pretty \
  --check
```

The inspector is read-only. It calls JetStream metadata APIs only and never creates, updates, deletes, publishes, fetches, ACKs, purges, or repairs resources.

Any mismatch is reported with bounded resource/field/code identity and makes topology health critical. Missing required streams or consumers are drift. Broker unavailability is reported as inspection unavailable rather than as a false match.

## Operational readiness

For one configured pipeline:

```bash
bin/inspect-kernel-event-pipeline-readiness \
  --pipeline inventory-position-projection \
  --pretty \
  --check
```

The command resolves runtime endpoints from the environment-variable names in this registry and resolves the external watchdog JSON file from `runtime.watchdog_state_path_env`. Explicit CLI endpoint overrides exist for labs, but the logical identities, topology, and policy still come only from this registry.

The readiness result is fail-closed:

- `READY` - configuration is enabled, live topology matches, and all required runtime signals are available and healthy.
- `DEGRADED` - at least one signal is warning and none is critical.
- `NOT_READY` - configuration is disabled/invalid, topology drifts, a required signal is unavailable, or any signal is critical.

The readiness gate is an operational deployment decision. It does not mutate the WMS database, NATS stream, consumer, Inbox, projection, SLI history, or external watchdog state.

## Intentional changes

Change the registry and provisioning as one reviewed rollout. Exact topology matching is deliberately strict, so changing only one side produces visible drift. A future staged-migration contract may allow explicitly declared old/new topology during a bounded rollout window without weakening steady-state drift detection.
