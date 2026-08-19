# Event pipeline deployment registry

This directory is the canonical machine-readable deployment contract for operational event pipelines.

The registry deliberately stores **identities, policy, and environment-variable names**, not database URLs, NATS credentials, tokens, or other secrets. Runtime endpoints remain deployment inputs supplied through the named environment variables.

Each pipeline binds four previously separate operational views:

```text
current component health
        +
external canary execution watchdog
        +
canary SLI and error-budget burn
        +
static deployment configuration
        ↓
READY / DEGRADED / NOT_READY
```

## Why this exists

Before this registry, stream names, durable consumer names, PostgreSQL Inbox consumer identities, canary identities, cadence, SLO targets, and watchdog thresholds could drift independently between commands and deployment manifests.

The registry makes those values one reviewed contract and gives the readiness gate one source of truth.

## Boundaries

- `transport.stream` is the JetStream stream carrying the business pipeline.
- `consumer.durable` is the durable JetStream consumer for the real pipeline.
- `consumer.inbox_consumer_name` is the logical PostgreSQL Inbox/projection consumer identity. It is intentionally separate from the NATS durable name.
- `canary.durable` and `canary.consumer_name` identify the dedicated synthetic canary path.
- `canary.cadence_seconds` is defined once and is also the expected watchdog cadence.
- `runtime.*_env` values name environment variables; they do not contain secrets.

## Validation

Run:

```bash
python scripts/validate_event_pipeline_deployments.py
```

or the full repository gate:

```bash
bin/check
```

Validation includes JSON Schema shape checks plus cross-field invariants such as unique pipeline IDs, warning/critical ordering, SLI freshness versus canary cadence, and watchdog timeout versus canary timeout.

## Operational readiness

For one configured pipeline:

```bash
bin/inspect-kernel-event-pipeline-readiness \
  --pipeline inventory-position-projection \
  --pretty \
  --check
```

The command resolves runtime endpoints from the environment-variable names in this registry and resolves the external watchdog JSON file from `runtime.watchdog_state_path_env`. Explicit CLI endpoint overrides exist for labs, but the logical identities and policy still come only from this registry.

The readiness result is fail-closed:

- `READY` - all required signals are available and healthy.
- `DEGRADED` - at least one signal is warning and none is critical.
- `NOT_READY` - configuration is disabled/invalid, a required signal is unavailable, or any signal is critical.

The readiness gate is an operational deployment decision. It does not mutate the WMS database, NATS stream, consumer, Inbox, projection, SLI history, or external watchdog state.
