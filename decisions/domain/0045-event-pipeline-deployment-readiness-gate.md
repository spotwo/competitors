# ADR 0045: Event pipeline deployment registry and readiness gate

- Status: accepted
- Date: 2026-08-19
- Scope: WMS event-pipeline operations

## Context

The event pipeline now has independent operational evidence for:

- PostgreSQL Outbox state;
- JetStream stream health;
- durable consumer health;
- malformed-delivery evidence;
- consumer-failure evidence;
- projection gaps;
- a synthetic end-to-end canary;
- historical canary SLI and error-budget burn;
- fault-injection/recovery drills;
- an external canary execution watchdog.

Those layers were intentionally built independently, but their deployment identities and thresholds were still supplied to separate commands. That creates a configuration-drift risk: the stream inspected by one command can differ from the stream used by the canary, a durable name can diverge from the logical Inbox consumer identity, or watchdog cadence can drift away from the SLI freshness policy.

A production operator also needs one bounded answer to a deployment question:

```text
Is this configured event pipeline READY, DEGRADED, or NOT_READY?
```

That answer must not replace detailed component telemetry, and it must not hide unavailable evidence.

## Decision

Create a canonical machine-readable deployment registry under `operations/event-pipelines/` and a read-only readiness gate that composes four required signals:

```text
static deployment configuration
        +
current pipeline component health
        +
external canary execution watchdog
        +
canary SLI / error-budget health
        ↓
READY / DEGRADED / NOT_READY
```

### One source of operational identity

Each registry entry owns:

- stable pipeline ID;
- JetStream stream and business subject prefix;
- business durable consumer identity;
- logical PostgreSQL Inbox/projection consumer identity;
- dedicated canary durable and canary consumer identity;
- canary cadence and timeout;
- SLO and burn-rate policy;
- watchdog policy;
- names of environment variables that supply runtime endpoints and external watchdog state.

Runtime URLs and credentials are not stored in the repository.

### Business and canary identities remain separate

The registry MUST keep the real business consumer separate from the synthetic canary consumer. The business NATS durable and PostgreSQL Inbox identity also remain separate fields because they are different namespaces and do not have to share a name.

### Cross-field invariants

Schema validation alone is insufficient. Machine validation also requires:

- unique pipeline IDs;
- dedicated canary durable and logical consumer identities;
- SLI staleness threshold at least as long as one configured canary cadence;
- watchdog start grace no longer than one canary cadence;
- watchdog execution timeout strictly greater than the canary timeout;
- warning/critical threshold ordering;
- normalized stable environment-variable names.

### Readiness semantics

The gate is fail-closed:

- `READY`: every required signal is available and `ok`.
- `DEGRADED`: one or more required signals are `warning` and none is `critical`.
- `NOT_READY`: deployment is disabled, any required signal is unavailable, or any signal is `critical`.

An unavailable source is never omitted from the result.

The gate returns a deterministic `root_cause_candidate` for operator triage. It is not a proof of causal ordering.

### Independent watchdog semantics remain independent

A successful external watchdog execution whose canary reports `critical` means the watchdog itself is healthy while the observed pipeline is unhealthy. The readiness gate MUST preserve that distinction instead of converting a critical canary result into a scheduler failure.

### Read-only boundary

The readiness collector may read PostgreSQL, JetStream metadata, canary SLI history, and external watchdog state. It MUST NOT:

- publish a canary;
- create/update/delete a stream or durable consumer;
- fetch or ACK business messages;
- modify Outbox, Inbox, projection, failure, poison, or SLI state;
- write external watchdog receipts;
- restart or scale any runtime.

### Metrics cardinality

Prometheus readiness metrics may label only stable configured pipeline IDs and bounded signal/status/code/severity dimensions. Runtime URLs, execution IDs, event IDs, message sequence IDs, payloads, raw errors, and watchdog receipt IDs are forbidden as labels.

## Consequences

Positive:

- operational identity becomes reviewable configuration rather than duplicated CLI arguments;
- drift between current health, canary SLI, and watchdog scope fails closed;
- deployment tooling receives one stable readiness contract;
- detailed component inspectors remain available for diagnosis;
- runtime credentials remain outside Git.

Trade-offs:

- readiness is a non-atomic composition of independent snapshots;
- a historical SLI burn-rate failure can keep a currently healthy pipeline not-ready by policy;
- the registry introduces another deployment artifact that must be maintained when topology changes;
- selecting a production scheduler/watchdog persistence provider remains a deployment decision outside this ADR.

## Non-goals

This ADR does not define:

- Kubernetes readiness probes or a specific orchestrator integration;
- automatic rollback, restart, or scaling;
- a service-discovery system;
- secret storage;
- multi-region quorum policy;
- a replacement for detailed observability dashboards;
- a claim that `root_cause_candidate` proves causality.
