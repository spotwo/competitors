# ADR 0046: Detect event pipeline deployment topology drift

- Status: Accepted
- Date: 2026-08-19
- Scope: WMS event pipeline operational readiness

## Context

The deployment registry introduced by ADR 0045 makes stream, consumer, canary, SLO, and watchdog identities machine-readable, but identity alone does not prove that the live JetStream resources were provisioned with the intended configuration.

A pipeline can remain superficially healthy while its deployed topology has drifted. Examples include a stream subject being narrowed or broadened, a replica count changing, a durable becoming a push consumer, explicit acknowledgement being replaced, a filter subject changing, or `max_ack_pending` being modified.

These are deployment-contract failures, not transient business backlog. They must be visible independently from current stream and consumer health.

## Decision

The canonical event pipeline registry SHALL include an explicit `topology` contract for every pipeline.

The first contract covers:

### Stream

- exact subject set;
- storage type;
- retention policy;
- replica count;
- JetStream duplicate window.

### Business durable consumer

- pull versus push delivery mode;
- deliver policy;
- ACK policy;
- exact filter subject;
- `max_ack_pending`;
- `max_deliver`.

### Synthetic canary durable consumer

The same consumer fields are checked, with an exact filter restricted to the synthetic canary subject.

The business and canary consumers remain explicit-ACK pull consumers. The canary durable remains isolated from the business durable.

## Read-only inspection

Topology inspection uses only JetStream `stream_info` and `consumer_info` calls. It MUST NOT:

- create, update, or delete streams;
- create, update, or delete consumers;
- publish messages;
- fetch business messages;
- acknowledge messages;
- purge stream data;
- repair drift automatically.

A missing stream or consumer is reported as topology drift. Failure to reach NATS is reported as topology inspection unavailable.

## Readiness semantics

Deployment readiness now composes:

```text
static configuration
        +
JetStream topology match
        +
current pipeline health
        +
external canary watchdog
        +
canary SLI / error budget
        ↓
READY / DEGRADED / NOT_READY
```

Any topology mismatch is critical and therefore makes the deployment `NOT_READY`.

This is intentionally stricter than current health. A consumer can have zero backlog and still be incorrectly configured.

## Registry invariants

The validator additionally requires:

- the stream subject contract to include `<subject_prefix>.>`;
- the business durable filter to equal `<subject_prefix>.>`;
- the canary durable filter to equal `<subject_prefix>.health_check.ping`;
- both durables to remain pull consumers;
- both durables to use explicit ACK;
- positive replica, duplicate-window, `max_ack_pending`, and `max_deliver` values.

The registry stores desired topology only. NATS URLs and credentials remain runtime inputs referenced by environment-variable name.

## Observability cardinality

Prometheus labels may include only bounded deployment dimensions such as pipeline, resource kind, field, and drift code.

Stream names, durable names, subject values, peer names, message IDs, sequence IDs, and raw errors are not exported as metric labels by the topology inspector.

JSON output may include expected and observed configuration values for operator diagnosis.

## Consequences

### Positive

- `READY` now means the live broker topology matches the reviewed deployment contract.
- Accidental console changes and provisioning drift become machine-detectable.
- Stream and durable configuration is reviewable in Git without storing secrets.
- Current health and desired topology remain separate signals, avoiding false equivalence between healthy backlog and correct configuration.

### Trade-offs

- Every intentional topology change must update the registry and deployment together.
- Exact matching is deliberately strict and can make rollout order visible as temporary `NOT_READY`.
- This slice does not yet compare NATS server/account limits, placement tags, cluster peer identity, or external infrastructure-as-code state.

## Follow-up

A later rollout-safety slice may add a staged topology migration contract so intentional changes can declare old and new acceptable topology during a bounded deployment window without weakening steady-state drift detection.
