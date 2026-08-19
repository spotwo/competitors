# Event pipeline topology drift

The topology inspector compares the canonical event-pipeline deployment contract with the live JetStream stream and durable-consumer configuration.

It answers a different question from backlog or stream-health telemetry:

```text
Is the broker topology deployed the way we reviewed it?
```

A stream can be available and a consumer can have zero backlog while still being incorrectly configured. Topology drift therefore participates in deployment readiness independently from current operational health.

## Canonical source

Desired topology lives in:

```text
operations/event-pipelines/registry.yml
```

The registry contains identities and desired configuration only. NATS URLs, credentials, tokens, and other secrets remain runtime inputs referenced by environment-variable name.

For the inventory position projection, the current contract checks:

### Stream

- exact subject set;
- storage type;
- retention policy;
- replica count;
- duplicate window.

### Business durable consumer

- pull versus push mode;
- delivery policy;
- ACK policy;
- exact filter subject;
- `max_ack_pending`;
- `max_deliver`.

### Synthetic canary durable consumer

The same consumer fields are checked, but its filter must be the exact synthetic canary subject rather than the broad business wildcard.

## Inspect topology

```bash
bin/inspect-kernel-event-pipeline-topology \
  --pipeline inventory-position-projection \
  --pretty
```

The NATS URL is resolved from the environment variable named by the registry. A lab override is available:

```bash
bin/inspect-kernel-event-pipeline-topology \
  --pipeline inventory-position-projection \
  --nats-url nats://127.0.0.1:4222 \
  --pretty
```

A healthy result has:

```json
{
  "status": "ok",
  "drift_count": 0
}
```

Any mismatch produces `status=critical` and one or more bounded drift records containing:

- resource kind;
- field;
- bounded drift code;
- expected value;
- actual value.

Missing required streams or consumers are represented as drift rather than silently ignored.

## Check mode

```bash
bin/inspect-kernel-event-pipeline-topology \
  --pipeline inventory-position-projection \
  --check
```

Exit codes:

```text
0  exact topology match
2  topology drift
```

If the registry is invalid or NATS cannot be inspected, the command exits non-zero without pretending that the live topology matched.

## Prometheus

```bash
bin/inspect-kernel-event-pipeline-topology \
  --pipeline inventory-position-projection \
  --format prometheus
```

The exported dimensions are intentionally bounded:

- pipeline;
- resource kind;
- field;
- drift code.

Stream names, durable names, subject values, message IDs, sequence IDs, peer names, and raw exceptions are not exported as metric labels.

## Readiness integration

The regular deployment readiness command now includes topology as a required signal:

```bash
bin/inspect-kernel-event-pipeline-readiness \
  --pipeline inventory-position-projection \
  --pretty \
  --check
```

Conceptually:

```text
static configuration
        +
JetStream topology match
        +
current component health
        +
external canary watchdog
        +
canary SLI / error budget
        ↓
READY / DEGRADED / NOT_READY
```

Any topology mismatch makes the deployment `NOT_READY` even when current stream and consumer health are otherwise green.

## Read-only boundary

The topology inspector uses JetStream metadata reads only:

- `stream_info`;
- `consumer_info`.

It does not:

- create or update a stream;
- create or update a consumer;
- delete resources;
- publish a message;
- fetch a business message;
- ACK a message;
- purge stream data;
- repair drift automatically.

The integration test captures stream message/byte counts and consumer pending/ACK-pending counters before and after inspection to verify that inspection does not mutate broker state.

## Intentional topology changes

For a deliberate topology change, update the canonical registry and the deployment provisioning in the same reviewed change plan. Exact matching is intentionally strict, so a rollout may temporarily report drift if the registry and live broker are changed at different times.

Do not weaken the steady-state contract just to hide rollout ordering. If a future deployment strategy needs overlapping old/new topology during a migration window, model that explicitly as a bounded migration contract.

## Current limitations

This slice does not yet compare:

- NATS server or account limits;
- cluster placement tags;
- individual peer identities;
- infrastructure-as-code state;
- consumer `ack_wait`;
- all possible stream limits such as max messages, bytes, or age.

Those fields should be added only when they become deployment invariants rather than incidental defaults. The inspector also does not perform automatic remediation.
