# Event pipeline deployment readiness

The readiness gate turns the existing operational evidence into one deployment decision without mutating the system being checked.

## Topology

```text
operations/event-pipelines/registry.yml
                |
                v
      static configuration
                |
     +----------+----------+
     |          |          |
     v          v          v
pipeline     external    canary SLI
health       watchdog    + burn rate
     |          |          |
     +----------+----------+
                |
                v
      READY / DEGRADED / NOT_READY
```

The detailed component telemetry remains authoritative for diagnosis. Readiness is only the deployment-level composition.

## Registry ownership

The registry owns logical identity and policy. It does not contain secrets.

For each pipeline it defines:

- JetStream stream;
- business durable consumer;
- logical PostgreSQL Inbox/projection consumer;
- canary durable and canary consumer;
- canary cadence and timeout;
- availability and latency SLO targets;
- burn-rate and failure-streak thresholds;
- external watchdog thresholds;
- names of environment variables supplying PostgreSQL URL, NATS URL, and watchdog JSON path.

Do not duplicate these values in deployment scripts if the registry can be consumed directly.

## Static validation

```bash
python scripts/validate_event_pipeline_deployments.py
```

The full repository validation also runs this check through `bin/check`.

Static validation checks both JSON Schema shape and semantic invariants that JSON Schema does not conveniently express, including dedicated canary identity and timeout/cadence relationships.

## Runtime command

```bash
bin/inspect-kernel-event-pipeline-readiness \
  --pipeline inventory-position-projection \
  --pretty
```

For a deployment check:

```bash
bin/inspect-kernel-event-pipeline-readiness \
  --pipeline inventory-position-projection \
  --check
```

Exit codes:

```text
0  READY
1  DEGRADED
2  NOT_READY
```

Prometheus:

```bash
bin/inspect-kernel-event-pipeline-readiness \
  --pipeline inventory-position-projection \
  --format prometheus
```

## Runtime inputs

The sample registry resolves:

```text
KERNEL_LAB_DATABASE_URL
KERNEL_LAB_NATS_URL
KERNEL_LAB_CANARY_WATCHDOG_STATE_PATH
```

The watchdog variable contains a **path to the external watchdog JSON state**, not the JSON body itself.

For disposable labs, endpoint/path overrides are available:

```bash
bin/inspect-kernel-event-pipeline-readiness \
  --pipeline inventory-position-projection \
  --database-url postgresql://... \
  --nats-url nats://127.0.0.1:4222 \
  --watchdog-state /var/run/spotwo/canary-watchdog.json
```

Overrides affect runtime connectivity only. Stream, consumer, canary, cadence, and policy identities remain registry-owned.

## Signal interpretation

### configuration

`critical` when the selected deployment is disabled. Invalid registry content fails before runtime collection and is rejected by validation.

### pipeline_health

Uses the existing read-only event-pipeline health collector for the business durable and logical Inbox consumer.

If PostgreSQL/NATS endpoints are missing or collection fails, readiness exposes a critical unavailable signal instead of silently dropping the component.

### watchdog

Uses the external execution receipt state introduced by ADR 0044.

The readiness gate additionally verifies that watchdog stream, canary durable, canary consumer, and cadence match this deployment registry. Scope drift is `critical` even if the watchdog file would otherwise be healthy.

### canary_sli

Reads the tracked canary 5m/1h/24h SLI windows and applies the configured failure-streak and multi-window burn-rate policy.

This means a currently healthy component snapshot can still be `NOT_READY` while error-budget policy remains critical. That behavior is intentional.

## Fail-closed examples

```text
pipeline health = ok
watchdog       = ok
canary SLI     = warning
=> DEGRADED
```

```text
pipeline health = critical
watchdog       = ok
canary SLI     = ok
=> NOT_READY
```

```text
pipeline health = ok
watchdog state = unavailable
canary SLI     = ok
=> NOT_READY
```

```text
pipeline health = ok
watchdog       = ok
canary SLI     = critical burn rate
=> NOT_READY
```

## Root-cause candidate

The JSON result exposes one deterministic `root_cause_candidate` by worst severity and canonical signal order. This is for triage only. Because the underlying reads are not distributed-atomic, the field is not proof that the selected signal caused every downstream symptom.

## Prometheus cardinality

The readiness exporter labels only:

```text
pipeline
signal
status
code
severity
```

Configured pipeline IDs are bounded deployment inventory. The exporter does not use durable names, execution IDs, event IDs, sequence IDs, runtime URLs, payloads, or raw exceptions as labels.

## Read-only guarantee

The readiness command does not publish a synthetic event. Use `run-kernel-event-pipeline-canary` for the mutating probe. The readiness command only reads the already existing operational evidence and the external watchdog state file.
