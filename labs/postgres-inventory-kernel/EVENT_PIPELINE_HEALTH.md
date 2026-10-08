# Event pipeline health

This runbook describes the aggregate read-only health view for one Spotwo WMS event pipeline.

The aggregate composes the existing health surfaces for:

```text
Outbox
  -> JetStream stream
  -> JetStream durable consumer
  -> malformed-delivery quarantine
  -> consumer failure lane
  -> projection gaps (only when the projector buffers version gaps)
```

It does not replace the component-specific inspectors. Use it first for triage, then drill into the component that is degraded.

## Required identity mapping

The command requires three explicit identities:

```text
JetStream stream
JetStream durable consumer
PostgreSQL logical consumer_name
```

The durable name and PostgreSQL `consumer_name` are allowed to differ. Do not infer one from the other. Select `--projection-gap-monitor` from the deployment registry, never by guessing from the consumer name.

- `inventory_position` (default): the Position projector stores out-of-order events in a PostgreSQL pending-gap buffer, so the sixth `projection_gap` health component is required.
- `none`: the strict Warehouse Work State projector rejects noncontiguous events and rolls back the Inbox receipt. Those events are retried/quarantined in the consumer failure lane; there is no pending-gap buffer, so **no `projection_gap` component is emitted**. A missing component is not proof that Work State has no delivery lag. Consumer failures and JetStream backlog remain authoritative signals.

Example:

```bash
export KERNEL_LAB_DATABASE_URL='postgresql://...'
export KERNEL_LAB_NATS_URL='nats://...'

bin/inspect-kernel-event-pipeline \
  --stream WMS_EVENTS \
  --durable POSITION_PROJECTOR \
  --consumer-name position_projection \
  --pretty
```

Work State health (lab proof exists; a deployment is not ready until its runtime signals pass):

```bash
bin/inspect-kernel-event-pipeline \\
  --stream WMS_EVENTS \\
  --durable WORK_STATE_PROJECTOR \\
  --consumer-name warehouse_work_state_projection \\
  --projection-gap-monitor none \\
  --pretty
```

## Monitoring mode

Use `--check` for monitoring integrations:

```bash
bin/inspect-kernel-event-pipeline \
  --stream WMS_EVENTS \
  --durable POSITION_PROJECTOR \
  --consumer-name position_projection \
  --check
```

Exit codes:

```text
0  ok
1  warning
2  critical
```

Prometheus summary:

```bash
bin/inspect-kernel-event-pipeline \
  --stream WMS_EVENTS \
  --durable POSITION_PROJECTOR \
  --consumer-name position_projection \
  --format prometheus
```

The aggregate Prometheus output is intentionally a summary. Detailed per-component metrics remain available from the existing component inspectors.

## Reading the JSON

Top-level fields:

```text
status
root_cause_candidate
degraded_components
alerts
components
```

A healthy result resembles:

```json
{
  "status": "ok",
  "root_cause_candidate": null,
  "degraded_components": []
}
```

A degraded result keeps every component result and also selects one deterministic triage candidate:

```json
{
  "status": "critical",
  "root_cause_candidate": {
    "component": "nats_consumer",
    "code": "nats_consumer_delivery_stalled",
    "severity": "critical"
  },
  "degraded_components": [
    "nats_consumer",
    "projection_gap"
  ]
}
```

`root_cause_candidate` is not causal proof. It is the highest-severity, upstream-first alert that should normally be investigated first.

## Component order

Equal-severity problems are triaged in this order:

```text
1. outbox
2. nats_stream
3. nats_consumer
4. malformed_delivery
5. consumer_failure
6. projection_gap (only for buffered Inventory Position)
```

Work State uses the first five components; its invalid/out-of-order events surface through the consumer failure lane rather than an Inventory Position gap view. This prevents an unrelated consumer from being declared healthy by another project's empty buffer.

This prevents a downstream symptom from hiding an equally severe upstream failure.

Severity always wins before topology order. A critical projection gap is selected ahead of an Outbox warning.

## Component availability

Each component exposes:

```json
{
  "status": "critical",
  "available": false,
  "error_code": "nats_stream_telemetry_unavailable",
  "telemetry": null
}
```

Unavailable telemetry is critical because the system cannot prove that component healthy.

The aggregate intentionally does not expose raw exception text in its stable output. Run the component inspector directly to diagnose connectivity, permissions, or broker/database API errors.

## Triage by root-cause component

### `outbox`

Run:

```bash
bin/inspect-kernel-outbox --pretty --check
```

Typical causes:

- publish worker stopped;
- ready events aging;
- retry backlog growing;
- quarantined producer events.

Do not investigate downstream consumer lag first if the Outbox is already critically stalled.

### `nats_stream`

Run:

```bash
bin/inspect-kernel-nats-stream \
  --stream WMS_EVENTS \
  --pretty \
  --check
```

Typical causes:

- JetStream-reported lost messages;
- missing cluster leader;
- offline followers;
- replica lag;
- finite message or byte capacity pressure.

Deleted sequence entries alone are not an incident because normal retention may create them.

### `nats_consumer`

Run:

```bash
bin/inspect-kernel-nats-consumer \
  --stream WMS_EVENTS \
  --durable POSITION_PROJECTOR \
  --pretty \
  --check
```

Typical causes:

- first-delivery backlog;
- ACK backlog;
- redeliveries;
- delivery stall;
- ACK stall;
- invalid or paused consumer configuration.

An old last-delivery timestamp with zero backlog is still normal idle behavior.

### `malformed_delivery`

Run:

```bash
bin/inspect-kernel-malformed-deliveries \
  --consumer-name position_projection \
  --pretty \
  --check
```

The aggregate uses a 300-second recent-activity lookback by default. Override it with:

```bash
--malformed-lookback-seconds 900
```

Retained old forensic poison records do not make pipeline health critical by themselves. Recent quarantine or reobservation activity does.

### `consumer_failure`

Run:

```bash
bin/inspect-kernel-consumer-failures \
  --consumer-name position_projection \
  --pretty \
  --check
```

Typical causes:

- handler/database failures moved to the durable failure lane;
- retry backlog aging;
- terminal or attempt-exhausted quarantine.

### `projection_gap`

Run:

```bash
bin/inspect-kernel-projection-gaps \
  --consumer-name position_projection \
  --pretty \
  --check
```

Typical causes:

- aggregate version gaps;
- pending events accumulating behind a missing version;
- old unresolved gaps.

Before rebuilding a projection, verify that Outbox, stream, durable consumer, and failure lane are healthy. A rebuild does not fix an upstream delivery stall.

## Prometheus contract

The aggregate exports bounded-cardinality summary metrics including:

```text
spotwo_wms_event_pipeline_health_status
spotwo_wms_event_pipeline_component_health_status
spotwo_wms_event_pipeline_component_available
spotwo_wms_event_pipeline_degraded_components
spotwo_wms_event_pipeline_alert
spotwo_wms_event_pipeline_root_cause_candidate
```

Labels are limited to:

```text
stream
durable
consumer
component
code
severity
```

Do not add event IDs, subjects, payload hashes, stream sequences, raw errors, worker IDs, or peer names as labels.

## Consistency boundary

The aggregate is read-only but not atomic across PostgreSQL and JetStream.

All component age calculations share one logical `observed_at`, but the individual reads happen sequentially. A busy pipeline can advance between reads. Treat the result as an operational snapshot, not a distributed transaction.

The collector intentionally does not calculate fake end-to-end lag by subtracting unrelated sequence spaces. JetStream filters, retention gaps, and aggregate-local projection versions make that arithmetic unsafe.

True commit-to-projection latency belongs to a later correlated canary/SLO slice.

## What this command never does

Inspection never:

- publishes a message;
- fetches a delivery;
- ACKs or NAKs;
- creates or changes a stream or consumer;
- inserts Inbox receipts;
- retries failures;
- rebuilds projections;
- archives or purges evidence.

If an operational action is required, perform it through the existing explicit command for that lifecycle rather than adding mutation to health inspection.
