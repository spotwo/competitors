# NATS Durable Consumer Health

Executable companion to ADR 0038.

## Why this exists

PostgreSQL consumer-failure and malformed-delivery telemetry start after JetStream has delivered a message to application code.

A durable consumer can fail earlier:

```text
JetStream receives events
  -> durable consumer stops pulling or ACKing
  -> broker backlog grows
  -> PostgreSQL failure lanes can still be empty
```

This inspector reads the broker-side durable consumer state without fetching or acknowledging messages.

## Inspect one durable consumer

```bash
KERNEL_LAB_NATS_URL=nats://127.0.0.1:4222 \
  bin/inspect-kernel-nats-consumer \
  --stream WMS_EVENTS \
  --durable AVAILABILITY_PROJECTOR \
  --pretty
```

Prometheus exposition:

```bash
KERNEL_LAB_NATS_URL=nats://127.0.0.1:4222 \
  bin/inspect-kernel-nats-consumer \
  --stream WMS_EVENTS \
  --durable AVAILABILITY_PROJECTOR \
  --format prometheus
```

Monitoring exit codes with `--check`:

```text
0  ok
1  warning
2  critical
```

Connection failures, a missing consumer, incomplete broker state, or invalid requested identity also exit critical.

## What the snapshot means

The snapshot exposes:

- `pending`: messages waiting for first delivery to this consumer;
- `ack_pending`: delivered messages not yet acknowledged;
- `redelivered`: broker-reported redelivery count;
- `waiting_pull_requests`: pull requests currently waiting at JetStream;
- delivered consumer and stream sequence positions;
- ACK-floor consumer and stream sequence positions;
- last delivery and ACK-floor activity timestamps;
- finite `max_ack_pending` and utilization when available;
- configured `max_deliver` and `ack_wait`;
- paused state;
- whether the consumer still matches Spotwo's explicit-ACK pull-consumer contract.

`waiting_pull_requests = 0` is not an incident by itself. A synchronous polling worker can legitimately have no waiting request between fetch cycles.

## Idle is not stalled

An old `last_delivery_at` timestamp does not mean the consumer is unhealthy.

```text
pending = 0
ack_pending = 0
last delivery = 12 hours ago
  -> healthy idle consumer
```

Stall age is only evaluated when the corresponding backlog exists.

For delivery backlog:

```text
pending > 0
  -> age since last delivery
  -> if nothing was ever delivered, age since consumer creation
```

For ACK backlog:

```text
ack_pending > 0
  -> age since last ACK-floor progress
  -> before first ACK, age since last delivery
```

This distinguishes "nothing to do" from "work exists but the consumer stopped progressing."

## Default alert starting points

| Signal | Warning | Critical |
|---|---:|---:|
| pending first deliveries | 100 | 1,000 |
| redelivered messages | 1 | 10 |
| delivery/ACK backlog without progress | 300 s | 1,800 s |
| finite ACK capacity used | 80% | 100% |

A configuration mismatch is critical. A paused consumer with backlog is warning.

Override thresholds for a deployment:

```bash
bin/inspect-kernel-nats-consumer \
  --stream WMS_EVENTS \
  --durable AVAILABILITY_PROJECTOR \
  --warning-pending 500 \
  --critical-pending 5000 \
  --warning-redelivered 5 \
  --critical-redelivered 50 \
  --warning-stalled-seconds 600 \
  --critical-stalled-seconds 3600 \
  --warning-ack-capacity-percent 85 \
  --critical-ack-capacity-percent 100 \
  --check
```

These values are deployment inputs, not kernel SLOs.

## Prometheus contract

The exporter uses deployment-controlled `stream` and `durable` labels plus bounded `kind`, `code`, and `severity` dimensions.

Core metrics include:

```text
spotwo_wms_nats_consumer_pending_messages
spotwo_wms_nats_consumer_ack_pending_messages
spotwo_wms_nats_consumer_redelivered_messages
spotwo_wms_nats_consumer_waiting_pull_requests
spotwo_wms_nats_consumer_sequence
spotwo_wms_nats_consumer_activity_age_seconds
spotwo_wms_nats_consumer_max_ack_pending
spotwo_wms_nats_consumer_ack_pending_ratio
spotwo_wms_nats_consumer_paused
spotwo_wms_nats_consumer_configuration_valid
spotwo_wms_nats_consumer_health_status
spotwo_wms_nats_consumer_alert
```

Do not add subjects, event IDs, payload identifiers, errors, worker IDs, tenant IDs, warehouse IDs, or individual stream sequence numbers as labels. Sequence positions are numeric metric values.

## Incident interpretation

### Pending rises, delivery age stays low

The consumer is progressing but traffic exceeds current drain capacity. Inspect rate trends before scaling or changing thresholds.

### Pending rises, delivery stall age rises

The consumer is not receiving new work despite available backlog. Check process availability, NATS connectivity, pull loop health, and deployment state.

### ACK pending rises, ACK stall age rises

Messages are being delivered but acknowledgements are not advancing. Check handler latency, database transactions, ACK confirmation errors, ACK wait/backoff, and worker health.

### Redeliveries rise

Inspect ACK confirmation failures, handler runtime relative to ACK wait/backoff, connectivity, and any repeated application failures.

### ACK capacity approaches 100%

The consumer is near its configured `max_ack_pending`. New delivery can stop even while the process remains alive. Investigate processing latency and concurrency before increasing the limit.

### Consumer is paused with backlog

Confirm that the pause is intentional and has an owner/end time. The inspector does not resume it automatically.

## Read-only boundary

The command calls JetStream `consumer_info` only.

It does not:

- create or update a consumer;
- bind a pull subscription;
- fetch messages;
- ACK or NAK messages;
- pause or resume delivery;
- alter stream state;
- modify PostgreSQL.

Consumer provisioning and remediation remain explicit deployment/operator actions.
