# NATS JetStream stream health

This runbook covers read-only operational health for the canonical JetStream stream. It complements durable-consumer health and PostgreSQL Inbox/failure telemetry.

## Inspect one stream

```bash
bin/inspect-kernel-nats-stream \
  --server-url nats://127.0.0.1:4222 \
  --stream WMS_EVENTS \
  --pretty
```

Or provide `KERNEL_LAB_NATS_URL` and omit `--server-url`.

Prometheus output:

```bash
bin/inspect-kernel-nats-stream \
  --stream WMS_EVENTS \
  --format prometheus
```

Monitoring exit codes:

```bash
bin/inspect-kernel-nats-stream \
  --stream WMS_EVENTS \
  --check
```

- `0` = ok
- `1` = warning
- `2` = critical or inspection failure

The inspector uses JetStream `stream_info` only. It never publishes, fetches, ACKs, purges, creates, deletes, or updates stream state.

## What the snapshot means

`messages` and `bytes` are current retained stream state. They are broker inventory, not consumer backlog.

`first_sequence` and `last_sequence` bound the currently retained sequence range. Gaps can exist because retention and deletion are valid JetStream behavior.

`deleted_messages` is informational. Do not page merely because it is non-zero.

`lost_messages` is different. It is server-reported storage loss and is critical whenever greater than zero.

Finite `max_msgs` and `max_bytes` limits produce capacity ratios. Unlimited limits do not produce ratios.

Default thresholds:

```text
message/byte capacity
  warning  >= 80%
  critical >= 95%

maximum follower lag
  warning  >= 1,000 messages
  critical >= 10,000 messages
```

Override them per deployment when retention and traffic characteristics are known:

```bash
bin/inspect-kernel-nats-stream \
  --stream WMS_EVENTS \
  --warning-capacity-percent 70 \
  --critical-capacity-percent 90 \
  --warning-replica-lag 500 \
  --critical-replica-lag 5000 \
  --check
```

## Cluster interpretation

Standalone streams do not fabricate cluster health. `clustered=false` with no leader field is normal in the single-node lab.

When JetStream returns cluster information:

```text
leader missing            -> critical
offline follower          -> critical
not-current follower      -> warning
follower lag above policy -> warning/critical
```

Prometheus intentionally exports only aggregate follower health. Replica/server names are not labels.

## Capacity interpretation

A stream at 85% of `max_msgs` is a warning even when every consumer is healthy. This is broker retention pressure, not consumer lag.

A stream at 10% capacity with a durable consumer holding 50,000 pending messages can be healthy at the stream layer and critical at the consumer layer. Inspect both.

`max_age` is exported as configuration context but does not alert. Nearing an age limit is expected retention behavior.

## Triage order

For an event pipeline incident, inspect in this order:

```text
1. PostgreSQL Outbox
2. JetStream stream
3. JetStream durable consumer
4. Inbox / projection
5. consumer failure and malformed-delivery lanes
```

Examples:

```text
Outbox growing + stream unchanged
  -> publisher or broker publish path

Stream lost_messages > 0
  -> JetStream storage incident

Stream healthy + consumer pending growing
  -> consumer runtime/downstream handler incident

Stream near capacity + all consumers healthy
  -> retention/capacity planning incident
```

## Prometheus cardinality boundary

Allowed labels are bounded operational dimensions such as stream, alert code, severity, and metric kind.

Do not add labels for:

- subject
- message sequence
- event ID
- payload hash
- consumer delivery identity
- replica/server name
- headers

If subject-level or peer-level diagnostics are required during an incident, query JetStream interactively rather than turning those identifiers into always-on metric dimensions.

## Next layer

Stream and consumer snapshots are intentionally independent. The next useful layer is an end-to-end event-pipeline snapshot that correlates Outbox age, broker state, consumer lag, Inbox progress, and projection freshness without collapsing their ownership boundaries.
