# ADR 0042: Event pipeline canary SLI history and burn-rate alerting

Status: accepted

## Context

ADR 0041 introduced a synthetic canary that measures the deployed event path through PostgreSQL Outbox, the normal publisher, NATS JetStream, a dedicated durable consumer, Inbox transaction, synthetic projection, and broker ACK confirmation.

A single canary answers whether the path works now and how long the current run took. It does not answer whether the service has met an objective over time, whether failures are consecutive, or whether the error budget is being consumed quickly enough to justify an alert.

The historical layer must also fail closed when the canary process itself disappears. A process that starts an event and crashes before recording a terminal result must not silently disappear from availability accounting.

## Decision

Persist a tracked canary lifecycle before the synthetic event transaction commits, aggregate terminal outcomes into fixed 5 minute, 1 hour, and 24 hour SLI windows, and evaluate multi-window error-budget burn for both availability and end-to-end latency.

The tracked start is atomic:

```text
start_tracked_event_pipeline_canary
  -> enqueue health_check.ping in shared Outbox
  -> create pending SLI row with deployment scope and deadline
  -> commit
```

The probe then follows the ADR 0041 path. When it reaches a terminal result it finalizes the pending row idempotently. If the probe disappears and the deadline passes, the read model treats the pending row as a failed observation with bounded code `canary_probe_abandoned`.

The persistent scope is:

```text
stream_name
durable_name
consumer_name
```

This makes publish-time failures attributable even when no JetStream delivery metadata was ever produced.

## Objectives

Initial defaults are:

```text
Availability objective       99.9%
Latency objective            99% <= 5 seconds end-to-end
Fast burn threshold          14.4x
Slow burn threshold           3.0x
Stale canary threshold      180 seconds
Warning failure streak         2
Critical failure streak        3
```

The availability SLI counts terminal successful canaries divided by all terminal canaries in the window. Expired pending probes count as failures.

The latency SLI is conditional on successful canaries. A successful canary is good for latency when `end_to_end_projection_seconds <= latency_target_seconds`.

For objective `T` and observed bad ratio `B`, burn rate is:

```text
burn_rate = B / (1 - T)
```

A burn rate of `1` consumes error budget at exactly the rate allowed by the objective. A burn rate above `1` consumes it faster.

## Multi-window alerting

Use paired windows rather than alerting from one noisy sample:

```text
critical fast burn
  5m burn >= 14.4x
  AND
  1h burn >= 14.4x

warning slow burn
  1h burn >= 3x
  AND
  24h burn >= 3x
```

Apply the same paired-window rule independently to availability and latency burn.

Consecutive failures and stale execution cadence remain independent alerts because they detect failure modes that ratio-based burn can hide at low sample volume.

## Latency distribution

For successful terminal canaries, expose p50, p95, and p99 for bounded stages:

```text
outbox_publish_confirm
broker_to_inbox
inbox_to_projection
end_to_end_projection
```

`broker_to_inbox` crosses broker and database clocks. Samples with detected clock skew or a negative cross-clock duration are excluded from that stage's quantiles. PostgreSQL-local publish, projection, and end-to-end values remain usable.

## Cardinality boundary

Prometheus labels may include only bounded operational dimensions:

```text
stream
durable
consumer
window: 5m | 1h | 24h
outcome: total | succeeded | failed
stage
quantile: p50 | p95 | p99
bounded terminal code
bounded alert code and severity
```

Never label metrics with:

```text
canary_id
event_id
transport_message_id
stream sequence
consumer sequence
payload
raw error text
```

Those identities remain available in PostgreSQL for forensic inspection but not in metrics.

## Persistence semantics

`event_pipeline_canary_outcomes` is append-by-identity and state-transition-only:

```text
pending -> succeeded
pending -> failed
```

A finalized row is immutable. Repeating the exact finalization is idempotent. A conflicting finalization fails closed.

An expired `pending` row is not physically rewritten by the SLI read path. It is interpreted as `failed / canary_probe_abandoned` at query time. This preserves the original evidence that the probe never finalized itself.

## Operational commands

Run one tracked canary with the existing command:

```bash
bin/run-kernel-event-pipeline-canary ...
```

Inspect historical SLI and burn rate:

```bash
bin/inspect-kernel-event-pipeline-sli --format json --pretty
bin/inspect-kernel-event-pipeline-sli --format prometheus
bin/inspect-kernel-event-pipeline-sli --check
```

## Consequences

Benefits:

- one-shot health becomes a durable SLI series;
- canary process crashes consume availability budget instead of disappearing;
- p50, p95, and p99 make latency distribution visible;
- multi-window burn detects fast and sustained objective violations;
- failure streak and stale cadence catch low-volume blind spots;
- Prometheus cardinality remains bounded.

Costs:

- every tracked canary adds one small durable history row;
- SLI accuracy depends on running the canary at a stable cadence;
- retention for canary history is intentionally deferred until real volume establishes a useful horizon.

## Non-goals

This decision does not:

- make the canary a business transaction;
- mutate inventory state;
- replace Outbox, JetStream, consumer-failure, malformed-delivery, or projection health telemetry;
- define customer-facing SLA credits;
- introduce automatic remediation;
- delete historical canary outcomes.
