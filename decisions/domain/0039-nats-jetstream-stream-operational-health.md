# ADR 0039: Observe JetStream stream operational health

- Status: accepted
- Date: 2026-08-19
- Scope: canonical NATS JetStream domain-event transport
- Depends on: ADR 0023, ADR 0024, ADR 0038

## Context

ADR 0038 makes one durable consumer observable, but consumer health does not establish that the underlying JetStream stream is healthy. A consumer can appear idle while storage is near a finite limit, replicas are unhealthy, or JetStream reports lost messages.

The transport therefore needs a stream-level read-only health boundary between publisher telemetry and consumer telemetry.

## Decision

Read `stream_info` for one explicitly named existing stream and expose a bounded operational snapshot. The inspector must not publish, fetch, ACK, purge, delete, alter retention, or create streams or consumers.

The snapshot includes:

- retained message and byte counts
- first and last stream sequence
- first and last retained message timestamps
- consumer count
- server-reported deleted sequence count
- server-reported lost-message count
- finite `max_msgs` and `max_bytes` utilization when configured
- configured `max_age` as telemetry only
- storage, retention, discard, and replica configuration
- cluster leader presence when cluster information exists
- aggregate follower count, offline follower count, not-current follower count, and maximum follower lag

No subject names, replica server names, message IDs, payloads, headers, event IDs, or per-sequence labels are exported to Prometheus.

## Health semantics

An empty or old stream is not unhealthy by itself.

Finite capacity is evaluated only when the stream has an explicit positive limit. Unlimited or disabled limits do not create a synthetic utilization percentage.

Default capacity thresholds are:

```text
warning  = 80%
critical = 95%
```

These defaults are deployment starting points, not universal SLOs.

JetStream-reported lost messages are always critical because they indicate storage evidence loss rather than ordinary retention.

For clustered streams:

```text
leader absent            -> critical
offline follower > 0     -> critical
not-current follower > 0 -> warning
```

Follower lag is also bounded by deployment-configurable message thresholds. Defaults are 1,000 warning and 10,000 critical.

Deleted sequence entries are telemetry only. Retention, explicit deletion, compaction, and limits can legitimately create deleted entries, so their existence alone is not an incident.

`max_age` is telemetry only in this slice. A message approaching its configured age limit is expected retention behavior and does not imply transport failure.

## Read-only contract

The inspector calls only the JetStream information API. It must not affect:

- message count
- byte count
- first or last sequence
- delivery state
- ACK state
- consumer state
- retention state

The integration test snapshots the stream before and after inspection to enforce this boundary.

## Operational placement

The event path is now observable at four different layers:

```text
PostgreSQL Outbox
      -> JetStream stream health
      -> durable consumer health
      -> Inbox / projection / failure lanes
```

Each layer answers a different question. Stream health is not a substitute for consumer backlog or end-to-end event latency.

## Consequences

- storage pressure becomes visible before hard finite limits are reached
- JetStream storage loss is surfaced explicitly
- replica availability and lag are visible without high-cardinality peer labels
- standalone single-node lab streams remain valid and healthy without fabricated cluster state
- the inspector is safe to scrape because it is read-only
- monitoring can distinguish broker/stream faults from downstream consumer faults

## Non-goals

This decision does not define automatic scaling, stream mutation, purge, retention changes, replica repair, subject-level cardinality, cross-stream aggregation, growth-rate prediction, end-to-end event latency, or SLO burn-rate alerting.
