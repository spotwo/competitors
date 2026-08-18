# ADR 0040: Event pipeline operational health

- Status: accepted
- Date: 2026-08-19
- Scope: PostgreSQL inventory kernel event path
- Depends on: ADR 0026, ADR 0030, ADR 0033, ADR 0036, ADR 0038, ADR 0039

## Context

The kernel now has independent operational health views for the producer Outbox, JetStream stream, JetStream durable consumer, malformed-delivery quarantine, consumer failure lane, and projection gaps.

Those views are individually useful but leave operators to correlate several commands during an incident:

```text
PostgreSQL commit
  -> Outbox
  -> JetStream stream
  -> durable consumer
  -> Inbox / handler
  -> projection
```

A downstream symptom can be caused by an upstream stall. For example, projection freshness may degrade because the durable consumer stopped receiving, while consumer backlog may grow because the stream is healthy but the worker is dead. A single operational snapshot should show the complete path without weakening the existing component-specific semantics.

The NATS durable name and PostgreSQL logical `consumer_name` are separate identities by ADR 0028. The aggregate view must therefore require the mapping explicitly rather than assume the names are equal.

## Decision

Add a read-only `EventPipelineHealthCollector` and `bin/inspect-kernel-event-pipeline` command that compose the existing component health contracts.

The canonical component order is:

```text
outbox
  -> nats_stream
  -> nats_consumer
  -> malformed_delivery
  -> consumer_failure
  -> projection_gap
```

The failure lanes are included because they explain deliveries that have already left the normal happy path.

The collector reuses the existing component stores and alert policies. It does not duplicate their SQL or JetStream interpretation logic.

## Explicit scope

One pipeline snapshot requires:

- JetStream stream name;
- JetStream durable consumer name;
- PostgreSQL logical consumer name.

Example:

```text
stream        = WMS_EVENTS
durable       = POSITION_PROJECTOR
consumer_name = position_projection
```

The durable and logical consumer name may differ. The deployment owns that mapping.

## Collection consistency

All component reads receive one shared timezone-aware `observed_at` value so ages are evaluated against the same logical clock boundary.

The collection is read-only but not transactionally atomic across PostgreSQL and JetStream. Components are queried sequentially and may advance while the snapshot is being assembled.

The JSON contract records:

```json
{
  "collection": {
    "read_only": true,
    "atomic": false
  }
}
```

The aggregate must not claim a distributed transaction or perfectly simultaneous state.

## Pipeline status

Pipeline status is the worst current component status:

```text
any critical -> critical
else any warning -> warning
else -> ok
```

The aggregate does not average or dilute component severity.

## Root-cause candidate

The collector reports a deterministic `root_cause_candidate` for degraded snapshots.

Selection is:

1. select alerts at the pipeline's highest active severity;
2. among equal-severity alerts, select the earliest component in canonical topology order;
3. within one component, preserve that component policy's alert order.

Examples:

```text
outbox warning + projection critical
  -> projection critical candidate

outbox critical + consumer critical + projection critical
  -> outbox critical candidate

stream storage loss critical + consumer backlog critical
  -> stream storage-loss candidate
```

This field is a triage candidate, not causal proof. Distributed snapshots cannot prove that one alert caused another. All active component alerts remain present beside the candidate.

## Component unavailability

Failure to read one component is itself critical health.

The aggregate records a bounded synthetic alert such as:

```text
nats_stream_telemetry_unavailable
```

and exposes:

```json
{
  "available": false,
  "error_code": "nats_stream_telemetry_unavailable",
  "telemetry": null
}
```

Raw exception text is intentionally excluded from the stable JSON and Prometheus contracts. Operators can run the component-specific inspector for transport or database diagnostics.

One unavailable component does not stop collection of the remaining components.

## JSON and Prometheus

JSON contains the complete child telemetry snapshots plus aggregate status, active alerts, degraded components, and root-cause candidate.

Prometheus exposes only bounded aggregate dimensions:

- pipeline health status;
- per-component health status;
- per-component availability;
- degraded component count;
- active alert code and severity;
- root-cause candidate.

Labels are limited to configured stream, durable consumer, logical consumer, bounded component names, stable alert codes, and severity.

The aggregate Prometheus view intentionally does not copy every detailed component metric. Existing component inspectors remain the canonical detailed metric surfaces.

## No synthetic sequence arithmetic

The aggregate does not infer end-to-end lag by subtracting arbitrary JetStream or PostgreSQL sequence numbers.

A durable consumer can filter subjects, stream retention can create sequence gaps, and PostgreSQL projection versions are aggregate-local rather than JetStream-global. Subtracting those values would create false precision.

True commit-to-projection latency requires a separately designed correlated canary or event timestamp measurement.

## Read-only boundary

The collector may call only the existing read paths:

- PostgreSQL telemetry functions;
- JetStream `stream_info`;
- JetStream `consumer_info`.

It does not:

- publish events;
- fetch deliveries;
- ACK or NAK messages;
- create or update streams or consumers;
- insert Inbox receipts;
- replay failures;
- rebuild projections;
- archive or purge evidence.

Integration coverage verifies that a healthy aggregate inspection leaves both PostgreSQL evidence counts and JetStream stream/consumer sequence state unchanged.

## Consequences

- operators get one health answer for the whole event path;
- upstream and downstream symptoms remain visible instead of collapsing into one boolean;
- component-specific alert semantics remain authoritative;
- missing telemetry becomes explicit critical state instead of silently disappearing;
- durable-to-logical consumer mapping stays deployment-owned and explicit;
- the root-cause field accelerates triage without pretending to prove causality;
- periodic aggregate collection opens multiple read connections, which is acceptable for health inspection and can be optimized later without changing the contract.

## Non-goals

This decision does not introduce synthetic canary events, end-to-end latency SLOs, tracing correlation, automated remediation, consumer restart, replay, projection rebuild, stream mutation, or a distributed atomic snapshot.
