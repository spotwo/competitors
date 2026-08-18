# ADR 0038: Observe NATS durable consumer backlog and progress

- Status: accepted
- Date: 2026-08-19
- Scope: PostgreSQL inventory kernel consumer transport
- Depends on: ADR 0024, ADR 0028, ADR 0032, ADR 0035

## Context

The kernel now has durable telemetry for PostgreSQL consumer failures and malformed JetStream deliveries. Those lanes only become visible after a delivery reaches application code.

A different failure mode exists entirely at the broker boundary:

```text
producer publishes successfully
  -> JetStream retains messages
  -> durable consumer stops pulling or ACKing
  -> PostgreSQL failure backlog can remain zero
  -> malformed-delivery backlog can remain zero
```

Without broker-side consumer telemetry, an idle healthy consumer and a dead consumer with an accumulating JetStream backlog can look identical from PostgreSQL alone.

## Decision

Add a read-only operational snapshot for each explicitly provisioned JetStream durable consumer.

The snapshot reads `ConsumerInfo` only. It does not bind a pull subscription, fetch a message, ACK, NAK, pause, resume, create, update, or delete a consumer.

The monitored transport identity is:

```text
JetStream stream
+ durable consumer name
```

Both values are deployment-controlled identities and may be used as bounded metric labels.

## Signals

Expose the broker-reported consumer state:

- messages pending first delivery
- delivered messages still awaiting ACK
- redelivery count
- waiting pull requests
- delivered consumer and stream sequences
- ACK-floor consumer and stream sequences
- last delivery activity time
- last ACK-floor activity time
- consumer creation time
- paused state
- finite `max_ack_pending`, when configured
- configured `max_deliver` and `ack_wait`
- whether the consumer still matches Spotwo's explicit-ACK pull-consumer contract

Derived values include ACK-capacity utilization and backlog-sensitive stall ages.

## Backlog-sensitive stall age

Old activity alone is not an incident. A consumer may legitimately receive no messages for hours or days.

Delivery stall age is evaluated only while `num_pending > 0`:

```text
pending backlog
  -> age since last delivery activity
  -> if never delivered, age since consumer creation
```

ACK stall age is evaluated only while `num_ack_pending > 0`:

```text
ACK backlog
  -> age since last ACK-floor activity
  -> before first ACK, age since last delivery
  -> if neither exists, age since consumer creation
```

This prevents an idle healthy consumer from alarming merely because its last delivery is old.

Broker and monitor wall clocks can differ slightly. Negative computed ages are clamped to zero instead of being interpreted as negative lag.

## Alert policy

Kernel defaults are operational starting points, not SLOs:

| Signal | Warning | Critical |
|---|---:|---:|
| pending first deliveries | 100 | 1,000 |
| redelivered messages | 1 | 10 |
| delivery or ACK backlog stall age | 300 s | 1,800 s |
| finite ACK capacity used | 80% | 100% |

A consumer configuration that no longer uses explicit ACK or is no longer a pull durable is critical.

A paused consumer with backlog is warning by default. Pause may be intentional, but backlog accumulation while paused must remain visible.

Deployments may override count, age, and capacity thresholds without changing kernel code.

## Health interpretation

```text
num_pending = 0
num_ack_pending = 0
old last-delivery timestamp
  -> healthy idle consumer

num_pending > 0
recent delivery progress
  -> active backlog, count thresholds apply

num_pending > 0
no delivery progress for threshold age
  -> delivery stalled

num_ack_pending > 0
no ACK-floor progress for threshold age
  -> ACK stalled

redeliveries rising
  -> delivery/ACK instability or handler timing pressure
```

A single snapshot does not claim throughput or an exact rate of change. Repeated Prometheus scrapes provide that time-series context.

## Metric cardinality

Prometheus labels are limited to:

- deployment-controlled stream name
- deployment-controlled durable consumer name
- bounded metric kind
- bounded alert code and severity

Do not export subjects, event IDs, stream message sequences as labels, payload data, exception text, worker IDs, NATS client IDs, tenant IDs, warehouse IDs, or other domain cardinality.

Consumer and stream sequence positions are numeric metric values, not labels.

## Failure behavior

If NATS is unreachable, the consumer does not exist, `ConsumerInfo` is incomplete, or the response belongs to another requested identity, inspection fails and monitoring exits critical.

Missing broker state is never converted into a synthetic healthy zero snapshot.

## Consequences

- broker backlog is visible even when application failure tables are empty
- idle consumers do not alert solely because their last activity is old
- delivery stall and ACK stall are separated
- finite ACK-capacity pressure is visible before hard saturation
- configuration drift is observable
- the inspector is read-only and cannot mutate consumer cursor state
- Prometheus cardinality remains bounded by deployment identities

## Non-goals

This decision does not automatically scale workers, pause or resume consumers, modify `max_ack_pending`, change ACK wait/backoff, create durable consumers, calculate application throughput SLOs, or replace NATS server/account-level monitoring.
