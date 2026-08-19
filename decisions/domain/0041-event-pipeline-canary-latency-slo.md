# ADR 0041 - Synthetic Event Pipeline Canary and Latency SLO

Status: Accepted

Date: 2026-08-19

## Context

ADR 0040 combines the existing read-only health signals for Outbox, JetStream, durable consumers, malformed delivery quarantine, consumer failures, and projection gaps. That view can explain visible pressure, but it cannot prove that a newly committed event can traverse the whole deployed path now.

A pipeline can appear healthy while a hidden integration boundary is broken. Examples include a publisher process that is alive but no longer draining the right database, a subject filter that no longer matches, a consumer that receives messages but cannot commit its local effect, or an ACK path that never advances.

Sequence arithmetic across PostgreSQL and JetStream is not a reliable substitute for an actual transaction. We therefore need a real correlated event that follows the same transport and Inbox rules without changing inventory state.

## Decision

Add a synthetic `health_check.ping` domain event whose aggregate type is `EventPipelineCanary`.

The canary uses the production-shaped event path:

```text
PostgreSQL transaction
  -> shared transactional Outbox
  -> normal Outbox publisher
  -> JetStream PUB ACK
  -> dedicated exact-filter durable consumer
  -> normal Inbox transaction
  -> synthetic canary projection row
  -> JetStream ACK
```

The canary never updates inventory positions, quantities, reservations, allocations, warehouse work, or a real inventory projection.

## Synthetic event contract

Each probe creates a fresh UUIDv7 canary identity. The Outbox event has:

```text
type              health_check.ping
aggregate_type    EventPipelineCanary
aggregate_id      <canary UUID>
aggregate_version 1
schema_version    1
subject           event-pipeline-canary/<canary UUID>
data.synthetic    true
```

The Outbox deduplication key is unique to the canary run. The normal canonical event envelope and NATS headers remain unchanged.

The event is intentionally a domain-event-shaped health contract rather than an inventory event. Production handlers must not route it into inventory projections.

## Shared producer path

The probe writes through `enqueue_domain_event()` into the same Outbox used by normal WMS events. It does not publish directly to NATS and does not invoke `PublisherRuntime` itself.

This is important operationally. A health probe must not claim or publish unrelated business Outbox rows just because it ran on a monitoring host. The deployed normal Outbox publisher is the component under test.

`published_at` remains the producer-side confirmation boundary. It is written only after JetStream returns a PUB ACK and the publisher subsequently commits the Outbox acknowledgement.

## Dedicated consumer path

The canary consumer is a separately provisioned durable pull consumer. Its filter must match exactly:

```text
<subject-prefix>.health_check.ping
```

For the default subject prefix this is:

```text
spotwo.wms.events.health_check.ping
```

The canary worker refuses to run if the durable is missing, is not a pull consumer, does not use explicit ACK, or does not have the exact canary filter.

This exact filter is a safety boundary. A canary worker must never consume ordinary WMS events.

The worker reuses `InboxConsumerRuntime`, `PostgresInboxStore`, the valid-event durable failure lane, and malformed-delivery quarantine. Therefore the same database commit-before-ACK rule applies to the synthetic event.

## Synthetic projection boundary

The Inbox handler validates the canary event type, aggregate type, versions, subject, aggregate identity, and `synthetic=true` marker. It then records a canary projection timestamp and trusted transport evidence in `event_pipeline_canary_runs` inside the same transaction as the Inbox receipt.

The persisted evidence includes:

- logical consumer identity;
- Inbox first-seen timestamp;
- JetStream stream and durable consumer;
- stream and consumer sequence;
- delivery count;
- transport message identity;
- broker timestamp.

A repeated application with different evidence fails closed.

## ACK confirmation

The Inbox transaction commits before `ack_sync`, so the projection timestamp proves the local effect committed but does not itself prove that JetStream processed the ACK.

The probe therefore reads the dedicated consumer's `consumer_info` after projection. ACK is considered confirmed only when the durable consumer's stream ACK floor is greater than or equal to the canary's trusted stream sequence.

This inference is safe only because the consumer is dedicated to the exact canary subject. It would be ambiguous on a broad business consumer.

The ACK observation time is not presented as an exact ACK timestamp. Polling provides confirmation state, not exact broker processing latency.

## Latency model

The canary exposes these stage measurements:

```text
outbox_publish_confirm = published_at - started_at
start_to_broker        = broker_timestamp - started_at
broker_to_inbox        = inbox_first_seen_at - broker_timestamp
inbox_to_projection    = projected_at - inbox_first_seen_at
end_to_end_projection  = projected_at - started_at
```

`outbox_publish_confirm`, `inbox_to_projection`, and `end_to_end_projection` use PostgreSQL timestamps and are the primary SLO evidence.

`start_to_broker` and `broker_to_inbox` cross PostgreSQL and NATS clocks. They are diagnostic stage measurements. Negative values beyond the configured tolerance produce a clock-skew alert instead of being clamped to zero.

ACK confirmation is a separate completion condition and does not fabricate an exact ACK latency.

## Default SLO policy

The initial lab defaults are deliberately configurable deployment policy:

| Stage | Warning | Critical |
|---|---:|---:|
| Outbox publish confirmation | 2 s | 5 s |
| Broker to Inbox delivery | 2 s | 5 s |
| Inbox to synthetic projection | 1 s | 3 s |
| End-to-end projection | 5 s | 15 s |

The default cross-clock skew tolerance is 1 second. The default overall probe timeout is 30 seconds.

A deployment should tune these thresholds from observed percentiles and business SLOs rather than treating the lab defaults as universal targets.

## Timeout diagnosis

If the probe reaches its deadline before completion, it emits one bounded stage-oriented timeout code based on the furthest durable evidence reached:

```text
no published_at       -> canary_publish_timeout
published, no Inbox   -> canary_delivery_timeout
Inbox, no projection  -> canary_projection_timeout
projection, no ACK    -> canary_ack_timeout
```

The diagnostic is evidence-based. It does not claim that the named component is the ultimate causal fault.

## Metric cardinality

Prometheus labels may include the configured stream, dedicated durable, logical consumer, bounded stage, alert code, and severity.

They must not include:

- `canary_id`;
- `event_id`;
- stream or consumer sequence values;
- transport message IDs;
- raw subject instances;
- payloads;
- error text.

Per-run identities remain available in JSON and PostgreSQL for forensic correlation.

## Provisioning and ownership

The probe and worker do not create, alter, or delete JetStream streams or consumers. Infrastructure provisioning owns the dedicated durable and its exact filter.

The probe is mutating by design because a real event is the measurement. It must run at a bounded cadence. Canary row and event retention are separate operational policy and are not automatically purged by the probe.

## Non-goals

This slice does not:

- mutate inventory as a health check;
- claim unrelated Outbox work from the probe process;
- create a stream or consumer at runtime;
- infer an exact ACK timestamp from polling;
- manufacture cross-system latency from unrelated sequence counters;
- automatically remediate a degraded pipeline;
- define a global production SLO for every deployment.

## Executable invariants

The kernel lab proves that:

1. starting a canary creates a synthetic Outbox event and no inventory mutation;
2. the canary handler records its projection inside the Inbox transaction;
3. transport evidence comes from trusted JetStream delivery metadata;
4. the canary consumer is constrained to the exact health-check subject;
5. a real PostgreSQL -> Outbox -> JetStream -> Inbox -> projection -> ACK-floor test completes;
6. producer, delivery, projection, and end-to-end stage latencies are derived from actual canary evidence;
7. clock skew is surfaced rather than silently clamped;
8. timeout codes identify the furthest durable stage reached;
9. Prometheus output excludes per-run identities and transport evidence labels.

## Consequences

### Positive

- monitoring now proves real event traversal rather than only inspecting component state;
- the existing Outbox, JetStream, Inbox, and ACK correctness boundaries are exercised together;
- synthetic traffic cannot mutate inventory state;
- the dedicated exact-filter consumer gives unambiguous ACK-floor confirmation;
- latency data can support an explicit event-pipeline SLO.

### Costs and limits

- each probe creates a small amount of durable synthetic state and JetStream traffic;
- a dedicated consumer must be provisioned and operated;
- stage measurements involving broker time require reasonably synchronized clocks;
- ACK polling proves completion but is not a precise ACK-latency timer;
- a successful canary proves the synthetic route, not every business handler or subject family.
