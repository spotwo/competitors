# Event Pipeline Canary

The event pipeline canary is an intentionally mutating synthetic probe. It proves that a newly committed event can traverse the deployed PostgreSQL Outbox, normal publisher, JetStream, dedicated consumer, Inbox transaction, synthetic projection, and broker ACK path.

It does not modify inventory state.

## Topology

```text
run-kernel-event-pipeline-canary
  -> enqueue health_check.ping in shared PostgreSQL Outbox
  -> deployed run-kernel-outbox-publisher
  -> JetStream PUB ACK
  -> dedicated exact-filter durable consumer
  -> run-kernel-event-pipeline-canary-consumer
  -> Inbox receipt + synthetic canary projection in one transaction
  -> ack_sync
  -> probe observes durable ACK floor
```

The probe deliberately does not run the Outbox publisher itself. Doing so could claim unrelated business work and would avoid testing the deployed publisher process.

## Required JetStream configuration

Use the same stream as the WMS event publisher, but provision a dedicated pull consumer for canary traffic.

For the default publisher prefix, the consumer filter must be exactly:

```text
spotwo.wms.events.health_check.ping
```

Required consumer properties:

```text
durable name      dedicated to canaries
ack policy        explicit
mode              pull consumer
filter subject    exactly <subject-prefix>.health_check.ping
```

The canary worker performs a read-only `consumer_info` preflight and refuses to start when this contract is not satisfied. It never creates or repairs broker configuration.

## Start the canary consumer

Example:

```bash
export KERNEL_LAB_DATABASE_URL='postgresql://...'
export KERNEL_LAB_NATS_URL='nats://...'

bin/run-kernel-event-pipeline-canary-consumer \
  --stream WMS_EVENTS \
  --durable EVENT_PIPELINE_CANARY \
  --consumer-name event_pipeline_canary
```

This worker reuses the normal Inbox runtime. A valid handler failure goes to the durable consumer-failure lane before broker ACK. A malformed delivery goes to malformed-delivery quarantine before ACK.

The handler accepts only the synthetic contract:

```text
type              health_check.ping
aggregate_type    EventPipelineCanary
data.synthetic    true
```

Any ordinary WMS event reaching this worker is rejected. The exact broker filter should prevent that situation before application code is involved.

## Run one probe

```bash
bin/run-kernel-event-pipeline-canary \
  --tenant-id 00000000-0000-0000-0000-000000000001 \
  --stream WMS_EVENTS \
  --durable EVENT_PIPELINE_CANARY \
  --consumer-name event_pipeline_canary \
  --pretty \
  --check
```

The tenant owns the synthetic Outbox event only. The event has no warehouse and never reaches inventory projections.

Exit codes with `--check` are:

```text
0  ok
1  warning
2  critical
```

Use `--format prometheus` for bounded-cardinality metrics.

## What completion means

A canary is complete only when all of these are true:

```text
Outbox published_at exists
synthetic projection committed
observed stream matches expected stream
observed durable matches expected durable
logical Inbox consumer matches expected consumer
JetStream ACK floor >= canary stream sequence
```

The last condition verifies that JetStream processed an ACK after the local projection committed.

The probe does not expose the polling timestamp as an exact ACK timestamp. It is confirmation evidence only.

## Latency measurements

The JSON report exposes:

```text
outbox_publish_confirm
start_to_broker
broker_to_inbox
inbox_to_projection
end_to_end_projection
```

Definitions:

```text
outbox_publish_confirm = published_at - started_at
start_to_broker        = broker_timestamp - started_at
broker_to_inbox        = inbox_first_seen_at - broker_timestamp
inbox_to_projection    = projected_at - inbox_first_seen_at
end_to_end_projection  = projected_at - started_at
```

The primary same-clock SLO measurements are `outbox_publish_confirm`, `inbox_to_projection`, and `end_to_end_projection` because their endpoints use PostgreSQL timestamps.

`start_to_broker` and `broker_to_inbox` cross PostgreSQL and NATS clocks. They are useful diagnostics when clocks are synchronized. Negative values beyond the configured tolerance generate `canary_clock_skew_detected` instead of being normalized away.

## Default thresholds

| Measurement | Warning | Critical |
|---|---:|---:|
| Outbox publish confirmation | 2 s | 5 s |
| Broker to Inbox delivery | 2 s | 5 s |
| Inbox to synthetic projection | 1 s | 3 s |
| End-to-end projection | 5 s | 15 s |

Probe timeout defaults to 30 seconds. Clock-skew tolerance defaults to 1 second.

All values are command-line configurable. Production thresholds should be tuned from actual latency distributions and business requirements.

## Timeout interpretation

When the probe reaches its deadline, it reports the furthest durable stage reached:

```text
canary_publish_timeout     no Outbox published_at
canary_delivery_timeout    published, but no Inbox receipt
canary_projection_timeout  Inbox receipt exists, but projection did not commit
canary_ack_timeout         projection committed, but ACK floor did not pass sequence
```

These are diagnostic stage names, not automatic causal proof. Combine them with `bin/inspect-kernel-event-pipeline` and the component-specific inspectors when debugging.

## Prometheus contract

The aggregate canary metrics are:

```text
spotwo_wms_event_pipeline_canary_health_status
spotwo_wms_event_pipeline_canary_complete
spotwo_wms_event_pipeline_canary_ack_confirmed
spotwo_wms_event_pipeline_canary_age_seconds
spotwo_wms_event_pipeline_canary_latency_seconds
spotwo_wms_event_pipeline_canary_alert
```

Allowed labels are bounded deployment scope and enums:

```text
stream
durable
consumer
stage
code
severity
```

Do not add `canary_id`, `event_id`, sequence values, transport message IDs, payloads, or error text as labels.

## Suggested scheduling

Run the canary at a bounded cadence appropriate for the deployment. A one-minute or five-minute interval is usually more useful than continuous synthetic traffic. Monitoring should alert on the `--check` result or Prometheus health metric.

The canary consumer itself should remain continuously available so a probe does not measure process startup time unless that is intentionally part of the SLO.

## Operational safety

The canary is intentionally separate from inventory state:

```text
shared event infrastructure    yes
shared Outbox publisher        yes
shared JetStream stream        yes
dedicated canary consumer      yes
normal Inbox transaction       yes
inventory mutation             no
inventory projection mutation  no
```

The probe creates durable synthetic rows and messages. It does not automatically purge them. Define retention for canary evidence separately once the desired forensic window is known.

## Failure workflow

For a critical canary result:

1. Inspect the timeout or latency alert code in the canary JSON.
2. Run `bin/inspect-kernel-event-pipeline` for the same stream and relevant business consumer.
3. Inspect the specific Outbox, stream, consumer, malformed, failure, or projection component identified by the aggregate health view.
4. Verify the dedicated canary durable still has the exact subject filter.
5. Check PostgreSQL and NATS clock synchronization when cross-clock stages are negative or implausible.

Do not replay, purge, recreate broker state, or modify inventory as an automatic response to a failed canary.
