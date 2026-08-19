# Event pipeline canary SLI operations

This runbook turns repeated synthetic event-pipeline canaries into a durable service-level indicator series.

The underlying canary remains intentionally synthetic. It exercises the deployed Outbox -> publisher -> JetStream -> Inbox -> synthetic projection -> ACK path and does not mutate inventory state.

## Recommended cadence

Run one tracked canary per deployment scope every minute.

A deployment scope is the tuple:

```text
stream
durable
consumer
```

The tracked canary command persists that scope and its deadline atomically with the synthetic Outbox event. If the process exits after the start but before finalization, the expired pending run is counted as `canary_probe_abandoned`.

Example:

```bash
bin/run-kernel-event-pipeline-canary \
  --database-url "$KERNEL_LAB_DATABASE_URL" \
  --nats-url "$KERNEL_LAB_NATS_URL" \
  --tenant-id "$KERNEL_LAB_CANARY_TENANT_ID" \
  --stream WMS_EVENTS \
  --durable EVENT_PIPELINE_CANARY \
  --consumer-name event_pipeline_canary \
  --timeout-seconds 30 \
  --check
```

The process exit code is still the current canary result:

```text
0  ok
1  warning
2  critical
```

## Historical SLI inspection

JSON:

```bash
bin/inspect-kernel-event-pipeline-sli \
  --database-url "$KERNEL_LAB_DATABASE_URL" \
  --stream WMS_EVENTS \
  --durable EVENT_PIPELINE_CANARY \
  --consumer-name event_pipeline_canary \
  --pretty
```

Prometheus exposition:

```bash
bin/inspect-kernel-event-pipeline-sli \
  --database-url "$KERNEL_LAB_DATABASE_URL" \
  --stream WMS_EVENTS \
  --durable EVENT_PIPELINE_CANARY \
  --consumer-name event_pipeline_canary \
  --format prometheus
```

Health-check exit code:

```bash
bin/inspect-kernel-event-pipeline-sli \
  --database-url "$KERNEL_LAB_DATABASE_URL" \
  --stream WMS_EVENTS \
  --durable EVENT_PIPELINE_CANARY \
  --consumer-name event_pipeline_canary \
  --check
```

## Default objectives

```text
Availability                 99.9%
End-to-end latency           99% <= 5s
Fast burn                    14.4x on both 5m and 1h
Slow burn                     3.0x on both 1h and 24h
Stale canary cadence         180s
Failure streak warning         2
Failure streak critical        3
```

Override objectives only when the deployment has an explicit service-level policy. Do not tune thresholds merely to silence an incident.

## Interpreting burn rate

A burn rate of `1` means the current bad-event ratio is consuming the error budget at exactly the rate permitted by the objective.

Examples for a 99.9% availability target:

```text
0.5x   consuming half the allowed error rate
1.0x   consuming exactly the allowed error rate
3.0x   sustained budget pressure
14.4x  fast budget exhaustion
100x   severe failure concentration
```

The alert policy requires both windows in a pair to cross the threshold. This suppresses a short isolated spike while still detecting fast and sustained failure.

## Latency quantiles

The inspector exposes p50, p95, and p99 for:

```text
outbox_publish_confirm
broker_to_inbox
inbox_to_projection
end_to_end_projection
```

`broker_to_inbox` crosses clocks. A canary with detected clock skew is excluded from that stage's distribution. The PostgreSQL-local stages remain valid.

## Failure codes

Terminal failures are intentionally bounded. Expected codes include:

```text
canary_publish_timeout
canary_delivery_timeout
canary_projection_timeout
canary_ack_timeout
canary_incomplete_timeout
canary_consumer_configuration_invalid
canary_stream_identity_mismatch
canary_durable_identity_mismatch
canary_consumer_identity_mismatch
canary_ack_observer_unavailable
canary_incomplete
canary_probe_abandoned
```

`canary_probe_abandoned` means the synthetic event was started with a durable scope and deadline, but the probe process did not finalize its observation before that deadline.

## Triage order

When SLI health is critical, inspect in this order:

```text
1. Is last_run_age_seconds above the stale threshold?
2. Are there consecutive failures?
3. Is availability fast burn active?
4. Is latency fast burn active?
5. Which bounded terminal code dominates the 5m and 1h windows?
6. Which latency stage shows the p95/p99 increase?
7. Use the one-shot event-pipeline health inspectors to localize Outbox, stream, durable, poison, failure-lane, or projection pressure.
```

Do not use canary IDs, event IDs, transport IDs, stream sequences, payloads, or raw errors as Prometheus labels. Query PostgreSQL directly when forensic identity is required.

## Scheduling guidance

The repository does not create a production scheduler. Run the tracked canary from the deployment's existing scheduler, monitoring agent, Kubernetes CronJob, systemd timer, or equivalent control plane.

For a one-minute cadence, keep `--timeout-seconds` comfortably below 60 seconds so runs normally do not overlap. The default 30 seconds is suitable for the initial deployment.

The SLI inspector can be scraped more frequently than the canary itself because it is read-only.
