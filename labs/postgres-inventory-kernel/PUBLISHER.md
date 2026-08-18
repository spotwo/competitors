# Outbox Publisher Runtime

Executable companion to ADR 0021 through ADR 0025.

## Boundary

The publisher deliberately separates three concerns:

```text
Domain posting -> PostgreSQL Outbox -> Publisher Runtime -> Transport Adapter
```

Inventory posting never calls a broker directly.

The runtime uses short database transactions:

```text
claim + lease -> COMMIT
                    |
                    v
             Transport.publish
               /        \
            ACK        failure
             |         /     \
           COMMIT   retry   quarantine
                      |         |
                    COMMIT    COMMIT
```

It never intentionally keeps the claim transaction open during network I/O.

## Runtime API

`publisher_runtime.py` provides:

- `ClaimedEvent` - typed claimed event plus lease token and attempt number;
- `RetryPolicy` - bounded exponential backoff, deterministic jitter, and attempt budget;
- `Transport` - broker-neutral publish protocol;
- `OutboxStore` - claim/ack/nack/quarantine persistence protocol;
- `PostgresOutboxStore` - PostgreSQL implementation using ADR 0021 functions;
- `PublisherRuntime.run_once()` - one bounded claim/publish/ack cycle;
- `InMemoryTransport` - deterministic test adapter;
- `NatsJetStreamTransport` - real JetStream adapter that waits for PUB ACK.

The runtime is synchronous on purpose in this slice. Horizontal concurrency is obtained by running multiple workers; PostgreSQL `FOR UPDATE SKIP LOCKED` gives each worker a disjoint batch.

## Development worker

After the lab database is running and migrations are applied, run one cycle:

```bash
KERNEL_LAB_DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55432/kernel_lab \
  python bin/run-kernel-outbox-publisher --once
```

Run continuously:

```bash
KERNEL_LAB_DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55432/kernel_lab \
  python bin/run-kernel-outbox-publisher
```

The default process adapter writes one canonical JSON event envelope per stdout line and cycle metrics to stderr.

Run the canonical NATS adapter after provisioning a stream for `spotwo.wms.events.>`:

```bash
KERNEL_LAB_DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55432/kernel_lab \
KERNEL_LAB_NATS_URL=nats://127.0.0.1:54222 \
  python bin/run-kernel-outbox-publisher \
    --transport nats \
    --nats-stream WMS_EVENTS
```

The adapter does not provision streams. Retention, storage, replicas, duplicate window, credentials, and subject ownership are deployment configuration.

The publisher defaults are configurable at process start:

| Option | Default | Meaning |
|---|---:|---|
| `--retry-base-seconds` | 5 | delay before retrying the first failed attempt |
| `--retry-max-seconds` | 300 | hard cap for any retry delay |
| `--retry-jitter-ratio` | 0.2 | deterministic +/- jitter range |
| `--max-attempts` | 10 | failed publish attempt that enters quarantine |

Jitter is derived from `event_id + attempt_count`. The same event attempt receives the same delay on every worker, while different events are spread across the retry window.

## NATS mapping

The canonical adapter maps one Outbox event to:

```text
subject       spotwo.wms.events.<event_type>
payload       canonical compact JSON envelope
Nats-Msg-Id   stable event_id
headers       event/aggregate identity and aggregate_version
acceptance    JetStream PUB ACK for the configured stream
```

Only after the PUB ACK does `PublisherRuntime` ACK PostgreSQL. A later retry of the same stable event ID can be deduplicated by JetStream within the stream duplicate window, but consumer Inbox idempotency remains mandatory.

## Delivery semantics

The runtime preserves **at-least-once transport delivery**.

A broker may confirm a publish and the worker may then crash, lose its PostgreSQL connection, or lose its lease before the ACK is persisted. The same stable `event_id` can therefore be published again.

Consumers must combine:

```text
event_id dedupe
+ aggregate_version handling
+ side effect and Inbox receipt in one consumer transaction
```

## Ordering

There is no global delivery-order guarantee. Multiple workers can publish claimed rows in different completion order.

Consumers that project aggregate state use:

```text
aggregate_type
aggregate_id
aggregate_version
```

to detect stale/out-of-order facts. Future broker adapters may use aggregate identity as a partition or message-group key where the transport supports ordered streams.

## Failure policy and quarantine

For failed attempts below the configured limit, the runtime computes:

```text
delay = min(max_delay, base_delay * 2^(attempt_count - 1) with jitter)
```

The result is always between one second and the configured maximum. PostgreSQL persists `available_at`, `last_error`, and `last_failed_at` before releasing the lease. A tight publisher loop therefore cannot immediately reclaim the same failed row.

When `attempt_count >= max_attempts`, the runtime uses the same live claim token to atomically mark the row with `quarantined_at` and `quarantine_reason`. Quarantined rows are excluded from both the ready index and `claim_domain_events()`.

This is an Outbox quarantine, not a broker dead-letter queue. The event may have failed before any broker accepted it, and PostgreSQL remains authoritative for its publication state.

An operator can replay a quarantined event only with an identity and reason:

```sql
SELECT kernel_lab.replay_quarantined_domain_event(
  '019c0000-0000-7000-8000-000000000001',
  'on-call@example.com',
  'schema registration repaired'
);
```

Replay records the prior error, quarantine reason, and attempt count in `domain_event_outbox_operator_actions`, then clears quarantine and resets the attempt budget. Replaying a published, ready, or unknown event returns `false` and creates no audit row.

## Executable tests

`tests/test_publisher_runtime.py` proves:

1. bounded batch claim -> publish -> ACK;
2. publish failure -> NACK -> retry of the same `event_id`;
3. claim transaction commits before external publish;
4. successful publish plus expired lease produces stale ACK and safe redelivery;
5. parallel publisher workers receive disjoint batches;
6. canonical event envelope identity/version metadata reaches the Transport unchanged;
7. retry delay is bounded and deterministic for one event attempt;
8. a poison event stops at the attempt limit and cannot be reclaimed;
9. operator replay is audited and gives the event a fresh attempt budget.

`tests/test_nats_transport.py` additionally proves against a real pinned NATS server:

1. JetStream PUB ACK precedes Outbox ACK;
2. `event_id` maps to `Nats-Msg-Id`;
3. an Outbox retry after ACK persistence failure does not append a second stream record;
4. the canonical JSON envelope round-trips unchanged;
5. a durable pull consumer explicitly ACKs and redelivers an unacknowledged message;
6. an unavailable broker does not roll back committed Inventory state or Outbox facts.

## Deferred policy

The runtime intentionally does not yet hide these choices behind defaults:

- outbox retention and archival;
- quarantine alerting and bulk operator tooling;
- async/bulk broker APIs;
- OpenTelemetry publisher spans and backlog metrics;
- NATS authentication, TLS, account isolation, clustering, Leaf Nodes, and production stream provisioning;
- strict per-aggregate processing lanes where a capability requires them.

Those remain subsequent runtime and deployment slices after the concrete transport path is executable.
