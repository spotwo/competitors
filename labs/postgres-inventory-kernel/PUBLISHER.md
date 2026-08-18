# Outbox Publisher Runtime

Executable companion to ADR 0021 and ADR 0022.

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
            ACK          NACK
             |            |
           COMMIT       COMMIT
```

It never intentionally keeps the claim transaction open during network I/O.

## Runtime API

`publisher_runtime.py` provides:

- `ClaimedEvent` - typed claimed event plus lease token;
- `Transport` - broker-neutral publish protocol;
- `OutboxStore` - claim/ack/nack persistence protocol;
- `PostgresOutboxStore` - PostgreSQL implementation using ADR 0021 functions;
- `PublisherRuntime.run_once()` - one bounded claim/publish/ack cycle;
- `InMemoryTransport` - deterministic test adapter.

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

The included process adapter writes one canonical JSON event envelope per stdout line and cycle metrics to stderr. It is for development/process evidence, not a production broker recommendation.

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

## Executable tests

`tests/test_publisher_runtime.py` proves:

1. bounded batch claim -> publish -> ACK;
2. publish failure -> NACK -> retry of the same `event_id`;
3. claim transaction commits before external publish;
4. successful publish plus expired lease produces stale ACK and safe redelivery;
5. parallel publisher workers receive disjoint batches;
6. canonical event envelope identity/version metadata reaches the Transport unchanged.

## Deferred policy

The runtime intentionally does not yet hide these choices behind defaults:

- concrete production broker;
- exponential backoff and jitter;
- poison-message threshold / dead-letter state;
- outbox retention and archival;
- async/bulk broker APIs;
- OpenTelemetry publisher spans and backlog metrics;
- per-transport ordering/partition strategy.

Those are subsequent v0.2 slices after the transport-neutral runtime contract is executable.
