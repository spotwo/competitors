# Inbox Consumer Runtime

Executable companion to ADR 0028.

## Boundary

The runtime converts at-least-once broker delivery into an exactly-once local database effect for one stable logical consumer:

```text
JetStream delivery
  -> validate envelope and headers
  -> Inbox receipt + handler SQL in one PostgreSQL transaction
  -> commit
  -> JetStream ACK sync
```

It does not make arbitrary network side effects exactly-once.

## Runtime API

`consumer_runtime.py` provides:

- `ConsumedEvent` - validated canonical event identity and envelope;
- `InboxDeliveryMetadata` - bounded transport evidence stored with the first receipt;
- `InboxHandler` - local transactional effect protocol;
- `InboxSource` and `InboxStore` - broker and database-neutral boundaries;
- `PostgresInboxStore` - Inbox receipt plus handler in one transaction;
- `InboxConsumerRuntime.run_once()` - one fetch, transaction, and ACK cycle.

`nats_consumer.py` provides `NatsJetStreamPullSource`, which binds to one existing durable pull consumer and confirms ACK with `ack_sync`.

## Composition

A concrete projection supplies its own handler:

```python
from consumer_runtime import InboxConsumerRuntime, PostgresInboxStore
from nats_consumer import NatsJetStreamPullSource
from psycopg.types.json import Jsonb


def apply_projection(event, transaction):
    transaction.execute(
        """
        INSERT INTO app_projection (aggregate_id, aggregate_version, payload)
        VALUES (%s, %s, %s)
        ON CONFLICT (aggregate_id) DO UPDATE
        SET aggregate_version = EXCLUDED.aggregate_version,
            payload = EXCLUDED.payload
        WHERE app_projection.aggregate_version < EXCLUDED.aggregate_version
        """,
        (event.aggregate_id, event.aggregate_version, Jsonb(event.data)),
    )


with NatsJetStreamPullSource(
    server_url="nats://127.0.0.1:4222",
    stream_name="WMS_EVENTS",
    durable_name="AVAILABILITY_PROJECTOR",
) as source:
    runtime = InboxConsumerRuntime(
        source=source,
        store=PostgresInboxStore(database_url),
        consumer_name="availability-projector",
        handler=apply_projection,
    )
    runtime.run_once()
```

The application process owns its loop, shutdown behavior, logging, handler routing, and dependency injection. The kernel does not ship a no-op consumer command that would ACK messages without applying a domain-owned effect.

## Durable consumer provisioning

Provision the JetStream stream and consumer before starting application workers. The deployment contract includes at least:

```text
stream             WMS_EVENTS
durable            AVAILABILITY_PROJECTOR
filter subject     spotwo.wms.events.inventory.position.changed
deliver policy     explicit deployment choice
ack policy         explicit
ack wait/backoff    longer than expected handler transaction
max deliveries     bounded deployment choice
replicas/storage    deployment durability choice
```

The adapter calls `consumer_info()` before binding. A missing durable consumer is a configuration error, not a signal to invent defaults.

## Consumer identity

Treat `consumer_name` as persisted application identity:

```text
(consumer_name, event_id) -> one committed local effect
```

All replicas of one logical projector use the same value. Changing the value intentionally creates a fresh consumer namespace and permits a full replay.

NATS durable identity controls broker cursor state. Inbox consumer identity controls application-effect deduplication. Configure both explicitly even when their strings are similar.

## Failure behavior

### Handler or PostgreSQL failure

The transaction rolls back, including the just-inserted Inbox receipt. The runtime does not ACK. JetStream can redeliver after its configured ACK wait.

### Commit succeeds, ACK fails

The Inbox receipt and effect remain committed. JetStream may redeliver. The next transaction sees the duplicate receipt, skips the handler, commits, and retries `ack_sync`.

### Invalid envelope or NATS header mismatch

No database transaction or ACK occurs. The process surfaces the exception. Configure JetStream maximum delivery and advisory/DLQ handling so malformed messages cannot retry forever without operator visibility.

### External effect

Do not call HTTP, email, devices, or third-party APIs and label the result exactly-once. Append a command to a local transactional Outbox in the handler, commit it with the Inbox receipt, then publish it independently.

## Ordering

Deduplication is not ordering. A projection uses `aggregate_version` to define its policy:

- lower/equal version - stale or duplicate semantic fact;
- next version - apply;
- version gap - defer, rebuild, or alert according to the projection contract.

The generic runtime does not silently discard gaps or serialize all aggregates globally.

## Retention

Keep Inbox receipts at least as long as the same logical consumer can receive or replay the corresponding broker messages. Producer Outbox archive age does not determine this window.

Deleting an Inbox receipt explicitly removes its deduplication protection. Treat Inbox cleanup as a consumer-specific destructive operation with its own proof and rollback plan.

## Executable tests

`tests/test_consumer_runtime.py` proves:

1. commit precedes transport ACK;
2. store failure produces no ACK;
3. receipt and local handler effect commit once;
4. handler failure rolls both back;
5. concurrent duplicates serialize to one handler;
6. canonical envelope validation fails closed.

`tests/test_nats_consumer.py` proves against the pinned real server:

1. ACK confirmation loss after commit redelivers the same event;
2. redelivery finds the receipt, skips the handler, and ACKs;
3. handler failure leaves no receipt and redelivers for a successful retry;
4. the adapter binds only to a pre-provisioned explicit-ACK pull consumer;
5. transport header and envelope mismatches fail closed.
