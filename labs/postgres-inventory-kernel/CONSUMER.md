# Inbox Consumer Runtime

Executable companion to ADR 0028, ADR 0032, ADR 0033, ADR 0034, ADR 0035, ADR 0036, and ADR 0037.

## Boundary

The valid-event runtime converts at-least-once broker delivery into an exactly-once local database effect for one stable logical consumer:

```text
JetStream delivery
  -> validate envelope and headers
  -> Inbox receipt + handler SQL in one PostgreSQL transaction
  -> commit
  -> JetStream ACK sync
```

Malformed bytes and invalid envelopes branch before Inbox identity:

```text
JetStream delivery
  -> trusted JetStream delivery identity
  -> decode / validate
  -> malformed: durable poison quarantine
  -> commit
  -> JetStream ACK sync
```

It does not make arbitrary network side effects exactly-once.

## Runtime API

`consumer_runtime.py` provides:

- `ConsumedEvent` - validated canonical event identity and envelope;
- `InboxDeliveryMetadata` - bounded transport evidence stored with the first receipt;
- `MalformedDeliveryEvidence` - bounded pre-`event_id` poison evidence;
- `InboxHandler` - local transactional effect protocol;
- `InboxSource` and `InboxStore` - broker and database-neutral boundaries;
- `PostgresInboxStore` - Inbox receipt plus handler in one transaction;
- `InboxFailureLane` - optional valid-event failure handoff boundary;
- `InboxMalformedDeliveryLane` - optional pre-`event_id` durable poison boundary;
- `InboxConsumerRuntime.run_once()` - one fetch, durable handoff or transaction, and ACK cycle.

`consumer_failure.py` provides:

- `ConsumerFailureClassifier` - bounded retryable or terminal failure codes;
- `DurableConsumerFailureLane` - complete-envelope capture before ACK;
- `PostgresConsumerFailureStore` - deferral, leased retry, quarantine, and audited replay persistence;
- `ConsumerFailureRetryRuntime` - broker-independent local retry cycle;
- `ConsumerRetryPolicy` - bounded exponential delay with deterministic jitter.

`consumer_failure_telemetry.py` provides:

- `PostgresConsumerFailureTelemetryStore` - read-only global or consumer-scoped backlog snapshots;
- `ConsumerFailureTelemetrySnapshot` - fail-closed state, quarantine-kind, attempt, and age invariants;
- `ConsumerFailureAlertPolicy` - stable quarantine, backlog, and ready-age alerts;
- bounded JSON data and Prometheus exposition without event or error labels.

`consumer_failure_retention.py` provides:

- `ConsumerFailureRetentionPolicy` - bounded live-history window and batch size;
- `PostgresConsumerFailureArchiveStore` - one transactional, consumer-scoped archive batch;
- `ConsumerFailureArchiveBatch` - archive run identity and reconciled failure/action counts.

`malformed_delivery.py` provides `PostgresNatsMalformedDeliveryQuarantine`, which persists pre-`event_id` poison evidence by trusted JetStream identity before ACK.

`malformed_delivery_telemetry.py` provides read-only live poison snapshots, bounded failure kinds, recent-activity alerts, JSON output, and Prometheus exposition without transport sequence or payload labels.

`malformed_delivery_retention.py` provides:

- `MalformedDeliveryRetentionPolicy` - bounded inactive-evidence window and batch size;
- `PostgresMalformedDeliveryArchiveStore` - one transactional, consumer-scoped archive batch;
- `MalformedDeliveryArchiveBatch` - archive run identity and archived delivery count.

`nats_consumer.py` provides `NatsJetStreamPullSource`, which binds to one existing durable pull consumer, captures trusted transport metadata before decoding application bytes, and confirms ACK with `ack_sync`.

## Composition

A concrete projection supplies its own handler plus durable lanes for valid-event failures and malformed transport poison:

```python
from consumer_failure import (
    DurableConsumerFailureLane,
    PostgresConsumerFailureStore,
)
from consumer_runtime import InboxConsumerRuntime, PostgresInboxStore
from malformed_delivery import PostgresNatsMalformedDeliveryQuarantine
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
        failure_lane=DurableConsumerFailureLane(
            store=PostgresConsumerFailureStore(database_url),
        ),
        malformed_lane=PostgresNatsMalformedDeliveryQuarantine(database_url),
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

Malformed deliveries have no trusted `event_id`. Their poison identity is instead:

```text
consumer_name
+ JetStream stream
+ JetStream durable consumer
+ JetStream stream sequence
```

## Failure behavior

### Handler or PostgreSQL failure

The transaction rolls back, including the just-inserted Inbox receipt.

Without a failure lane, the runtime does not ACK and JetStream can redeliver after its configured ACK wait.

With `DurableConsumerFailureLane`, the runtime classifies the error and commits the complete valid envelope as `deferred` or `quarantined`. It ACKs only after that handoff commits. If capture fails, the broker remains unacknowledged.

### Deferred local retry

A `deferred` event is no longer transport-owned. A leased PostgreSQL worker inserts the normal Inbox receipt, runs the original handler, and marks the failure row `resolved` in one transaction. A retry failure rolls that transaction back, then records another bounded delay or quarantine at the local attempt limit.

For the concrete Position projector:

```bash
KERNEL_LAB_DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55432/kernel_lab \
  bin/run-kernel-position-projection-retries \
  --consumer-name inventory-position-quantity-projector
```

Use `--once` for one bounded claim cycle. Multiple workers can share the same consumer identity; `FOR UPDATE SKIP LOCKED` and claim tokens prevent concurrent ownership of one row.

### Quarantine and replay

Terminal contract failures and exhausted retries remain queryable with their full envelope. Replay requires one stable command identity, operator, and reason:

```sql
SELECT
  consumer_name,
  event_id,
  failure_code,
  retryable,
  attempt_count,
  quarantined_at,
  quarantine_reason,
  envelope
FROM kernel_lab.domain_event_consumer_failures
WHERE status = 'quarantined'
ORDER BY quarantined_at, consumer_name, event_id;
```

```bash
KERNEL_LAB_DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55432/kernel_lab \
  bin/replay-kernel-consumer-failure \
  --replay-id 018f0000-0000-7000-8000-000000000010 \
  --consumer-name inventory-position-quantity-projector \
  --event-id 018f0000-0000-7000-8000-000000000011 \
  --operator operator@example.com \
  --reason "validated event after handler repair"
```

Replay changes `quarantined` to `deferred` and starts one fresh bounded attempt budget while retaining the prior budget in the action audit. It does not create an Inbox receipt, run the handler, or declare success. The retry worker still uses the normal transaction path.

### Failure telemetry

Inspect all unresolved failure lanes or one stable logical consumer independently of the retry worker:

```bash
KERNEL_LAB_DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55432/kernel_lab \
  bin/inspect-kernel-consumer-failures \
  --consumer-name inventory-position-quantity-projector \
  --pretty
```

Add `--format prometheus` for scraping or `--check` for monitoring exit codes. The snapshot classifies every unresolved row as `ready`, `delayed`, `leased`, or `quarantined`; resolved history is excluded. Quarantine is split into bounded `terminal` and `attempt_limit` counts. Metric labels never contain event IDs, Position IDs, failure codes, exception text, envelopes, or worker identity.

Default alert starting points are any quarantine entry as critical, backlog at 100/1,000 warning/critical, and oldest ready age at 300/1,800 seconds. These are deployment inputs, not kernel SLOs. See `OBSERVABILITY.md` for the full metric contract.

### Resolved failure archive

Archive old resolved history independently for each stable logical consumer:

```bash
KERNEL_LAB_DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55432/kernel_lab \
  bin/archive-kernel-consumer-failures \
  --consumer-name inventory-position-quantity-projector \
  --pretty
```

Eligibility requires `status = 'resolved'`, `resolved_at` before the cutoff, and a matching Inbox receipt. Recent resolved rows, unresolved rows, and resolved rows without Inbox evidence remain live. The complete failure and replay-action history moves in one transaction while the Inbox receipt remains unchanged.

Exact retries of an archived `replay_id` still return `duplicate`; changed reuse fails closed. The command runs one bounded parent-failure batch and never loops inside the retry worker. See `RETENTION.md` for scheduling, controlled cutoffs, verification, and deferred purge boundaries.

### Commit succeeds, ACK fails

The Inbox receipt and effect remain committed. JetStream may redeliver. The next transaction sees the duplicate receipt, skips the handler, commits, and retries `ack_sync`.

### Malformed bytes, invalid envelope, or NATS header mismatch

The adapter captures trusted JetStream stream, durable consumer, stream sequence, subject, and delivery metadata before application bytes or headers are trusted. Invalid UTF-8, invalid JSON, invalid Domain Event envelopes, and transport-header mismatches enter `PostgresNatsMalformedDeliveryQuarantine`.

```text
malformed delivery
  -> trusted JetStream identity
  -> durable poison record
  -> commit
  -> ACK
```

No synthetic `event_id`, Inbox receipt, or valid-event consumer failure row is created. If poison persistence fails, ACK is forbidden. If ACK confirmation fails after commit, redelivery idempotently updates the same poison identity.

Inspect recent malformed activity with `bin/inspect-kernel-malformed-deliveries`. Archive old inactive evidence with `bin/archive-kernel-malformed-deliveries`. Archive eligibility uses `last_seen_at`, so a recently reobserved record stays live.

An archived transport identity is still identity-bearing. Matching reobservation reactivates the live record with cumulative observation history; conflicting subject, payload evidence, or headers fails closed. See `MALFORMED_DELIVERIES.md` for the full operational contract.

### External effect

Do not call HTTP, email, devices, or third-party APIs and label the result exactly-once. Append a command to a local transactional Outbox in the handler, commit it with the Inbox receipt, then publish it independently.

## Ordering

Deduplication is not ordering. A projection uses `aggregate_version` to define its policy:

- lower version - stale semantic fact;
- equal version - duplicate only for the same event identity, otherwise conflict;
- next version - apply;
- version gap - defer, rebuild, or alert according to the projection contract.

The generic runtime does not silently discard gaps or serialize all aggregates globally.

ADR 0029 defines one concrete policy for `inventory.position.changed`: a per-Position PostgreSQL cursor applies the next version, audits lower versions as stale, rejects same-version conflicts, and durably buffers gaps before ACK. See `PROJECTIONS.md` for composition, inspection, bootstrap, and failure behavior.

## Inbox retention

Keep Inbox receipts at least as long as the same logical consumer can receive or replay the corresponding broker messages. Producer Outbox archive age does not determine this window.

Deleting an Inbox receipt explicitly removes its deduplication protection. Treat Inbox cleanup as a consumer-specific destructive operation with its own proof and rollback plan.

## Executable tests

`tests/test_consumer_runtime.py` proves:

1. commit precedes transport ACK;
2. store failure produces no ACK;
3. receipt and local handler effect commit once;
4. handler failure rolls both back;
5. concurrent duplicates serialize to one handler;
6. durable failure handoff commits before ACK;
7. handoff failure produces no ACK;
8. an existing handoff cannot bypass its local lane;
9. canonical envelope validation fails closed;
10. malformed durable capture commits before ACK and never enters Inbox identity.

`tests/test_consumer_failures.py` proves classification, payload binding, deferred retry, terminal quarantine, attempt exhaustion, exclusive claims, and stable audited replay.

`tests/test_consumer_failure_telemetry.py` proves exclusive state classification, resolved-row exclusion, scoped/global aggregation, quarantine and attempt partitions, deterministic alerts, and bounded-cardinality export.

`tests/test_consumer_failure_retention.py` proves consumer-scoped eligibility, preserved Inbox evidence, complete failure/action archival, payload-bound replay IDs after archival, disjoint concurrent batches, transactional rollback, and bounded inputs.

`tests/test_malformed_delivery_telemetry.py` proves live poison snapshot reconciliation, scoped/global aggregation, recent-activity alerting, bounded failure kinds, and bounded-cardinality export.

`tests/test_malformed_delivery_retention.py` proves last-observation eligibility, complete forensic archival, bounded disjoint concurrent batches, archived-identity reactivation, archived collision failure, and bounded retention inputs.

`tests/test_nats_consumer.py` proves against the pinned real server:

1. ACK confirmation loss after commit redelivers the same event;
2. redelivery finds the receipt, skips the handler, and ACKs;
3. base-runtime handler failure leaves no receipt and redelivers for a successful retry;
4. the adapter binds only to a pre-provisioned explicit-ACK pull consumer;
5. transport header and envelope mismatches fail closed or enter durable malformed quarantine when configured;
6. an out-of-order Position event ACKs after durable buffering and drains when the missing version arrives;
7. a rebuild fence with `max_deliver=1` hands off and ACKs once, then applies through local retry without broker redelivery;
8. malformed JetStream deliveries are captured by trusted transport identity before ACK.

`tests/test_position_projection.py` proves the concrete version policy, quantity invariants, gap inspection, and per-Position concurrency independently of transport timing.
