from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import psycopg
import pytest

import conftest as lab
from publisher_runtime import (
    ClaimedEvent,
    InMemoryTransport,
    PostgresOutboxStore,
    PublishReceipt,
    PublisherRuntime,
)


def enqueue_event(conn: psycopg.Connection, *, dedup_key: str, ordinal: int = 1):
    return conn.execute(
        """
        SELECT kernel_lab.enqueue_domain_event(
          %s,
          %s,
          'inventory.test.event',
          %s,
          'InventoryPosition',
          %s,
          %s,
          %s::jsonb,
          clock_timestamp(),
          NULL,
          %s,
          NULL,
          %s,
          1
        )
        """,
        (
            lab.TENANT,
            dedup_key,
            f"inventory-position/test-{ordinal}",
            f"test-{ordinal}",
            ordinal,
            f'{{"ordinal": {ordinal}}}',
            f"corr-{ordinal}",
            lab.WAREHOUSE,
        ),
    ).fetchone()[0]


def runtime(transport, *, worker: str = "publisher-a", batch_size: int = 100, retry: int = 0):
    return PublisherRuntime(
        store=PostgresOutboxStore(lab.DATABASE_URL),
        transport=transport,
        worker_id=worker,
        batch_size=batch_size,
        lease_seconds=30,
        retry_after_seconds=retry,
    )


def test_runtime_claims_publishes_and_acks_batch():
    with lab.connect() as conn:
        event_ids = [
            enqueue_event(conn, dedup_key=f"publisher:batch:{index}", ordinal=index + 1)
            for index in range(3)
        ]
        conn.commit()

    transport = InMemoryTransport()
    result = runtime(transport).run_once()

    assert result.claimed == 3
    assert result.published == 3
    assert result.failed == 0
    assert result.stale_ack == 0
    assert {event.event_id for event in transport.messages} == set(event_ids)

    with lab.connect() as conn:
        rows = conn.execute(
            """
            SELECT event_id, published_at IS NOT NULL, claimed_by, claim_token, claimed_until
            FROM kernel_lab.domain_event_outbox
            ORDER BY event_id
            """
        ).fetchall()
        assert len(rows) == 3
        assert all(row[1] is True for row in rows)
        assert all(row[2:] == (None, None, None) for row in rows)


def test_publish_failure_nacks_and_retries_same_event_id():
    with lab.connect() as conn:
        event_id = enqueue_event(conn, dedup_key="publisher:retry", ordinal=1)
        conn.commit()

    class FailsOnce:
        def __init__(self):
            self.calls = 0
            self.seen = []

        def publish(self, event):
            self.calls += 1
            self.seen.append(event.event_id)
            if self.calls == 1:
                raise ConnectionError("broker unavailable")
            return PublishReceipt(str(event.event_id))

    transport = FailsOnce()
    publisher = runtime(transport, retry=0)

    first = publisher.run_once()
    assert first.claimed == 1
    assert first.failed == 1
    assert first.published == 0

    with lab.connect() as conn:
        published, attempts, error = conn.execute(
            """
            SELECT published_at IS NOT NULL, attempt_count, last_error
            FROM kernel_lab.domain_event_outbox
            WHERE event_id = %s
            """,
            (event_id,),
        ).fetchone()
        assert published is False
        assert attempts == 1
        assert "ConnectionError" in error

    second = publisher.run_once()
    assert second.claimed == 1
    assert second.published == 1
    assert second.failed == 0
    assert transport.seen == [event_id, event_id]

    with lab.connect() as conn:
        published, attempts, error = conn.execute(
            """
            SELECT published_at IS NOT NULL, attempt_count, last_error
            FROM kernel_lab.domain_event_outbox
            WHERE event_id = %s
            """,
            (event_id,),
        ).fetchone()
        assert published is True
        assert attempts == 2
        assert error is None


def test_claim_transaction_is_committed_before_transport_publish():
    with lab.connect() as conn:
        event_id = enqueue_event(conn, dedup_key="publisher:no-open-db-tx", ordinal=1)
        conn.commit()

    class LockProbeTransport:
        def __init__(self):
            self.lock_acquired = False

        def publish(self, event):
            # If PublisherRuntime held the claim transaction open while calling the
            # transport, NOWAIT would fail because claim_domain_events() updated and
            # still owned this row lock.
            with lab.connect() as probe:
                probe.execute(
                    """
                    SELECT event_id
                    FROM kernel_lab.domain_event_outbox
                    WHERE event_id = %s
                    FOR UPDATE NOWAIT
                    """,
                    (event.event_id,),
                ).fetchone()
                self.lock_acquired = True
                probe.rollback()
            return PublishReceipt(str(event.event_id))

    transport = LockProbeTransport()
    result = runtime(transport).run_once()

    assert result == result.__class__(claimed=1, published=1, failed=0, stale_ack=0, stale_nack=0)
    assert transport.lock_acquired is True

    with lab.connect() as conn:
        assert conn.execute(
            "SELECT published_at IS NOT NULL FROM kernel_lab.domain_event_outbox WHERE event_id = %s",
            (event_id,),
        ).fetchone()[0] is True


def test_successful_publish_with_expired_lease_is_redelivered():
    with lab.connect() as conn:
        event_id = enqueue_event(conn, dedup_key="publisher:stale-ack", ordinal=1)
        conn.commit()

    class ExpiresLeaseAfterPublish:
        def __init__(self):
            self.seen = []

        def publish(self, event):
            self.seen.append(event.event_id)
            with lab.connect() as conn:
                conn.execute(
                    """
                    UPDATE kernel_lab.domain_event_outbox
                    SET claimed_until = clock_timestamp() - interval '1 second'
                    WHERE event_id = %s
                    """,
                    (event.event_id,),
                )
                conn.commit()
            return PublishReceipt(str(event.event_id))

    first_transport = ExpiresLeaseAfterPublish()
    first = runtime(first_transport, worker="publisher-a").run_once()

    assert first.claimed == 1
    assert first.published == 1
    assert first.stale_ack == 1

    with lab.connect() as conn:
        assert conn.execute(
            "SELECT published_at IS NULL FROM kernel_lab.domain_event_outbox WHERE event_id = %s",
            (event_id,),
        ).fetchone()[0] is True

    second_transport = InMemoryTransport()
    second = runtime(second_transport, worker="publisher-b").run_once()

    assert second.claimed == 1
    assert second.published == 1
    assert second.stale_ack == 0
    assert first_transport.seen == [event_id]
    assert [event.event_id for event in second_transport.messages] == [event_id]


def test_parallel_runtimes_claim_disjoint_batches():
    with lab.connect() as conn:
        event_ids = {
            enqueue_event(conn, dedup_key=f"publisher:parallel:{index}", ordinal=index + 1)
            for index in range(20)
        }
        conn.commit()

    transport_a = InMemoryTransport()
    transport_b = InMemoryTransport()
    publisher_a = runtime(transport_a, worker="publisher-a", batch_size=10)
    publisher_b = runtime(transport_b, worker="publisher-b", batch_size=10)

    with ThreadPoolExecutor(max_workers=2) as pool:
        result_a, result_b = list(pool.map(lambda p: p.run_once(), [publisher_a, publisher_b]))

    seen_a = {event.event_id for event in transport_a.messages}
    seen_b = {event.event_id for event in transport_b.messages}

    assert result_a.claimed == 10
    assert result_b.claimed == 10
    assert result_a.published == 10
    assert result_b.published == 10
    assert seen_a.isdisjoint(seen_b)
    assert seen_a | seen_b == event_ids

    with lab.connect() as conn:
        assert conn.execute(
            "SELECT count(*) FROM kernel_lab.domain_event_outbox WHERE published_at IS NOT NULL"
        ).fetchone()[0] == 20


def test_transport_receives_stable_domain_event_envelope():
    with lab.connect() as conn:
        event_id = enqueue_event(conn, dedup_key="publisher:envelope", ordinal=7)
        conn.commit()

    transport = InMemoryTransport()
    result = runtime(transport).run_once()

    assert result.published == 1
    event = transport.messages[0]
    envelope = event.envelope

    assert event.event_id == event_id
    assert envelope["event_id"] == str(event_id)
    assert envelope["type"] == "inventory.test.event"
    assert envelope["source"] == "spotwo.wms.inventory-kernel"
    assert envelope["aggregate_type"] == "InventoryPosition"
    assert envelope["aggregate_id"] == "test-7"
    assert envelope["aggregate_version"] == 7
    assert envelope["correlation_id"] == "corr-7"
    assert envelope["warehouse_id"] == str(lab.WAREHOUSE)
    assert envelope["schema_version"] == 1
    assert envelope["data"] == {"ordinal": 7}


def test_ack_storage_failure_after_confirmed_publish_is_not_converted_to_nack():
    event = ClaimedEvent(
        event_id=uuid4(),
        claim_token=uuid4(),
        event_type="inventory.test.event",
        aggregate_type="InventoryPosition",
        aggregate_id="test-ack-failure",
        aggregate_version=1,
        envelope={"event_id": "test"},
    )

    class AckFailsStore:
        def __init__(self):
            self.nack_calls = 0

        def claim(self, *, worker_id, limit, lease_seconds):
            return [event]

        def ack(self, *, event_id, claim_token):
            raise OSError("database unavailable after broker confirmation")

        def nack(self, *, event_id, claim_token, error, retry_after_seconds):
            self.nack_calls += 1
            return True

    store = AckFailsStore()
    transport = InMemoryTransport()
    publisher = PublisherRuntime(
        store=store,
        transport=transport,
        worker_id="publisher-a",
        batch_size=1,
        lease_seconds=30,
        retry_after_seconds=5,
    )

    with pytest.raises(OSError, match="database unavailable"):
        publisher.run_once()

    assert [message.event_id for message in transport.messages] == [event.event_id]
    assert store.nack_calls == 0
