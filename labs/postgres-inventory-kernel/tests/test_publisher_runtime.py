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
    RetryPolicy,
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


def runtime(
    transport,
    *,
    worker: str = "publisher-a",
    batch_size: int = 100,
    retry_policy: RetryPolicy | None = None,
):
    return PublisherRuntime(
        store=PostgresOutboxStore(lab.DATABASE_URL),
        transport=transport,
        worker_id=worker,
        batch_size=batch_size,
        lease_seconds=30,
        retry_policy=retry_policy
        or RetryPolicy(
            base_delay_seconds=30,
            max_delay_seconds=30,
            jitter_ratio=0,
            max_attempts=5,
        ),
    )


def test_retry_policy_is_bounded_deterministic_and_quarantines_at_limit():
    event_id = uuid4()
    no_jitter = RetryPolicy(
        base_delay_seconds=10,
        max_delay_seconds=25,
        jitter_ratio=0,
        max_attempts=4,
    )

    assert no_jitter.decide(event_id=event_id, attempt_count=1).retry_after_seconds == 10
    assert no_jitter.decide(event_id=event_id, attempt_count=2).retry_after_seconds == 20
    assert no_jitter.decide(event_id=event_id, attempt_count=3).retry_after_seconds == 25
    exhausted = no_jitter.decide(event_id=event_id, attempt_count=4)
    assert exhausted.quarantines is True
    assert exhausted.quarantine_reason == "publication failed on attempt 4 of 4"

    with_jitter = RetryPolicy(
        base_delay_seconds=10,
        max_delay_seconds=100,
        jitter_ratio=0.2,
        max_attempts=10,
    )
    first = with_jitter.decide(event_id=event_id, attempt_count=3)
    second = with_jitter.decide(event_id=event_id, attempt_count=3)
    assert first == second
    assert 32 <= first.retry_after_seconds <= 48


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
    publisher = runtime(transport)

    first = publisher.run_once()
    assert first.claimed == 1
    assert first.failed == 1
    assert first.published == 0

    with lab.connect() as conn:
        published, attempts, error, delay_is_future = conn.execute(
            """
            SELECT
              published_at IS NOT NULL,
              attempt_count,
              last_error,
              available_at > clock_timestamp()
            FROM kernel_lab.domain_event_outbox
            WHERE event_id = %s
            """,
            (event_id,),
        ).fetchone()
        assert published is False
        assert attempts == 1
        assert "ConnectionError" in error
        assert delay_is_future is True
        conn.execute(
            """
            UPDATE kernel_lab.domain_event_outbox
            SET available_at = clock_timestamp() - interval '1 second'
            WHERE event_id = %s
            """,
            (event_id,),
        )
        conn.commit()

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


def test_poison_event_is_quarantined_then_replayed_with_operator_audit():
    with lab.connect() as conn:
        event_id = enqueue_event(conn, dedup_key="publisher:poison", ordinal=1)
        conn.commit()

    class AlwaysFails:
        def publish(self, _event):
            raise ValueError("schema rejected")

    policy = RetryPolicy(
        base_delay_seconds=1,
        max_delay_seconds=4,
        jitter_ratio=0,
        max_attempts=3,
    )
    publisher = runtime(AlwaysFails(), retry_policy=policy)

    for attempt in range(1, 4):
        result = publisher.run_once()
        assert result.claimed == 1
        assert result.failed == 1
        assert result.quarantined == (1 if attempt == 3 else 0)
        if attempt < 3:
            with lab.connect() as conn:
                conn.execute(
                    """
                    UPDATE kernel_lab.domain_event_outbox
                    SET available_at = clock_timestamp() - interval '1 second'
                    WHERE event_id = %s
                    """,
                    (event_id,),
                )
                conn.commit()

    assert publisher.run_once().claimed == 0

    with lab.connect() as conn:
        attempts, quarantined, reason, error = conn.execute(
            """
            SELECT
              attempt_count,
              quarantined_at IS NOT NULL,
              quarantine_reason,
              last_error
            FROM kernel_lab.domain_event_outbox
            WHERE event_id = %s
            """,
            (event_id,),
        ).fetchone()
        assert attempts == 3
        assert quarantined is True
        assert reason == "publication failed on attempt 3 of 3"
        assert "ValueError: schema rejected" in error

    store = PostgresOutboxStore(lab.DATABASE_URL)
    assert store.replay_quarantined(
        event_id=event_id,
        operator_id="operator@example.com",
        reason="schema registration repaired",
    ) is True
    assert store.replay_quarantined(
        event_id=event_id,
        operator_id="operator@example.com",
        reason="duplicate replay",
    ) is False

    with lab.connect() as conn:
        state = conn.execute(
            """
            SELECT attempt_count, quarantined_at, quarantine_reason, last_error
            FROM kernel_lab.domain_event_outbox
            WHERE event_id = %s
            """,
            (event_id,),
        ).fetchone()
        assert state == (0, None, None, None)
        action = conn.execute(
            """
            SELECT
              action,
              operator_id,
              reason,
              previous_attempt_count,
              previous_quarantine_reason
            FROM kernel_lab.domain_event_outbox_operator_actions
            WHERE event_id = %s
            """,
            (event_id,),
        ).fetchone()
        assert action == (
            "replay",
            "operator@example.com",
            "schema registration repaired",
            3,
            "publication failed on attempt 3 of 3",
        )

    successful_transport = InMemoryTransport()
    replay_result = runtime(successful_transport).run_once()
    assert replay_result.published == 1
    assert [event.event_id for event in successful_transport.messages] == [event_id]


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

    assert result == result.__class__(claimed=1, published=1, failed=0)
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
        attempt_count=1,
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
        retry_policy=RetryPolicy(),
    )

    with pytest.raises(OSError, match="database unavailable"):
        publisher.run_once()

    assert [message.event_id for message in transport.messages] == [event.event_id]
    assert store.nack_calls == 0
