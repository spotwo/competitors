from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

import psycopg
import pytest

import conftest as lab
from consumer_failure import (
    ConsumerFailureClassification,
    ConsumerFailureClassifier,
    ConsumerFailureRetryRuntime,
    ConsumerRetryPolicy,
    DurableConsumerFailureLane,
    PostgresConsumerFailureStore,
)
from consumer_runtime import (
    ConsumedEvent,
    InboxConsumerRuntime,
    InboxDeliveryMetadata,
    PostgresInboxStore,
)
from position_projection import InventoryPositionQuantityProjector

CONSUMER_NAME = "inventory-position-quantity-projector"
RECORDED_AT = datetime(2026, 8, 18, 23, 0, tzinfo=timezone.utc)
NO_JITTER_RETRY = ConsumerRetryPolicy(
    base_delay_seconds=1,
    max_delay_seconds=1,
    jitter_ratio=0,
    max_attempts=3,
)


def position_event(
    *,
    position_id=None,
    event_id=None,
    version: int = 1,
    physical_delta: int = 0,
    reserved_delta: int = 0,
    allocated_delta: int = 0,
) -> ConsumedEvent:
    position_id = position_id or lab.new_id()
    event_id = event_id or lab.new_id()
    recorded_at = RECORDED_AT + timedelta(seconds=version)
    return ConsumedEvent.from_envelope(
        {
            "event_id": str(event_id),
            "type": "inventory.position.changed",
            "source": "spotwo.wms.inventory-kernel",
            "subject": f"inventory-position/{position_id}",
            "occurred_at": recorded_at.isoformat(),
            "recorded_at": recorded_at.isoformat(),
            "aggregate_type": "InventoryPosition",
            "aggregate_id": str(position_id),
            "aggregate_version": version,
            "schema_version": 1,
            "data": {
                "transaction_id": str(lab.new_id()),
                "transaction_type": "consumer-failure-test",
                "position_id": str(position_id),
                "physical_delta": physical_delta,
                "reserved_delta": reserved_delta,
                "allocated_delta": allocated_delta,
            },
        }
    )


def delivery_metadata(event: ConsumedEvent, delivery_count: int = 1):
    return InboxDeliveryMetadata(
        transport="test",
        transport_message_id=f"test:{event.event_id}",
        subject="spotwo.wms.events.inventory.position.changed",
        delivery_count=delivery_count,
    )


class Delivery:
    def __init__(
        self,
        event: ConsumedEvent,
        *,
        delivery_count: int = 1,
        on_ack: Callable[[], None] | None = None,
    ):
        self.envelope = event.envelope
        self.metadata = delivery_metadata(event, delivery_count)
        self.acknowledged = False
        self.on_ack = on_ack

    def ack(self) -> None:
        if self.on_ack is not None:
            self.on_ack()
        self.acknowledged = True


class OneDeliverySource:
    def __init__(self, delivery: Delivery):
        self.delivery = delivery
        self.fetched = False

    def fetch_one(self):
        if self.fetched:
            return None
        self.fetched = True
        return self.delivery


def failure_store() -> PostgresConsumerFailureStore:
    return PostgresConsumerFailureStore(lab.DATABASE_URL)


def failure_lane(
    retry_policy: ConsumerRetryPolicy = NO_JITTER_RETRY,
) -> DurableConsumerFailureLane:
    return DurableConsumerFailureLane(
        store=failure_store(),
        retry_policy=retry_policy,
    )


def run_delivery(
    event: ConsumedEvent,
    *,
    handler,
    lane: DurableConsumerFailureLane,
    delivery_count: int = 1,
    on_ack: Callable[[], None] | None = None,
):
    delivery = Delivery(event, delivery_count=delivery_count, on_ack=on_ack)
    result = InboxConsumerRuntime(
        source=OneDeliverySource(delivery),
        store=PostgresInboxStore(lab.DATABASE_URL),
        consumer_name=CONSUMER_NAME,
        handler=handler,
        failure_lane=lane,
    ).run_once()
    return result, delivery


def make_due(event_id) -> None:
    with lab.connect() as conn:
        conn.execute(
            """
            UPDATE kernel_lab.domain_event_consumer_failures
            SET available_at = clock_timestamp() - interval '1 second'
            WHERE consumer_name = %s AND event_id = %s
            """,
            (CONSUMER_NAME, event_id),
        )
        conn.commit()


def mark_effect(event: ConsumedEvent, transaction) -> None:
    transaction.execute(
        """
        UPDATE kernel_lab.domain_event_inbox
        SET metadata = metadata || jsonb_build_object('effect', 'committed')
        WHERE consumer_name = %s AND event_id = %s
        """,
        (CONSUMER_NAME, event.event_id),
    )


def retry_runtime(handler, *, max_attempts: int = 3):
    return ConsumerFailureRetryRuntime(
        store=failure_store(),
        consumer_name=CONSUMER_NAME,
        handler=handler,
        worker_id="consumer-retry-test",
        batch_size=10,
        lease_seconds=30,
        retry_policy=ConsumerRetryPolicy(
            base_delay_seconds=1,
            max_delay_seconds=1,
            jitter_ratio=0,
            max_attempts=max_attempts,
        ),
    )


def test_classifier_separates_fence_conflict_contract_and_transient_failures():
    class FenceError(Exception):
        sqlstate = "55000"

    class ConflictError(Exception):
        sqlstate = "23505"

    class SerializationError(Exception):
        sqlstate = "40001"

    classifier = ConsumerFailureClassifier()

    assert classifier.classify(
        FenceError("projection rebuild fence is active")
    ) == ConsumerFailureClassification(
        code="projection_rebuild_fence_active",
        retryable=True,
    )
    assert classifier.classify(
        ConflictError("current projection version belongs to another event")
    ) == ConsumerFailureClassification(
        code="projection_version_conflict",
        retryable=False,
    )
    assert classifier.classify(ValueError("invalid event")) == (
        ConsumerFailureClassification(code="invalid_event_contract", retryable=False)
    )
    assert classifier.classify(SerializationError("retry transaction")).retryable
    assert classifier.classify(RuntimeError("unknown handler outage")) == (
        ConsumerFailureClassification(code="handler_failure", retryable=True)
    )


def test_consumer_retry_delay_is_deterministic_bounded_and_stops_at_budget():
    event_id = lab.new_id()
    policy = ConsumerRetryPolicy(
        base_delay_seconds=5,
        max_delay_seconds=20,
        jitter_ratio=0.2,
        max_attempts=4,
    )

    first = policy.delay_seconds(event_id=event_id, failure_count=1)
    assert first == policy.delay_seconds(event_id=event_id, failure_count=1)
    assert 4 <= first <= 6
    assert 16 <= policy.delay_seconds(event_id=event_id, failure_count=3) <= 20
    assert policy.delay_seconds(event_id=event_id, failure_count=4) == 0

    with pytest.raises(ValueError, match="max_attempts"):
        ConsumerRetryPolicy(max_attempts=0)


def test_retryable_failure_is_durable_before_ack_and_local_retry_resolves_it():
    event = position_event()

    def failing_handler(current_event, transaction):
        mark_effect(current_event, transaction)
        raise RuntimeError("projection temporarily unavailable")

    def assert_handoff_visible_before_ack() -> None:
        with lab.connect() as conn:
            row = conn.execute(
                """
                SELECT status, attempt_count, envelope
                FROM kernel_lab.domain_event_consumer_failures
                WHERE consumer_name = %s AND event_id = %s
                """,
                (CONSUMER_NAME, event.event_id),
            ).fetchone()
        assert row == ("deferred", 1, event.envelope)

    lane = failure_lane()
    result, delivery = run_delivery(
        event,
        handler=failing_handler,
        lane=lane,
        on_ack=assert_handoff_visible_before_ack,
    )

    assert delivery.acknowledged
    assert (result.applied, result.deferred, result.acknowledged) == (0, 1, 1)
    assert result.failure_code == "handler_failure"
    with lab.connect() as conn:
        assert conn.execute(
            """
            SELECT count(*) FROM kernel_lab.domain_event_inbox
            WHERE consumer_name = %s AND event_id = %s
            """,
            (CONSUMER_NAME, event.event_id),
        ).fetchone()[0] == 0

    duplicate, duplicate_delivery = run_delivery(
        event,
        handler=lambda _event, _transaction: pytest.fail(
            "broker duplicate must remain in the durable failure lane"
        ),
        lane=lane,
        delivery_count=2,
    )
    assert duplicate_delivery.acknowledged
    assert (duplicate.deferred, duplicate.delivery_count) == (1, 2)

    make_due(event.event_id)
    retried = retry_runtime(mark_effect).run_once()
    assert (retried.claimed, retried.resolved, retried.applied) == (1, 1, 1)

    with lab.connect() as conn:
        row = conn.execute(
            """
            SELECT f.status, f.resolution, i.metadata
            FROM kernel_lab.domain_event_consumer_failures AS f
            JOIN kernel_lab.domain_event_inbox AS i
              USING (consumer_name, event_id)
            WHERE f.consumer_name = %s AND f.event_id = %s
            """,
            (CONSUMER_NAME, event.event_id),
        ).fetchone()
    assert row[0:2] == ("resolved", "applied")
    assert row[2]["effect"] == "committed"
    assert row[2]["consumer_failure_retry"] == {
        "attempt_count": 1,
        "failure_code": "handler_failure",
    }


def test_position_version_conflict_is_quarantined_without_inbox_receipt():
    position_id = lab.new_id()
    first = position_event(
        position_id=position_id,
        version=1,
        physical_delta=10,
    )
    conflict = position_event(
        position_id=position_id,
        version=1,
        physical_delta=11,
    )
    projector = InventoryPositionQuantityProjector(consumer_name=CONSUMER_NAME)
    assert PostgresInboxStore(lab.DATABASE_URL).process_once(
        consumer_name=CONSUMER_NAME,
        event=first,
        delivery_metadata=delivery_metadata(first).to_dict(),
        handler=projector,
    )

    result, delivery = run_delivery(
        conflict,
        handler=projector,
        lane=failure_lane(),
    )

    assert delivery.acknowledged
    assert (result.quarantined, result.acknowledged) == (1, 1)
    assert result.failure_code == "projection_version_conflict"
    with lab.connect() as conn:
        failure = conn.execute(
            """
            SELECT status, retryable, attempt_count, quarantine_reason, envelope
            FROM kernel_lab.domain_event_consumer_failures
            WHERE consumer_name = %s AND event_id = %s
            """,
            (CONSUMER_NAME, conflict.event_id),
        ).fetchone()
        inbox_count = conn.execute(
            """
            SELECT count(*) FROM kernel_lab.domain_event_inbox
            WHERE consumer_name = %s AND event_id = %s
            """,
            (CONSUMER_NAME, conflict.event_id),
        ).fetchone()[0]
    assert failure == (
        "quarantined",
        False,
        1,
        "terminal:projection_version_conflict",
        conflict.envelope,
    )
    assert inbox_count == 0


def test_operator_replay_is_payload_bound_audited_and_uses_normal_inbox_path():
    event = position_event()

    def rejected_handler(_event, _transaction):
        raise ValueError("deployed handler rejected a valid event")

    result, _delivery = run_delivery(
        event,
        handler=rejected_handler,
        lane=failure_lane(),
    )
    assert result.failure_code == "invalid_event_contract"
    assert result.quarantined == 1

    replay_id = lab.new_id()
    store = failure_store()
    first = store.replay(
        replay_id=replay_id,
        consumer_name=CONSUMER_NAME,
        event_id=event.event_id,
        operator_id="operator@example.com",
        reason="validated event after handler fix",
    )
    duplicate = store.replay(
        replay_id=replay_id,
        consumer_name=CONSUMER_NAME,
        event_id=event.event_id,
        operator_id="operator@example.com",
        reason="validated event after handler fix",
    )
    assert (first.outcome, duplicate.outcome) == ("replayed", "duplicate")

    with pytest.raises(psycopg.errors.UniqueViolation, match="another request"):
        store.replay(
            replay_id=replay_id,
            consumer_name=CONSUMER_NAME,
            event_id=event.event_id,
            operator_id="operator@example.com",
            reason="changed replay request",
        )

    make_due(event.event_id)
    retried = retry_runtime(mark_effect).run_once()
    assert (retried.resolved, retried.applied) == (1, 1)
    with lab.connect() as conn:
        row = conn.execute(
            """
            SELECT f.status, f.resolution, a.previous_failure_code,
                   a.previous_retryable, a.operator_id, a.reason
            FROM kernel_lab.domain_event_consumer_failures AS f
            JOIN kernel_lab.domain_event_consumer_failure_actions AS a
              USING (consumer_name, event_id)
            WHERE a.replay_id = %s
            """,
            (replay_id,),
        ).fetchone()
    assert row == (
        "resolved",
        "applied",
        "invalid_event_contract",
        False,
        "operator@example.com",
        "validated event after handler fix",
    )


def test_retryable_failure_quarantines_at_local_attempt_budget():
    event = position_event()

    def failing_handler(_event, _transaction):
        raise RuntimeError("dependency remains unavailable")

    initial, _delivery = run_delivery(
        event,
        handler=failing_handler,
        lane=failure_lane(),
    )
    assert initial.deferred == 1

    make_due(event.event_id)
    second = retry_runtime(failing_handler).run_once()
    assert (second.failed, second.deferred, second.quarantined) == (1, 1, 0)

    make_due(event.event_id)
    third = retry_runtime(failing_handler).run_once()
    assert (third.failed, third.deferred, third.quarantined) == (1, 0, 1)
    with lab.connect() as conn:
        row = conn.execute(
            """
            SELECT status, attempt_count, quarantine_reason
            FROM kernel_lab.domain_event_consumer_failures
            WHERE consumer_name = %s AND event_id = %s
            """,
            (CONSUMER_NAME, event.event_id),
        ).fetchone()
        inbox_count = conn.execute(
            """
            SELECT count(*) FROM kernel_lab.domain_event_inbox
            WHERE consumer_name = %s AND event_id = %s
            """,
            (CONSUMER_NAME, event.event_id),
        ).fetchone()[0]
    assert row == ("quarantined", 3, "attempt_limit_exhausted:handler_failure")
    assert inbox_count == 0


def test_failure_identity_is_envelope_bound_and_due_claim_is_exclusive():
    event = position_event()
    store = failure_store()
    classification = ConsumerFailureClassification(
        code="handler_failure",
        retryable=True,
    )
    store.capture(
        consumer_name=CONSUMER_NAME,
        event=event,
        delivery_metadata=delivery_metadata(event).to_dict(),
        classification=classification,
        retry_after_seconds=1,
        max_attempts=3,
        error=RuntimeError("failed"),
    )

    changed_envelope: dict[str, Any] = dict(event.envelope)
    changed_envelope["data"] = dict(event.data, physical_delta=99)
    changed = ConsumedEvent.from_envelope(changed_envelope)
    with pytest.raises(psycopg.errors.UniqueViolation, match="another envelope"):
        store.capture(
            consumer_name=CONSUMER_NAME,
            event=changed,
            delivery_metadata=delivery_metadata(changed).to_dict(),
            classification=classification,
            retry_after_seconds=1,
            max_attempts=3,
            error=RuntimeError("changed payload"),
        )

    make_due(event.event_id)

    def claim(worker: int):
        return PostgresConsumerFailureStore(lab.DATABASE_URL).claim(
            consumer_name=CONSUMER_NAME,
            worker_id=f"retry-worker-{worker}",
            limit=1,
            lease_seconds=30,
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        claims = list(pool.map(claim, range(2)))
    assert sorted(len(rows) for rows in claims) == [0, 1]
    claimed = next(rows[0] for rows in claims if rows)
    assert claimed.event_id == event.event_id
