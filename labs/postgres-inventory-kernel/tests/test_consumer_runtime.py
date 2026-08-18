from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any

import pytest

import conftest as lab
from consumer_runtime import (
    ConsumedEvent,
    InboxConsumerRuntime,
    InboxDeliveryMetadata,
    PostgresInboxStore,
)


def event_envelope(*, event_id=None, ordinal: int = 1) -> dict[str, Any]:
    event_id = event_id or lab.new_id()
    recorded_at = datetime(2026, 8, 18, 18, 0, tzinfo=timezone.utc)
    return {
        "event_id": str(event_id),
        "type": "inventory.position.changed",
        "source": "spotwo.wms.inventory-kernel",
        "subject": f"inventory-position/test-{ordinal}",
        "occurred_at": recorded_at.isoformat(),
        "recorded_at": recorded_at.isoformat(),
        "aggregate_type": "InventoryPosition",
        "aggregate_id": f"test-{ordinal}",
        "aggregate_version": ordinal,
        "schema_version": 1,
        "data": {"ordinal": ordinal},
    }


def delivery_metadata(*, delivery_count: int = 1) -> InboxDeliveryMetadata:
    return InboxDeliveryMetadata(
        transport="test",
        transport_message_id="test:1",
        subject="spotwo.wms.events.inventory.position.changed",
        delivery_count=delivery_count,
    )


def mark_effect(consumer_name: str, calls: list) -> Any:
    def handler(event, transaction):
        calls.append(event.event_id)
        transaction.execute(
            """
            UPDATE kernel_lab.domain_event_inbox
            SET metadata = metadata || jsonb_build_object(
              'effect_count',
              COALESCE((metadata ->> 'effect_count')::integer, 0) + 1
            )
            WHERE consumer_name = %s
              AND event_id = %s
            """,
            (consumer_name, event.event_id),
        )

    return handler


def test_runtime_acknowledges_only_after_store_commit():
    order: list[str] = []
    envelope = event_envelope()

    class Delivery:
        metadata = delivery_metadata()

        def __init__(self):
            self.envelope = envelope

        def ack(self):
            order.append("transport_ack")

    class Source:
        def fetch_one(self):
            order.append("transport_fetch")
            return Delivery()

    class Store:
        def process_once(self, *, handler, event, **_kwargs):
            order.append("transaction_begin")
            handler(event, None)
            order.append("transaction_commit")
            return True

    def handler(_event, _transaction):
        order.append("handler_effect")

    result = InboxConsumerRuntime(
        source=Source(),
        store=Store(),
        consumer_name="availability-projector",
        handler=handler,
    ).run_once()

    assert order == [
        "transport_fetch",
        "transaction_begin",
        "handler_effect",
        "transaction_commit",
        "transport_ack",
    ]
    assert (result.received, result.applied, result.duplicate, result.acknowledged) == (
        1,
        1,
        0,
        1,
    )


def test_runtime_does_not_acknowledge_store_failure():
    acknowledgements: list[str] = []

    class Delivery:
        envelope = event_envelope()
        metadata = delivery_metadata()

        def ack(self):
            acknowledgements.append("ack")

    class Source:
        def fetch_one(self):
            return Delivery()

    class FailingStore:
        def process_once(self, **_kwargs):
            raise RuntimeError("handler transaction failed")

    runtime = InboxConsumerRuntime(
        source=Source(),
        store=FailingStore(),
        consumer_name="availability-projector",
        handler=lambda _event, _transaction: None,
    )

    with pytest.raises(RuntimeError, match="transaction failed"):
        runtime.run_once()
    assert acknowledgements == []


def test_postgres_receipt_and_handler_effect_commit_exactly_once():
    consumer_name = "availability-projector"
    event = ConsumedEvent.from_envelope(event_envelope())
    calls: list = []
    store = PostgresInboxStore(lab.DATABASE_URL)

    first = store.process_once(
        consumer_name=consumer_name,
        event=event,
        delivery_metadata=delivery_metadata().to_dict(),
        handler=mark_effect(consumer_name, calls),
    )
    duplicate = store.process_once(
        consumer_name=consumer_name,
        event=event,
        delivery_metadata=delivery_metadata(delivery_count=2).to_dict(),
        handler=mark_effect(consumer_name, calls),
    )

    assert first is True
    assert duplicate is False
    assert calls == [event.event_id]

    with lab.connect() as conn:
        metadata = conn.execute(
            """
            SELECT metadata
            FROM kernel_lab.domain_event_inbox
            WHERE consumer_name = %s
              AND event_id = %s
            """,
            (consumer_name, event.event_id),
        ).fetchone()[0]
    assert metadata["effect_count"] == 1
    assert metadata["delivery_count"] == 1


def test_handler_failure_rolls_back_receipt_and_partial_effect():
    consumer_name = "availability-projector"
    event = ConsumedEvent.from_envelope(event_envelope())
    store = PostgresInboxStore(lab.DATABASE_URL)

    def failing_handler(current_event, transaction):
        mark_effect(consumer_name, [])(current_event, transaction)
        raise RuntimeError("projection rejected event")

    with pytest.raises(RuntimeError, match="projection rejected"):
        store.process_once(
            consumer_name=consumer_name,
            event=event,
            delivery_metadata=delivery_metadata().to_dict(),
            handler=failing_handler,
        )

    with lab.connect() as conn:
        assert conn.execute(
            """
            SELECT count(*)
            FROM kernel_lab.domain_event_inbox
            WHERE consumer_name = %s
              AND event_id = %s
            """,
            (consumer_name, event.event_id),
        ).fetchone()[0] == 0

    calls: list = []
    assert store.process_once(
        consumer_name=consumer_name,
        event=event,
        delivery_metadata=delivery_metadata(delivery_count=2).to_dict(),
        handler=mark_effect(consumer_name, calls),
    ) is True
    assert calls == [event.event_id]


def test_parallel_duplicate_receipts_serialize_to_one_handler_effect():
    consumer_name = "availability-projector"
    event = ConsumedEvent.from_envelope(event_envelope())
    calls: list = []

    def slow_handler(current_event, transaction):
        calls.append(current_event.event_id)
        transaction.execute("SELECT pg_sleep(0.2)")
        mark_effect(consumer_name, [])(current_event, transaction)

    def process(_worker: int) -> bool:
        return PostgresInboxStore(lab.DATABASE_URL).process_once(
            consumer_name=consumer_name,
            event=event,
            delivery_metadata=delivery_metadata().to_dict(),
            handler=slow_handler,
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(process, range(2)))

    assert sorted(results) == [False, True]
    assert calls == [event.event_id]
    with lab.connect() as conn:
        metadata = conn.execute(
            """
            SELECT metadata
            FROM kernel_lab.domain_event_inbox
            WHERE consumer_name = %s
              AND event_id = %s
            """,
            (consumer_name, event.event_id),
        ).fetchone()[0]
    assert metadata["effect_count"] == 1


def test_envelope_and_delivery_metadata_validation_fail_closed():
    invalid = event_envelope()
    invalid["recorded_at"] = "2026-08-18T18:00:00"
    with pytest.raises(ValueError, match="timezone"):
        ConsumedEvent.from_envelope(invalid)

    invalid = event_envelope()
    invalid["aggregate_version"] = True
    with pytest.raises(ValueError, match="positive integer"):
        ConsumedEvent.from_envelope(invalid)

    with pytest.raises(ValueError, match="delivery_count"):
        delivery_metadata(delivery_count=0)

    with pytest.raises(ValueError, match="surrounding whitespace"):
        InboxConsumerRuntime(
            source=None,
            store=None,
            consumer_name=" availability-projector",
            handler=lambda _event, _transaction: None,
        )
