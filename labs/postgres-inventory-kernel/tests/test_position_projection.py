from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from threading import Barrier
from typing import Any

import psycopg
import pytest

import conftest as lab
from consumer_runtime import ConsumedEvent, InboxDeliveryMetadata, PostgresInboxStore
from position_projection import (
    InventoryPositionQuantityProjector,
    ProjectionApplyResult,
)

CONSUMER_NAME = "inventory-position-quantity-projector"
RECORDED_AT = datetime(2026, 8, 18, 20, 0, tzinfo=timezone.utc)


def position_event(
    *,
    position_id,
    version: int,
    physical_delta: int | float = 0,
    reserved_delta: int | float = 0,
    allocated_delta: int | float = 0,
    event_id=None,
    transaction_id=None,
) -> ConsumedEvent:
    event_id = event_id or lab.new_id()
    transaction_id = transaction_id or lab.new_id()
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
                "transaction_id": str(transaction_id),
                "transaction_type": "projection-test",
                "position_id": str(position_id),
                "physical_delta": physical_delta,
                "reserved_delta": reserved_delta,
                "allocated_delta": allocated_delta,
            },
        }
    )


def process_event(
    event: ConsumedEvent,
    *,
    handler=None,
    consumer_name: str = CONSUMER_NAME,
) -> bool:
    handler = handler or InventoryPositionQuantityProjector(
        consumer_name=consumer_name
    )
    return PostgresInboxStore(lab.DATABASE_URL).process_once(
        consumer_name=consumer_name,
        event=event,
        delivery_metadata=InboxDeliveryMetadata(
            transport="test",
            transport_message_id=f"test:{event.event_id}",
            subject="spotwo.wms.events.inventory.position.changed",
            delivery_count=1,
        ).to_dict(),
        handler=handler,
    )


def projection_row(position_id) -> tuple[Any, ...]:
    with lab.connect() as conn:
        return conn.execute(
            """
            SELECT aggregate_version, physical_qty, reserved_qty, allocated_qty,
                   last_event_id
            FROM kernel_lab.inventory_position_quantity_projection
            WHERE consumer_name = %s
              AND position_id = %s
            """,
            (CONSUMER_NAME, position_id),
        ).fetchone()


def inbox_projection(event_id) -> dict[str, Any]:
    with lab.connect() as conn:
        return conn.execute(
            """
            SELECT metadata -> 'projection'
            FROM kernel_lab.domain_event_inbox
            WHERE consumer_name = %s
              AND event_id = %s
            """,
            (CONSUMER_NAME, event_id),
        ).fetchone()[0]


def test_position_event_producer_uses_consecutive_authoritative_versions():
    with lab.connect() as conn:
        position_id = lab.insert_position(conn, physical_qty=0)
        conn.execute(
            "SELECT kernel_lab.post_inventory_receipt(%s, %s, %s, %s, 10, %s)",
            (lab.new_id(), lab.TENANT, "projection:receipt", position_id, "ASN-PROJECTION"),
        )
        conn.execute(
            "SELECT kernel_lab.allocate_inventory_position(%s, %s, %s, %s, 3)",
            (lab.new_id(), lab.TENANT, "projection:allocation", position_id),
        )

        rows = conn.execute(
            """
            SELECT aggregate_version,
                   data ->> 'physical_delta',
                   data ->> 'allocated_delta'
            FROM kernel_lab.domain_event_outbox
            WHERE event_type = 'inventory.position.changed'
              AND aggregate_id = %s
            ORDER BY aggregate_version
            """,
            (str(position_id),),
        ).fetchall()
        version = conn.execute(
            "SELECT version FROM kernel_lab.inventory_positions WHERE id = %s",
            (position_id,),
        ).fetchone()[0]

    assert [row[0] for row in rows] == [1, 2]
    assert [Decimal(row[1]) for row in rows] == [Decimal("10"), Decimal("0")]
    assert [Decimal(row[2]) for row in rows] == [Decimal("0"), Decimal("3")]
    assert version == 2


def test_sequential_events_apply_exactly_once_in_version_order():
    position_id = lab.new_id()
    first = position_event(position_id=position_id, version=1, physical_delta=10)
    second = position_event(position_id=position_id, version=2, reserved_delta=4)

    assert process_event(first) is True
    assert process_event(second) is True
    assert process_event(second) is False

    assert projection_row(position_id) == (
        2,
        Decimal("10"),
        Decimal("4"),
        Decimal("0"),
        second.event_id,
    )
    assert inbox_projection(first.event_id)["status"] == "applied"
    assert inbox_projection(second.event_id)["status"] == "applied"


def test_gap_is_durably_buffered_then_drained_by_missing_versions():
    position_id = lab.new_id()
    third = position_event(position_id=position_id, version=3, allocated_delta=2)
    first = position_event(position_id=position_id, version=1, physical_delta=10)
    second = position_event(position_id=position_id, version=2, reserved_delta=4)

    assert process_event(third) is True
    assert projection_row(position_id)[:4] == (
        0,
        Decimal("0"),
        Decimal("0"),
        Decimal("0"),
    )
    assert inbox_projection(third.event_id)["status"] == "buffered"
    with lab.connect() as conn:
        gap = conn.execute(
            """
            SELECT expected_version, first_pending_version,
                   last_pending_version, pending_count
            FROM kernel_lab.inventory_position_projection_gaps
            WHERE consumer_name = %s
              AND position_id = %s
            """,
            (CONSUMER_NAME, position_id),
        ).fetchone()
    assert gap == (1, 3, 3, 1)
    with lab.connect() as conn:
        with pytest.raises(psycopg.errors.RestrictViolation):
            conn.execute(
                """
                DELETE FROM kernel_lab.domain_event_inbox
                WHERE consumer_name = %s
                  AND event_id = %s
                """,
                (CONSUMER_NAME, third.event_id),
            )
        conn.rollback()

    assert process_event(first) is True
    assert projection_row(position_id)[0] == 1
    outcomes: list[ProjectionApplyResult] = []

    def capture_outcome(event, transaction):
        outcomes.append(
            InventoryPositionQuantityProjector(
                consumer_name=CONSUMER_NAME
            ).apply(event, transaction)
        )

    assert process_event(second, handler=capture_outcome) is True

    assert projection_row(position_id) == (
        3,
        Decimal("10"),
        Decimal("4"),
        Decimal("2"),
        third.event_id,
    )
    with lab.connect() as conn:
        assert conn.execute(
            "SELECT count(*) FROM kernel_lab.inventory_position_projection_pending"
        ).fetchone()[0] == 0
    third_metadata = inbox_projection(third.event_id)
    assert third_metadata["status"] == "applied"
    assert "buffered_at" in third_metadata
    assert third_metadata["drained_by_event_id"] == str(second.event_id)
    assert outcomes == [
        ProjectionApplyResult(
            outcome="applied",
            applied_count=2,
            projection_version=3,
            expected_version=4,
        )
    ]


def test_late_stale_version_is_audited_and_does_not_change_projection():
    position_id = lab.new_id()
    first = position_event(position_id=position_id, version=1, physical_delta=10)
    second = position_event(position_id=position_id, version=2, reserved_delta=4)
    stale = position_event(position_id=position_id, version=1, physical_delta=999)

    process_event(first)
    process_event(second)
    assert process_event(stale) is True

    assert projection_row(position_id) == (
        2,
        Decimal("10"),
        Decimal("4"),
        Decimal("0"),
        second.event_id,
    )
    metadata = inbox_projection(stale.event_id)
    assert metadata["status"] == "stale"
    assert metadata["aggregate_version"] == 1
    assert metadata["projection_version"] == 2


def test_parallel_adjacent_versions_serialize_to_the_same_projection():
    position_id = lab.new_id()
    first = position_event(position_id=position_id, version=1, physical_delta=10)
    second = position_event(position_id=position_id, version=2, reserved_delta=4)
    barrier = Barrier(2)
    projector = InventoryPositionQuantityProjector(consumer_name=CONSUMER_NAME)

    def synchronized_handler(event, transaction):
        barrier.wait(timeout=3)
        projector(event, transaction)

    def worker(event):
        return process_event(event, handler=synchronized_handler)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(worker, [second, first]))

    assert results == [True, True]
    assert projection_row(position_id)[:4] == (
        2,
        Decimal("10"),
        Decimal("4"),
        Decimal("0"),
    )
    with lab.connect() as conn:
        assert conn.execute(
            "SELECT count(*) FROM kernel_lab.inventory_position_projection_pending"
        ).fetchone()[0] == 0


def test_conflicting_event_for_one_buffered_version_fails_closed():
    position_id = lab.new_id()
    first_third = position_event(position_id=position_id, version=3, allocated_delta=2)
    conflicting_third = position_event(
        position_id=position_id,
        version=3,
        allocated_delta=5,
    )

    assert process_event(first_third) is True
    with pytest.raises(psycopg.errors.UniqueViolation, match="another event"):
        process_event(conflicting_third)

    with lab.connect() as conn:
        pending = conn.execute(
            """
            SELECT event_id, data ->> 'allocated_delta'
            FROM kernel_lab.inventory_position_projection_pending
            """
        ).fetchall()
        conflicting_receipts = conn.execute(
            """
            SELECT count(*)
            FROM kernel_lab.domain_event_inbox
            WHERE consumer_name = %s
              AND event_id = %s
            """,
            (CONSUMER_NAME, conflicting_third.event_id),
        ).fetchone()[0]
    assert pending == [(first_third.event_id, "2")]
    assert conflicting_receipts == 0


def test_conflicting_event_for_current_version_fails_closed():
    position_id = lab.new_id()
    first = position_event(position_id=position_id, version=1, physical_delta=10)
    conflict = position_event(position_id=position_id, version=1, physical_delta=11)

    assert process_event(first) is True
    with pytest.raises(psycopg.errors.UniqueViolation, match="current projection version"):
        process_event(conflict)

    assert projection_row(position_id) == (
        1,
        Decimal("10"),
        Decimal("0"),
        Decimal("0"),
        first.event_id,
    )
    with lab.connect() as conn:
        assert conn.execute(
            """
            SELECT count(*)
            FROM kernel_lab.domain_event_inbox
            WHERE consumer_name = %s
              AND event_id = %s
            """,
            (CONSUMER_NAME, conflict.event_id),
        ).fetchone()[0] == 0


def test_invalid_or_impossible_delta_rolls_back_inbox_and_projection():
    position_id = lab.new_id()
    invalid = position_event(position_id=position_id, version=1, physical_delta=1)
    del invalid.data["reserved_delta"]

    with pytest.raises(ValueError, match="reserved_delta is required"):
        process_event(invalid)

    mismatched = position_event(position_id=position_id, version=1, physical_delta=1)
    mismatched.data["position_id"] = str(lab.new_id())
    with pytest.raises(ValueError, match="position_id must match"):
        process_event(mismatched)

    impossible = position_event(position_id=position_id, version=1, physical_delta=-1)
    with pytest.raises(psycopg.errors.CheckViolation):
        process_event(impossible)

    with lab.connect() as conn:
        assert conn.execute(
            "SELECT count(*) FROM kernel_lab.domain_event_inbox"
        ).fetchone()[0] == 0
        assert conn.execute(
            "SELECT count(*) FROM kernel_lab.inventory_position_quantity_projection"
        ).fetchone()[0] == 0
