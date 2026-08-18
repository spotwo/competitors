from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from threading import Barrier

import psycopg
import pytest

import conftest as lab
from consumer_runtime import ConsumedEvent, InboxDeliveryMetadata, PostgresInboxStore
from position_projection import InventoryPositionQuantityProjector
from position_projection_rebuild import (
    REQUIRE_EMPTY,
    SUPERSEDE_COVERED_RETAIN_FUTURE,
    PostgresPositionProjectionRebuildStore,
)
from position_projection_snapshot import (
    InventoryPositionQuantitySnapshot,
    PostgresPositionProjectionBootstrapStore,
)

CONSUMER_NAME = "inventory-position-quantity-projector"
RECORDED_AT = datetime(2026, 8, 18, 22, 0, tzinfo=timezone.utc)
CHECKSUM = "sha256:" + ("b" * 64)


def position_event(
    *,
    position_id,
    version: int,
    physical_delta: int | float = 0,
    reserved_delta: int | float = 0,
    allocated_delta: int | float = 0,
) -> ConsumedEvent:
    event_id = lab.new_id()
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
                "transaction_type": "projection-rebuild-test",
                "position_id": str(position_id),
                "physical_delta": physical_delta,
                "reserved_delta": reserved_delta,
                "allocated_delta": allocated_delta,
            },
        }
    )


def process_event(event: ConsumedEvent) -> bool:
    return PostgresInboxStore(lab.DATABASE_URL).process_once(
        consumer_name=CONSUMER_NAME,
        event=event,
        delivery_metadata=InboxDeliveryMetadata(
            transport="test",
            transport_message_id=f"test:{event.event_id}",
            subject="spotwo.wms.events.inventory.position.changed",
            delivery_count=1,
        ).to_dict(),
        handler=InventoryPositionQuantityProjector(consumer_name=CONSUMER_NAME),
    )


def snapshot(
    *,
    position_id,
    version: int,
    physical_qty: str = "10",
    reserved_qty: str = "0",
    allocated_qty: str = "0",
) -> InventoryPositionQuantitySnapshot:
    return InventoryPositionQuantitySnapshot.from_document(
        {
            "position_id": str(position_id),
            "aggregate_version": version,
            "physical_qty": physical_qty,
            "reserved_qty": reserved_qty,
            "allocated_qty": allocated_qty,
            "source": "authoritative-rebuild-export",
            "reference": f"rebuild-snapshot-v{version}",
            "recorded_at": RECORDED_AT.isoformat(),
        },
        artifact_checksum=CHECKSUM,
    )


def rebuild_store() -> PostgresPositionProjectionRebuildStore:
    return PostgresPositionProjectionRebuildStore(lab.DATABASE_URL)


def prepare(
    position_id,
    *,
    rebuild_id=None,
    version: int,
    expected_version: int,
    expected_pending_count: int,
    pending_disposition: str = REQUIRE_EMPTY,
    physical_qty: str = "10",
    reserved_qty: str = "0",
    allocated_qty: str = "0",
):
    rebuild_id = rebuild_id or lab.new_id()
    result = rebuild_store().prepare(
        rebuild_id=rebuild_id,
        consumer_name=CONSUMER_NAME,
        snapshot=snapshot(
            position_id=position_id,
            version=version,
            physical_qty=physical_qty,
            reserved_qty=reserved_qty,
            allocated_qty=allocated_qty,
        ),
        expected_projection_version=expected_version,
        expected_pending_count=expected_pending_count,
        pending_disposition=pending_disposition,
        operator="planner@example.com",
        reason="repair verified Position quantity projection",
    )
    return rebuild_id, result


def projection_row(position_id):
    with lab.connect() as conn:
        return conn.execute(
            """
            SELECT aggregate_version, physical_qty, reserved_qty, allocated_qty,
                   last_event_id, cursor_source, bootstrap_id, bootstrap_version,
                   bootstrap_checksum, bootstrapped_by
            FROM kernel_lab.inventory_position_quantity_projection
            WHERE consumer_name = %s AND position_id = %s
            """,
            (CONSUMER_NAME, position_id),
        ).fetchone()


def inbox_projection(event_id):
    with lab.connect() as conn:
        return conn.execute(
            """
            SELECT metadata -> 'projection'
            FROM kernel_lab.domain_event_inbox
            WHERE consumer_name = %s AND event_id = %s
            """,
            (CONSUMER_NAME, event_id),
        ).fetchone()[0]


def test_prepare_fences_inbox_transaction_and_cancel_resumes_delivery():
    position_id = lab.new_id()
    first = position_event(position_id=position_id, version=1, physical_delta=10)
    second = position_event(position_id=position_id, version=2, physical_delta=1)
    assert process_event(first) is True

    rebuild_id, prepared = prepare(
        position_id,
        version=1,
        expected_version=1,
        expected_pending_count=0,
        physical_qty="12",
    )
    _, duplicate = prepare(
        position_id,
        rebuild_id=rebuild_id,
        version=1,
        expected_version=1,
        expected_pending_count=0,
        physical_qty="12",
    )
    assert prepared.outcome == "prepared"
    assert duplicate.outcome == "duplicate"

    with pytest.raises(
        psycopg.errors.ObjectNotInPrerequisiteState,
        match="rebuild fence is active",
    ):
        process_event(second)

    with lab.connect() as conn:
        assert conn.execute(
            """
            SELECT count(*) FROM kernel_lab.domain_event_inbox
            WHERE consumer_name = %s AND event_id = %s
            """,
            (CONSUMER_NAME, second.event_id),
        ).fetchone()[0] == 0
    assert projection_row(position_id)[:4] == (
        1,
        Decimal("10"),
        Decimal("0"),
        Decimal("0"),
    )

    cancelled = rebuild_store().cancel(
        rebuild_id=rebuild_id,
        operator="operator@example.com",
        reason="replacement snapshot needs review",
    )
    duplicate_cancel = rebuild_store().cancel(
        rebuild_id=rebuild_id,
        operator="operator@example.com",
        reason="replacement snapshot needs review",
    )
    assert cancelled.outcome == "cancelled"
    assert duplicate_cancel.outcome == "duplicate"
    assert process_event(second) is True
    assert projection_row(position_id)[:4] == (
        2,
        Decimal("11"),
        Decimal("0"),
        Decimal("0"),
    )


def test_execute_replaces_cursor_atomically_and_preserves_audit_evidence():
    position_id = lab.new_id()
    first = position_event(position_id=position_id, version=1, physical_delta=10)
    assert process_event(first) is True

    rebuild_id, _ = prepare(
        position_id,
        version=1,
        expected_version=1,
        expected_pending_count=0,
        physical_qty="12",
        reserved_qty="2",
    )
    completed = rebuild_store().execute(
        rebuild_id=rebuild_id,
        operator="executor@example.com",
    )
    duplicate = rebuild_store().execute(
        rebuild_id=rebuild_id,
        operator="executor@example.com",
    )

    assert completed.outcome == "completed"
    assert duplicate.outcome == "duplicate"
    assert completed.projection_version == 1
    assert projection_row(position_id) == (
        1,
        Decimal("12"),
        Decimal("2"),
        Decimal("0"),
        None,
        "snapshot",
        rebuild_id,
        1,
        CHECKSUM,
        "executor@example.com",
    )

    with lab.connect() as conn:
        audit = conn.execute(
            """
            SELECT status, pending_disposition, before_projection,
                   before_pending_count, final_projection,
                   executed_by, superseded_pending_count,
                   drained_pending_count, remaining_pending_count
            FROM kernel_lab.inventory_position_projection_rebuilds
            WHERE rebuild_id = %s
            """,
            (rebuild_id,),
        ).fetchone()
        active_fence = conn.execute(
            """
            SELECT active_rebuild_id
            FROM kernel_lab.inventory_position_projection_control
            WHERE consumer_name = %s AND position_id = %s
            """,
            (CONSUMER_NAME, position_id),
        ).fetchone()[0]
    assert audit[0:2] == ("completed", REQUIRE_EMPTY)
    assert audit[2]["physical_qty"] == 10
    assert audit[3] == 0
    assert audit[4]["physical_qty"] == 12
    assert audit[5:] == ("executor@example.com", 0, 0, 0)
    assert active_fence is None

    next_event = position_event(position_id=position_id, version=2, reserved_delta=1)
    assert process_event(next_event) is True
    assert projection_row(position_id)[:8] == (
        2,
        Decimal("12"),
        Decimal("3"),
        Decimal("0"),
        next_event.event_id,
        "event",
        rebuild_id,
        1,
    )


def test_rebuild_supersedes_covered_pending_and_drains_contiguous_future():
    position_id = lab.new_id()
    version_3 = position_event(position_id=position_id, version=3)
    version_4 = position_event(position_id=position_id, version=4, reserved_delta=1)
    assert process_event(version_3) is True
    assert process_event(version_4) is True

    rebuild_id, _ = prepare(
        position_id,
        version=3,
        expected_version=0,
        expected_pending_count=2,
        pending_disposition=SUPERSEDE_COVERED_RETAIN_FUTURE,
        physical_qty="10",
        reserved_qty="2",
    )
    completed = rebuild_store().execute(
        rebuild_id=rebuild_id,
        operator="executor@example.com",
    )

    assert completed.superseded_pending_count == 1
    assert completed.drained_pending_count == 1
    assert completed.remaining_pending_count == 0
    assert completed.projection_version == 4
    assert projection_row(position_id)[:6] == (
        4,
        Decimal("10"),
        Decimal("3"),
        Decimal("0"),
        version_4.event_id,
        "event",
    )
    superseded = inbox_projection(version_3.event_id)
    drained = inbox_projection(version_4.event_id)
    assert superseded["status"] == "superseded"
    assert superseded["superseded_by_rebuild_id"] == str(rebuild_id)
    assert superseded["snapshot_version"] == 3
    assert drained["status"] == "applied"
    assert drained["drained_by_rebuild_id"] == str(rebuild_id)


def test_rebuild_retains_noncontiguous_future_until_missing_event_arrives():
    position_id = lab.new_id()
    version_5 = position_event(position_id=position_id, version=5, allocated_delta=2)
    assert process_event(version_5) is True

    rebuild_id, _ = prepare(
        position_id,
        version=3,
        expected_version=0,
        expected_pending_count=1,
        pending_disposition=SUPERSEDE_COVERED_RETAIN_FUTURE,
        physical_qty="10",
    )
    completed = rebuild_store().execute(
        rebuild_id=rebuild_id,
        operator="executor@example.com",
    )
    assert completed.superseded_pending_count == 0
    assert completed.drained_pending_count == 0
    assert completed.remaining_pending_count == 1
    assert projection_row(position_id)[:6] == (
        3,
        Decimal("10"),
        Decimal("0"),
        Decimal("0"),
        None,
        "snapshot",
    )

    version_4 = position_event(position_id=position_id, version=4, reserved_delta=1)
    assert process_event(version_4) is True
    assert projection_row(position_id)[:6] == (
        5,
        Decimal("10"),
        Decimal("1"),
        Decimal("2"),
        version_5.event_id,
        "event",
    )
    assert inbox_projection(version_5.event_id)["status"] == "applied"


def test_prepare_rejects_pending_without_explicit_disposition_and_stale_cas():
    position_id = lab.new_id()
    pending = position_event(position_id=position_id, version=3)
    assert process_event(pending) is True

    with pytest.raises(psycopg.errors.CheckViolation, match="rejects pending"):
        prepare(
            position_id,
            version=3,
            expected_version=0,
            expected_pending_count=1,
            pending_disposition=REQUIRE_EMPTY,
        )

    with pytest.raises(psycopg.errors.SerializationFailure, match="changed"):
        prepare(
            position_id,
            version=3,
            expected_version=1,
            expected_pending_count=1,
            pending_disposition=SUPERSEDE_COVERED_RETAIN_FUTURE,
        )

    with lab.connect() as conn:
        assert conn.execute(
            "SELECT count(*) FROM kernel_lab.inventory_position_projection_rebuilds"
        ).fetchone()[0] == 0
        assert conn.execute(
            """
            SELECT active_rebuild_id
            FROM kernel_lab.inventory_position_projection_control
            WHERE consumer_name = %s AND position_id = %s
            """,
            (CONSUMER_NAME, position_id),
        ).fetchone()[0] is None


def test_prepare_rejects_backward_snapshot_and_execute_identity_collision():
    position_id = lab.new_id()
    first = position_event(position_id=position_id, version=1, physical_delta=10)
    second = position_event(position_id=position_id, version=2, reserved_delta=1)
    assert process_event(first) is True
    assert process_event(second) is True

    with pytest.raises(psycopg.errors.CheckViolation, match="backward"):
        prepare(
            position_id,
            version=1,
            expected_version=2,
            expected_pending_count=0,
        )

    rebuild_id, _ = prepare(
        position_id,
        version=2,
        expected_version=2,
        expected_pending_count=0,
        reserved_qty="1",
    )
    rebuild_store().execute(
        rebuild_id=rebuild_id,
        operator="executor@example.com",
    )
    with pytest.raises(psycopg.errors.UniqueViolation, match="another executor"):
        rebuild_store().execute(
            rebuild_id=rebuild_id,
            operator="different@example.com",
        )


def test_fence_blocks_non_destructive_bootstrap_and_other_positions_continue():
    fenced_position = lab.new_id()
    other_position = lab.new_id()
    assert process_event(
        position_event(position_id=fenced_position, version=1, physical_delta=10)
    ) is True
    assert process_event(
        position_event(position_id=other_position, version=1, physical_delta=5)
    ) is True

    rebuild_id, _ = prepare(
        fenced_position,
        version=1,
        expected_version=1,
        expected_pending_count=0,
    )
    with pytest.raises(
        psycopg.errors.ObjectNotInPrerequisiteState,
        match="rebuild fence is active",
    ):
        PostgresPositionProjectionBootstrapStore(lab.DATABASE_URL).bootstrap(
            bootstrap_id=lab.new_id(),
            consumer_name=CONSUMER_NAME,
            snapshot=snapshot(position_id=fenced_position, version=1),
            operator="operator@example.com",
            reason="must not replace a fenced cursor",
        )

    other_next = position_event(
        position_id=other_position,
        version=2,
        physical_delta=1,
    )
    assert process_event(other_next) is True
    assert projection_row(other_position)[:4] == (
        2,
        Decimal("6"),
        Decimal("0"),
        Decimal("0"),
    )
    rebuild_store().cancel(
        rebuild_id=rebuild_id,
        operator="operator@example.com",
        reason="test cleanup",
    )


def test_prepare_event_race_has_one_serialized_winner():
    position_id = lab.new_id()
    assert process_event(
        position_event(position_id=position_id, version=1, physical_delta=10)
    ) is True
    next_event = position_event(position_id=position_id, version=2, physical_delta=1)
    rebuild_id = lab.new_id()
    barrier = Barrier(2)

    def run_prepare():
        barrier.wait(timeout=3)
        try:
            return prepare(
                position_id,
                rebuild_id=rebuild_id,
                version=1,
                expected_version=1,
                expected_pending_count=0,
                physical_qty="12",
            )[1].outcome
        except psycopg.errors.SerializationFailure:
            return "lost-cas"

    def run_event():
        barrier.wait(timeout=3)
        try:
            process_event(next_event)
            return "applied"
        except psycopg.errors.ObjectNotInPrerequisiteState:
            return "fenced"

    with ThreadPoolExecutor(max_workers=2) as pool:
        prepare_future = pool.submit(run_prepare)
        event_future = pool.submit(run_event)
        outcomes = (prepare_future.result(), event_future.result())

    assert outcomes in {("prepared", "fenced"), ("lost-cas", "applied")}
    if outcomes[0] == "prepared":
        with lab.connect() as conn:
            assert conn.execute(
                """
                SELECT count(*) FROM kernel_lab.domain_event_inbox
                WHERE consumer_name = %s AND event_id = %s
                """,
                (CONSUMER_NAME, next_event.event_id),
            ).fetchone()[0] == 0
        rebuild_store().cancel(
            rebuild_id=rebuild_id,
            operator="operator@example.com",
            reason="race test cleanup",
        )
    else:
        assert projection_row(position_id)[:4] == (
            2,
            Decimal("11"),
            Decimal("0"),
            Decimal("0"),
        )
