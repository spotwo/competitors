from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import threading

import psycopg
import pytest

import conftest as lab


def create_policy(
    conn,
    *,
    code,
    blocks_allocation=False,
    blocks_movement=False,
    blocks_picking=False,
    blocks_shipping=False,
):
    policy_id = lab.new_id()
    conn.execute(
        """
        INSERT INTO kernel_lab.inventory_hold_policies (
          id, tenant_id, code,
          blocks_allocation, blocks_movement, blocks_picking, blocks_shipping
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (
            policy_id,
            lab.TENANT,
            code,
            blocks_allocation,
            blocks_movement,
            blocks_picking,
            blocks_shipping,
        ),
    )
    return policy_id


def apply_scope_hold(
    conn,
    *,
    policy_id,
    reason="TEST",
    key="hold-scope-1",
    hold_id=None,
    transaction_id=None,
    starts_at=None,
    expires_at=None,
):
    hold_id = hold_id or lab.new_id()
    transaction_id = transaction_id or lab.new_id()
    returned = conn.execute(
        """
        SELECT kernel_lab.apply_inventory_scope_hold(
          %s, %s, %s, %s, %s, %s,
          %s, %s, %s, %s,
          NULL, NULL, NULL, NULL,
          %s, %s
        )
        """,
        (
            hold_id,
            transaction_id,
            lab.TENANT,
            key,
            policy_id,
            reason,
            lab.WAREHOUSE,
            lab.ITEM,
            lab.OWNER,
            lab.CONDITION,
            starts_at,
            expires_at,
        ),
    ).fetchone()[0]
    return hold_id, transaction_id, returned


def apply_position_hold(
    conn,
    *,
    policy_id,
    position_id,
    reason="TEST",
    key="hold-position-1",
    hold_id=None,
    transaction_id=None,
    starts_at=None,
    expires_at=None,
):
    hold_id = hold_id or lab.new_id()
    transaction_id = transaction_id or lab.new_id()
    returned = conn.execute(
        """
        SELECT kernel_lab.apply_inventory_position_hold(
          %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """,
        (
            hold_id,
            transaction_id,
            lab.TENANT,
            key,
            policy_id,
            reason,
            position_id,
            starts_at,
            expires_at,
        ),
    ).fetchone()[0]
    return hold_id, transaction_id, returned


def release_hold(conn, hold_id, *, key="hold-release-1"):
    transaction_id = lab.new_id()
    returned = conn.execute(
        "SELECT kernel_lab.release_inventory_hold(%s, %s, %s, %s)",
        (transaction_id, lab.TENANT, key, hold_id),
    ).fetchone()[0]
    return transaction_id, returned


def create_reservation(conn, *, qty, key="reserve-hold-test", demand="ORDER-HOLD"):
    reservation_id = lab.new_id()
    transaction_id = lab.new_id()
    conn.execute(
        """
        SELECT kernel_lab.create_inventory_reservation(
          %s, %s, %s, %s, %s,
          %s, %s, %s, %s, %s
        )
        """,
        (
            reservation_id,
            transaction_id,
            lab.TENANT,
            key,
            demand,
            lab.WAREHOUSE,
            lab.ITEM,
            lab.OWNER,
            lab.CONDITION,
            qty,
        ),
    )
    return reservation_id


def create_allocation(
    conn,
    *,
    position_id,
    qty,
    key="allocate-hold-test",
    demand="ORDER-HOLD",
    reservation_id=None,
):
    allocation_id = lab.new_id()
    transaction_id = lab.new_id()
    conn.execute(
        """
        SELECT kernel_lab.allocate_inventory_commitment(
          %s, %s, %s, %s, %s, %s, %s, %s
        )
        """,
        (
            allocation_id,
            transaction_id,
            lab.TENANT,
            key,
            demand,
            position_id,
            qty,
            reservation_id,
        ),
    )
    return allocation_id


def availability(conn):
    row = conn.execute(
        """
        SELECT physical_qty, exact_reserved_qty, allocated_qty,
               coarse_reserved_remaining_qty, available_qty
        FROM kernel_lab.inventory_scope_availability
        WHERE tenant_id = %s
          AND warehouse_id = %s
          AND item_id = %s
          AND owner_id = %s
          AND inventory_condition_id = %s
          AND lot_id IS NULL
          AND stock_scope_id IS NULL
          AND attribute_set_id IS NULL
          AND stock_segment_id IS NULL
        """,
        (lab.TENANT, lab.WAREHOUSE, lab.ITEM, lab.OWNER, lab.CONDITION),
    ).fetchone()
    assert row is not None
    return row


def eligible(conn, position_id, action):
    return conn.execute(
        "SELECT kernel_lab.inventory_position_action_eligible(%s, %s)",
        (position_id, action),
    ).fetchone()[0]


def run_two(fn_a, fn_b):
    barrier = threading.Barrier(2)

    def wrapped(fn):
        barrier.wait(timeout=5)
        return fn()

    with ThreadPoolExecutor(max_workers=2) as pool:
        return pool.submit(wrapped, fn_a), pool.submit(wrapped, fn_b)


def test_scope_allocation_hold_removes_capacity_without_changing_physical_stock():
    with lab.connect() as conn:
        position_id = lab.insert_position(conn, physical_qty=10)
        policy_id = create_policy(conn, code="QUALITY", blocks_allocation=True)
        apply_scope_hold(conn, policy_id=policy_id)

    with lab.connect() as conn:
        state = availability(conn)
        physical = conn.execute(
            "SELECT physical_qty FROM kernel_lab.inventory_positions WHERE id = %s",
            (position_id,),
        ).fetchone()[0]
        assert physical == Decimal("10.000000")
        assert state[0] == Decimal("10.000000")
        assert state[4] == Decimal("0.000000")
        assert eligible(conn, position_id, "allocation") is False
        assert eligible(conn, position_id, "movement") is True

    with lab.connect(autocommit=True) as conn:
        with pytest.raises(psycopg.errors.CheckViolation):
            create_reservation(conn, qty=1)


def test_movement_only_hold_does_not_remove_allocation_capacity_but_blocks_move():
    with lab.connect() as conn:
        source = lab.insert_position(conn, location_id=lab.LOCATION_A, physical_qty=10)
        target = lab.insert_position(conn, location_id=lab.LOCATION_B, physical_qty=0)
        policy_id = create_policy(conn, code="NO-MOVE", blocks_movement=True)
        apply_position_hold(conn, policy_id=policy_id, position_id=source)

    with lab.connect() as conn:
        assert availability(conn)[4] == Decimal("10.000000")
        conn.execute(
            "SELECT kernel_lab.allocate_inventory_position(%s, %s, %s, %s, 2)",
            (lab.new_id(), lab.TENANT, "allocate-under-move-hold", source),
        )
        assert eligible(conn, source, "allocation") is True
        assert eligible(conn, source, "movement") is False

    with lab.connect(autocommit=True) as conn:
        with pytest.raises(psycopg.errors.CheckViolation):
            conn.execute(
                "SELECT kernel_lab.transfer_inventory_quantity(%s, %s, %s, %s, %s, 1)",
                (lab.new_id(), lab.TENANT, "blocked-move", source, target),
            )


def test_position_allocation_hold_excludes_only_that_position():
    with lab.connect() as conn:
        position_a = lab.insert_position(conn, location_id=lab.LOCATION_A, physical_qty=6)
        position_b = lab.insert_position(conn, location_id=lab.LOCATION_B, physical_qty=4)
        policy_id = create_policy(conn, code="ALLOC-BLOCK", blocks_allocation=True)
        apply_position_hold(conn, policy_id=policy_id, position_id=position_a)

    with lab.connect() as conn:
        assert availability(conn)[4] == Decimal("4.000000")

    with lab.connect(autocommit=True) as conn:
        with pytest.raises(psycopg.errors.CheckViolation):
            conn.execute(
                "SELECT kernel_lab.allocate_inventory_position(%s, %s, %s, %s, 1)",
                (lab.new_id(), lab.TENANT, "held-position", position_a),
            )

    with lab.connect() as conn:
        conn.execute(
            "SELECT kernel_lab.allocate_inventory_position(%s, %s, %s, %s, 3)",
            (lab.new_id(), lab.TENANT, "eligible-position", position_b),
        )
        allocated = conn.execute(
            "SELECT allocated_qty FROM kernel_lab.inventory_positions WHERE id = %s",
            (position_b,),
        ).fetchone()[0]

    assert allocated == Decimal("3.000000")


def test_multiple_holds_compose_action_restrictions_by_or():
    with lab.connect() as conn:
        position_id = lab.insert_position(conn, physical_qty=10)
        allocation_policy = create_policy(conn, code="ALLOC", blocks_allocation=True)
        shipping_policy = create_policy(conn, code="SHIP", blocks_shipping=True)
        apply_position_hold(
            conn,
            policy_id=allocation_policy,
            position_id=position_id,
            key="hold-alloc",
        )
        apply_position_hold(
            conn,
            policy_id=shipping_policy,
            position_id=position_id,
            key="hold-ship",
        )

    with lab.connect() as conn:
        assert eligible(conn, position_id, "allocation") is False
        assert eligible(conn, position_id, "shipping") is False
        assert eligible(conn, position_id, "movement") is True
        assert eligible(conn, position_id, "picking") is True


def test_releasing_hold_restores_eligibility_without_changing_physical_quantity():
    with lab.connect() as conn:
        position_id = lab.insert_position(conn, physical_qty=10)
        policy_id = create_policy(conn, code="TEMP", blocks_allocation=True)
        hold_id, _, _ = apply_scope_hold(conn, policy_id=policy_id)

    with lab.connect() as conn:
        assert availability(conn)[4] == Decimal("0.000000")
        transaction_id, returned = release_hold(conn, hold_id)
        assert returned == transaction_id
        assert availability(conn)[4] == Decimal("10.000000")
        assert eligible(conn, position_id, "allocation") is True

        physical = conn.execute(
            "SELECT physical_qty FROM kernel_lab.inventory_positions WHERE id = %s",
            (position_id,),
        ).fetchone()[0]
        hold_state = conn.execute(
            """
            SELECT released_at IS NOT NULL, released_transaction_id
            FROM kernel_lab.inventory_holds
            WHERE id = %s
            """,
            (hold_id,),
        ).fetchone()

    assert physical == Decimal("10.000000")
    assert hold_state == (True, transaction_id)


def test_expired_hold_is_retained_for_audit_but_does_not_block():
    now = datetime.now(timezone.utc)

    with lab.connect() as conn:
        position_id = lab.insert_position(conn, physical_qty=10)
        policy_id = create_policy(conn, code="EXPIRED", blocks_allocation=True)
        hold_id, _, _ = apply_scope_hold(
            conn,
            policy_id=policy_id,
            starts_at=now - timedelta(hours=2),
            expires_at=now - timedelta(hours=1),
        )

    with lab.connect() as conn:
        assert availability(conn)[4] == Decimal("10.000000")
        assert eligible(conn, position_id, "allocation") is True
        count = conn.execute(
            "SELECT count(*) FROM kernel_lab.inventory_holds WHERE id = %s",
            (hold_id,),
        ).fetchone()[0]

    assert count == 1


def test_existing_reservation_survives_new_position_hold_and_can_use_other_stock():
    with lab.connect() as conn:
        position_a = lab.insert_position(conn, location_id=lab.LOCATION_A, physical_qty=6)
        position_b = lab.insert_position(conn, location_id=lab.LOCATION_B, physical_qty=4)
        reservation_id = create_reservation(conn, qty=8, key="reserve-before-hold")
        policy_id = create_policy(conn, code="LATE-HOLD", blocks_allocation=True)
        apply_position_hold(conn, policy_id=policy_id, position_id=position_a)

    with lab.connect() as conn:
        remaining = conn.execute(
            "SELECT remaining_qty FROM kernel_lab.inventory_reservations WHERE id = %s",
            (reservation_id,),
        ).fetchone()[0]
        assert remaining == Decimal("8.000000")
        assert availability(conn)[4] == Decimal("0.000000")

    with lab.connect(autocommit=True) as conn:
        with pytest.raises(psycopg.errors.CheckViolation):
            create_allocation(
                conn,
                position_id=position_a,
                qty=4,
                reservation_id=reservation_id,
                key="allocate-held-a",
            )

    with lab.connect() as conn:
        create_allocation(
            conn,
            position_id=position_b,
            qty=4,
            reservation_id=reservation_id,
            key="allocate-eligible-b",
        )
        reservation = conn.execute(
            """
            SELECT allocated_qty, remaining_qty
            FROM kernel_lab.inventory_reservations
            WHERE id = %s
            """,
            (reservation_id,),
        ).fetchone()

    assert reservation == (Decimal("4.000000"), Decimal("4.000000"))


def test_full_hu_relocation_cannot_bypass_contained_movement_hold():
    with lab.connect() as conn:
        hu_id = lab.insert_hu(conn, location_id=lab.LOCATION_A)
        position_id = lab.insert_position(
            conn,
            location_id=None,
            handling_unit_id=hu_id,
            physical_qty=10,
        )
        policy_id = create_policy(conn, code="HU-FREEZE", blocks_movement=True)
        hold_id, _, _ = apply_position_hold(
            conn,
            policy_id=policy_id,
            position_id=position_id,
        )

    with lab.connect(autocommit=True) as conn:
        with pytest.raises(psycopg.errors.CheckViolation):
            conn.execute(
                "SELECT kernel_lab.relocate_handling_unit(%s, %s, %s, %s)",
                (lab.new_id(), lab.TENANT, hu_id, lab.LOCATION_B),
            )

    with lab.connect() as conn:
        location = conn.execute(
            "SELECT location_id FROM kernel_lab.handling_units WHERE id = %s",
            (hu_id,),
        ).fetchone()[0]
        assert location == lab.LOCATION_A

        release_hold(conn, hold_id, key="release-hu-hold")
        conn.execute(
            "SELECT kernel_lab.relocate_handling_unit(%s, %s, %s, %s)",
            (lab.new_id(), lab.TENANT, hu_id, lab.LOCATION_B),
        )
        location = conn.execute(
            "SELECT location_id FROM kernel_lab.handling_units WHERE id = %s",
            (hu_id,),
        ).fetchone()[0]

    assert location == lab.LOCATION_B


def test_hold_apply_idempotency_replays_result_and_rejects_payload_collision():
    original_hold = lab.new_id()
    original_transaction = lab.new_id()

    with lab.connect() as conn:
        lab.insert_position(conn, physical_qty=10)
        policy_id = create_policy(conn, code="IDEM", blocks_allocation=True)
        first = apply_scope_hold(
            conn,
            policy_id=policy_id,
            reason="QUALITY",
            key="hold-idem",
            hold_id=original_hold,
            transaction_id=original_transaction,
        )[2]

    with lab.connect() as conn:
        replay = apply_scope_hold(
            conn,
            policy_id=policy_id,
            reason="QUALITY",
            key="hold-idem",
            hold_id=lab.new_id(),
            transaction_id=lab.new_id(),
        )[2]

    assert first == original_hold
    assert replay == original_hold

    with lab.connect(autocommit=True) as conn:
        with pytest.raises(psycopg.errors.UniqueViolation):
            apply_scope_hold(
                conn,
                policy_id=policy_id,
                reason="CUSTOMS",
                key="hold-idem",
            )

    with lab.connect() as conn:
        hold_count = conn.execute(
            "SELECT count(*) FROM kernel_lab.inventory_holds"
        ).fetchone()[0]
        transaction_count = conn.execute(
            """
            SELECT count(*)
            FROM kernel_lab.inventory_transactions
            WHERE idempotency_key = 'hold-idem'
            """
        ).fetchone()[0]

    assert hold_count == 1
    assert transaction_count == 1


def test_hold_and_reservation_race_serializes_on_same_stock_scope():
    with lab.connect() as conn:
        lab.insert_position(conn, physical_qty=10)
        policy_id = create_policy(conn, code="RACE", blocks_allocation=True)

    def reserve():
        try:
            with lab.connect() as conn:
                create_reservation(conn, qty=8, key="race-reserve")
            return "reserved"
        except psycopg.errors.CheckViolation:
            return "rejected"

    def hold():
        with lab.connect() as conn:
            apply_scope_hold(conn, policy_id=policy_id, key="race-hold")
        return "held"

    future_reservation, future_hold = run_two(reserve, hold)
    reservation_result = future_reservation.result(timeout=8)
    hold_result = future_hold.result(timeout=8)

    assert reservation_result in {"reserved", "rejected"}
    assert hold_result == "held"

    with lab.connect() as conn:
        hold_count = conn.execute(
            "SELECT count(*) FROM kernel_lab.inventory_holds"
        ).fetchone()[0]
        reservation_remaining = conn.execute(
            "SELECT COALESCE(sum(remaining_qty), 0) FROM kernel_lab.inventory_reservations"
        ).fetchone()[0]
        available = availability(conn)[4]

    assert hold_count == 1
    assert reservation_remaining in {Decimal("0"), Decimal("8.000000")}
    assert available == Decimal("0.000000")
