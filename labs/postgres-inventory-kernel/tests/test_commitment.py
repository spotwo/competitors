from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
import threading

import psycopg

import conftest as lab


def create_reservation(conn, *, qty, demand="ORDER-1", key="reserve-1"):
    reservation_id = lab.new_id()
    transaction_id = lab.new_id()
    returned = conn.execute(
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
    ).fetchone()[0]
    return reservation_id, transaction_id, returned


def create_allocation(
    conn,
    *,
    position_id,
    qty,
    demand="ORDER-1",
    reservation_id=None,
    key="allocate-1",
):
    allocation_id = lab.new_id()
    transaction_id = lab.new_id()
    returned = conn.execute(
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
    ).fetchone()[0]
    return allocation_id, transaction_id, returned


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


def run_two(fn_a, fn_b):
    barrier = threading.Barrier(2)

    def wrapped(fn):
        barrier.wait(timeout=5)
        return fn()

    with ThreadPoolExecutor(max_workers=2) as pool:
        return pool.submit(wrapped, fn_a), pool.submit(wrapped, fn_b)


def test_coarse_reservation_reduces_available_without_touching_position_quantity():
    with lab.connect() as conn:
        position_id = lab.insert_position(conn, physical_qty=10)
        reservation_id, _, _ = create_reservation(conn, qty=6)

        physical, exact_reserved, allocated, coarse, available = availability(conn)
        position_reserved = conn.execute(
            "SELECT reserved_qty FROM kernel_lab.inventory_positions WHERE id = %s",
            (position_id,),
        ).fetchone()[0]
        remaining = conn.execute(
            "SELECT remaining_qty FROM kernel_lab.inventory_reservations WHERE id = %s",
            (reservation_id,),
        ).fetchone()[0]

    assert physical == Decimal("10.000000")
    assert exact_reserved == Decimal("0.000000")
    assert allocated == Decimal("0.000000")
    assert coarse == Decimal("6.000000")
    assert available == Decimal("4.000000")
    assert position_reserved == Decimal("0.000000")
    assert remaining == Decimal("6.000000")


def test_reservation_to_allocation_preserves_global_availability():
    with lab.connect() as conn:
        position_id = lab.insert_position(conn, physical_qty=10)
        reservation_id, _, _ = create_reservation(conn, qty=6)
        before = availability(conn)

        allocation_id, _, _ = create_allocation(
            conn,
            position_id=position_id,
            qty=4,
            reservation_id=reservation_id,
        )
        after = availability(conn)

        reservation = conn.execute(
            "SELECT allocated_qty, remaining_qty FROM kernel_lab.inventory_reservations WHERE id = %s",
            (reservation_id,),
        ).fetchone()
        allocation_qty = conn.execute(
            "SELECT quantity FROM kernel_lab.inventory_allocations WHERE id = %s",
            (allocation_id,),
        ).fetchone()[0]

    assert before[4] == Decimal("4.000000")
    assert after[2] == Decimal("4.000000")
    assert after[3] == Decimal("2.000000")
    assert after[4] == Decimal("4.000000")
    assert reservation == (Decimal("4.000000"), Decimal("2.000000"))
    assert allocation_qty == Decimal("4.000000")


def test_unreserved_allocation_cannot_steal_coarse_reserved_capacity():
    with lab.connect() as conn:
        position_id = lab.insert_position(conn, physical_qty=10)
        create_reservation(conn, qty=8)

    with lab.connect() as conn:
        try:
            conn.execute(
                "SELECT kernel_lab.allocate_inventory_position(%s, %s, %s, %s, 3)",
                (lab.new_id(), lab.TENANT, "steal-3", position_id),
            )
            assert False, "allocation should have been rejected"
        except psycopg.errors.CheckViolation:
            pass

    with lab.connect() as conn:
        conn.execute(
            "SELECT kernel_lab.allocate_inventory_position(%s, %s, %s, %s, 2)",
            (lab.new_id(), lab.TENANT, "free-2", position_id),
        )
        allocated = conn.execute(
            "SELECT allocated_qty FROM kernel_lab.inventory_positions WHERE id = %s",
            (position_id,),
        ).fetchone()[0]
        available = availability(conn)[4]

    assert allocated == Decimal("2.000000")
    assert available == Decimal("0.000000")


def test_concurrent_reservations_cannot_overcommit_one_scope():
    with lab.connect() as conn:
        lab.insert_position(conn, physical_qty=10)

    def reserve(label):
        try:
            with lab.connect() as conn:
                create_reservation(
                    conn,
                    qty=8,
                    demand=f"ORDER-{label}",
                    key=f"reserve-{label}",
                )
            return "reserved"
        except psycopg.errors.CheckViolation:
            return "rejected"

    future_a, future_b = run_two(lambda: reserve("A"), lambda: reserve("B"))
    results = sorted([future_a.result(timeout=8), future_b.result(timeout=8)])

    with lab.connect() as conn:
        total_remaining = conn.execute(
            "SELECT COALESCE(sum(remaining_qty), 0) FROM kernel_lab.inventory_reservations"
        ).fetchone()[0]
        count = conn.execute(
            "SELECT count(*) FROM kernel_lab.inventory_reservations"
        ).fetchone()[0]

    assert results == ["rejected", "reserved"]
    assert total_remaining == Decimal("8.000000")
    assert count == 1


def test_one_reservation_can_allocate_across_multiple_positions():
    with lab.connect() as conn:
        position_a = lab.insert_position(conn, location_id=lab.LOCATION_A, physical_qty=6)
        position_b = lab.insert_position(conn, location_id=lab.LOCATION_B, physical_qty=4)
        reservation_id, _, _ = create_reservation(conn, qty=8)

        create_allocation(
            conn,
            position_id=position_a,
            qty=5,
            reservation_id=reservation_id,
            key="allocate-a",
        )
        create_allocation(
            conn,
            position_id=position_b,
            qty=3,
            reservation_id=reservation_id,
            key="allocate-b",
        )

        state = availability(conn)
        reservation = conn.execute(
            "SELECT allocated_qty, remaining_qty FROM kernel_lab.inventory_reservations WHERE id = %s",
            (reservation_id,),
        ).fetchone()

    assert state[0] == Decimal("10.000000")
    assert state[2] == Decimal("8.000000")
    assert state[3] == Decimal("0.000000")
    assert state[4] == Decimal("2.000000")
    assert reservation == (Decimal("8.000000"), Decimal("0.000000"))


def test_releasing_allocation_restores_active_reservation_not_free_capacity():
    with lab.connect() as conn:
        position_id = lab.insert_position(conn, physical_qty=10)
        reservation_id, _, _ = create_reservation(conn, qty=6)
        allocation_id, _, _ = create_allocation(
            conn,
            position_id=position_id,
            qty=4,
            reservation_id=reservation_id,
        )

        conn.execute(
            "SELECT kernel_lab.release_inventory_allocation(%s, %s, %s, %s)",
            (lab.new_id(), lab.TENANT, "release-allocation-1", allocation_id),
        )
        after_allocation_release = availability(conn)

        conn.execute(
            "SELECT kernel_lab.release_inventory_reservation(%s, %s, %s, %s)",
            (lab.new_id(), lab.TENANT, "release-reservation-1", reservation_id),
        )
        after_reservation_release = availability(conn)

    assert after_allocation_release[2] == Decimal("0.000000")
    assert after_allocation_release[3] == Decimal("6.000000")
    assert after_allocation_release[4] == Decimal("4.000000")
    assert after_reservation_release[3] == Decimal("0.000000")
    assert after_reservation_release[4] == Decimal("10.000000")


def test_releasing_allocation_after_reservation_close_does_not_reopen_reservation():
    with lab.connect() as conn:
        position_id = lab.insert_position(conn, physical_qty=10)
        reservation_id, _, _ = create_reservation(conn, qty=6)
        allocation_id, _, _ = create_allocation(
            conn,
            position_id=position_id,
            qty=6,
            reservation_id=reservation_id,
        )

        conn.execute(
            "SELECT kernel_lab.release_inventory_reservation(%s, %s, %s, %s)",
            (lab.new_id(), lab.TENANT, "close-reservation", reservation_id),
        )
        conn.execute(
            "SELECT kernel_lab.release_inventory_allocation(%s, %s, %s, %s)",
            (lab.new_id(), lab.TENANT, "release-after-close", allocation_id),
        )

        reservation = conn.execute(
            "SELECT allocated_qty, released_qty, remaining_qty, released_at IS NOT NULL FROM kernel_lab.inventory_reservations WHERE id = %s",
            (reservation_id,),
        ).fetchone()
        state = availability(conn)

    assert reservation == (
        Decimal("0.000000"),
        Decimal("6.000000"),
        Decimal("0.000000"),
        True,
    )
    assert state[4] == Decimal("10.000000")


def test_reservation_idempotency_replays_exact_request_and_rejects_payload_collision():
    reservation_id = lab.new_id()
    transaction_id = lab.new_id()

    with lab.connect() as conn:
        lab.insert_position(conn, physical_qty=10)
        first = conn.execute(
            """
            SELECT kernel_lab.create_inventory_reservation(
              %s, %s, %s, 'reserve-idem', 'ORDER-IDEM',
              %s, %s, %s, %s, 5
            )
            """,
            (
                reservation_id,
                transaction_id,
                lab.TENANT,
                lab.WAREHOUSE,
                lab.ITEM,
                lab.OWNER,
                lab.CONDITION,
            ),
        ).fetchone()[0]

    with lab.connect() as conn:
        replay = conn.execute(
            """
            SELECT kernel_lab.create_inventory_reservation(
              %s, %s, %s, 'reserve-idem', 'ORDER-IDEM',
              %s, %s, %s, %s, 5
            )
            """,
            (
                lab.new_id(),
                lab.new_id(),
                lab.TENANT,
                lab.WAREHOUSE,
                lab.ITEM,
                lab.OWNER,
                lab.CONDITION,
            ),
        ).fetchone()[0]

    assert first == transaction_id
    assert replay == transaction_id

    with lab.connect() as conn:
        try:
            conn.execute(
                """
                SELECT kernel_lab.create_inventory_reservation(
                  %s, %s, %s, 'reserve-idem', 'ORDER-IDEM',
                  %s, %s, %s, %s, 6
                )
                """,
                (
                    lab.new_id(),
                    lab.new_id(),
                    lab.TENANT,
                    lab.WAREHOUSE,
                    lab.ITEM,
                    lab.OWNER,
                    lab.CONDITION,
                ),
            )
            assert False, "payload collision should have been rejected"
        except psycopg.errors.UniqueViolation:
            pass

    with lab.connect() as conn:
        reservation_count = conn.execute(
            "SELECT count(*) FROM kernel_lab.inventory_reservations"
        ).fetchone()[0]
        transaction_count = conn.execute(
            "SELECT count(*) FROM kernel_lab.inventory_transactions WHERE idempotency_key = 'reserve-idem'"
        ).fetchone()[0]

    assert reservation_count == 1
    assert transaction_count == 1
