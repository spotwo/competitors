from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
import threading

import psycopg

import conftest as lab


def run_two(fn_a, fn_b):
    barrier = threading.Barrier(2)

    def wrapped(fn):
        barrier.wait(timeout=5)
        return fn()

    with ThreadPoolExecutor(max_workers=2) as pool:
        future_a = pool.submit(wrapped, fn_a)
        future_b = pool.submit(wrapped, fn_b)
        return future_a, future_b


def ensure_location_position(candidate_id):
    with lab.connect() as conn:
        row = conn.execute(
            """
            SELECT kernel_lab.ensure_inventory_position(
              %s, %s, %s, %s, NULL, %s, %s, %s,
              NULL, NULL, NULL, NULL
            )
            """,
            (
                candidate_id,
                lab.TENANT,
                lab.WAREHOUSE,
                lab.LOCATION_A,
                lab.ITEM,
                lab.OWNER,
                lab.CONDITION,
            ),
        ).fetchone()
        assert row is not None
        return row[0]


def test_merge_race_converges_on_one_semantic_position():
    candidate_a = lab.new_id()
    candidate_b = lab.new_id()

    future_a, future_b = run_two(
        lambda: ensure_location_position(candidate_a),
        lambda: ensure_location_position(candidate_b),
    )

    result_a = future_a.result(timeout=8)
    result_b = future_b.result(timeout=8)

    assert result_a == result_b
    assert result_a in {candidate_a, candidate_b}

    with lab.connect() as conn:
        count = conn.execute(
            "SELECT count(*) FROM kernel_lab.inventory_positions"
        ).fetchone()[0]
        assert count == 1


def test_double_allocation_allows_only_one_competing_commitment():
    with lab.connect() as conn:
        position_id = lab.insert_position(conn, physical_qty=10)

    def allocate(label):
        try:
            with lab.connect() as conn:
                conn.execute(
                    "SELECT kernel_lab.allocate_inventory_position(%s, %s, %s, %s, 8)",
                    (lab.new_id(), lab.TENANT, f"allocation-{label}", position_id),
                )
            return "committed"
        except psycopg.errors.CheckViolation:
            return "rejected"

    future_a, future_b = run_two(lambda: allocate("a"), lambda: allocate("b"))
    results = sorted([future_a.result(timeout=8), future_b.result(timeout=8)])

    assert results == ["committed", "rejected"]

    with lab.connect() as conn:
        allocated, physical = conn.execute(
            "SELECT allocated_qty, physical_qty FROM kernel_lab.inventory_positions WHERE id = %s",
            (position_id,),
        ).fetchone()
        transactions = conn.execute(
            "SELECT count(*) FROM kernel_lab.inventory_transactions WHERE transaction_type = 'allocation'"
        ).fetchone()[0]

    assert allocated == Decimal("8.000000")
    assert physical == Decimal("10.000000")
    assert transactions == 1


def test_serial_double_assignment_is_exclusive():
    with lab.connect() as conn:
        position_a = lab.insert_position(
            conn, location_id=lab.LOCATION_A, item_id=lab.SERIAL_ITEM, physical_qty=1
        )
        position_b = lab.insert_position(
            conn, location_id=lab.LOCATION_B, item_id=lab.SERIAL_ITEM, physical_qty=1
        )
        serial_id = lab.new_id()
        conn.execute(
            "INSERT INTO kernel_lab.serials (id, tenant_id, item_id, serial_no) VALUES (%s, %s, %s, 'SERIAL-001')",
            (serial_id, lab.TENANT, lab.SERIAL_ITEM),
        )

    def assign(position_id):
        try:
            with lab.connect() as conn:
                conn.execute(
                    "SELECT kernel_lab.assign_serial_to_position(%s, %s, %s)",
                    (lab.TENANT, serial_id, position_id),
                )
            return "assigned"
        except psycopg.errors.UniqueViolation:
            return "duplicate"

    future_a, future_b = run_two(lambda: assign(position_a), lambda: assign(position_b))
    results = sorted([future_a.result(timeout=8), future_b.result(timeout=8)])

    assert results == ["assigned", "duplicate"]

    with lab.connect() as conn:
        rows = conn.execute(
            "SELECT position_id FROM kernel_lab.inventory_serial_memberships WHERE tenant_id = %s AND serial_id = %s",
            (lab.TENANT, serial_id),
        ).fetchall()

    assert len(rows) == 1
    assert rows[0][0] in {position_a, position_b}


def test_partial_split_posts_balanced_source_and_target_legs():
    with lab.connect() as conn:
        source = lab.insert_position(conn, location_id=lab.LOCATION_A, physical_qty=10)
        target = lab.insert_position(conn, location_id=lab.LOCATION_B, physical_qty=0)
        transaction_id = lab.new_id()
        conn.execute(
            "SELECT kernel_lab.transfer_inventory_quantity(%s, %s, 'split-1', %s, %s, 4)",
            (transaction_id, lab.TENANT, source, target),
        )

    with lab.connect() as conn:
        quantities = dict(
            conn.execute(
                "SELECT id, physical_qty FROM kernel_lab.inventory_positions WHERE id IN (%s, %s)",
                (source, target),
            ).fetchall()
        )
        legs = conn.execute(
            "SELECT position_id, physical_delta FROM kernel_lab.inventory_transaction_legs WHERE transaction_id = %s",
            (transaction_id,),
        ).fetchall()

    assert quantities[source] == Decimal("6.000000")
    assert quantities[target] == Decimal("4.000000")
    assert sum(delta for _, delta in legs) == Decimal("0.000000")
    assert sorted(delta for _, delta in legs) == [Decimal("-4.000000"), Decimal("4.000000")]


def test_same_warehouse_full_hu_move_does_not_rewrite_contained_position():
    with lab.connect() as conn:
        hu_id = lab.insert_hu(conn, location_id=lab.LOCATION_A)
        position_id = lab.insert_position(
            conn, location_id=None, handling_unit_id=hu_id, physical_qty=10
        )
        before = conn.execute(
            "SELECT id, handling_unit_id, physical_qty, version FROM kernel_lab.inventory_positions WHERE id = %s",
            (position_id,),
        ).fetchone()
        conn.execute(
            "SELECT kernel_lab.relocate_handling_unit(%s, %s, %s, %s)",
            (lab.new_id(), lab.TENANT, hu_id, lab.LOCATION_B),
        )

    with lab.connect() as conn:
        after = conn.execute(
            "SELECT id, handling_unit_id, physical_qty, version FROM kernel_lab.inventory_positions WHERE id = %s",
            (position_id,),
        ).fetchone()
        hu = conn.execute(
            "SELECT location_id, version FROM kernel_lab.handling_units WHERE id = %s",
            (hu_id,),
        ).fetchone()
        inventory_transactions = conn.execute(
            "SELECT count(*) FROM kernel_lab.inventory_transactions"
        ).fetchone()[0]
        hu_movements = conn.execute(
            "SELECT count(*) FROM kernel_lab.handling_unit_movements WHERE handling_unit_id = %s",
            (hu_id,),
        ).fetchone()[0]

    assert after == before
    assert hu == (lab.LOCATION_B, 1)
    assert inventory_transactions == 0
    assert hu_movements == 1


def test_repack_is_a_real_inventory_split_between_hu_anchors():
    with lab.connect() as conn:
        hu_a = lab.insert_hu(conn, location_id=lab.LOCATION_A)
        hu_b = lab.insert_hu(conn, location_id=lab.LOCATION_A)
        source = lab.insert_position(
            conn, location_id=None, handling_unit_id=hu_a, physical_qty=10
        )
        target = lab.insert_position(
            conn, location_id=None, handling_unit_id=hu_b, physical_qty=0
        )
        transaction_id = lab.new_id()
        conn.execute(
            "SELECT kernel_lab.transfer_inventory_quantity(%s, %s, 'repack-1', %s, %s, 4)",
            (transaction_id, lab.TENANT, source, target),
        )

    with lab.connect() as conn:
        rows = conn.execute(
            "SELECT handling_unit_id, physical_qty FROM kernel_lab.inventory_positions WHERE id IN (%s, %s) ORDER BY handling_unit_id",
            (source, target),
        ).fetchall()
        net = conn.execute(
            "SELECT sum(physical_delta) FROM kernel_lab.inventory_transaction_legs WHERE transaction_id = %s",
            (transaction_id,),
        ).fetchone()[0]

    assert sorted(qty for _, qty in rows) == [Decimal("4.000000"), Decimal("6.000000")]
    assert {hu for hu, _ in rows} == {hu_a, hu_b}
    assert net == Decimal("0.000000")


def test_deterministic_multi_position_lock_order_avoids_deadlock():
    with lab.connect() as conn:
        position_a = lab.insert_position(conn, location_id=lab.LOCATION_A, physical_qty=10)
        position_b = lab.insert_position(conn, location_id=lab.LOCATION_B, physical_qty=10)

    for iteration in range(12):
        def move_a_to_b():
            with lab.connect() as conn:
                conn.execute(
                    "SELECT kernel_lab.transfer_inventory_quantity(%s, %s, %s, %s, %s, 1)",
                    (
                        lab.new_id(),
                        lab.TENANT,
                        f"deadlock-a-{iteration}",
                        position_a,
                        position_b,
                    ),
                )
            return "ok"

        def move_b_to_a():
            with lab.connect() as conn:
                conn.execute(
                    "SELECT kernel_lab.transfer_inventory_quantity(%s, %s, %s, %s, %s, 1)",
                    (
                        lab.new_id(),
                        lab.TENANT,
                        f"deadlock-b-{iteration}",
                        position_b,
                        position_a,
                    ),
                )
            return "ok"

        future_a, future_b = run_two(move_a_to_b, move_b_to_a)
        assert future_a.result(timeout=8) == "ok"
        assert future_b.result(timeout=8) == "ok"

    with lab.connect() as conn:
        quantities = dict(
            conn.execute(
                "SELECT id, physical_qty FROM kernel_lab.inventory_positions WHERE id IN (%s, %s)",
                (position_a, position_b),
            ).fetchall()
        )
        tx_count = conn.execute(
            "SELECT count(*) FROM kernel_lab.inventory_transactions WHERE transaction_type = 'movement'"
        ).fetchone()[0]

    assert quantities[position_a] == Decimal("10.000000")
    assert quantities[position_b] == Decimal("10.000000")
    assert tx_count == 24
