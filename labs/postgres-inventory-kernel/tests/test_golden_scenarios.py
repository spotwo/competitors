from __future__ import annotations

from decimal import Decimal

import psycopg
import pytest

import conftest as lab
import test_work as work


def scope_available(conn: psycopg.Connection) -> Decimal:
    return conn.execute(
        """
        SELECT kernel_lab.inventory_scope_available_qty(
          %s, %s, %s, %s, %s, NULL, NULL, NULL, NULL
        )
        """,
        (lab.TENANT, lab.WAREHOUSE, lab.ITEM, lab.OWNER, lab.CONDITION),
    ).fetchone()[0]


def position_qty(conn: psycopg.Connection, position_id):
    return conn.execute(
        """
        SELECT physical_qty, reserved_qty, allocated_qty
        FROM kernel_lab.inventory_positions
        WHERE id = %s
        """,
        (position_id,),
    ).fetchone()


def start_single_task_work(
    conn: psycopg.Connection,
    *,
    prefix: str,
    capability: str,
    domain_reference: str,
    operation: str,
    planned_quantity=None,
    requires_domain_confirmation: bool = True,
    channel: str = "rf",
):
    work_id, task_ids = work.create_work(
        conn,
        capability=capability,
        domain_reference=domain_reference,
        tasks=[
            {
                "operation": operation,
                "planned_quantity": planned_quantity,
                "uom": "EA" if planned_quantity is not None else None,
                "requires_domain_confirmation": requires_domain_confirmation,
            }
        ],
    )
    task_id = task_ids[0]
    work.release(conn, work_id, key=f"{prefix}:release")
    assignment_id, _ = work.claim(conn, work_id, key=f"{prefix}:claim")
    execution_id, _ = work.start(conn, task_id, key=f"{prefix}:start", channel=channel)
    return work_id, task_id, assignment_id, execution_id


def insert_allocation_hold_policy(conn: psycopg.Connection, *, code: str = "PICKED-STAGING"):
    policy_id = lab.new_id()
    conn.execute(
        """
        INSERT INTO kernel_lab.inventory_hold_policies (
          id, tenant_id, code, description,
          blocks_allocation, blocks_movement, blocks_picking, blocks_shipping
        ) VALUES (%s, %s, %s, 'Picked stock awaiting shipment', true, false, false, false)
        """,
        (policy_id, lab.TENANT, code),
    )
    return policy_id


def test_order_fulfillment_happy_path():
    with lab.connect() as conn:
        inbound = lab.insert_position(conn, location_id=lab.LOCATION_A, physical_qty=0)
        storage = lab.insert_position(conn, location_id=lab.LOCATION_B, physical_qty=0)
        staging = lab.insert_position(conn, location_id=lab.LOCATION_C, physical_qty=0)
        staging_policy = insert_allocation_hold_policy(conn)

        receipt_tx = lab.new_id()
        conn.execute(
            "SELECT kernel_lab.post_inventory_receipt(%s, %s, %s, %s, 100, 'ASN-100')",
            (receipt_tx, lab.TENANT, "golden:receipt", inbound),
        )
        assert position_qty(conn, inbound) == (Decimal("100"), Decimal("0"), Decimal("0"))
        assert scope_available(conn) == Decimal("100")

        putaway_work, putaway_task, _, _ = start_single_task_work(
            conn,
            prefix="golden:putaway",
            capability="putaway",
            domain_reference="RECEIPT-100",
            operation="putaway",
            planned_quantity=100,
        )
        putaway_tx = lab.new_id()
        putaway_confirmation = lab.new_id()
        conn.execute(
            """
            SELECT kernel_lab.confirm_inventory_movement_task(
              %s, %s, %s, %s, %s, %s, 'human', 'worker-1', %s, %s, 100
            )
            """,
            (
                putaway_confirmation,
                putaway_tx,
                lab.TENANT,
                "golden:putaway:move",
                "golden:putaway:confirm",
                putaway_task,
                inbound,
                storage,
            ),
        )
        assert position_qty(conn, inbound)[0] == Decimal("0")
        assert position_qty(conn, storage)[0] == Decimal("100")
        assert conn.execute(
            "SELECT state FROM kernel_lab.warehouse_works WHERE id = %s", (putaway_work,)
        ).fetchone()[0] == "completed"

        reservation_id = lab.new_id()
        reservation_tx = lab.new_id()
        conn.execute(
            """
            SELECT kernel_lab.create_inventory_reservation(
              %s, %s, %s, %s, 'ORDER-100', %s, %s, %s, %s, 20
            )
            """,
            (
                reservation_id,
                reservation_tx,
                lab.TENANT,
                "golden:reserve",
                lab.WAREHOUSE,
                lab.ITEM,
                lab.OWNER,
                lab.CONDITION,
            ),
        )
        assert scope_available(conn) == Decimal("80")

        allocation_id = lab.new_id()
        allocation_tx = lab.new_id()
        conn.execute(
            """
            SELECT kernel_lab.allocate_inventory_commitment(
              %s, %s, %s, %s, 'ORDER-100', %s, 20, %s
            )
            """,
            (
                allocation_id,
                allocation_tx,
                lab.TENANT,
                "golden:allocate",
                storage,
                reservation_id,
            ),
        )
        reservation_state = conn.execute(
            """
            SELECT allocated_qty, consumed_qty, remaining_qty
            FROM kernel_lab.inventory_reservations WHERE id = %s
            """,
            (reservation_id,),
        ).fetchone()
        assert reservation_state == (Decimal("20"), Decimal("0"), Decimal("0"))
        assert position_qty(conn, storage)[2] == Decimal("20")
        assert scope_available(conn) == Decimal("80")

        pick_work, pick_task, _, _ = start_single_task_work(
            conn,
            prefix="golden:pick",
            capability="picking",
            domain_reference="ORDER-100",
            operation="pick",
            planned_quantity=20,
        )
        consumption_id = lab.new_id()
        pick_tx = lab.new_id()
        staging_hold_id = lab.new_id()
        staging_hold_tx = lab.new_id()
        pick_confirmation = lab.new_id()
        conn.execute(
            """
            SELECT kernel_lab.confirm_pick_task(
              %s, %s, %s, %s, %s, %s,
              %s, %s, %s,
              %s, 'human', 'worker-1',
              %s, %s, 20, %s
            )
            """,
            (
                pick_confirmation,
                consumption_id,
                pick_tx,
                staging_hold_id,
                staging_hold_tx,
                lab.TENANT,
                "golden:pick:move",
                "golden:pick:hold",
                "golden:pick:confirm",
                pick_task,
                allocation_id,
                staging,
                staging_policy,
            ),
        )
        assert position_qty(conn, storage) == (Decimal("80"), Decimal("0"), Decimal("0"))
        assert position_qty(conn, staging)[0] == Decimal("20")
        reservation_state = conn.execute(
            """
            SELECT allocated_qty, consumed_qty, released_qty, remaining_qty
            FROM kernel_lab.inventory_reservations WHERE id = %s
            """,
            (reservation_id,),
        ).fetchone()
        allocation_state = conn.execute(
            """
            SELECT consumed_qty, released_qty, remaining_qty
            FROM kernel_lab.inventory_allocations WHERE id = %s
            """,
            (allocation_id,),
        ).fetchone()
        assert reservation_state == (
            Decimal("0"), Decimal("20"), Decimal("0"), Decimal("0")
        )
        assert allocation_state == (Decimal("20"), Decimal("0"), Decimal("0"))
        assert scope_available(conn) == Decimal("80")
        assert conn.execute(
            "SELECT state FROM kernel_lab.warehouse_works WHERE id = %s", (pick_work,)
        ).fetchone()[0] == "completed"

        pack_work, pack_task, _, _ = start_single_task_work(
            conn,
            prefix="golden:pack",
            capability="packing",
            domain_reference="ORDER-100",
            operation="pack",
            planned_quantity=20,
            requires_domain_confirmation=False,
            channel="workstation",
        )
        work.confirm(
            conn,
            pack_task,
            key="golden:pack:confirm",
            actual_quantity=20,
            outcome="packed",
        )
        assert conn.execute(
            "SELECT sum(physical_qty) FROM kernel_lab.inventory_positions"
        ).fetchone()[0] == Decimal("100")
        assert conn.execute(
            "SELECT state FROM kernel_lab.warehouse_works WHERE id = %s", (pack_work,)
        ).fetchone()[0] == "completed"

        ship_work, ship_task, _, _ = start_single_task_work(
            conn,
            prefix="golden:ship",
            capability="shipping",
            domain_reference="ORDER-100",
            operation="ship",
            planned_quantity=20,
        )
        ship_tx = lab.new_id()
        hold_release_tx = lab.new_id()
        ship_confirmation = lab.new_id()
        conn.execute(
            """
            SELECT kernel_lab.confirm_shipping_task(
              %s, %s, %s, %s,
              %s, %s, %s,
              %s, 'human', 'worker-1',
              %s, 20, %s, 'SHIPMENT-100'
            )
            """,
            (
                ship_confirmation,
                ship_tx,
                hold_release_tx,
                lab.TENANT,
                "golden:ship:issue",
                "golden:ship:hold-release",
                "golden:ship:confirm",
                ship_task,
                staging,
                staging_hold_id,
            ),
        )

        assert conn.execute(
            "SELECT sum(physical_qty) FROM kernel_lab.inventory_positions"
        ).fetchone()[0] == Decimal("80")
        assert position_qty(conn, staging)[0] == Decimal("0")
        assert scope_available(conn) == Decimal("80")
        assert conn.execute(
            "SELECT state FROM kernel_lab.warehouse_works WHERE id = %s", (ship_work,)
        ).fetchone()[0] == "completed"
        assert conn.execute(
            "SELECT released_at IS NOT NULL FROM kernel_lab.inventory_holds WHERE id = %s",
            (staging_hold_id,),
        ).fetchone()[0] is True

        tx_types = [
            row[0]
            for row in conn.execute(
                "SELECT transaction_type FROM kernel_lab.inventory_transactions ORDER BY recorded_at, id"
            ).fetchall()
        ]
        assert "receipt" in tx_types
        assert "issue" in tx_types
        assert tx_types.count("movement") >= 2


def test_whole_hu_relocation():
    with lab.connect() as conn:
        hu_id = lab.insert_hu(conn, location_id=lab.LOCATION_A)
        position_id = lab.insert_position(
            conn,
            location_id=None,
            handling_unit_id=hu_id,
            physical_qty=40,
        )
        original_anchor = conn.execute(
            "SELECT handling_unit_id FROM kernel_lab.inventory_positions WHERE id = %s",
            (position_id,),
        ).fetchone()[0]

        relocation_work, task_id, _, _ = start_single_task_work(
            conn,
            prefix="golden:hu-relocation",
            capability="relocation",
            domain_reference="HU-RELOCATE-1",
            operation="relocate-hu",
        )
        movement_id = lab.new_id()
        confirmation_id = lab.new_id()
        conn.execute(
            """
            SELECT kernel_lab.confirm_hu_relocation_task(
              %s, %s, %s, %s, %s, 'human', 'worker-1', %s, %s
            )
            """,
            (
                confirmation_id,
                movement_id,
                lab.TENANT,
                "golden:hu-relocation:confirm",
                task_id,
                hu_id,
                lab.LOCATION_B,
            ),
        )

        hu_location = conn.execute(
            "SELECT location_id FROM kernel_lab.handling_units WHERE id = %s", (hu_id,)
        ).fetchone()[0]
        position_after = conn.execute(
            """
            SELECT handling_unit_id, physical_qty, version
            FROM kernel_lab.inventory_positions WHERE id = %s
            """,
            (position_id,),
        ).fetchone()
        work_state = conn.execute(
            "SELECT state FROM kernel_lab.warehouse_works WHERE id = %s", (relocation_work,)
        ).fetchone()[0]

    assert hu_location == lab.LOCATION_B
    assert position_after == (original_anchor, Decimal("40"), 0)
    assert work_state == "completed"


def test_short_pick_recovery():
    with lab.connect() as conn:
        source = lab.insert_position(conn, location_id=lab.LOCATION_A, physical_qty=10)
        staging = lab.insert_position(conn, location_id=lab.LOCATION_C, physical_qty=0)
        staging_policy = insert_allocation_hold_policy(conn, code="SHORT-PICK-STAGING")

        reservation_id = lab.new_id()
        conn.execute(
            """
            SELECT kernel_lab.create_inventory_reservation(
              %s, %s, %s, %s, 'ORDER-200', %s, %s, %s, %s, 10
            )
            """,
            (
                reservation_id,
                lab.new_id(),
                lab.TENANT,
                "golden:short:reserve",
                lab.WAREHOUSE,
                lab.ITEM,
                lab.OWNER,
                lab.CONDITION,
            ),
        )
        allocation_id = lab.new_id()
        conn.execute(
            """
            SELECT kernel_lab.allocate_inventory_commitment(
              %s, %s, %s, %s, 'ORDER-200', %s, 10, %s
            )
            """,
            (
                allocation_id,
                lab.new_id(),
                lab.TENANT,
                "golden:short:allocate",
                source,
                reservation_id,
            ),
        )

        work_id, task_id, _, first_execution = start_single_task_work(
            conn,
            prefix="golden:short:work",
            capability="picking",
            domain_reference="ORDER-200",
            operation="pick",
            planned_quantity=10,
        )

        conn.execute(
            """
            SELECT kernel_lab.consume_inventory_allocation_movement(
              %s, %s, %s, %s, %s, %s, 6
            )
            """,
            (
                lab.new_id(),
                lab.new_id(),
                lab.TENANT,
                "golden:short:pick-6",
                allocation_id,
                staging,
            ),
        )
        staging_hold_id = lab.new_id()
        conn.execute(
            """
            SELECT kernel_lab.apply_inventory_position_hold(
              %s, %s, %s, %s, %s, 'short-picked stock awaiting completion', %s
            )
            """,
            (
                staging_hold_id,
                lab.new_id(),
                lab.TENANT,
                "golden:short:staging-hold",
                staging_policy,
                staging,
            ),
        )

        allocation_state = conn.execute(
            "SELECT consumed_qty, remaining_qty FROM kernel_lab.inventory_allocations WHERE id = %s",
            (allocation_id,),
        ).fetchone()
        reservation_state = conn.execute(
            """
            SELECT consumed_qty, allocated_qty, remaining_qty
            FROM kernel_lab.inventory_reservations WHERE id = %s
            """,
            (reservation_id,),
        ).fetchone()
        assert allocation_state == (Decimal("6"), Decimal("4"))
        assert reservation_state == (Decimal("6"), Decimal("4"), Decimal("0"))

        exception_id, _ = work.raise_exception(
            conn,
            task_id,
            key="golden:short:exception",
            code="SHORT_PICK",
            details='{"picked":6,"planned":10}',
        )
        states = conn.execute(
            """
            SELECT w.state, t.state, e.state
            FROM kernel_lab.warehouse_works w
            JOIN kernel_lab.warehouse_tasks t ON t.work_id = w.id
            JOIN kernel_lab.warehouse_task_executions e ON e.task_id = t.id
            WHERE w.id = %s AND e.id = %s
            """,
            (work_id, first_execution),
        ).fetchone()
        assert states == ("exception", "exception", "blocked")

        conn.execute(
            "SELECT kernel_lab.resolve_warehouse_task_exception(%s, %s, %s, 'resume')",
            (lab.TENANT, "golden:short:resolve", exception_id),
        )
        second_execution, _ = work.start(
            conn,
            task_id,
            key="golden:short:retry-start",
            execution_id=lab.new_id(),
        )
        first_state = conn.execute(
            "SELECT state FROM kernel_lab.warehouse_task_executions WHERE id = %s",
            (first_execution,),
        ).fetchone()[0]
        second_state = conn.execute(
            "SELECT state FROM kernel_lab.warehouse_task_executions WHERE id = %s",
            (second_execution,),
        ).fetchone()[0]
        assert first_state == "aborted"
        assert second_state == "in_progress"

        conn.execute(
            """
            SELECT kernel_lab.consume_inventory_allocation_movement(
              %s, %s, %s, %s, %s, %s, 4
            )
            """,
            (
                lab.new_id(),
                lab.new_id(),
                lab.TENANT,
                "golden:short:pick-4",
                allocation_id,
                staging,
            ),
        )
        work.confirm(
            conn,
            task_id,
            key="golden:short:confirm",
            actual_quantity=10,
            domain_result_reference=f"inventory-allocation:{allocation_id}",
        )

        allocation_state = conn.execute(
            "SELECT consumed_qty, remaining_qty FROM kernel_lab.inventory_allocations WHERE id = %s",
            (allocation_id,),
        ).fetchone()
        reservation_state = conn.execute(
            """
            SELECT consumed_qty, allocated_qty, remaining_qty
            FROM kernel_lab.inventory_reservations WHERE id = %s
            """,
            (reservation_id,),
        ).fetchone()
        final_task = conn.execute(
            "SELECT state FROM kernel_lab.warehouse_tasks WHERE id = %s", (task_id,)
        ).fetchone()[0]

    assert allocation_state == (Decimal("10"), Decimal("0"))
    assert reservation_state == (Decimal("10"), Decimal("0"), Decimal("0"))
    assert final_task == "completed"


def test_cycle_count_reconciliation():
    with lab.connect() as conn:
        position_id = lab.insert_position(conn, location_id=lab.LOCATION_A, physical_qty=100)
        count_work, task_id, _, _ = start_single_task_work(
            conn,
            prefix="golden:count",
            capability="cycle-counting",
            domain_reference="COUNT-001",
            operation="count",
            planned_quantity=None,
        )
        count_result_id = lab.new_id()
        confirmation_id = lab.new_id()
        conn.execute(
            """
            SELECT kernel_lab.confirm_count_task(
              %s, %s, %s, %s, %s, %s, 'human', 'worker-1', %s, 97, 'COUNT-001'
            )
            """,
            (
                confirmation_id,
                count_result_id,
                lab.TENANT,
                "golden:count:record",
                "golden:count:confirm",
                task_id,
                position_id,
            ),
        )

        count_state = conn.execute(
            """
            SELECT system_quantity_snapshot, counted_quantity, variance_quantity, state
            FROM kernel_lab.inventory_count_results WHERE id = %s
            """,
            (count_result_id,),
        ).fetchone()
        physical_before = position_qty(conn, position_id)[0]
        work_state = conn.execute(
            "SELECT state FROM kernel_lab.warehouse_works WHERE id = %s", (count_work,)
        ).fetchone()[0]
        assert count_state == (Decimal("100"), Decimal("97"), Decimal("-3"), "observed")
        assert physical_before == Decimal("100")
        assert work_state == "completed"

        reconciliation_tx = lab.new_id()
        conn.execute(
            "SELECT kernel_lab.reconcile_inventory_count_result(%s, %s, %s, %s)",
            (reconciliation_tx, lab.TENANT, "golden:count:reconcile", count_result_id),
        )
        physical_after = position_qty(conn, position_id)[0]
        count_after = conn.execute(
            """
            SELECT state, reconciliation_transaction_id
            FROM kernel_lab.inventory_count_results WHERE id = %s
            """,
            (count_result_id,),
        ).fetchone()
        leg = conn.execute(
            """
            SELECT physical_delta FROM kernel_lab.inventory_transaction_legs
            WHERE transaction_id = %s AND position_id = %s
            """,
            (reconciliation_tx, position_id),
        ).fetchone()[0]

    assert physical_after == Decimal("97")
    assert count_after == ("reconciled", reconciliation_tx)
    assert leg == Decimal("-3")


def test_count_reconciliation_rejects_stale_snapshot():
    with lab.connect() as conn:
        position_id = lab.insert_position(conn, location_id=lab.LOCATION_A, physical_qty=100)
        count_result_id = lab.new_id()
        conn.execute(
            "SELECT kernel_lab.record_inventory_count_result(%s, %s, %s, %s, 97, 'COUNT-STALE')",
            (count_result_id, lab.TENANT, "golden:count:stale-record", position_id),
        )
        conn.execute(
            """
            UPDATE kernel_lab.inventory_positions
            SET physical_qty = 99, version = version + 1
            WHERE id = %s
            """,
            (position_id,),
        )

        with conn.transaction():
            with pytest.raises(psycopg.errors.SerializationFailure):
                conn.execute(
                    "SELECT kernel_lab.reconcile_inventory_count_result(%s, %s, %s, %s)",
                    (lab.new_id(), lab.TENANT, "golden:count:stale-reconcile", count_result_id),
                )

        count_state = conn.execute(
            "SELECT state FROM kernel_lab.inventory_count_results WHERE id = %s",
            (count_result_id,),
        ).fetchone()[0]

    assert count_state == "observed"
