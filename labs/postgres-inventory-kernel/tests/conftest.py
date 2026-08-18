from __future__ import annotations

import os
import sys
import uuid
from pathlib import Path

import psycopg
import pytest

LAB_DIR = Path(__file__).resolve().parents[1]
if str(LAB_DIR) not in sys.path:
    sys.path.insert(0, str(LAB_DIR))

DATABASE_URL = os.getenv(
    "KERNEL_LAB_DATABASE_URL",
    "postgresql://postgres:postgres@127.0.0.1:55432/kernel_lab",
)

TENANT = uuid.UUID("00000000-0000-0000-0000-000000000001")
WAREHOUSE = uuid.UUID("00000000-0000-0000-0000-000000000010")
LOCATION_A = uuid.UUID("00000000-0000-0000-0000-000000000101")
LOCATION_B = uuid.UUID("00000000-0000-0000-0000-000000000102")
LOCATION_C = uuid.UUID("00000000-0000-0000-0000-000000000103")
ITEM = uuid.UUID("00000000-0000-0000-0000-000000001001")
SERIAL_ITEM = uuid.UUID("00000000-0000-0000-0000-000000001002")
OWNER = uuid.UUID("00000000-0000-0000-0000-000000002001")
CONDITION = uuid.UUID("00000000-0000-0000-0000-000000003001")


def new_id() -> uuid.UUID:
    """Fixture identity only. Production candidate IDs remain application-generated UUIDv7."""
    return uuid.uuid4()


def connect(*, autocommit: bool = False) -> psycopg.Connection:
    conn = psycopg.connect(DATABASE_URL, autocommit=autocommit)
    conn.execute("SET statement_timeout = '8s'")
    conn.execute("SET lock_timeout = '5s'")
    return conn


def insert_position(
    conn: psycopg.Connection,
    *,
    position_id: uuid.UUID | None = None,
    location_id: uuid.UUID | None = LOCATION_A,
    handling_unit_id: uuid.UUID | None = None,
    item_id: uuid.UUID = ITEM,
    physical_qty: int | float = 0,
) -> uuid.UUID:
    position_id = position_id or new_id()
    conn.execute(
        """
        INSERT INTO kernel_lab.inventory_positions (
          id, tenant_id, warehouse_id, location_id, handling_unit_id,
          item_id, owner_id, inventory_condition_id, physical_qty
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            position_id,
            TENANT,
            WAREHOUSE,
            location_id,
            handling_unit_id,
            item_id,
            OWNER,
            CONDITION,
            physical_qty,
        ),
    )
    return position_id


def insert_hu(
    conn: psycopg.Connection,
    *,
    hu_id: uuid.UUID | None = None,
    location_id: uuid.UUID | None = LOCATION_A,
    parent_hu_id: uuid.UUID | None = None,
) -> uuid.UUID:
    hu_id = hu_id or new_id()
    conn.execute(
        """
        INSERT INTO kernel_lab.handling_units (
          id, tenant_id, warehouse_id, parent_handling_unit_id, location_id
        ) VALUES (%s, %s, %s, %s, %s)
        """,
        (hu_id, TENANT, WAREHOUSE, parent_hu_id, location_id),
    )
    return hu_id


@pytest.fixture(autouse=True)
def clean_database():
    with connect(autocommit=True) as conn:
        conn.execute(
            """
            TRUNCATE TABLE
              kernel_lab.inventory_position_projection_control,
              kernel_lab.inventory_position_projection_rebuilds,
              kernel_lab.inventory_position_projection_pending,
              kernel_lab.inventory_position_quantity_projection,
              kernel_lab.domain_event_consumer_failure_action_archive,
              kernel_lab.domain_event_consumer_failure_archive,
              kernel_lab.domain_event_consumer_failure_actions,
              kernel_lab.domain_event_consumer_failures,
              kernel_lab.domain_event_inbox,
              kernel_lab.domain_event_outbox_operator_action_archive,
              kernel_lab.domain_event_outbox_archive,
              kernel_lab.domain_event_outbox_operator_actions,
              kernel_lab.domain_event_outbox,
              kernel_lab.domain_event_idempotency_keys,
              kernel_lab.inventory_count_results,
              kernel_lab.inventory_allocation_consumptions,
              kernel_lab.warehouse_task_confirmations,
              kernel_lab.warehouse_task_exceptions,
              kernel_lab.warehouse_task_executions,
              kernel_lab.warehouse_work_assignments,
              kernel_lab.warehouse_work_command_receipts,
              kernel_lab.warehouse_tasks,
              kernel_lab.warehouse_works,
              kernel_lab.inventory_holds,
              kernel_lab.inventory_hold_policies,
              kernel_lab.inventory_allocations,
              kernel_lab.inventory_reservations,
              kernel_lab.inventory_transaction_legs,
              kernel_lab.inventory_transactions,
              kernel_lab.inventory_serial_memberships,
              kernel_lab.handling_unit_movements,
              kernel_lab.inventory_positions,
              kernel_lab.serials,
              kernel_lab.handling_units,
              kernel_lab.inventory_attribute_sets,
              kernel_lab.stock_segments,
              kernel_lab.stock_scopes,
              kernel_lab.lots,
              kernel_lab.locations,
              kernel_lab.inventory_conditions,
              kernel_lab.owners,
              kernel_lab.items,
              kernel_lab.warehouses,
              kernel_lab.tenants
            CASCADE
            """
        )
        conn.execute(
            "INSERT INTO kernel_lab.tenants (id, name) VALUES (%s, 'Test Tenant')",
            (TENANT,),
        )
        conn.execute(
            "INSERT INTO kernel_lab.warehouses (id, tenant_id, code) VALUES (%s, %s, 'W1')",
            (WAREHOUSE, TENANT),
        )
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO kernel_lab.locations (id, tenant_id, warehouse_id, code) VALUES (%s, %s, %s, %s)",
                [
                    (LOCATION_A, TENANT, WAREHOUSE, "A-01"),
                    (LOCATION_B, TENANT, WAREHOUSE, "B-01"),
                    (LOCATION_C, TENANT, WAREHOUSE, "C-01"),
                ],
            )
            cur.executemany(
                "INSERT INTO kernel_lab.items (id, tenant_id, sku, exact_serial_tracking) VALUES (%s, %s, %s, %s)",
                [
                    (ITEM, TENANT, "SKU-001", False),
                    (SERIAL_ITEM, TENANT, "SER-001", True),
                ],
            )
        conn.execute(
            "INSERT INTO kernel_lab.owners (id, tenant_id, code) VALUES (%s, %s, 'OWNER')",
            (OWNER, TENANT),
        )
        conn.execute(
            "INSERT INTO kernel_lab.inventory_conditions (id, tenant_id, code) VALUES (%s, %s, 'NORMAL')",
            (CONDITION, TENANT),
        )
    yield
