from decimal import Decimal

import conftest as lab


def test_allocation_retry_returns_original_transaction_without_double_post():
    with lab.connect() as conn:
        position_id = lab.insert_position(conn, physical_qty=10)

    original_transaction_id = lab.new_id()
    retry_transaction_id = lab.new_id()

    with lab.connect() as conn:
        first = conn.execute(
            "SELECT kernel_lab.allocate_inventory_position(%s, %s, 'retry-safe-allocation', %s, 4)",
            (original_transaction_id, lab.TENANT, position_id),
        ).fetchone()[0]

    with lab.connect() as conn:
        second = conn.execute(
            "SELECT kernel_lab.allocate_inventory_position(%s, %s, 'retry-safe-allocation', %s, 4)",
            (retry_transaction_id, lab.TENANT, position_id),
        ).fetchone()[0]

    with lab.connect() as conn:
        allocated = conn.execute(
            "SELECT allocated_qty FROM kernel_lab.inventory_positions WHERE id = %s",
            (position_id,),
        ).fetchone()[0]
        transactions = conn.execute(
            "SELECT count(*) FROM kernel_lab.inventory_transactions WHERE idempotency_key = 'retry-safe-allocation'"
        ).fetchone()[0]
        legs = conn.execute(
            "SELECT count(*) FROM kernel_lab.inventory_transaction_legs WHERE transaction_id = %s",
            (original_transaction_id,),
        ).fetchone()[0]

    assert first == original_transaction_id
    assert second == original_transaction_id
    assert second != retry_transaction_id
    assert allocated == Decimal("4.000000")
    assert transactions == 1
    assert legs == 1
