"""Project immutable InventoryTransaction posting events into a searchable read model."""

from __future__ import annotations

from typing import Any, Mapping
from uuid import UUID

from consumer_runtime import ConsumedEvent, InboxTransaction

EVENT_TYPE = "inventory.transaction.posted"
AGGREGATE_TYPE = "InventoryTransaction"
SCHEMA_VERSION = 1


def _uuid(value: Any, name: str) -> UUID:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a canonical UUID string")
    try:
        parsed = UUID(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be a canonical UUID string") from exc
    if str(parsed) != value:
        raise ValueError(f"{name} must be a canonical UUID string")
    return parsed


class InventoryTransactionIndexProjector:
    """Apply exactly one immutable posting fact inside the Inbox transaction."""

    def __init__(self, *, consumer_name: str):
        if not consumer_name or consumer_name != consumer_name.strip():
            raise ValueError("consumer_name is required without surrounding whitespace")
        self.consumer_name = consumer_name

    def __call__(self, event: ConsumedEvent, transaction: InboxTransaction) -> None:
        self.apply(event, transaction)

    def apply(self, event: ConsumedEvent, transaction: InboxTransaction) -> str:
        if event.event_type != EVENT_TYPE or event.aggregate_type != AGGREGATE_TYPE:
            raise ValueError("unexpected transaction index event type or aggregate")
        if event.aggregate_version != 1 or event.schema_version != SCHEMA_VERSION:
            raise ValueError("immutable transaction posting requires version 1")
        transaction_id = _uuid(event.aggregate_id, "aggregate_id")
        if event.subject != f"inventory-transaction/{transaction_id}":
            raise ValueError("transaction posting subject does not match aggregate")
        if not isinstance(event.data, Mapping):
            raise ValueError("transaction posting data must be an object")
        if _uuid(event.data.get("transaction_id"), "data.transaction_id") != transaction_id:
            raise ValueError("event transaction_id does not match aggregate")
        event_type = event.data.get("transaction_type")
        if not isinstance(event_type, str) or not event_type.strip():
            raise ValueError("transaction_type must be nonblank")
        source_ref = event.data.get("source_reference")
        if source_ref is not None and not isinstance(source_ref, str):
            raise ValueError("source_reference must be a string or absent")

        # The existing v1 event contains no tenant id. In this PostgreSQL lab,
        # resolve the authoritative tenant and verify the immutable source row.
        # A remote projection must first add a tenant identity to the event contract.
        row = transaction.execute(
            """
            SELECT tenant_id, transaction_type, source_reference, recorded_at
            FROM kernel_lab.inventory_transactions
            WHERE id = %s
            """,
            (transaction_id,),
        ).fetchone()
        if row is None:
            raise ValueError("transaction index cannot resolve source transaction")
        tenant_id, stored_type, stored_ref, stored_time = row
        if (
            event_type != stored_type
            or source_ref != stored_ref
            or event.occurred_at != stored_time
        ):
            raise ValueError("transaction posting event differs from committed ledger")

        transaction.execute(
            """
            INSERT INTO kernel_lab.inventory_transaction_index_projection (
              consumer_name, transaction_id, tenant_id, transaction_type,
              source_reference, occurred_at, event_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (consumer_name, transaction_id) DO NOTHING
            """,
            (
                self.consumer_name,
                transaction_id,
                tenant_id,
                event_type,
                source_ref,
                event.occurred_at,
                event.event_id,
            ),
        )
        existing = transaction.execute(
            """
            SELECT event_id, tenant_id, transaction_type, source_reference, occurred_at
            FROM kernel_lab.inventory_transaction_index_projection
            WHERE consumer_name = %s AND transaction_id = %s
            """,
            (self.consumer_name, transaction_id),
        ).fetchone()
        if existing != (
            event.event_id, tenant_id, event_type, source_ref, event.occurred_at
        ):
            raise ValueError("transaction projection identity collision")

        transaction.execute(
            """
            UPDATE kernel_lab.domain_event_inbox
            SET metadata = metadata || jsonb_build_object(
              'projection', jsonb_build_object(
                'name', 'inventory-transaction-index',
                'status', 'applied',
                'transaction_id', %s,
                'indexed_at', clock_timestamp()
              )
            )
            WHERE consumer_name = %s AND event_id = %s
            """,
            (transaction_id, self.consumer_name, event.event_id),
        )
        return "applied"
