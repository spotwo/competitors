"""Project immutable InventoryTransaction posting events into a searchable read model."""

from __future__ import annotations

from typing import Any, Mapping
from uuid import UUID

from consumer_runtime import ConsumedEvent, InboxTransaction

EVENT_TYPE = "inventory.transaction.posted"
AGGREGATE_TYPE = "InventoryTransaction"
SCHEMA_VERSION = 2


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

    def __init__(self, *, consumer_name: str, expected_tenant_id: UUID | None = None):
        if not consumer_name or consumer_name != consumer_name.strip():
            raise ValueError("consumer_name is required without surrounding whitespace")
        if expected_tenant_id is not None and not isinstance(expected_tenant_id, UUID):
            raise ValueError("expected_tenant_id must be a UUID")
        self.consumer_name = consumer_name
        self.expected_tenant_id = expected_tenant_id

    def __call__(self, event: ConsumedEvent, transaction: InboxTransaction) -> None:
        self.apply(event, transaction)

    def apply(self, event: ConsumedEvent, transaction: InboxTransaction) -> str:
        if event.event_type != EVENT_TYPE or event.aggregate_type != AGGREGATE_TYPE:
            raise ValueError("unexpected transaction index event type or aggregate")
        if event.aggregate_version != 1:
            raise ValueError("immutable transaction posting requires aggregate version 1")
        if event.schema_version not in (1, SCHEMA_VERSION):
            raise ValueError("unsupported transaction posting schema version")
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

        if event.schema_version == SCHEMA_VERSION:
            # V2 never reads the producer ledger/tenant tables: tenant is
            # a first-class, canonical identity in the trusted envelope.
            if event.tenant_id is None:
                raise ValueError("V2 transaction posting requires tenant_id")
            tenant_id = event.tenant_id
            projection_mode = "v2"
        else:
            # Migration bridge ONLY. V1 lacked tenant_id, so old in-flight
            # events can be drained against the original co-located ledger.
            # Standalone remote projectors must run after the V1 backlog drains.
            if event.tenant_id is not None:
                raise ValueError("V1 transaction posting cannot claim tenant_id")
            legacy = transaction.execute(
                """
                SELECT tenant_id, transaction_type, source_reference, recorded_at
                FROM kernel_lab.inventory_transactions
                WHERE id = %s
                """,
                (transaction_id,),
            ).fetchone()
            if legacy is None:
                raise ValueError("V1 transaction posting requires local source ledger")
            tenant_id, legacy_type, legacy_ref, legacy_time = legacy
            if (event_type, source_ref, event.occurred_at) != (
                legacy_type, legacy_ref, legacy_time
            ):
                raise ValueError("V1 transaction posting differs from source ledger")
            projection_mode = "v1_legacy"

        if self.expected_tenant_id is not None and tenant_id != self.expected_tenant_id:
            raise ValueError("transaction posting tenant_id is outside the consumer scope")
        transaction.execute(
            """
            INSERT INTO kernel_lab.inventory_transaction_index_projection (
              consumer_name, transaction_id, tenant_id, transaction_type,
              source_reference, occurred_at, event_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (consumer_name, tenant_id, transaction_id) DO NOTHING
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
            WHERE consumer_name = %s AND tenant_id = %s AND transaction_id = %s
            """,
            (self.consumer_name, tenant_id, transaction_id),
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
                'mode', %s::text,
                'tenant_id', %s::uuid,
                'transaction_id', %s::uuid,
                'indexed_at', clock_timestamp()
              )
            )
            WHERE consumer_name = %s AND event_id = %s
            """,
            (projection_mode, tenant_id, transaction_id, self.consumer_name, event.event_id),
        )
        return "applied"
