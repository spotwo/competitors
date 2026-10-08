"""The third pipeline is a real immutable Inventory Transaction projection, not a stub."""

from __future__ import annotations

from dataclasses import replace
from datetime import timezone

import pytest

import conftest as lab
from consumer_runtime import ConsumedEvent, PostgresInboxStore
from event_pipeline_handler_loader import load_pipeline_handler_type
from event_pipeline_readiness import load_registry
from event_pipeline_handlers.inventory_transaction_index import (
    InventoryTransactionIndexProjector,
)
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
REGISTRY = ROOT / "operations" / "event-pipelines" / "registry.yml"
CONSUMER_NAME = "inventory_transaction_index"


def create_receipt(*, key: str = "transaction-index:receipt", reference: str = "ASN-TX-INDEX"):
    with lab.connect() as conn:
        position_id = lab.insert_position(conn, physical_qty=0)
        transaction_id = lab.new_id()
        posted = conn.execute(
            "SELECT kernel_lab.post_inventory_receipt(%s, %s, %s, %s, 5, %s)",
            (transaction_id, lab.TENANT, key, position_id, reference),
        ).fetchone()[0]
    assert posted == transaction_id
    return transaction_id


def load_transaction_event(transaction_id):
    with lab.connect() as conn:
        row = conn.execute(
            """
            SELECT event_id, event_type, source, subject, occurred_at,
                   recorded_at, aggregate_type, aggregate_id, aggregate_version,
                   schema_version, data
            FROM kernel_lab.domain_event_outbox
            WHERE event_type = 'inventory.transaction.posted'
              AND aggregate_id = %s
            """,
            (str(transaction_id),),
        ).fetchone()
    assert row is not None
    return ConsumedEvent.from_envelope({
        "event_id": str(row[0]),
        "type": row[1],
        "source": row[2],
        "subject": row[3],
        "occurred_at": row[4].isoformat(),
        "recorded_at": row[5].isoformat(),
        "aggregate_type": row[6],
        "aggregate_id": row[7],
        "aggregate_version": row[8],
        "schema_version": row[9],
        "data": row[10],
    })


def process(event, *, consumer_name=CONSUMER_NAME):
    return PostgresInboxStore(lab.DATABASE_URL).process_once(
        consumer_name=consumer_name,
        event=event,
        delivery_metadata={
            "transport": "test",
            "transport_message_id": str(event.event_id),
            "subject": "spotwo.wms.events.inventory.transaction.posted",
            "delivery_count": 1,
        },
        handler=InventoryTransactionIndexProjector(consumer_name=consumer_name),
    )


def test_third_pipeline_is_enabled_and_handler_is_bound_by_registry():
    registry = load_registry(REGISTRY)
    spec = registry.get("inventory-transaction-index")
    assert spec.enabled is True
    assert spec.consumer.projection_gap_monitor == "none"
    assert spec.consumer.durable == "INVENTORY_TRANSACTION_INDEX_PROJECTOR"
    assert spec.consumer.handler_module == "event_pipeline_handlers.inventory_transaction_index"
    assert spec.consumer.handler_class == "InventoryTransactionIndexProjector"
    assert load_pipeline_handler_type(spec.consumer) is InventoryTransactionIndexProjector


def test_posted_transaction_projects_atomically_and_replay_is_idempotent():
    transaction_id = create_receipt()
    event = load_transaction_event(transaction_id)

    assert process(event) is True
    assert process(event) is False
    with lab.connect() as conn:
        indexed = conn.execute(
            """
            SELECT tenant_id, transaction_type, source_reference, event_id, occurred_at
            FROM kernel_lab.inventory_transaction_index_projection
            WHERE consumer_name = %s AND transaction_id = %s
            """,
            (CONSUMER_NAME, transaction_id),
        ).fetchone()
        receipt_count = conn.execute(
            "SELECT count(*) FROM kernel_lab.domain_event_inbox WHERE consumer_name = %s",
            (CONSUMER_NAME,),
        ).fetchone()[0]
        receipt_metadata = conn.execute(
            """
            SELECT metadata #>> '{projection,status}'
            FROM kernel_lab.domain_event_inbox
            WHERE consumer_name = %s AND event_id = %s
            """,
            (CONSUMER_NAME, event.event_id),
        ).fetchone()[0]

    assert indexed == (
        lab.TENANT, "receipt", "ASN-TX-INDEX", event.event_id, event.occurred_at
    )
    assert receipt_count == 1
    assert receipt_metadata == "applied"


def test_tampered_payload_rolls_back_inbox_and_projection():
    transaction_id = create_receipt()
    original = load_transaction_event(transaction_id)
    altered = replace(
        original,
        data={**original.data, "transaction_type": "movement"},
    )
    with pytest.raises(ValueError, match="differs from committed ledger"):
        process(altered)

    with lab.connect() as conn:
        assert conn.execute(
            "SELECT count(*) FROM kernel_lab.domain_event_inbox WHERE consumer_name = %s",
            (CONSUMER_NAME,),
        ).fetchone()[0] == 0
        assert conn.execute(
            "SELECT count(*) FROM kernel_lab.inventory_transaction_index_projection",
        ).fetchone()[0] == 0
    assert process(original) is True


def test_noncanonical_or_wrong_version_is_rejected_without_inbox_receipt():
    transaction_id = create_receipt()
    original = load_transaction_event(transaction_id)

    with pytest.raises(ValueError, match="version 1"):
        process(replace(original, aggregate_version=2))
    with pytest.raises(ValueError, match="aggregate_id"):
        process(replace(original, aggregate_id=str(transaction_id).upper()))
    with lab.connect() as conn:
        assert conn.execute(
            "SELECT count(*) FROM kernel_lab.domain_event_inbox WHERE consumer_name = %s",
            (CONSUMER_NAME,),
        ).fetchone()[0] == 0
