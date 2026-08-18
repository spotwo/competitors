from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping
from uuid import UUID

from psycopg.types.json import Jsonb

from consumer_runtime import ConsumedEvent, InboxTransaction


POSITION_EVENT_TYPE = "inventory.position.changed"
POSITION_AGGREGATE_TYPE = "InventoryPosition"
POSITION_SCHEMA_VERSION = 1
DELTA_FIELDS = ("physical_delta", "reserved_delta", "allocated_delta")


def _required_name(value: str, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} is required")
    if value != value.strip():
        raise ValueError(f"{field} cannot contain surrounding whitespace")
    return value


def _finite_json_number(value: Any, field: str) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        raise ValueError(f"event data {field} must be a JSON number")
    try:
        number = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError(f"event data {field} must be a JSON number") from exc
    if not number.is_finite():
        raise ValueError(f"event data {field} must be finite")
    return number


@dataclass(frozen=True)
class ProjectionApplyResult:
    outcome: str
    applied_count: int
    projection_version: int
    expected_version: int


class InventoryPositionQuantityProjector:
    """Projects ordered Inventory Position quantity deltas inside the Inbox transaction."""

    def __init__(self, *, consumer_name: str):
        self.consumer_name = _required_name(consumer_name, "consumer_name")

    def __call__(
        self,
        event: ConsumedEvent,
        transaction: InboxTransaction,
    ) -> None:
        self.apply(event, transaction)

    def apply(
        self,
        event: ConsumedEvent,
        transaction: InboxTransaction,
    ) -> ProjectionApplyResult:
        position_id, data = self._validate_event(event)
        row = transaction.execute(
            """
            SELECT outcome, applied_count, projection_version, expected_version
            FROM kernel_lab.apply_inventory_position_quantity_event(
              %s, %s, %s, %s, %s, %s
            )
            """,
            (
                self.consumer_name,
                event.event_id,
                position_id,
                event.aggregate_version,
                event.recorded_at,
                Jsonb(dict(data)),
            ),
        ).fetchone()
        if row is None:
            raise RuntimeError("projection function returned no result")
        return ProjectionApplyResult(
            outcome=row[0],
            applied_count=row[1],
            projection_version=row[2],
            expected_version=row[3],
        )

    @staticmethod
    def _validate_event(event: ConsumedEvent) -> tuple[UUID, Mapping[str, Any]]:
        if event.event_type != POSITION_EVENT_TYPE:
            raise ValueError(f"projector requires event type {POSITION_EVENT_TYPE}")
        if event.aggregate_type != POSITION_AGGREGATE_TYPE:
            raise ValueError(
                f"projector requires aggregate type {POSITION_AGGREGATE_TYPE}"
            )
        if event.schema_version != POSITION_SCHEMA_VERSION:
            raise ValueError(
                f"projector requires schema version {POSITION_SCHEMA_VERSION}"
            )
        try:
            position_id = UUID(event.aggregate_id)
        except ValueError as exc:
            raise ValueError("InventoryPosition aggregate_id must be a UUID") from exc
        if str(position_id) != event.aggregate_id:
            raise ValueError("InventoryPosition aggregate_id must be a canonical UUID")
        if event.subject != f"inventory-position/{event.aggregate_id}":
            raise ValueError("event subject must match InventoryPosition aggregate_id")
        if not isinstance(event.data, Mapping):
            raise ValueError("inventory.position.changed data must be a JSON object")

        for field in DELTA_FIELDS:
            if field not in event.data:
                raise ValueError(f"event data {field} is required")
            _finite_json_number(event.data[field], field)

        transaction_id = event.data.get("transaction_id")
        if not isinstance(transaction_id, str):
            raise ValueError("event data transaction_id must be a UUID string")
        try:
            parsed_transaction_id = UUID(transaction_id)
        except ValueError as exc:
            raise ValueError("event data transaction_id must be a UUID string") from exc
        if str(parsed_transaction_id) != transaction_id:
            raise ValueError("event data transaction_id must be a canonical UUID")
        if event.data.get("position_id") != event.aggregate_id:
            raise ValueError("event data position_id must match aggregate_id")
        _required_name(event.data.get("transaction_type"), "event data transaction_type")
        return position_id, event.data
