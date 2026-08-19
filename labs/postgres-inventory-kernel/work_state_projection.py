from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping
from uuid import UUID

from psycopg.types.json import Jsonb

from consumer_runtime import ConsumedEvent, InboxTransaction


WORK_STATE_EVENT_TYPE = "warehouse.work.state.changed"
WORK_AGGREGATE_TYPE = "WarehouseWork"
WORK_SCHEMA_VERSION = 1
WORK_STATES = frozenset(
    {
        "planned",
        "released",
        "assigned",
        "in_progress",
        "exception",
        "completed",
        "cancelled",
    }
)


def _required_name(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} is required")
    if value != value.strip():
        raise ValueError(f"{field} cannot contain surrounding whitespace")
    return value


def _canonical_uuid(value: Any, field: str) -> UUID:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a UUID string")
    try:
        parsed = UUID(value)
    except ValueError as exc:
        raise ValueError(f"{field} must be a UUID string") from exc
    if str(parsed) != value:
        raise ValueError(f"{field} must be a canonical UUID")
    return parsed


def _integer_at_least(value: Any, minimum: int, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"{field} must be an integer >= {minimum}")
    return value


@dataclass(frozen=True)
class WorkProjectionApplyResult:
    outcome: str


class WarehouseWorkStateProjector:
    """Projects ordered Warehouse Work state transitions inside the Inbox transaction."""

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
    ) -> WorkProjectionApplyResult:
        work_id, data = self._validate_event(event)
        row = transaction.execute(
            """
            SELECT kernel_lab.apply_warehouse_work_state_event(
              %s, %s, %s, %s, %s, %s
            )
            """,
            (
                self.consumer_name,
                event.event_id,
                work_id,
                event.aggregate_version,
                event.recorded_at,
                Jsonb(dict(data)),
            ),
        ).fetchone()
        if row is None:
            raise RuntimeError("Warehouse Work projection function returned no result")
        return WorkProjectionApplyResult(outcome=row[0])

    @staticmethod
    def _validate_event(event: ConsumedEvent) -> tuple[UUID, Mapping[str, Any]]:
        if event.event_type != WORK_STATE_EVENT_TYPE:
            raise ValueError(f"projector requires event type {WORK_STATE_EVENT_TYPE}")
        if event.aggregate_type != WORK_AGGREGATE_TYPE:
            raise ValueError(
                f"projector requires aggregate type {WORK_AGGREGATE_TYPE}"
            )
        if event.schema_version != WORK_SCHEMA_VERSION:
            raise ValueError(f"projector requires schema version {WORK_SCHEMA_VERSION}")

        work_id = _canonical_uuid(event.aggregate_id, "WarehouseWork aggregate_id")
        if event.subject != f"warehouse-work/{event.aggregate_id}":
            raise ValueError("event subject must match WarehouseWork aggregate_id")
        if not isinstance(event.data, Mapping):
            raise ValueError("warehouse.work.state.changed data must be a JSON object")

        if event.data.get("work_id") != event.aggregate_id:
            raise ValueError("event data work_id must match aggregate_id")
        _canonical_uuid(event.data.get("tenant_id"), "event data tenant_id")
        _canonical_uuid(event.data.get("warehouse_id"), "event data warehouse_id")
        _required_name(event.data.get("capability"), "event data capability")
        _required_name(
            event.data.get("domain_reference"), "event data domain_reference"
        )

        previous_state = event.data.get("previous_state")
        state = event.data.get("state")
        if previous_state not in WORK_STATES:
            raise ValueError("event data previous_state is invalid")
        if state not in WORK_STATES:
            raise ValueError("event data state is invalid")
        if previous_state == state:
            raise ValueError("Warehouse Work state event must change state")

        previous_work_version = _integer_at_least(
            event.data.get("previous_work_version"),
            0,
            "event data previous_work_version",
        )
        work_version = _integer_at_least(
            event.data.get("work_version"), 1, "event data work_version"
        )
        if work_version != previous_work_version + 1:
            raise ValueError("Warehouse Work internal version must advance exactly once")

        return work_id, event.data
