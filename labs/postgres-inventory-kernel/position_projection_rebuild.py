from __future__ import annotations

from dataclasses import dataclass
from typing import Final
from uuid import UUID

import psycopg

from position_projection_snapshot import InventoryPositionQuantitySnapshot

REQUIRE_EMPTY: Final = "require-empty"
SUPERSEDE_COVERED_RETAIN_FUTURE: Final = "supersede-covered-retain-future"
PENDING_DISPOSITIONS: Final = frozenset(
    {REQUIRE_EMPTY, SUPERSEDE_COVERED_RETAIN_FUTURE}
)


def _required_name(value: str, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} is required")
    if value != value.strip():
        raise ValueError(f"{field} cannot contain surrounding whitespace")
    return value


def _nonnegative_integer(value: int, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field} must be a nonnegative integer")
    return value


def _rebuild_id(value: UUID) -> UUID:
    if not isinstance(value, UUID):
        raise ValueError("rebuild_id must be a UUID")
    return value


@dataclass(frozen=True)
class ProjectionRebuildPreparationResult:
    outcome: str
    status: str
    rebuild_id: UUID
    projection_version: int
    pending_event_count: int
    snapshot_version: int
    artifact_checksum: str

    def __post_init__(self) -> None:
        if self.outcome not in {"prepared", "duplicate"}:
            raise ValueError("unexpected projection rebuild preparation outcome")
        if self.status not in {"prepared", "completed", "cancelled"}:
            raise ValueError("unexpected projection rebuild status")
        _nonnegative_integer(self.projection_version, "projection_version")
        _nonnegative_integer(self.pending_event_count, "pending_event_count")
        if self.snapshot_version < 1:
            raise ValueError("snapshot_version must be positive")

    def to_dict(self) -> dict:
        return {
            "outcome": self.outcome,
            "status": self.status,
            "rebuild_id": str(self.rebuild_id),
            "projection_version": self.projection_version,
            "pending_event_count": self.pending_event_count,
            "snapshot_version": self.snapshot_version,
            "artifact_checksum": self.artifact_checksum,
        }


@dataclass(frozen=True)
class ProjectionRebuildExecutionResult:
    outcome: str
    status: str
    rebuild_id: UUID
    projection_version: int
    superseded_pending_count: int
    drained_pending_count: int
    remaining_pending_count: int

    def __post_init__(self) -> None:
        if self.outcome not in {"completed", "duplicate"}:
            raise ValueError("unexpected projection rebuild execution outcome")
        if self.status != "completed":
            raise ValueError("executed projection rebuild must be completed")
        _nonnegative_integer(self.projection_version, "projection_version")
        _nonnegative_integer(
            self.superseded_pending_count, "superseded_pending_count"
        )
        _nonnegative_integer(self.drained_pending_count, "drained_pending_count")
        _nonnegative_integer(
            self.remaining_pending_count, "remaining_pending_count"
        )

    def to_dict(self) -> dict:
        return {
            "outcome": self.outcome,
            "status": self.status,
            "rebuild_id": str(self.rebuild_id),
            "projection_version": self.projection_version,
            "pending_events": {
                "superseded": self.superseded_pending_count,
                "drained": self.drained_pending_count,
                "remaining": self.remaining_pending_count,
            },
        }


@dataclass(frozen=True)
class ProjectionRebuildCancellationResult:
    outcome: str
    status: str
    rebuild_id: UUID

    def __post_init__(self) -> None:
        if self.outcome not in {"cancelled", "duplicate"}:
            raise ValueError("unexpected projection rebuild cancellation outcome")
        if self.status != "cancelled":
            raise ValueError("cancelled projection rebuild must have cancelled status")

    def to_dict(self) -> dict:
        return {
            "outcome": self.outcome,
            "status": self.status,
            "rebuild_id": str(self.rebuild_id),
        }


class PostgresPositionProjectionRebuildStore:
    def __init__(self, database_url: str):
        self.database_url = database_url

    def _connect(self) -> psycopg.Connection:
        conn = psycopg.connect(self.database_url)
        conn.execute("SET statement_timeout = '30s'")
        conn.execute("SET lock_timeout = '5s'")
        return conn

    def prepare(
        self,
        *,
        rebuild_id: UUID,
        consumer_name: str,
        snapshot: InventoryPositionQuantitySnapshot,
        expected_projection_version: int,
        expected_pending_count: int,
        pending_disposition: str,
        operator: str,
        reason: str,
    ) -> ProjectionRebuildPreparationResult:
        _rebuild_id(rebuild_id)
        _required_name(consumer_name, "consumer_name")
        _nonnegative_integer(
            expected_projection_version, "expected_projection_version"
        )
        _nonnegative_integer(expected_pending_count, "expected_pending_count")
        if pending_disposition not in PENDING_DISPOSITIONS:
            raise ValueError("unsupported pending_disposition")
        _required_name(operator, "operator")
        _required_name(reason, "reason")

        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT outcome, rebuild_status, projection_version,
                       pending_event_count
                FROM kernel_lab.prepare_inventory_position_quantity_projection_rebuild(
                  %s, %s, %s, %s, %s, %s, %s, %s,
                  %s, %s, %s, %s, %s, %s, %s, %s
                )
                """,
                (
                    rebuild_id,
                    consumer_name,
                    snapshot.position_id,
                    expected_projection_version,
                    expected_pending_count,
                    pending_disposition,
                    snapshot.aggregate_version,
                    snapshot.physical_qty,
                    snapshot.reserved_qty,
                    snapshot.allocated_qty,
                    snapshot.source,
                    snapshot.reference,
                    snapshot.artifact_checksum,
                    snapshot.recorded_at,
                    operator,
                    reason,
                ),
            ).fetchone()

        if row is None:
            raise RuntimeError("projection rebuild prepare function returned no result")
        return ProjectionRebuildPreparationResult(
            outcome=row[0],
            status=row[1],
            rebuild_id=rebuild_id,
            projection_version=row[2],
            pending_event_count=row[3],
            snapshot_version=snapshot.aggregate_version,
            artifact_checksum=snapshot.artifact_checksum,
        )

    def execute(
        self,
        *,
        rebuild_id: UUID,
        operator: str,
    ) -> ProjectionRebuildExecutionResult:
        _rebuild_id(rebuild_id)
        _required_name(operator, "operator")

        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT outcome, rebuild_status, projection_version,
                       superseded_pending_count, drained_pending_count,
                       remaining_pending_count
                FROM kernel_lab.execute_inventory_position_quantity_projection_rebuild(
                  %s, %s
                )
                """,
                (rebuild_id, operator),
            ).fetchone()

        if row is None:
            raise RuntimeError("projection rebuild execute function returned no result")
        return ProjectionRebuildExecutionResult(
            outcome=row[0],
            status=row[1],
            rebuild_id=rebuild_id,
            projection_version=row[2],
            superseded_pending_count=row[3],
            drained_pending_count=row[4],
            remaining_pending_count=row[5],
        )

    def cancel(
        self,
        *,
        rebuild_id: UUID,
        operator: str,
        reason: str,
    ) -> ProjectionRebuildCancellationResult:
        _rebuild_id(rebuild_id)
        _required_name(operator, "operator")
        _required_name(reason, "reason")

        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT outcome, rebuild_status
                FROM kernel_lab.cancel_inventory_position_quantity_projection_rebuild(
                  %s, %s, %s
                )
                """,
                (rebuild_id, operator, reason),
            ).fetchone()

        if row is None:
            raise RuntimeError("projection rebuild cancel function returned no result")
        return ProjectionRebuildCancellationResult(
            outcome=row[0],
            status=row[1],
            rebuild_id=rebuild_id,
        )
