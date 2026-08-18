from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping
from uuid import UUID

import psycopg

SNAPSHOT_FIELDS = frozenset(
    {
        "position_id",
        "aggregate_version",
        "physical_qty",
        "reserved_qty",
        "allocated_qty",
        "source",
        "reference",
        "recorded_at",
    }
)
MAX_QUANTITY = Decimal("1000000000000000000")
QUANTITY_QUANTUM = Decimal("0.000001")


def _required_name(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} is required")
    if value != value.strip():
        raise ValueError(f"{field} cannot contain surrounding whitespace")
    return value


def _uuid(value: Any, field: str) -> UUID:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a UUID string")
    try:
        parsed = UUID(value)
    except ValueError as exc:
        raise ValueError(f"{field} must be a UUID string") from exc
    if str(parsed) != value:
        raise ValueError(f"{field} must be a canonical UUID")
    return parsed


def _positive_integer(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{field} must be a positive integer")
    return value


def _quantity(value: Any, field: str) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (str, int, Decimal)):
        raise ValueError(f"{field} must be an exact decimal string or integer")
    try:
        number = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError(f"{field} must be an exact decimal string or integer") from exc
    if not number.is_finite() or number < 0:
        raise ValueError(f"{field} must be a finite nonnegative quantity")
    try:
        quantized = number.quantize(QUANTITY_QUANTUM)
    except InvalidOperation as exc:
        raise ValueError(f"{field} exceeds projection precision") from exc
    if number != quantized:
        raise ValueError(f"{field} supports at most six fractional digits")
    if abs(quantized) >= MAX_QUANTITY:
        raise ValueError(f"{field} exceeds projection precision")
    return quantized


def _aware_timestamp(value: Any, field: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be an ISO 8601 timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field} must be an ISO 8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field} must include a timezone")
    return parsed


@dataclass(frozen=True)
class InventoryPositionQuantitySnapshot:
    position_id: UUID
    aggregate_version: int
    physical_qty: Decimal
    reserved_qty: Decimal
    allocated_qty: Decimal
    source: str
    reference: str
    recorded_at: datetime
    artifact_checksum: str

    def __post_init__(self) -> None:
        if self.reserved_qty > self.physical_qty:
            raise ValueError("reserved_qty cannot exceed physical_qty")
        if self.allocated_qty > self.physical_qty:
            raise ValueError("allocated_qty cannot exceed physical_qty")

    @classmethod
    def from_document(
        cls,
        document: Mapping[str, Any],
        *,
        artifact_checksum: str,
    ) -> InventoryPositionQuantitySnapshot:
        if not isinstance(document, Mapping):
            raise ValueError("snapshot artifact must contain one JSON object")
        fields = set(document)
        missing = sorted(SNAPSHOT_FIELDS - fields)
        extra = sorted(fields - SNAPSHOT_FIELDS)
        if missing:
            raise ValueError(f"snapshot artifact is missing fields: {', '.join(missing)}")
        if extra:
            raise ValueError(f"snapshot artifact has unknown fields: {', '.join(extra)}")
        if not isinstance(artifact_checksum, str) or re.fullmatch(
            r"sha256:[0-9a-f]{64}", artifact_checksum
        ) is None:
            raise ValueError("artifact_checksum must be lowercase sha256:<64 hex>")

        return cls(
            position_id=_uuid(document["position_id"], "position_id"),
            aggregate_version=_positive_integer(
                document["aggregate_version"], "aggregate_version"
            ),
            physical_qty=_quantity(document["physical_qty"], "physical_qty"),
            reserved_qty=_quantity(document["reserved_qty"], "reserved_qty"),
            allocated_qty=_quantity(document["allocated_qty"], "allocated_qty"),
            source=_required_name(document["source"], "source"),
            reference=_required_name(document["reference"], "reference"),
            recorded_at=_aware_timestamp(document["recorded_at"], "recorded_at"),
            artifact_checksum=artifact_checksum,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "position_id": str(self.position_id),
            "aggregate_version": self.aggregate_version,
            "physical_qty": str(self.physical_qty),
            "reserved_qty": str(self.reserved_qty),
            "allocated_qty": str(self.allocated_qty),
            "source": self.source,
            "reference": self.reference,
            "recorded_at": self.recorded_at.isoformat(),
            "artifact_checksum": self.artifact_checksum,
        }


def load_snapshot_artifact(path: Path) -> InventoryPositionQuantitySnapshot:
    artifact = path.read_bytes()
    checksum = f"sha256:{hashlib.sha256(artifact).hexdigest()}"
    try:
        document = json.loads(artifact)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("snapshot artifact must be valid UTF-8 JSON") from exc
    return InventoryPositionQuantitySnapshot.from_document(
        document,
        artifact_checksum=checksum,
    )


@dataclass(frozen=True)
class ProjectionSnapshotBootstrapResult:
    outcome: str
    bootstrap_id: UUID
    bootstrap_version: int
    projection_version: int
    artifact_checksum: str

    def __post_init__(self) -> None:
        if self.outcome not in {"bootstrapped", "duplicate"}:
            raise ValueError("unexpected projection bootstrap outcome")
        if self.bootstrap_version < 1:
            raise ValueError("bootstrap_version must be positive")
        if self.projection_version < self.bootstrap_version:
            raise ValueError("projection_version cannot precede bootstrap_version")

    def to_dict(self) -> dict[str, Any]:
        return {
            "outcome": self.outcome,
            "bootstrap_id": str(self.bootstrap_id),
            "bootstrap_version": self.bootstrap_version,
            "projection_version": self.projection_version,
            "artifact_checksum": self.artifact_checksum,
        }


class PostgresPositionProjectionBootstrapStore:
    def __init__(self, database_url: str):
        self.database_url = database_url

    def _connect(self) -> psycopg.Connection:
        conn = psycopg.connect(self.database_url)
        conn.execute("SET statement_timeout = '30s'")
        conn.execute("SET lock_timeout = '5s'")
        return conn

    def bootstrap(
        self,
        *,
        bootstrap_id: UUID,
        consumer_name: str,
        snapshot: InventoryPositionQuantitySnapshot,
        operator: str,
        reason: str,
    ) -> ProjectionSnapshotBootstrapResult:
        _required_name(consumer_name, "consumer_name")
        _required_name(operator, "operator")
        _required_name(reason, "reason")
        if not isinstance(bootstrap_id, UUID):
            raise ValueError("bootstrap_id must be a UUID")

        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT outcome, bootstrap_version, projection_version
                FROM kernel_lab.bootstrap_inventory_position_quantity_projection(
                  %s, %s, %s, %s, %s, %s, %s,
                  %s, %s, %s, %s, %s, %s
                )
                """,
                (
                    bootstrap_id,
                    consumer_name,
                    snapshot.position_id,
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
            raise RuntimeError("projection bootstrap function returned no result")
        return ProjectionSnapshotBootstrapResult(
            outcome=row[0],
            bootstrap_id=bootstrap_id,
            bootstrap_version=row[1],
            projection_version=row[2],
            artifact_checksum=snapshot.artifact_checksum,
        )
