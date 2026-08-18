from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from uuid import UUID

import psycopg

DEFAULT_RETENTION_DAYS = 30
MAX_ARCHIVE_BATCH_SIZE = 1000


def _require_aware(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must include a timezone")


def _required_name(value: str, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} is required")
    if value != value.strip():
        raise ValueError(f"{field} cannot contain surrounding whitespace")
    return value


@dataclass(frozen=True)
class MalformedDeliveryRetentionPolicy:
    retention_days: int = DEFAULT_RETENTION_DAYS
    batch_size: int = MAX_ARCHIVE_BATCH_SIZE

    def __post_init__(self) -> None:
        if not 1 <= self.retention_days <= 36_500:
            raise ValueError("retention_days must be between 1 and 36500")
        if not 1 <= self.batch_size <= MAX_ARCHIVE_BATCH_SIZE:
            raise ValueError(
                f"batch_size must be between 1 and {MAX_ARCHIVE_BATCH_SIZE}"
            )

    def cutoff(self, *, observed_at: datetime | None = None) -> datetime:
        observed_at = observed_at or datetime.now(timezone.utc)
        _require_aware(observed_at, "observed_at")
        return observed_at - timedelta(days=self.retention_days)


@dataclass(frozen=True)
class MalformedDeliveryArchiveBatch:
    consumer_name: str
    cutoff: datetime
    archived_at: datetime
    archive_run_id: UUID | None
    archived_delivery_count: int

    def __post_init__(self) -> None:
        _required_name(self.consumer_name, "consumer_name")
        _require_aware(self.cutoff, "cutoff")
        _require_aware(self.archived_at, "archived_at")
        if self.archived_delivery_count < 0:
            raise ValueError("archived_delivery_count must be nonnegative")
        if (self.archived_delivery_count == 0) != (self.archive_run_id is None):
            raise ValueError("archive_run_id presence must match archived deliveries")

    def to_dict(self) -> dict:
        return {
            "archive_run_id": (
                str(self.archive_run_id) if self.archive_run_id is not None else None
            ),
            "consumer_name": self.consumer_name,
            "cutoff": self.cutoff.isoformat(),
            "archived_at": self.archived_at.isoformat(),
            "archived": {
                "deliveries": self.archived_delivery_count,
            },
        }


class PostgresMalformedDeliveryArchiveStore:
    def __init__(self, database_url: str):
        self.database_url = database_url

    def _connect(self) -> psycopg.Connection:
        conn = psycopg.connect(self.database_url)
        conn.execute("SET statement_timeout = '30s'")
        conn.execute("SET lock_timeout = '5s'")
        return conn

    def archive_batch(
        self,
        *,
        consumer_name: str,
        before: datetime,
        batch_size: int = MAX_ARCHIVE_BATCH_SIZE,
    ) -> MalformedDeliveryArchiveBatch:
        _required_name(consumer_name, "consumer_name")
        _require_aware(before, "before")
        if not 1 <= batch_size <= MAX_ARCHIVE_BATCH_SIZE:
            raise ValueError(
                f"batch_size must be between 1 and {MAX_ARCHIVE_BATCH_SIZE}"
            )

        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM kernel_lab.archive_nats_jetstream_consumer_poison_deliveries(
                  %s, %s, %s
                )
                """,
                (consumer_name, before, batch_size),
            ).fetchone()

        if row is None:
            raise RuntimeError("malformed delivery archive function returned no result")
        if row[2] != consumer_name:
            raise RuntimeError("malformed delivery archive returned another scope")

        return MalformedDeliveryArchiveBatch(
            consumer_name=row[2],
            cutoff=before,
            archive_run_id=row[0],
            archived_at=row[1],
            archived_delivery_count=row[3],
        )
