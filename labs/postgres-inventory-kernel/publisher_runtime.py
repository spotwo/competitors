from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol
from uuid import UUID

import psycopg


@dataclass(frozen=True)
class ClaimedEvent:
    event_id: UUID
    claim_token: UUID
    event_type: str
    aggregate_type: str
    aggregate_id: str
    aggregate_version: int
    envelope: dict[str, Any]


@dataclass(frozen=True)
class PublishReceipt:
    transport_message_id: str | None = None
    deduplicated: bool = False


@dataclass(frozen=True)
class PublishCycleResult:
    claimed: int = 0
    published: int = 0
    failed: int = 0
    stale_ack: int = 0
    stale_nack: int = 0


class Transport(Protocol):
    def publish(self, event: ClaimedEvent) -> PublishReceipt:
        """Publish one event or raise an exception when delivery was not confirmed."""


class OutboxStore(Protocol):
    def claim(self, *, worker_id: str, limit: int, lease_seconds: int) -> list[ClaimedEvent]:
        ...

    def ack(self, *, event_id: UUID, claim_token: UUID) -> bool:
        ...

    def nack(
        self,
        *,
        event_id: UUID,
        claim_token: UUID,
        error: str,
        retry_after_seconds: int,
    ) -> bool:
        ...


class PostgresOutboxStore:
    """Short-transaction adapter around the PostgreSQL outbox primitives.

    Each public method owns and closes its own database transaction. In particular,
    `claim()` commits before the caller invokes any external transport.
    """

    def __init__(self, database_url: str):
        self.database_url = database_url

    def _connect(self) -> psycopg.Connection:
        conn = psycopg.connect(self.database_url)
        conn.execute("SET statement_timeout = '8s'")
        conn.execute("SET lock_timeout = '5s'")
        return conn

    def claim(self, *, worker_id: str, limit: int, lease_seconds: int) -> list[ClaimedEvent]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM kernel_lab.claim_domain_events(%s, %s, %s)",
                (worker_id, limit, lease_seconds),
            ).fetchall()
            conn.commit()

        return [
            ClaimedEvent(
                event_id=row[0],
                claim_token=row[1],
                event_type=row[2],
                aggregate_type=row[3],
                aggregate_id=row[4],
                aggregate_version=row[5],
                envelope=row[6],
            )
            for row in rows
        ]

    def ack(self, *, event_id: UUID, claim_token: UUID) -> bool:
        with self._connect() as conn:
            acknowledged = conn.execute(
                "SELECT kernel_lab.ack_domain_event(%s, %s)",
                (event_id, claim_token),
            ).fetchone()[0]
            conn.commit()
            return acknowledged

    def nack(
        self,
        *,
        event_id: UUID,
        claim_token: UUID,
        error: str,
        retry_after_seconds: int,
    ) -> bool:
        with self._connect() as conn:
            released = conn.execute(
                "SELECT kernel_lab.nack_domain_event(%s, %s, %s, %s)",
                (event_id, claim_token, error, retry_after_seconds),
            ).fetchone()[0]
            conn.commit()
            return released


class PublisherRuntime:
    def __init__(
        self,
        *,
        store: OutboxStore,
        transport: Transport,
        worker_id: str,
        batch_size: int = 100,
        lease_seconds: int = 30,
        retry_after_seconds: int = 5,
    ):
        if not worker_id.strip():
            raise ValueError("worker_id is required")
        if not 1 <= batch_size <= 1000:
            raise ValueError("batch_size must be between 1 and 1000")
        if not 1 <= lease_seconds <= 3600:
            raise ValueError("lease_seconds must be between 1 and 3600")
        if not 0 <= retry_after_seconds <= 86400:
            raise ValueError("retry_after_seconds must be between 0 and 86400")

        self.store = store
        self.transport = transport
        self.worker_id = worker_id
        self.batch_size = batch_size
        self.lease_seconds = lease_seconds
        self.retry_after_seconds = retry_after_seconds

    def run_once(self) -> PublishCycleResult:
        events = self.store.claim(
            worker_id=self.worker_id,
            limit=self.batch_size,
            lease_seconds=self.lease_seconds,
        )

        published = failed = stale_ack = stale_nack = 0

        for event in events:
            try:
                self.transport.publish(event)
            except Exception as exc:
                released = self.store.nack(
                    event_id=event.event_id,
                    claim_token=event.claim_token,
                    error=f"{type(exc).__name__}: {exc}",
                    retry_after_seconds=self.retry_after_seconds,
                )
                failed += 1
                if not released:
                    stale_nack += 1
                continue

            acknowledged = self.store.ack(
                event_id=event.event_id,
                claim_token=event.claim_token,
            )
            published += 1
            if not acknowledged:
                stale_ack += 1

        return PublishCycleResult(
            claimed=len(events),
            published=published,
            failed=failed,
            stale_ack=stale_ack,
            stale_nack=stale_nack,
        )


class InMemoryTransport:
    """Deterministic adapter used for executable contract tests."""

    def __init__(self):
        self.messages: list[ClaimedEvent] = []

    def publish(self, event: ClaimedEvent) -> PublishReceipt:
        self.messages.append(event)
        return PublishReceipt(transport_message_id=str(event.event_id))
