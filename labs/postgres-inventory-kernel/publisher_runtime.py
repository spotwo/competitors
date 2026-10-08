from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from typing import Any, Protocol
from uuid import UUID

import psycopg


@dataclass(frozen=True)
class ClaimedEvent:
    event_id: UUID
    claim_token: UUID
    attempt_count: int
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
    quarantined: int = 0
    stale_ack: int = 0
    stale_nack: int = 0
    stale_quarantine: int = 0


@dataclass(frozen=True)
class RetryDecision:
    retry_after_seconds: int | None = None
    quarantine_reason: str | None = None

    @property
    def quarantines(self) -> bool:
        return self.quarantine_reason is not None


@dataclass(frozen=True)
class RetryPolicy:
    base_delay_seconds: int = 5
    max_delay_seconds: int = 300
    jitter_ratio: float = 0.2
    max_attempts: int = 10

    def __post_init__(self) -> None:
        if not 1 <= self.base_delay_seconds <= 86400:
            raise ValueError("base_delay_seconds must be between 1 and 86400")
        if not self.base_delay_seconds <= self.max_delay_seconds <= 86400:
            raise ValueError(
                "max_delay_seconds must be between base_delay_seconds and 86400"
            )
        if not 0 <= self.jitter_ratio <= 1:
            raise ValueError("jitter_ratio must be between 0 and 1")
        if not 1 <= self.max_attempts <= 1000:
            raise ValueError("max_attempts must be between 1 and 1000")

    def decide(self, *, event_id: UUID, attempt_count: int) -> RetryDecision:
        if attempt_count < 1:
            raise ValueError("attempt_count must be positive")
        if attempt_count >= self.max_attempts:
            return RetryDecision(
                quarantine_reason=(
                    f"publication failed on attempt {attempt_count} of "
                    f"{self.max_attempts}"
                )
            )

        exponential_delay = min(
            self.max_delay_seconds,
            self.base_delay_seconds * (2 ** (attempt_count - 1)),
        )
        digest = sha256(f"{event_id}:{attempt_count}".encode()).digest()
        unit_interval = int.from_bytes(digest[:8], "big") / ((1 << 64) - 1)
        jitter_multiplier = 1 + self.jitter_ratio * ((2 * unit_interval) - 1)
        delay = round(exponential_delay * jitter_multiplier)
        delay = min(self.max_delay_seconds, max(1, delay))
        return RetryDecision(retry_after_seconds=delay)


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

    def quarantine(
        self,
        *,
        event_id: UUID,
        claim_token: UUID,
        error: str,
        reason: str,
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
                attempt_count=row[2],
                event_type=row[3],
                aggregate_type=row[4],
                aggregate_id=row[5],
                aggregate_version=row[6],
                envelope=row[7],
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

    def quarantine(
        self,
        *,
        event_id: UUID,
        claim_token: UUID,
        error: str,
        reason: str,
    ) -> bool:
        with self._connect() as conn:
            quarantined = conn.execute(
                "SELECT kernel_lab.quarantine_domain_event(%s, %s, %s, %s)",
                (event_id, claim_token, error, reason),
            ).fetchone()[0]
            conn.commit()
            return quarantined

    def replay_quarantined(
        self,
        *,
        event_id: UUID,
        operator_id: str,
        reason: str,
    ) -> bool:
        with self._connect() as conn:
            replayed = conn.execute(
                "SELECT kernel_lab.replay_quarantined_domain_event(%s, %s, %s)",
                (event_id, operator_id, reason),
            ).fetchone()[0]
            conn.commit()
            return replayed


class PostgresTenantOutboxStore(PostgresOutboxStore):
    """Tenant-owned Outbox lease path; no global-claim fallback.

    Requires a pre-reviewed, fenced delivery_route='tenant' transition for
    new V2 InventoryTransaction posting events. ACK/NACK/quarantine remain
    bound by the same claim token and short independent DB transactions.
    """

    def __init__(self, database_url: str, *, tenant_id: UUID):
        super().__init__(database_url)
        if not isinstance(tenant_id, UUID):
            raise ValueError("tenant_id must be a UUID")
        self.tenant_id = tenant_id

    def claim(self, *, worker_id: str, limit: int, lease_seconds: int) -> list[ClaimedEvent]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM kernel_lab.claim_tenant_domain_events(%s, %s, %s, %s)",
                (self.tenant_id, worker_id, limit, lease_seconds),
            ).fetchall()
            conn.commit()

        result = [
            ClaimedEvent(
                event_id=row[0],
                claim_token=row[1],
                attempt_count=row[2],
                event_type=row[3],
                aggregate_type=row[4],
                aggregate_id=row[5],
                aggregate_version=row[6],
                envelope=row[7],
            )
            for row in rows
        ]
        for event in result:
            if (
                event.event_type != "inventory.transaction.posted"
                or event.envelope.get("schema_version") != 2
                or event.envelope.get("tenant_id") != str(self.tenant_id)
            ):
                raise ValueError("scoped Outbox claim returned wrong tenant/event contract")
        return result


class PublisherRuntime:
    def __init__(
        self,
        *,
        store: OutboxStore,
        transport: Transport,
        worker_id: str,
        batch_size: int = 100,
        lease_seconds: int = 30,
        retry_policy: RetryPolicy | None = None,
    ):
        if not worker_id.strip():
            raise ValueError("worker_id is required")
        if not 1 <= batch_size <= 1000:
            raise ValueError("batch_size must be between 1 and 1000")
        if not 1 <= lease_seconds <= 3600:
            raise ValueError("lease_seconds must be between 1 and 3600")

        self.store = store
        self.transport = transport
        self.worker_id = worker_id
        self.batch_size = batch_size
        self.lease_seconds = lease_seconds
        self.retry_policy = retry_policy or RetryPolicy()

    def run_once(self) -> PublishCycleResult:
        events = self.store.claim(
            worker_id=self.worker_id,
            limit=self.batch_size,
            lease_seconds=self.lease_seconds,
        )

        published = failed = quarantined = 0
        stale_ack = stale_nack = stale_quarantine = 0

        for event in events:
            try:
                self.transport.publish(event)
            except Exception as exc:
                error = f"{type(exc).__name__}: {exc}"
                decision = self.retry_policy.decide(
                    event_id=event.event_id,
                    attempt_count=event.attempt_count,
                )
                failed += 1
                if decision.quarantines:
                    assert decision.quarantine_reason is not None
                    persisted = self.store.quarantine(
                        event_id=event.event_id,
                        claim_token=event.claim_token,
                        error=error,
                        reason=decision.quarantine_reason,
                    )
                    if persisted:
                        quarantined += 1
                    else:
                        stale_quarantine += 1
                else:
                    assert decision.retry_after_seconds is not None
                    released = self.store.nack(
                        event_id=event.event_id,
                        claim_token=event.claim_token,
                        error=error,
                        retry_after_seconds=decision.retry_after_seconds,
                    )
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
            quarantined=quarantined,
            stale_ack=stale_ack,
            stale_nack=stale_nack,
            stale_quarantine=stale_quarantine,
        )


class InMemoryTransport:
    """Deterministic adapter used for executable contract tests."""

    def __init__(self):
        self.messages: list[ClaimedEvent] = []

    def publish(self, event: ClaimedEvent) -> PublishReceipt:
        self.messages.append(event)
        return PublishReceipt(transport_message_id=str(event.event_id))
