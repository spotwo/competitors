from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from typing import Any, Mapping
from uuid import UUID

import psycopg
from psycopg.types.json import Jsonb

from consumer_runtime import (
    ConsumedEvent,
    ConsumerFailureDisposition,
    InboxHandler,
)


def _required_name(value: str, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} is required")
    if value != value.strip():
        raise ValueError(f"{field} cannot contain surrounding whitespace")
    return value


def _positive_integer(value: int, field: str, maximum: int) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value < 1
        or value > maximum
    ):
        raise ValueError(f"{field} must be between 1 and {maximum}")
    return value


def _error_text(error: Exception) -> str:
    return f"{type(error).__name__}: {error}"


@dataclass(frozen=True)
class ConsumerFailureClassification:
    code: str
    retryable: bool

    def __post_init__(self) -> None:
        _required_name(self.code, "failure code")


class ConsumerFailureClassifier:
    """Maps handler failures to bounded operational codes and retry semantics."""

    RETRYABLE_SQLSTATES = frozenset({"40001", "40P01", "55P03", "57014"})

    def classify(self, error: Exception) -> ConsumerFailureClassification:
        sqlstate = getattr(error, "sqlstate", None)
        message = str(error).lower()

        if sqlstate == "55000" and "projection rebuild fence is active" in message:
            return ConsumerFailureClassification(
                code="projection_rebuild_fence_active",
                retryable=True,
            )
        if sqlstate in self.RETRYABLE_SQLSTATES:
            return ConsumerFailureClassification(
                code={
                    "40001": "database_serialization_failure",
                    "40P01": "database_deadlock",
                    "55P03": "database_lock_unavailable",
                    "57014": "database_statement_cancelled",
                }[sqlstate],
                retryable=True,
            )
        if isinstance(sqlstate, str) and sqlstate.startswith("08"):
            return ConsumerFailureClassification(
                code="database_connection_failure",
                retryable=True,
            )
        if sqlstate == "23505":
            code = (
                "projection_version_conflict"
                if "projection" in message or "version" in message
                else "handler_identity_conflict"
            )
            return ConsumerFailureClassification(code=code, retryable=False)
        if sqlstate == "23514":
            return ConsumerFailureClassification(
                code="handler_constraint_violation",
                retryable=False,
            )
        if isinstance(error, ValueError):
            return ConsumerFailureClassification(
                code="invalid_event_contract",
                retryable=False,
            )
        if isinstance(error, (psycopg.IntegrityError, psycopg.DataError)):
            return ConsumerFailureClassification(
                code="handler_data_violation",
                retryable=False,
            )
        if isinstance(error, psycopg.OperationalError):
            return ConsumerFailureClassification(
                code="database_unavailable",
                retryable=True,
            )
        return ConsumerFailureClassification(code="handler_failure", retryable=True)


@dataclass(frozen=True)
class ConsumerRetryPolicy:
    base_delay_seconds: int = 5
    max_delay_seconds: int = 300
    jitter_ratio: float = 0.2
    max_attempts: int = 10

    def __post_init__(self) -> None:
        _positive_integer(self.base_delay_seconds, "base_delay_seconds", 86400)
        if not self.base_delay_seconds <= self.max_delay_seconds <= 86400:
            raise ValueError(
                "max_delay_seconds must be between base_delay_seconds and 86400"
            )
        if not 0 <= self.jitter_ratio <= 1:
            raise ValueError("jitter_ratio must be between 0 and 1")
        _positive_integer(self.max_attempts, "max_attempts", 1000)

    def delay_seconds(self, *, event_id: UUID, failure_count: int) -> int:
        _positive_integer(failure_count, "failure_count", 1000)
        if failure_count >= self.max_attempts:
            return 0

        exponential_delay = min(
            self.max_delay_seconds,
            self.base_delay_seconds * (2 ** (failure_count - 1)),
        )
        digest = sha256(f"consumer:{event_id}:{failure_count}".encode()).digest()
        unit_interval = int.from_bytes(digest[:8], "big") / ((1 << 64) - 1)
        jitter_multiplier = 1 + self.jitter_ratio * ((2 * unit_interval) - 1)
        delay = round(exponential_delay * jitter_multiplier)
        return min(self.max_delay_seconds, max(1, delay))


@dataclass(frozen=True)
class ClaimedConsumerFailure:
    consumer_name: str
    event_id: UUID
    claim_token: UUID
    attempt_count: int
    failure_code: str
    envelope: dict[str, Any]
    delivery_metadata: dict[str, Any]


@dataclass(frozen=True)
class ConsumerFailureProcessResult:
    applied: bool


@dataclass(frozen=True)
class ConsumerFailureRetryCycleResult:
    claimed: int = 0
    resolved: int = 0
    applied: int = 0
    duplicate: int = 0
    failed: int = 0
    deferred: int = 0
    quarantined: int = 0
    stale_claim: int = 0
    stale_failure: int = 0


@dataclass(frozen=True)
class ConsumerFailureReplayResult:
    outcome: str
    replay_id: UUID
    consumer_name: str
    event_id: UUID

    def __post_init__(self) -> None:
        if self.outcome not in {"replayed", "duplicate", "not_quarantined"}:
            raise ValueError("unexpected consumer failure replay outcome")

    def to_dict(self) -> dict[str, str]:
        return {
            "outcome": self.outcome,
            "replay_id": str(self.replay_id),
            "consumer_name": self.consumer_name,
            "event_id": str(self.event_id),
        }


class PostgresConsumerFailureStore:
    """Durable valid-event deferral, quarantine, replay, and retry primitives."""

    def __init__(self, database_url: str):
        self.database_url = database_url

    def _connect(self) -> psycopg.Connection:
        conn = psycopg.connect(self.database_url)
        conn.execute("SET statement_timeout = '30s'")
        conn.execute("SET lock_timeout = '5s'")
        return conn

    def find(
        self,
        *,
        consumer_name: str,
        event: ConsumedEvent,
    ) -> ConsumerFailureDisposition | None:
        _required_name(consumer_name, "consumer_name")
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT status, failure_code, attempt_count, envelope
                FROM kernel_lab.domain_event_consumer_failures
                WHERE consumer_name = %s AND event_id = %s
                """,
                (consumer_name, event.event_id),
            ).fetchone()

        if row is None:
            return None
        if row[3] != event.envelope:
            raise ValueError(
                "consumer failure event identity belongs to another envelope"
            )
        return ConsumerFailureDisposition(
            status=row[0],
            failure_code=row[1],
            attempt_count=row[2],
        )

    def capture(
        self,
        *,
        consumer_name: str,
        event: ConsumedEvent,
        delivery_metadata: Mapping[str, Any],
        classification: ConsumerFailureClassification,
        retry_after_seconds: int,
        max_attempts: int,
        error: Exception,
    ) -> ConsumerFailureDisposition:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT failure_status, persisted_failure_code,
                       failure_attempt_count, persisted_retryable, created
                FROM kernel_lab.capture_domain_event_consumer_failure(
                  %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                """,
                (
                    consumer_name,
                    event.event_id,
                    Jsonb(event.envelope),
                    Jsonb(dict(delivery_metadata)),
                    classification.code,
                    classification.retryable,
                    _error_text(error),
                    retry_after_seconds,
                    max_attempts,
                ),
            ).fetchone()

        if row is None:
            raise RuntimeError("consumer failure capture returned no result")
        return ConsumerFailureDisposition(
            status=row[0],
            failure_code=row[1],
            attempt_count=row[2],
        )

    def claim(
        self,
        *,
        consumer_name: str,
        worker_id: str,
        limit: int,
        lease_seconds: int,
    ) -> list[ClaimedConsumerFailure]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM kernel_lab.claim_domain_event_consumer_failures(%s, %s, %s, %s)
                """,
                (consumer_name, worker_id, limit, lease_seconds),
            ).fetchall()

        return [
            ClaimedConsumerFailure(
                consumer_name=row[0],
                event_id=row[1],
                claim_token=row[2],
                attempt_count=row[3],
                failure_code=row[4],
                envelope=row[5],
                delivery_metadata=row[6],
            )
            for row in rows
        ]

    def process_claimed(
        self,
        *,
        claimed: ClaimedConsumerFailure,
        handler: InboxHandler,
    ) -> ConsumerFailureProcessResult | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT envelope, delivery_metadata, attempt_count, failure_code
                FROM kernel_lab.domain_event_consumer_failures
                WHERE consumer_name = %s
                  AND event_id = %s
                  AND status = 'deferred'
                  AND claim_token = %s
                  AND claimed_until > clock_timestamp()
                FOR UPDATE
                """,
                (claimed.consumer_name, claimed.event_id, claimed.claim_token),
            ).fetchone()
            if row is None:
                return None

            event = ConsumedEvent.from_envelope(row[0])
            metadata = dict(row[1])
            metadata["consumer_failure_retry"] = {
                "attempt_count": row[2],
                "failure_code": row[3],
            }
            first_receipt = conn.execute(
                "SELECT kernel_lab.try_record_domain_event_receipt(%s, %s, %s)",
                (
                    claimed.consumer_name,
                    claimed.event_id,
                    Jsonb(metadata),
                ),
            ).fetchone()[0]
            if first_receipt:
                handler(event, conn)

            updated = conn.execute(
                """
                UPDATE kernel_lab.domain_event_consumer_failures
                SET status = 'resolved',
                    claimed_by = NULL,
                    claim_token = NULL,
                    claimed_until = NULL,
                    quarantined_at = NULL,
                    quarantine_reason = NULL,
                    resolved_at = clock_timestamp(),
                    resolution = %s
                WHERE consumer_name = %s
                  AND event_id = %s
                  AND claim_token = %s
                """,
                (
                    "applied" if first_receipt else "duplicate",
                    claimed.consumer_name,
                    claimed.event_id,
                    claimed.claim_token,
                ),
            ).rowcount
            if updated != 1:
                raise RuntimeError("consumer failure resolution lost its claim")

        return ConsumerFailureProcessResult(applied=first_receipt)

    def fail_claimed(
        self,
        *,
        claimed: ClaimedConsumerFailure,
        classification: ConsumerFailureClassification,
        retry_after_seconds: int,
        max_attempts: int,
        error: Exception,
    ) -> ConsumerFailureDisposition | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT failure_status, failure_attempt_count
                FROM kernel_lab.fail_domain_event_consumer_failure(
                  %s, %s, %s, %s, %s, %s, %s, %s
                )
                """,
                (
                    claimed.consumer_name,
                    claimed.event_id,
                    claimed.claim_token,
                    classification.code,
                    classification.retryable,
                    _error_text(error),
                    retry_after_seconds,
                    max_attempts,
                ),
            ).fetchone()

        if row is None:
            return None
        return ConsumerFailureDisposition(
            status=row[0],
            failure_code=classification.code,
            attempt_count=row[1],
        )

    def replay(
        self,
        *,
        replay_id: UUID,
        consumer_name: str,
        event_id: UUID,
        operator_id: str,
        reason: str,
    ) -> ConsumerFailureReplayResult:
        with self._connect() as conn:
            outcome = conn.execute(
                """
                SELECT kernel_lab.replay_domain_event_consumer_failure(
                  %s, %s, %s, %s, %s
                )
                """,
                (replay_id, consumer_name, event_id, operator_id, reason),
            ).fetchone()[0]
        return ConsumerFailureReplayResult(
            outcome=outcome,
            replay_id=replay_id,
            consumer_name=consumer_name,
            event_id=event_id,
        )


class DurableConsumerFailureLane:
    def __init__(
        self,
        *,
        store: PostgresConsumerFailureStore,
        classifier: ConsumerFailureClassifier | None = None,
        retry_policy: ConsumerRetryPolicy | None = None,
    ):
        self.store = store
        self.classifier = classifier or ConsumerFailureClassifier()
        self.retry_policy = retry_policy or ConsumerRetryPolicy()

    def find(
        self,
        *,
        consumer_name: str,
        event: ConsumedEvent,
    ) -> ConsumerFailureDisposition | None:
        return self.store.find(consumer_name=consumer_name, event=event)

    def capture(
        self,
        *,
        consumer_name: str,
        event: ConsumedEvent,
        delivery_metadata: Mapping[str, Any],
        error: Exception,
    ) -> ConsumerFailureDisposition:
        classification = self.classifier.classify(error)
        retry_after_seconds = (
            self.retry_policy.delay_seconds(event_id=event.event_id, failure_count=1)
            if classification.retryable
            else 0
        )
        return self.store.capture(
            consumer_name=consumer_name,
            event=event,
            delivery_metadata=delivery_metadata,
            classification=classification,
            retry_after_seconds=retry_after_seconds,
            max_attempts=self.retry_policy.max_attempts,
            error=error,
        )


class ConsumerFailureRetryRuntime:
    def __init__(
        self,
        *,
        store: PostgresConsumerFailureStore,
        consumer_name: str,
        handler: InboxHandler,
        worker_id: str,
        batch_size: int = 100,
        lease_seconds: int = 30,
        classifier: ConsumerFailureClassifier | None = None,
        retry_policy: ConsumerRetryPolicy | None = None,
    ):
        _required_name(consumer_name, "consumer_name")
        _required_name(worker_id, "worker_id")
        _positive_integer(batch_size, "batch_size", 1000)
        _positive_integer(lease_seconds, "lease_seconds", 3600)
        self.store = store
        self.consumer_name = consumer_name
        self.handler = handler
        self.worker_id = worker_id
        self.batch_size = batch_size
        self.lease_seconds = lease_seconds
        self.classifier = classifier or ConsumerFailureClassifier()
        self.retry_policy = retry_policy or ConsumerRetryPolicy()

    def run_once(self) -> ConsumerFailureRetryCycleResult:
        claims = self.store.claim(
            consumer_name=self.consumer_name,
            worker_id=self.worker_id,
            limit=self.batch_size,
            lease_seconds=self.lease_seconds,
        )
        resolved = applied = duplicate = failed = 0
        deferred = quarantined = stale_claim = stale_failure = 0

        for claimed in claims:
            try:
                result = self.store.process_claimed(
                    claimed=claimed,
                    handler=self.handler,
                )
            except Exception as exc:
                failed += 1
                classification = self.classifier.classify(exc)
                failure_count = claimed.attempt_count + 1
                retry_after_seconds = (
                    self.retry_policy.delay_seconds(
                        event_id=claimed.event_id,
                        failure_count=failure_count,
                    )
                    if classification.retryable
                    else 0
                )
                disposition = self.store.fail_claimed(
                    claimed=claimed,
                    classification=classification,
                    retry_after_seconds=retry_after_seconds,
                    max_attempts=self.retry_policy.max_attempts,
                    error=exc,
                )
                if disposition is None:
                    stale_failure += 1
                elif disposition.status == "deferred":
                    deferred += 1
                else:
                    quarantined += 1
                continue

            if result is None:
                stale_claim += 1
                continue
            resolved += 1
            applied += int(result.applied)
            duplicate += int(not result.applied)

        return ConsumerFailureRetryCycleResult(
            claimed=len(claims),
            resolved=resolved,
            applied=applied,
            duplicate=duplicate,
            failed=failed,
            deferred=deferred,
            quarantined=quarantined,
            stale_claim=stale_claim,
            stale_failure=stale_failure,
        )
