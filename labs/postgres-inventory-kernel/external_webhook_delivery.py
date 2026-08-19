from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Sequence
from uuid import UUID, uuid4

import psycopg
from psycopg.rows import dict_row

_RETRY_DELAYS_SECONDS = (5, 30, 120, 600, 1800, 3600, 7200, 21600)
_RETRYABLE_HTTP_STATUSES = frozenset({408, 425, 429})
_TERMINAL_STATES = frozenset({"delivered", "dead_letter"})


def _required_name(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise ValueError(f"{field} must be a normalized non-empty string")
    return value


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def retry_delay_seconds(attempt_no: int) -> int:
    if attempt_no <= 0:
        raise ValueError("attempt_no must be positive")
    index = min(attempt_no - 1, len(_RETRY_DELAYS_SECONDS) - 1)
    return _RETRY_DELAYS_SECONDS[index]


def classify_http_status(status: int | None) -> str:
    if status is None:
        return "retry"
    if 200 <= status <= 299:
        return "success"
    if status in _RETRYABLE_HTTP_STATUSES or 500 <= status <= 599:
        return "retry"
    if 400 <= status <= 499:
        return "terminal"
    raise ValueError(f"unsupported HTTP status: {status}")


class ExternalWebhookDeliveryError(RuntimeError):
    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


@dataclass(frozen=True)
class ExternalEventSubscription:
    subscription_id: UUID
    tenant_id: UUID
    endpoint_url: str
    state: str
    event_types: tuple[str, ...]
    signing_secret_ref: str
    signing_secret_version: int
    max_attempts: int


@dataclass(frozen=True)
class ExternalEventDelivery:
    delivery_id: UUID
    subscription_id: UUID
    event_id: str
    event_type: str
    generation: int
    envelope: Mapping[str, Any]
    state: str
    attempt_count: int
    next_attempt_at: datetime
    lease_owner: str | None
    lease_expires_at: datetime | None
    signing_secret_ref: str
    signing_secret_version: int
    replay_of_delivery_id: UUID | None
    delivered_at: datetime | None
    dead_lettered_at: datetime | None


@dataclass(frozen=True)
class ExternalWebhookDeliveryClaim:
    delivery: ExternalEventDelivery
    attempt_id: UUID
    attempt_no: int
    endpoint_url: str


class PostgresExternalWebhookDeliveryJournal:
    """Durable claim, retry, recovery, and replay state for public webhook delivery."""

    def __init__(
        self,
        database_url: str,
        *,
        worker_id: str,
        lease_seconds: float = 60.0,
    ):
        self.database_url = _required_name(database_url, "database_url")
        self.worker_id = _required_name(worker_id, "worker_id")
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        self.lease_seconds = float(lease_seconds)

    def _connect(self) -> psycopg.Connection:
        conn = psycopg.connect(self.database_url, row_factory=dict_row)
        conn.execute("SET statement_timeout = '8s'")
        conn.execute("SET lock_timeout = '5s'")
        return conn

    @staticmethod
    def _subscription(row: Mapping[str, Any]) -> ExternalEventSubscription:
        return ExternalEventSubscription(
            subscription_id=row["subscription_id"],
            tenant_id=row["tenant_id"],
            endpoint_url=row["endpoint_url"],
            state=row["state"],
            event_types=tuple(row["event_types"]),
            signing_secret_ref=row["signing_secret_ref"],
            signing_secret_version=int(row["signing_secret_version"]),
            max_attempts=int(row["max_attempts"]),
        )

    @staticmethod
    def _delivery(row: Mapping[str, Any]) -> ExternalEventDelivery:
        return ExternalEventDelivery(
            delivery_id=row["delivery_id"],
            subscription_id=row["subscription_id"],
            event_id=row["event_id"],
            event_type=row["event_type"],
            generation=int(row["generation"]),
            envelope=dict(row["envelope"]),
            state=row["state"],
            attempt_count=int(row["attempt_count"]),
            next_attempt_at=row["next_attempt_at"],
            lease_owner=row["lease_owner"],
            lease_expires_at=row["lease_expires_at"],
            signing_secret_ref=row["signing_secret_ref"],
            signing_secret_version=int(row["signing_secret_version"]),
            replay_of_delivery_id=row["replay_of_delivery_id"],
            delivered_at=row["delivered_at"],
            dead_lettered_at=row["dead_lettered_at"],
        )

    def create_subscription(
        self,
        *,
        tenant_id: UUID,
        endpoint_url: str,
        event_types: Sequence[str],
        signing_secret_ref: str,
        signing_secret_version: int = 1,
        max_attempts: int = 8,
        subscription_id: UUID | None = None,
    ) -> ExternalEventSubscription:
        if not endpoint_url.startswith("https://"):
            raise ValueError("endpoint_url must use https")
        normalized_event_types = tuple(_required_name(item, "event_type") for item in event_types)
        if not normalized_event_types or len(set(normalized_event_types)) != len(normalized_event_types):
            raise ValueError("event_types must be a non-empty unique sequence")
        signing_secret_ref = _required_name(signing_secret_ref, "signing_secret_ref")
        if signing_secret_version <= 0:
            raise ValueError("signing_secret_version must be positive")
        if max_attempts < 1 or max_attempts > 32:
            raise ValueError("max_attempts must be between 1 and 32")
        subscription_id = subscription_id or uuid4()
        with self._connect() as conn:
            row = conn.execute(
                """
                INSERT INTO kernel_lab.external_event_subscriptions (
                  subscription_id, tenant_id, endpoint_url, state, event_types,
                  signing_secret_ref, signing_secret_version, max_attempts
                ) VALUES (%s, %s, %s, 'active', %s, %s, %s, %s)
                RETURNING *
                """,
                (
                    subscription_id,
                    tenant_id,
                    endpoint_url,
                    list(normalized_event_types),
                    signing_secret_ref,
                    signing_secret_version,
                    max_attempts,
                ),
            ).fetchone()
            return self._subscription(row)

    def set_subscription_state(self, subscription_id: UUID, state: str) -> ExternalEventSubscription:
        if state not in {"active", "paused", "revoked"}:
            raise ValueError("unsupported subscription state")
        with self._connect() as conn:
            row = conn.execute(
                """
                UPDATE kernel_lab.external_event_subscriptions
                SET state = %s,
                    updated_at = clock_timestamp()
                WHERE subscription_id = %s
                RETURNING *
                """,
                (state, subscription_id),
            ).fetchone()
            if row is None:
                raise ExternalWebhookDeliveryError("external_webhook_subscription_not_found")
            return self._subscription(row)

    def rotate_signing_secret(
        self,
        subscription_id: UUID,
        *,
        signing_secret_ref: str,
        signing_secret_version: int,
    ) -> ExternalEventSubscription:
        signing_secret_ref = _required_name(signing_secret_ref, "signing_secret_ref")
        if signing_secret_version <= 0:
            raise ValueError("signing_secret_version must be positive")
        with self._connect() as conn:
            row = conn.execute(
                """
                UPDATE kernel_lab.external_event_subscriptions
                SET signing_secret_ref = %s,
                    signing_secret_version = %s,
                    updated_at = clock_timestamp()
                WHERE subscription_id = %s
                  AND state <> 'revoked'
                  AND signing_secret_version < %s
                RETURNING *
                """,
                (signing_secret_ref, signing_secret_version, subscription_id, signing_secret_version),
            ).fetchone()
            if row is None:
                raise ExternalWebhookDeliveryError("external_webhook_secret_rotation_rejected")
            return self._subscription(row)

    def enqueue(
        self,
        *,
        subscription_id: UUID,
        envelope: Mapping[str, Any],
        now: datetime | None = None,
    ) -> ExternalEventDelivery:
        event_id = _required_name(envelope.get("id"), "envelope.id")
        event_type = _required_name(envelope.get("type"), "envelope.type")
        if envelope.get("specversion") != "1.0":
            raise ValueError("envelope.specversion must be 1.0")
        now = now or _utc_now()
        encoded = json.dumps(dict(envelope), sort_keys=True, separators=(",", ":"))
        delivery_id = uuid4()
        with self._connect() as conn:
            subscription = conn.execute(
                """
                SELECT *
                FROM kernel_lab.external_event_subscriptions
                WHERE subscription_id = %s
                FOR UPDATE
                """,
                (subscription_id,),
            ).fetchone()
            if subscription is None:
                raise ExternalWebhookDeliveryError("external_webhook_subscription_not_found")
            if subscription["state"] != "active":
                raise ExternalWebhookDeliveryError("external_webhook_subscription_not_active")
            if event_type not in subscription["event_types"] and "*" not in subscription["event_types"]:
                raise ExternalWebhookDeliveryError("external_webhook_event_type_not_subscribed")

            existing = conn.execute(
                """
                SELECT *
                FROM kernel_lab.external_event_deliveries
                WHERE subscription_id = %s
                  AND event_id = %s
                  AND generation = 0
                FOR UPDATE
                """,
                (subscription_id, event_id),
            ).fetchone()
            if existing is not None:
                if dict(existing["envelope"]) != dict(envelope) or existing["event_type"] != event_type:
                    raise ExternalWebhookDeliveryError("external_webhook_event_identity_conflict")
                return self._delivery(existing)

            row = conn.execute(
                """
                INSERT INTO kernel_lab.external_event_deliveries (
                  delivery_id, subscription_id, event_id, event_type, generation,
                  envelope, state, next_attempt_at, signing_secret_ref, signing_secret_version
                ) VALUES (%s, %s, %s, %s, 0, %s::jsonb, 'pending', %s, %s, %s)
                RETURNING *
                """,
                (
                    delivery_id,
                    subscription_id,
                    event_id,
                    event_type,
                    encoded,
                    now,
                    subscription["signing_secret_ref"],
                    subscription["signing_secret_version"],
                ),
            ).fetchone()
            return self._delivery(row)

    def claim_next(self, *, now: datetime | None = None) -> ExternalWebhookDeliveryClaim | None:
        now = now or _utc_now()
        attempt_id = uuid4()
        with self._connect() as conn:
            candidate = conn.execute(
                """
                SELECT d.delivery_id, d.state, d.lease_expires_at
                FROM kernel_lab.external_event_deliveries AS d
                JOIN kernel_lab.external_event_subscriptions AS s
                  ON s.subscription_id = d.subscription_id
                WHERE s.state = 'active'
                  AND (
                    (d.state = 'pending' AND d.next_attempt_at <= %s)
                    OR
                    (d.state = 'in_flight' AND d.lease_expires_at <= %s)
                  )
                ORDER BY d.next_attempt_at, d.created_at, d.delivery_id
                FOR UPDATE OF d SKIP LOCKED
                LIMIT 1
                """,
                (now, now),
            ).fetchone()
            if candidate is None:
                return None

            if candidate["state"] == "in_flight":
                conn.execute(
                    """
                    UPDATE kernel_lab.external_event_delivery_attempts
                    SET completed_at = %s,
                        outcome = 'lease_expired',
                        error_code = 'lease_expired'
                    WHERE delivery_id = %s
                      AND completed_at IS NULL
                    """,
                    (now, candidate["delivery_id"]),
                )

            row = conn.execute(
                """
                UPDATE kernel_lab.external_event_deliveries AS d
                SET state = 'in_flight',
                    attempt_count = attempt_count + 1,
                    lease_owner = %s,
                    lease_expires_at = %s + (%s * interval '1 second'),
                    updated_at = %s
                FROM kernel_lab.external_event_subscriptions AS s
                WHERE d.delivery_id = %s
                  AND s.subscription_id = d.subscription_id
                RETURNING d.*, s.endpoint_url, s.max_attempts
                """,
                (self.worker_id, now, self.lease_seconds, now, candidate["delivery_id"]),
            ).fetchone()
            conn.execute(
                """
                INSERT INTO kernel_lab.external_event_delivery_attempts (
                  attempt_id, delivery_id, attempt_no, worker_id, started_at, signing_secret_version
                ) VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    attempt_id,
                    row["delivery_id"],
                    row["attempt_count"],
                    self.worker_id,
                    now,
                    row["signing_secret_version"],
                ),
            )
            return ExternalWebhookDeliveryClaim(
                delivery=self._delivery(row),
                attempt_id=attempt_id,
                attempt_no=int(row["attempt_count"]),
                endpoint_url=row["endpoint_url"],
            )

    def record_result(
        self,
        claim: ExternalWebhookDeliveryClaim,
        *,
        http_status: int | None = None,
        error_code: str | None = None,
        now: datetime | None = None,
    ) -> ExternalEventDelivery:
        now = now or _utc_now()
        classification = classify_http_status(http_status)
        if error_code is not None:
            error_code = _required_name(error_code, "error_code")
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT d.*, s.max_attempts
                FROM kernel_lab.external_event_deliveries AS d
                JOIN kernel_lab.external_event_subscriptions AS s
                  ON s.subscription_id = d.subscription_id
                WHERE d.delivery_id = %s
                FOR UPDATE OF d
                """,
                (claim.delivery.delivery_id,),
            ).fetchone()
            if row is None:
                raise ExternalWebhookDeliveryError("external_webhook_delivery_not_found")
            if (
                row["state"] != "in_flight"
                or row["lease_owner"] != self.worker_id
                or row["lease_expires_at"] <= now
                or int(row["attempt_count"]) != claim.attempt_no
            ):
                raise ExternalWebhookDeliveryError("external_webhook_delivery_lease_lost")

            attempt = conn.execute(
                """
                SELECT attempt_id
                FROM kernel_lab.external_event_delivery_attempts
                WHERE attempt_id = %s
                  AND delivery_id = %s
                  AND worker_id = %s
                  AND completed_at IS NULL
                FOR UPDATE
                """,
                (claim.attempt_id, row["delivery_id"], self.worker_id),
            ).fetchone()
            if attempt is None:
                raise ExternalWebhookDeliveryError("external_webhook_delivery_attempt_lost")

            attempt_count = int(row["attempt_count"])
            max_attempts = int(row["max_attempts"])
            next_attempt_at: datetime | None = None
            terminal_error = error_code

            if classification == "success":
                state = "delivered"
                outcome = "success"
            elif classification == "terminal" or attempt_count >= max_attempts:
                state = "dead_letter"
                outcome = "dead_letter"
                if classification == "retry" and attempt_count >= max_attempts:
                    terminal_error = "max_attempts_exhausted"
            else:
                state = "pending"
                outcome = "retry"
                next_attempt_at = now + timedelta(seconds=retry_delay_seconds(attempt_count))

            updated = conn.execute(
                """
                UPDATE kernel_lab.external_event_deliveries
                SET state = %s,
                    next_attempt_at = COALESCE(%s, next_attempt_at),
                    lease_owner = NULL,
                    lease_expires_at = NULL,
                    last_http_status = %s,
                    last_error_code = %s,
                    delivered_at = CASE WHEN %s = 'delivered' THEN %s ELSE NULL END,
                    dead_lettered_at = CASE WHEN %s = 'dead_letter' THEN %s ELSE NULL END,
                    updated_at = %s
                WHERE delivery_id = %s
                RETURNING *
                """,
                (
                    state,
                    next_attempt_at,
                    http_status,
                    terminal_error,
                    state,
                    now,
                    state,
                    now,
                    now,
                    row["delivery_id"],
                ),
            ).fetchone()
            conn.execute(
                """
                UPDATE kernel_lab.external_event_delivery_attempts
                SET completed_at = %s,
                    outcome = %s,
                    http_status = %s,
                    error_code = %s,
                    next_attempt_at = %s
                WHERE attempt_id = %s
                """,
                (now, outcome, http_status, terminal_error, next_attempt_at, claim.attempt_id),
            )
            return self._delivery(updated)

    def request_replay(
        self,
        delivery_id: UUID,
        *,
        reason: str,
        requested_by: str,
        now: datetime | None = None,
    ) -> ExternalEventDelivery:
        reason = _required_name(reason, "reason")
        requested_by = _required_name(requested_by, "requested_by")
        now = now or _utc_now()
        with self._connect() as conn:
            original = conn.execute(
                """
                SELECT d.*
                FROM kernel_lab.external_event_deliveries AS d
                WHERE d.delivery_id = %s
                FOR UPDATE
                """,
                (delivery_id,),
            ).fetchone()
            if original is None:
                raise ExternalWebhookDeliveryError("external_webhook_delivery_not_found")
            if original["state"] not in _TERMINAL_STATES:
                raise ExternalWebhookDeliveryError("external_webhook_replay_requires_terminal_delivery")

            subscription = conn.execute(
                """
                SELECT *
                FROM kernel_lab.external_event_subscriptions
                WHERE subscription_id = %s
                FOR UPDATE
                """,
                (original["subscription_id"],),
            ).fetchone()
            if subscription["state"] != "active":
                raise ExternalWebhookDeliveryError("external_webhook_subscription_not_active")

            generation = conn.execute(
                """
                SELECT COALESCE(max(generation), -1) + 1 AS generation
                FROM kernel_lab.external_event_deliveries
                WHERE subscription_id = %s
                  AND event_id = %s
                """,
                (original["subscription_id"], original["event_id"]),
            ).fetchone()["generation"]
            row = conn.execute(
                """
                INSERT INTO kernel_lab.external_event_deliveries (
                  delivery_id, subscription_id, event_id, event_type, generation, envelope,
                  state, next_attempt_at, signing_secret_ref, signing_secret_version,
                  replay_of_delivery_id, replay_reason, replay_requested_by
                ) VALUES (
                  %s, %s, %s, %s, %s, %s::jsonb,
                  'pending', %s, %s, %s, %s, %s, %s
                )
                RETURNING *
                """,
                (
                    uuid4(),
                    original["subscription_id"],
                    original["event_id"],
                    original["event_type"],
                    generation,
                    json.dumps(dict(original["envelope"]), sort_keys=True, separators=(",", ":")),
                    now,
                    subscription["signing_secret_ref"],
                    subscription["signing_secret_version"],
                    original["delivery_id"],
                    reason,
                    requested_by,
                ),
            ).fetchone()
            return self._delivery(row)

    def get_delivery(self, delivery_id: UUID) -> ExternalEventDelivery:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM kernel_lab.external_event_deliveries WHERE delivery_id = %s",
                (delivery_id,),
            ).fetchone()
            if row is None:
                raise ExternalWebhookDeliveryError("external_webhook_delivery_not_found")
            return self._delivery(row)

    def attempt_history(self, delivery_id: UUID) -> tuple[Mapping[str, Any], ...]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM kernel_lab.external_event_delivery_attempts
                WHERE delivery_id = %s
                ORDER BY attempt_no
                """,
                (delivery_id,),
            ).fetchall()
            return tuple(dict(row) for row in rows)
