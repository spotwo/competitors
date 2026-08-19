from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal, Mapping
from uuid import UUID

from nats.aio.client import Client as NatsClient
from nats.js.api import AckPolicy
import psycopg

from consumer_runtime import ConsumedEvent, InboxTransaction

CANARY_EVENT_TYPE = "health_check.ping"
CANARY_AGGREGATE_TYPE = "EventPipelineCanary"
CANARY_SUBJECT_PREFIX = "event-pipeline-canary/"

HealthStatus = Literal["ok", "warning", "critical"]
AlertSeverity = Literal["warning", "critical"]


def _required_name(value: str, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} is required")
    if value != value.strip():
        raise ValueError(f"{field} cannot contain surrounding whitespace")
    return value


def _aware(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must include a timezone")
    return value


def _optional_aware(value: datetime | None, field: str) -> datetime | None:
    if value is None:
        return None
    return _aware(value, field)


def _seconds(later: datetime | None, earlier: datetime | None) -> float | None:
    if later is None or earlier is None:
        return None
    return (later - earlier).total_seconds()


def _positive_float(value: float, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
        raise ValueError(f"{field} must be positive")
    return float(value)


@dataclass(frozen=True)
class EventPipelineCanaryIdentity:
    canary_id: UUID
    event_id: UUID
    recorded_at: datetime

    def __post_init__(self) -> None:
        _aware(self.recorded_at, "recorded_at")


@dataclass(frozen=True)
class EventPipelineCanaryDbSnapshot:
    canary_id: UUID
    tenant_id: UUID
    event_id: UUID
    started_at: datetime
    published_at: datetime | None
    projected_at: datetime | None
    projected_consumer_name: str | None
    receipt_consumer_name: str | None
    inbox_first_seen_at: datetime | None
    transport_message_id: str | None
    stream_name: str | None
    durable_name: str | None
    stream_sequence: int | None
    consumer_sequence: int | None
    delivery_count: int | None
    broker_timestamp: datetime | None

    def __post_init__(self) -> None:
        _aware(self.started_at, "started_at")
        _optional_aware(self.published_at, "published_at")
        _optional_aware(self.projected_at, "projected_at")
        _optional_aware(self.inbox_first_seen_at, "inbox_first_seen_at")
        _optional_aware(self.broker_timestamp, "broker_timestamp")
        if self.published_at is not None and self.published_at < self.started_at:
            raise ValueError("published_at cannot precede canary start")
        if self.projected_at is not None and self.projected_at < self.started_at:
            raise ValueError("projected_at cannot precede canary start")
        if (
            self.projected_at is not None
            and self.inbox_first_seen_at is not None
            and self.projected_at < self.inbox_first_seen_at
        ):
            raise ValueError("projection cannot precede Inbox receipt")
        for field, value in (
            ("projected_consumer_name", self.projected_consumer_name),
            ("receipt_consumer_name", self.receipt_consumer_name),
            ("transport_message_id", self.transport_message_id),
            ("stream_name", self.stream_name),
            ("durable_name", self.durable_name),
        ):
            if value is not None:
                _required_name(value, field)
        for field, value in (
            ("stream_sequence", self.stream_sequence),
            ("consumer_sequence", self.consumer_sequence),
            ("delivery_count", self.delivery_count),
        ):
            if value is not None and (
                isinstance(value, bool) or not isinstance(value, int) or value < 1
            ):
                raise ValueError(f"{field} must be positive when present")
        if self.projected_at is not None and self.projected_consumer_name is None:
            raise ValueError("projected canary requires projected consumer identity")

    @property
    def outbox_publish_confirm_seconds(self) -> float | None:
        return _seconds(self.published_at, self.started_at)

    @property
    def start_to_broker_seconds(self) -> float | None:
        return _seconds(self.broker_timestamp, self.started_at)

    @property
    def broker_to_inbox_seconds(self) -> float | None:
        return _seconds(self.inbox_first_seen_at, self.broker_timestamp)

    @property
    def inbox_to_projection_seconds(self) -> float | None:
        return _seconds(self.projected_at, self.inbox_first_seen_at)

    @property
    def end_to_end_projection_seconds(self) -> float | None:
        return _seconds(self.projected_at, self.started_at)

    def to_dict(self) -> dict[str, Any]:
        return {
            "canary_id": str(self.canary_id),
            "tenant_id": str(self.tenant_id),
            "event_id": str(self.event_id),
            "timestamps": {
                "started_at": self.started_at.isoformat(),
                "published_at": self.published_at.isoformat() if self.published_at else None,
                "broker_timestamp": (
                    self.broker_timestamp.isoformat() if self.broker_timestamp else None
                ),
                "inbox_first_seen_at": (
                    self.inbox_first_seen_at.isoformat()
                    if self.inbox_first_seen_at
                    else None
                ),
                "projected_at": self.projected_at.isoformat() if self.projected_at else None,
            },
            "delivery": {
                "projected_consumer_name": self.projected_consumer_name,
                "receipt_consumer_name": self.receipt_consumer_name,
                "transport_message_id": self.transport_message_id,
                "stream": self.stream_name,
                "durable": self.durable_name,
                "stream_sequence": self.stream_sequence,
                "consumer_sequence": self.consumer_sequence,
                "delivery_count": self.delivery_count,
            },
            "latency_seconds": {
                "outbox_publish_confirm": self.outbox_publish_confirm_seconds,
                "start_to_broker": self.start_to_broker_seconds,
                "broker_to_inbox": self.broker_to_inbox_seconds,
                "inbox_to_projection": self.inbox_to_projection_seconds,
                "end_to_end_projection": self.end_to_end_projection_seconds,
            },
        }


@dataclass(frozen=True)
class CanaryAckState:
    observed_at: datetime
    available: bool
    configuration_valid: bool
    ack_floor_stream_sequence: int | None
    filter_subject: str | None
    last_ack_at: datetime | None
    error_code: str | None = None

    def __post_init__(self) -> None:
        _aware(self.observed_at, "observed_at")
        _optional_aware(self.last_ack_at, "last_ack_at")
        if not isinstance(self.available, bool) or not isinstance(
            self.configuration_valid, bool
        ):
            raise ValueError("ACK observer state flags must be boolean")
        if self.ack_floor_stream_sequence is not None and (
            isinstance(self.ack_floor_stream_sequence, bool)
            or not isinstance(self.ack_floor_stream_sequence, int)
            or self.ack_floor_stream_sequence < 0
        ):
            raise ValueError("ACK floor stream sequence must be nonnegative")
        if self.filter_subject is not None:
            _required_name(self.filter_subject, "filter_subject")
        if self.available and self.error_code is not None:
            raise ValueError("available ACK state cannot contain an error code")
        if not self.available and self.error_code is None:
            raise ValueError("unavailable ACK state requires an error code")

    def to_dict(self) -> dict[str, Any]:
        return {
            "available": self.available,
            "configuration_valid": self.configuration_valid,
            "ack_floor_stream_sequence": self.ack_floor_stream_sequence,
            "filter_subject": self.filter_subject,
            "last_ack_at": self.last_ack_at.isoformat() if self.last_ack_at else None,
            "error_code": self.error_code,
        }


class NatsJetStreamCanaryAckObserver:
    """Read only one dedicated canary durable consumer's ACK floor."""

    def __init__(
        self,
        *,
        server_url: str,
        stream_name: str,
        durable_name: str,
        expected_filter_subject: str,
        client_name: str = "spotwo-wms-event-pipeline-canary-observer",
        connect_timeout_seconds: float = 2.0,
    ):
        self.server_url = _required_name(server_url, "server_url")
        self.stream_name = _required_name(stream_name, "stream_name")
        self.durable_name = _required_name(durable_name, "durable_name")
        self.expected_filter_subject = _required_name(
            expected_filter_subject, "expected_filter_subject"
        )
        self.client_name = _required_name(client_name, "client_name")
        self.connect_timeout_seconds = _positive_float(
            connect_timeout_seconds, "connect_timeout_seconds"
        )
        self._runner = asyncio.Runner()
        self._client: NatsClient | None = None
        self._closed = False

    def snapshot(self, *, observed_at: datetime | None = None) -> CanaryAckState:
        if self._closed:
            raise RuntimeError("NatsJetStreamCanaryAckObserver is closed")
        observed_at = observed_at or datetime.now(timezone.utc)
        _aware(observed_at, "observed_at")
        try:
            info = self._runner.run(self._read_consumer_info())
        except Exception:
            return CanaryAckState(
                observed_at=observed_at,
                available=False,
                configuration_valid=False,
                ack_floor_stream_sequence=None,
                filter_subject=None,
                last_ack_at=None,
                error_code="nats_consumer_info_unavailable",
            )

        config = getattr(info, "config", None)
        ack_floor = getattr(info, "ack_floor", None)
        filter_subject = getattr(config, "filter_subject", None) if config else None
        filter_subjects = tuple(getattr(config, "filter_subjects", None) or ()) if config else ()
        if filter_subjects:
            filter_valid = len(filter_subjects) == 1 and filter_subjects[0] == self.expected_filter_subject
            rendered_filter = filter_subjects[0] if len(filter_subjects) == 1 else None
        else:
            filter_valid = filter_subject == self.expected_filter_subject
            rendered_filter = filter_subject

        configuration_valid = bool(
            config is not None
            and getattr(info, "stream_name", None) == self.stream_name
            and getattr(info, "name", None) == self.durable_name
            and getattr(config, "durable_name", None) == self.durable_name
            and getattr(config, "ack_policy", None) == AckPolicy.EXPLICIT
            and getattr(config, "deliver_subject", None) is None
            and filter_valid
        )

        ack_floor_stream_sequence = None
        last_ack_at = None
        if ack_floor is not None:
            raw_sequence = getattr(ack_floor, "stream_seq", None)
            if isinstance(raw_sequence, int) and not isinstance(raw_sequence, bool):
                ack_floor_stream_sequence = max(0, raw_sequence)
            raw_last_ack = getattr(ack_floor, "last_active", None)
            if isinstance(raw_last_ack, datetime):
                last_ack_at = _aware(raw_last_ack, "last ACK time")

        return CanaryAckState(
            observed_at=observed_at,
            available=True,
            configuration_valid=configuration_valid,
            ack_floor_stream_sequence=ack_floor_stream_sequence,
            filter_subject=rendered_filter,
            last_ack_at=last_ack_at,
        )

    async def _read_consumer_info(self):
        client = self._client
        if client is None or not client.is_connected:
            if client is not None and not client.is_closed:
                await client.close()
            client = NatsClient()
            await asyncio.wait_for(
                client.connect(
                    servers=[self.server_url],
                    name=self.client_name,
                    error_cb=self._capture_client_error,
                    allow_reconnect=False,
                    connect_timeout=self.connect_timeout_seconds,
                    reconnect_time_wait=0,
                    max_reconnect_attempts=1,
                ),
                timeout=self.connect_timeout_seconds,
            )
            self._client = client
        return await client.jetstream().consumer_info(self.stream_name, self.durable_name)

    def close(self) -> None:
        if self._closed:
            return
        try:
            client = self._client
            self._client = None
            if client is not None and not client.is_closed:
                self._runner.run(client.close())
        finally:
            self._runner.close()
            self._closed = True

    @staticmethod
    async def _capture_client_error(_error: Exception) -> None:
        return None

    def __enter__(self) -> "NatsJetStreamCanaryAckObserver":
        return self

    def __exit__(self, _exc_type: Any, _exc: Any, _traceback: Any) -> None:
        self.close()


class PostgresEventPipelineCanaryStore:
    def __init__(self, database_url: str):
        self.database_url = _required_name(database_url, "database_url")

    def _connect(self) -> psycopg.Connection:
        conn = psycopg.connect(self.database_url)
        conn.execute("SET statement_timeout = '8s'")
        conn.execute("SET lock_timeout = '5s'")
        return conn

    def start(self, *, tenant_id: UUID) -> EventPipelineCanaryIdentity:
        if not isinstance(tenant_id, UUID):
            raise ValueError("tenant_id must be a UUID")
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM kernel_lab.start_event_pipeline_canary(%s)",
                (tenant_id,),
            ).fetchone()
        if row is None:
            raise RuntimeError("canary start returned no identity")
        return EventPipelineCanaryIdentity(
            canary_id=row[0],
            event_id=row[1],
            recorded_at=row[2],
        )

    def snapshot(
        self,
        *,
        canary_id: UUID,
        consumer_name: str,
    ) -> EventPipelineCanaryDbSnapshot:
        if not isinstance(canary_id, UUID):
            raise ValueError("canary_id must be a UUID")
        consumer_name = _required_name(consumer_name, "consumer_name")

        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM kernel_lab.read_event_pipeline_canary(%s)",
                (canary_id,),
            ).fetchone()
            if row is None:
                raise LookupError("event pipeline canary does not exist")
            receipt = conn.execute(
                """
                SELECT consumer_name, first_seen_at, metadata
                FROM kernel_lab.domain_event_inbox
                WHERE consumer_name = %s AND event_id = %s
                """,
                (consumer_name, row[2]),
            ).fetchone()

        projected_at = row[5]
        stored_projected_consumer = row[6]
        stored_first_seen = row[7]
        stored_transport_message_id = row[8]
        stored_stream = row[9]
        stored_durable = row[10]
        stored_stream_sequence = row[11]
        stored_consumer_sequence = row[12]
        stored_delivery_count = row[13]
        stored_broker_timestamp = row[14]

        receipt_consumer_name = None
        receipt_first_seen = None
        receipt_metadata: Mapping[str, Any] = {}
        if receipt is not None:
            receipt_consumer_name = receipt[0]
            receipt_first_seen = receipt[1]
            receipt_metadata = receipt[2] or {}

        def receipt_text(key: str) -> str | None:
            value = receipt_metadata.get(key)
            return value if isinstance(value, str) and value else None

        def receipt_int(key: str) -> int | None:
            value = receipt_metadata.get(key)
            return value if isinstance(value, int) and not isinstance(value, bool) else None

        broker_timestamp = stored_broker_timestamp
        if broker_timestamp is None:
            raw = receipt_text("broker_timestamp")
            if raw is not None:
                broker_timestamp = datetime.fromisoformat(raw.replace("Z", "+00:00"))

        return EventPipelineCanaryDbSnapshot(
            canary_id=row[0],
            tenant_id=row[1],
            event_id=row[2],
            started_at=row[3],
            published_at=row[4],
            projected_at=projected_at,
            projected_consumer_name=stored_projected_consumer,
            receipt_consumer_name=receipt_consumer_name,
            inbox_first_seen_at=stored_first_seen or receipt_first_seen,
            transport_message_id=(
                stored_transport_message_id or receipt_text("transport_message_id")
            ),
            stream_name=stored_stream or receipt_text("stream"),
            durable_name=stored_durable or receipt_text("consumer"),
            stream_sequence=(
                stored_stream_sequence or receipt_int("stream_sequence")
            ),
            consumer_sequence=(
                stored_consumer_sequence or receipt_int("consumer_sequence")
            ),
            delivery_count=stored_delivery_count or receipt_int("delivery_count"),
            broker_timestamp=broker_timestamp,
        )


class EventPipelineCanaryHandler:
    """Synthetic projection handler that never mutates inventory domain state."""

    def __init__(self, *, consumer_name: str):
        self.consumer_name = _required_name(consumer_name, "consumer_name")

    def __call__(self, event: ConsumedEvent, transaction: InboxTransaction) -> None:
        if event.event_type != CANARY_EVENT_TYPE:
            raise ValueError("canary consumer received an unexpected event type")
        if event.aggregate_type != CANARY_AGGREGATE_TYPE:
            raise ValueError("canary consumer received an unexpected aggregate type")
        if event.aggregate_version != 1 or event.schema_version != 1:
            raise ValueError("canary event versions must equal 1")
        if not isinstance(event.data, Mapping):
            raise ValueError("canary event data must be an object")
        raw_canary_id = event.data.get("canary_id")
        if not isinstance(raw_canary_id, str):
            raise ValueError("canary event data requires canary_id")
        try:
            canary_id = UUID(raw_canary_id)
        except ValueError as exc:
            raise ValueError("canary_id must be a UUID") from exc
        if event.aggregate_id != str(canary_id):
            raise ValueError("canary aggregate identity mismatch")
        if event.subject != f"{CANARY_SUBJECT_PREFIX}{canary_id}":
            raise ValueError("canary subject identity mismatch")
        if event.data.get("synthetic") is not True:
            raise ValueError("canary event must declare synthetic=true")

        row = transaction.execute(
            "SELECT kernel_lab.apply_event_pipeline_canary(%s, %s, %s)",
            (canary_id, event.event_id, self.consumer_name),
        ).fetchone()
        if row is None or row[0] is None:
            raise RuntimeError("canary projection returned no timestamp")


@dataclass(frozen=True)
class EventPipelineCanaryRunSnapshot:
    observed_at: datetime
    expected_stream_name: str
    expected_durable_name: str
    expected_consumer_name: str
    expected_filter_subject: str
    database: EventPipelineCanaryDbSnapshot
    ack: CanaryAckState

    def __post_init__(self) -> None:
        _aware(self.observed_at, "observed_at")
        _required_name(self.expected_stream_name, "expected_stream_name")
        _required_name(self.expected_durable_name, "expected_durable_name")
        _required_name(self.expected_consumer_name, "expected_consumer_name")
        _required_name(self.expected_filter_subject, "expected_filter_subject")
        if self.ack.observed_at != self.observed_at:
            raise ValueError("ACK and canary snapshots must share observation time")

    @property
    def ack_confirmed(self) -> bool:
        sequence = self.database.stream_sequence
        floor = self.ack.ack_floor_stream_sequence
        return bool(
            sequence is not None
            and self.ack.available
            and self.ack.configuration_valid
            and floor is not None
            and floor >= sequence
        )

    @property
    def scope_valid(self) -> bool:
        db = self.database
        if db.stream_name is not None and db.stream_name != self.expected_stream_name:
            return False
        if db.durable_name is not None and db.durable_name != self.expected_durable_name:
            return False
        if (
            db.projected_consumer_name is not None
            and db.projected_consumer_name != self.expected_consumer_name
        ):
            return False
        if (
            db.receipt_consumer_name is not None
            and db.receipt_consumer_name != self.expected_consumer_name
        ):
            return False
        return True

    @property
    def complete(self) -> bool:
        db = self.database
        return bool(
            db.published_at is not None
            and db.projected_at is not None
            and self.scope_valid
            and self.ack_confirmed
        )

    @property
    def age_seconds(self) -> float:
        return max(0.0, (self.observed_at - self.database.started_at).total_seconds())

    def to_dict(self) -> dict[str, Any]:
        return {
            "observed_at": self.observed_at.isoformat(),
            "expected_scope": {
                "stream": self.expected_stream_name,
                "durable": self.expected_durable_name,
                "consumer_name": self.expected_consumer_name,
                "filter_subject": self.expected_filter_subject,
            },
            "complete": self.complete,
            "scope_valid": self.scope_valid,
            "ack_confirmed": self.ack_confirmed,
            "age_seconds": self.age_seconds,
            "database": self.database.to_dict(),
            "ack": self.ack.to_dict(),
        }


@dataclass(frozen=True)
class EventPipelineCanaryAlert:
    code: str
    severity: AlertSeverity
    observed_value: int | float | str
    threshold: int | float | str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "severity": self.severity,
            "observed_value": self.observed_value,
            "threshold": self.threshold,
        }


@dataclass(frozen=True)
class EventPipelineCanaryHealthReport:
    status: HealthStatus
    snapshot: EventPipelineCanaryRunSnapshot
    timed_out: bool
    alerts: tuple[EventPipelineCanaryAlert, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "timed_out": self.timed_out,
            "alerts": [alert.to_dict() for alert in self.alerts],
            "canary": self.snapshot.to_dict(),
        }


@dataclass(frozen=True)
class EventPipelineCanarySloPolicy:
    warning_publish_seconds: float = 2.0
    critical_publish_seconds: float = 5.0
    warning_delivery_seconds: float = 2.0
    critical_delivery_seconds: float = 5.0
    warning_projection_seconds: float = 1.0
    critical_projection_seconds: float = 3.0
    warning_end_to_end_seconds: float = 5.0
    critical_end_to_end_seconds: float = 15.0
    clock_skew_tolerance_seconds: float = 1.0

    def __post_init__(self) -> None:
        for name, warning, critical in (
            ("publish", self.warning_publish_seconds, self.critical_publish_seconds),
            ("delivery", self.warning_delivery_seconds, self.critical_delivery_seconds),
            (
                "projection",
                self.warning_projection_seconds,
                self.critical_projection_seconds,
            ),
            (
                "end_to_end",
                self.warning_end_to_end_seconds,
                self.critical_end_to_end_seconds,
            ),
        ):
            if not 0 < warning <= critical:
                raise ValueError(f"{name} SLO thresholds must be positive and ordered")
        if self.clock_skew_tolerance_seconds < 0:
            raise ValueError("clock skew tolerance must be nonnegative")

    def evaluate(
        self,
        snapshot: EventPipelineCanaryRunSnapshot,
        *,
        timed_out: bool,
    ) -> EventPipelineCanaryHealthReport:
        alerts: list[EventPipelineCanaryAlert] = []
        db = snapshot.database

        if snapshot.ack.available and not snapshot.ack.configuration_valid:
            alerts.append(
                EventPipelineCanaryAlert(
                    "canary_consumer_configuration_invalid",
                    "critical",
                    "invalid",
                )
            )

        if db.stream_name is not None and db.stream_name != snapshot.expected_stream_name:
            alerts.append(
                EventPipelineCanaryAlert(
                    "canary_stream_identity_mismatch",
                    "critical",
                    db.stream_name,
                    snapshot.expected_stream_name,
                )
            )
        if db.durable_name is not None and db.durable_name != snapshot.expected_durable_name:
            alerts.append(
                EventPipelineCanaryAlert(
                    "canary_durable_identity_mismatch",
                    "critical",
                    db.durable_name,
                    snapshot.expected_durable_name,
                )
            )
        if (
            db.projected_consumer_name is not None
            and db.projected_consumer_name != snapshot.expected_consumer_name
        ):
            alerts.append(
                EventPipelineCanaryAlert(
                    "canary_consumer_identity_mismatch",
                    "critical",
                    db.projected_consumer_name,
                    snapshot.expected_consumer_name,
                )
            )

        cross_clock_values = (
            db.start_to_broker_seconds,
            db.broker_to_inbox_seconds,
        )
        negative = [
            value
            for value in cross_clock_values
            if value is not None and value < -self.clock_skew_tolerance_seconds
        ]
        if negative:
            alerts.append(
                EventPipelineCanaryAlert(
                    "canary_clock_skew_detected",
                    "critical",
                    min(negative),
                    -self.clock_skew_tolerance_seconds,
                )
            )

        self._evaluate_latency(
            alerts,
            "canary_publish_latency",
            db.outbox_publish_confirm_seconds,
            self.warning_publish_seconds,
            self.critical_publish_seconds,
        )
        self._evaluate_latency(
            alerts,
            "canary_delivery_latency",
            db.broker_to_inbox_seconds,
            self.warning_delivery_seconds,
            self.critical_delivery_seconds,
        )
        self._evaluate_latency(
            alerts,
            "canary_projection_latency",
            db.inbox_to_projection_seconds,
            self.warning_projection_seconds,
            self.critical_projection_seconds,
        )
        self._evaluate_latency(
            alerts,
            "canary_end_to_end_latency",
            db.end_to_end_projection_seconds,
            self.warning_end_to_end_seconds,
            self.critical_end_to_end_seconds,
        )

        if timed_out and not snapshot.complete:
            if db.published_at is None:
                code = "canary_publish_timeout"
            elif db.inbox_first_seen_at is None:
                code = "canary_delivery_timeout"
            elif db.projected_at is None:
                code = "canary_projection_timeout"
            elif not snapshot.ack_confirmed:
                code = "canary_ack_timeout"
            else:
                code = "canary_incomplete_timeout"
            alerts.append(
                EventPipelineCanaryAlert(
                    code,
                    "critical",
                    snapshot.age_seconds,
                )
            )

        if not snapshot.ack.available and timed_out:
            alerts.append(
                EventPipelineCanaryAlert(
                    "canary_ack_observer_unavailable",
                    "critical",
                    snapshot.ack.error_code or "unavailable",
                )
            )

        if any(alert.severity == "critical" for alert in alerts):
            status: HealthStatus = "critical"
        elif alerts:
            status = "warning"
        else:
            status = "ok"
        return EventPipelineCanaryHealthReport(
            status=status,
            snapshot=snapshot,
            timed_out=timed_out,
            alerts=tuple(alerts),
        )

    @staticmethod
    def _evaluate_latency(
        alerts: list[EventPipelineCanaryAlert],
        code: str,
        value: float | None,
        warning: float,
        critical: float,
    ) -> None:
        if value is None or value < 0:
            return
        if value >= critical:
            alerts.append(EventPipelineCanaryAlert(code, "critical", value, critical))
        elif value >= warning:
            alerts.append(EventPipelineCanaryAlert(code, "warning", value, warning))


class EventPipelineCanaryProbe:
    """Start one synthetic event and wait for deployed publisher/consumer progress."""

    def __init__(
        self,
        *,
        database_url: str,
        nats_server_url: str,
        policy: EventPipelineCanarySloPolicy | None = None,
    ):
        self.store = PostgresEventPipelineCanaryStore(database_url)
        self.nats_server_url = _required_name(nats_server_url, "nats_server_url")
        self.policy = policy or EventPipelineCanarySloPolicy()

    def run(
        self,
        *,
        tenant_id: UUID,
        stream_name: str,
        durable_name: str,
        consumer_name: str,
        subject_prefix: str = "spotwo.wms.events",
        timeout_seconds: float = 30.0,
        poll_interval_seconds: float = 0.25,
    ) -> EventPipelineCanaryHealthReport:
        stream_name = _required_name(stream_name, "stream_name")
        durable_name = _required_name(durable_name, "durable_name")
        consumer_name = _required_name(consumer_name, "consumer_name")
        subject_prefix = _required_name(subject_prefix, "subject_prefix")
        timeout_seconds = _positive_float(timeout_seconds, "timeout_seconds")
        poll_interval_seconds = _positive_float(
            poll_interval_seconds, "poll_interval_seconds"
        )
        if poll_interval_seconds > timeout_seconds:
            raise ValueError("poll interval cannot exceed timeout")

        expected_filter_subject = f"{subject_prefix}.{CANARY_EVENT_TYPE}"
        identity = self.store.start(tenant_id=tenant_id)
        deadline = time.monotonic() + timeout_seconds

        with NatsJetStreamCanaryAckObserver(
            server_url=self.nats_server_url,
            stream_name=stream_name,
            durable_name=durable_name,
            expected_filter_subject=expected_filter_subject,
        ) as observer:
            while True:
                observed_at = datetime.now(timezone.utc)
                database = self.store.snapshot(
                    canary_id=identity.canary_id,
                    consumer_name=consumer_name,
                )
                ack = observer.snapshot(observed_at=observed_at)
                snapshot = EventPipelineCanaryRunSnapshot(
                    observed_at=observed_at,
                    expected_stream_name=stream_name,
                    expected_durable_name=durable_name,
                    expected_consumer_name=consumer_name,
                    expected_filter_subject=expected_filter_subject,
                    database=database,
                    ack=ack,
                )
                report = self.policy.evaluate(snapshot, timed_out=False)
                if snapshot.complete:
                    return report
                if ack.available and not ack.configuration_valid:
                    return report

                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return self.policy.evaluate(snapshot, timed_out=True)
                time.sleep(min(poll_interval_seconds, remaining))


def render_prometheus(report: EventPipelineCanaryHealthReport) -> str:
    snapshot = report.snapshot
    db = snapshot.database
    scope = _labels(
        {
            "stream": snapshot.expected_stream_name,
            "durable": snapshot.expected_durable_name,
            "consumer": snapshot.expected_consumer_name,
        }
    )
    lines = [
        "# HELP spotwo_wms_event_pipeline_canary_health_status Canary health: 0 ok, 1 warning, 2 critical.",
        "# TYPE spotwo_wms_event_pipeline_canary_health_status gauge",
        f"spotwo_wms_event_pipeline_canary_health_status{scope} {_status_number(report.status)}",
        "# HELP spotwo_wms_event_pipeline_canary_complete Whether the synthetic event completed publish, projection, and ACK confirmation.",
        "# TYPE spotwo_wms_event_pipeline_canary_complete gauge",
        f"spotwo_wms_event_pipeline_canary_complete{scope} {1 if snapshot.complete else 0}",
        "# HELP spotwo_wms_event_pipeline_canary_ack_confirmed Whether the dedicated durable ACK floor passed the canary stream sequence.",
        "# TYPE spotwo_wms_event_pipeline_canary_ack_confirmed gauge",
        f"spotwo_wms_event_pipeline_canary_ack_confirmed{scope} {1 if snapshot.ack_confirmed else 0}",
        "# HELP spotwo_wms_event_pipeline_canary_age_seconds Age of the current canary run.",
        "# TYPE spotwo_wms_event_pipeline_canary_age_seconds gauge",
        f"spotwo_wms_event_pipeline_canary_age_seconds{scope} {_number(snapshot.age_seconds)}",
        "# HELP spotwo_wms_event_pipeline_canary_latency_seconds Measured canary latency by bounded stage.",
        "# TYPE spotwo_wms_event_pipeline_canary_latency_seconds gauge",
    ]

    stages = {
        "outbox_publish_confirm": db.outbox_publish_confirm_seconds,
        "start_to_broker": db.start_to_broker_seconds,
        "broker_to_inbox": db.broker_to_inbox_seconds,
        "inbox_to_projection": db.inbox_to_projection_seconds,
        "end_to_end_projection": db.end_to_end_projection_seconds,
    }
    for stage, value in stages.items():
        if value is not None:
            labels = _labels(
                {
                    "stream": snapshot.expected_stream_name,
                    "durable": snapshot.expected_durable_name,
                    "consumer": snapshot.expected_consumer_name,
                    "stage": stage,
                }
            )
            lines.append(
                f"spotwo_wms_event_pipeline_canary_latency_seconds{labels} {_number(value)}"
            )

    if report.alerts:
        lines.extend(
            [
                "# HELP spotwo_wms_event_pipeline_canary_alert Active canary alert by bounded code and severity.",
                "# TYPE spotwo_wms_event_pipeline_canary_alert gauge",
            ]
        )
        for alert in report.alerts:
            labels = _labels(
                {
                    "stream": snapshot.expected_stream_name,
                    "durable": snapshot.expected_durable_name,
                    "consumer": snapshot.expected_consumer_name,
                    "code": alert.code,
                    "severity": alert.severity,
                }
            )
            lines.append(f"spotwo_wms_event_pipeline_canary_alert{labels} 1")

    return "\n".join(lines) + "\n"


def _labels(values: Mapping[str, str]) -> str:
    body = ",".join(
        f'{key}="{_escape_label(value)}"' for key, value in values.items()
    )
    return "{" + body + "}"


def _escape_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _number(value: int | float) -> str:
    if isinstance(value, int):
        return str(value)
    return format(value, ".15g")


def _status_number(status: HealthStatus) -> int:
    return {"ok": 0, "warning": 1, "critical": 2}[status]
