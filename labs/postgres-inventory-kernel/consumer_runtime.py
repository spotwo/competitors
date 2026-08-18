from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping, Protocol, Sequence
from uuid import UUID

import psycopg
from psycopg.types.json import Jsonb


def _required_text(envelope: Mapping[str, Any], field: str) -> str:
    value = envelope.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"envelope {field} must be a nonblank string")
    return value


def _stable_name(value: str, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} is required")
    if value != value.strip():
        raise ValueError(f"{field} cannot contain surrounding whitespace")
    return value


def _positive_integer(envelope: Mapping[str, Any], field: str) -> int:
    value = envelope.get(field)
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"envelope {field} must be a positive integer")
    return value


def _aware_timestamp(envelope: Mapping[str, Any], field: str) -> datetime:
    value = _required_text(envelope, field)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"envelope {field} must be an ISO 8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"envelope {field} must include a timezone")
    return parsed


@dataclass(frozen=True)
class ConsumedEvent:
    event_id: UUID
    event_type: str
    source: str
    subject: str
    occurred_at: datetime
    recorded_at: datetime
    aggregate_type: str
    aggregate_id: str
    aggregate_version: int
    schema_version: int
    data: Any
    envelope: dict[str, Any]

    @classmethod
    def from_envelope(cls, envelope: Mapping[str, Any]) -> ConsumedEvent:
        if not isinstance(envelope, Mapping):
            raise ValueError("event envelope must be a JSON object")
        if "data" not in envelope:
            raise ValueError("envelope data is required")

        raw_event_id = _required_text(envelope, "event_id")
        try:
            event_id = UUID(raw_event_id)
        except ValueError as exc:
            raise ValueError("envelope event_id must be a UUID") from exc

        return cls(
            event_id=event_id,
            event_type=_required_text(envelope, "type"),
            source=_required_text(envelope, "source"),
            subject=_required_text(envelope, "subject"),
            occurred_at=_aware_timestamp(envelope, "occurred_at"),
            recorded_at=_aware_timestamp(envelope, "recorded_at"),
            aggregate_type=_required_text(envelope, "aggregate_type"),
            aggregate_id=_required_text(envelope, "aggregate_id"),
            aggregate_version=_positive_integer(envelope, "aggregate_version"),
            schema_version=_positive_integer(envelope, "schema_version"),
            data=envelope["data"],
            envelope=dict(envelope),
        )


@dataclass(frozen=True)
class InboxDeliveryMetadata:
    transport: str
    transport_message_id: str
    subject: str
    delivery_count: int
    stream: str | None = None
    consumer: str | None = None
    stream_sequence: int | None = None
    consumer_sequence: int | None = None
    pending_count: int | None = None
    broker_timestamp: datetime | None = None

    def __post_init__(self) -> None:
        for field, value in (
            ("transport", self.transport),
            ("transport_message_id", self.transport_message_id),
            ("subject", self.subject),
        ):
            _stable_name(value, field)
        if (
            isinstance(self.delivery_count, bool)
            or not isinstance(self.delivery_count, int)
            or self.delivery_count < 1
        ):
            raise ValueError("delivery_count must be positive")
        for field, value in (
            ("stream_sequence", self.stream_sequence),
            ("consumer_sequence", self.consumer_sequence),
            ("pending_count", self.pending_count),
        ):
            if value is not None and (
                isinstance(value, bool) or not isinstance(value, int) or value < 0
            ):
                raise ValueError(f"{field} must be nonnegative")
        if self.broker_timestamp is not None:
            if not isinstance(self.broker_timestamp, datetime):
                raise ValueError("broker_timestamp must be a datetime")
            if (
                self.broker_timestamp.tzinfo is None
                or self.broker_timestamp.utcoffset() is None
            ):
                raise ValueError("broker_timestamp must include a timezone")

    def to_dict(self) -> dict[str, Any]:
        values: dict[str, Any] = {
            "transport": self.transport,
            "transport_message_id": self.transport_message_id,
            "subject": self.subject,
            "delivery_count": self.delivery_count,
            "stream": self.stream,
            "consumer": self.consumer,
            "stream_sequence": self.stream_sequence,
            "consumer_sequence": self.consumer_sequence,
            "pending_count": self.pending_count,
            "broker_timestamp": (
                self.broker_timestamp.isoformat()
                if self.broker_timestamp is not None
                else None
            ),
        }
        return {key: value for key, value in values.items() if value is not None}


@dataclass(frozen=True)
class MalformedDeliveryEvidence:
    """Bounded evidence captured before any event_id can be trusted."""

    failure_code: str
    error: str
    payload_sha256: str
    payload_size: int
    payload_preview: bytes
    payload_truncated: bool
    headers: Mapping[str, Any]

    def __post_init__(self) -> None:
        _stable_name(self.failure_code, "failure_code")
        if not self.failure_code.replace("_", "a").isalnum() or not self.failure_code[0].isalpha():
            raise ValueError("failure_code must be a stable lowercase identifier")
        if self.failure_code != self.failure_code.lower():
            raise ValueError("failure_code must be a stable lowercase identifier")
        if not isinstance(self.error, str) or not self.error.strip():
            raise ValueError("error is required")
        if (
            not isinstance(self.payload_sha256, str)
            or len(self.payload_sha256) != 64
            or any(character not in "0123456789abcdef" for character in self.payload_sha256)
        ):
            raise ValueError("payload_sha256 must be lowercase SHA-256 hex")
        if (
            isinstance(self.payload_size, bool)
            or not isinstance(self.payload_size, int)
            or self.payload_size < 0
        ):
            raise ValueError("payload_size must be nonnegative")
        if not isinstance(self.payload_preview, bytes):
            raise ValueError("payload_preview must be bytes")
        if len(self.payload_preview) > self.payload_size:
            raise ValueError("payload_preview cannot exceed payload_size")
        if self.payload_truncated != (len(self.payload_preview) < self.payload_size):
            raise ValueError("payload_truncated must match the bounded preview")
        if not isinstance(self.headers, Mapping):
            raise ValueError("headers must be a mapping")


@dataclass(frozen=True)
class MalformedDeliveryDisposition:
    failure_code: str
    observation_count: int
    created: bool

    def __post_init__(self) -> None:
        _stable_name(self.failure_code, "failure_code")
        if (
            isinstance(self.observation_count, bool)
            or not isinstance(self.observation_count, int)
            or self.observation_count < 1
        ):
            raise ValueError("observation_count must be positive")


class InboxTransaction(Protocol):
    def execute(
        self,
        query: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> Any:
        ...


class InboxHandler(Protocol):
    def __call__(
        self,
        event: ConsumedEvent,
        transaction: InboxTransaction,
    ) -> None:
        """Apply only local transactional effects or append a local Outbox record."""


class InboxDelivery(Protocol):
    envelope: Mapping[str, Any] | None
    metadata: InboxDeliveryMetadata
    malformed: MalformedDeliveryEvidence | None

    def ack(self) -> None:
        """Confirm the transport delivery or raise when confirmation is uncertain."""


class InboxSource(Protocol):
    def fetch_one(self) -> InboxDelivery | None:
        ...


class InboxStore(Protocol):
    def process_once(
        self,
        *,
        consumer_name: str,
        event: ConsumedEvent,
        delivery_metadata: Mapping[str, Any],
        handler: InboxHandler,
    ) -> bool:
        """Commit receipt and handler together, returning true only for first receipt."""


@dataclass(frozen=True)
class ConsumerFailureDisposition:
    status: str
    failure_code: str
    attempt_count: int

    def __post_init__(self) -> None:
        if self.status not in {"deferred", "quarantined", "resolved"}:
            raise ValueError("unexpected consumer failure status")
        _stable_name(self.failure_code, "failure_code")
        if (
            isinstance(self.attempt_count, bool)
            or not isinstance(self.attempt_count, int)
            or self.attempt_count < 0
        ):
            raise ValueError("attempt_count must be nonnegative")


class InboxFailureLane(Protocol):
    def find(
        self,
        *,
        consumer_name: str,
        event: ConsumedEvent,
    ) -> ConsumerFailureDisposition | None:
        """Return a durable handoff already bound to this exact event, if any."""

    def capture(
        self,
        *,
        consumer_name: str,
        event: ConsumedEvent,
        delivery_metadata: Mapping[str, Any],
        error: Exception,
    ) -> ConsumerFailureDisposition:
        """Durably hand off a failed valid event before transport acknowledgement."""


class InboxMalformedDeliveryLane(Protocol):
    def capture(
        self,
        *,
        consumer_name: str,
        evidence: MalformedDeliveryEvidence,
        delivery_metadata: Mapping[str, Any],
    ) -> MalformedDeliveryDisposition:
        """Persist transport poison evidence before acknowledging the broker delivery."""


@dataclass(frozen=True)
class ConsumeCycleResult:
    received: int = 0
    applied: int = 0
    duplicate: int = 0
    deferred: int = 0
    quarantined: int = 0
    malformed: int = 0
    acknowledged: int = 0
    event_id: UUID | None = None
    transport_message_id: str | None = None
    delivery_count: int | None = None
    failure_code: str | None = None


class PostgresInboxStore:
    """Runs Inbox receipt insertion and handler SQL in one database transaction."""

    def __init__(self, database_url: str):
        self.database_url = database_url

    def _connect(self) -> psycopg.Connection:
        conn = psycopg.connect(self.database_url)
        conn.execute("SET statement_timeout = '30s'")
        conn.execute("SET lock_timeout = '5s'")
        return conn

    def process_once(
        self,
        *,
        consumer_name: str,
        event: ConsumedEvent,
        delivery_metadata: Mapping[str, Any],
        handler: InboxHandler,
    ) -> bool:
        _stable_name(consumer_name, "consumer_name")

        with self._connect() as conn:
            first_receipt = conn.execute(
                "SELECT kernel_lab.try_record_domain_event_receipt(%s, %s, %s)",
                (
                    consumer_name,
                    event.event_id,
                    Jsonb(dict(delivery_metadata)),
                ),
            ).fetchone()[0]
            if first_receipt:
                handler(event, conn)

        return first_receipt


class InboxConsumerRuntime:
    def __init__(
        self,
        *,
        source: InboxSource,
        store: InboxStore,
        consumer_name: str,
        handler: InboxHandler,
        failure_lane: InboxFailureLane | None = None,
        malformed_lane: InboxMalformedDeliveryLane | None = None,
    ):
        _stable_name(consumer_name, "consumer_name")
        self.source = source
        self.store = store
        self.consumer_name = consumer_name
        self.handler = handler
        self.failure_lane = failure_lane
        self.malformed_lane = malformed_lane

    @staticmethod
    def _failure_result(
        *,
        event: ConsumedEvent,
        delivery: InboxDelivery,
        disposition: ConsumerFailureDisposition,
    ) -> ConsumeCycleResult:
        return ConsumeCycleResult(
            received=1,
            duplicate=int(disposition.status == "resolved"),
            deferred=int(disposition.status == "deferred"),
            quarantined=int(disposition.status == "quarantined"),
            acknowledged=1,
            event_id=event.event_id,
            transport_message_id=delivery.metadata.transport_message_id,
            delivery_count=delivery.metadata.delivery_count,
            failure_code=disposition.failure_code,
        )

    @staticmethod
    def _malformed_result(
        *,
        delivery: InboxDelivery,
        disposition: MalformedDeliveryDisposition,
    ) -> ConsumeCycleResult:
        return ConsumeCycleResult(
            received=1,
            quarantined=1,
            malformed=1,
            acknowledged=1,
            transport_message_id=delivery.metadata.transport_message_id,
            delivery_count=delivery.metadata.delivery_count,
            failure_code=disposition.failure_code,
        )

    def run_once(self) -> ConsumeCycleResult:
        delivery = self.source.fetch_one()
        if delivery is None:
            return ConsumeCycleResult()

        malformed = getattr(delivery, "malformed", None)
        if malformed is not None:
            if self.malformed_lane is None:
                raise ValueError(malformed.error)
            disposition = self.malformed_lane.capture(
                consumer_name=self.consumer_name,
                evidence=malformed,
                delivery_metadata=delivery.metadata.to_dict(),
            )
            # The quarantine lane returns only after the poison record commits.
            delivery.ack()
            return self._malformed_result(
                delivery=delivery,
                disposition=disposition,
            )

        if delivery.envelope is None:
            raise ValueError("delivery envelope is required when delivery is not malformed")
        event = ConsumedEvent.from_envelope(delivery.envelope)
        if self.failure_lane is not None:
            existing = self.failure_lane.find(
                consumer_name=self.consumer_name,
                event=event,
            )
            if existing is not None:
                delivery.ack()
                return self._failure_result(
                    event=event,
                    delivery=delivery,
                    disposition=existing,
                )

        delivery_metadata = delivery.metadata.to_dict()
        try:
            applied = self.store.process_once(
                consumer_name=self.consumer_name,
                event=event,
                delivery_metadata=delivery_metadata,
                handler=self.handler,
            )
        except Exception as exc:
            if self.failure_lane is None:
                raise
            disposition = self.failure_lane.capture(
                consumer_name=self.consumer_name,
                event=event,
                delivery_metadata=delivery_metadata,
                error=exc,
            )
            # Capture returns only after the complete event is durable locally.
            delivery.ack()
            return self._failure_result(
                event=event,
                delivery=delivery,
                disposition=disposition,
            )

        # The store method returns only after its transaction commits.
        delivery.ack()

        return ConsumeCycleResult(
            received=1,
            applied=int(applied),
            duplicate=int(not applied),
            acknowledged=1,
            event_id=event.event_id,
            transport_message_id=delivery.metadata.transport_message_id,
            delivery_count=delivery.metadata.delivery_count,
        )
