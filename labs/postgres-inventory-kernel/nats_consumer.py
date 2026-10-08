from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from hashlib import sha256
from typing import Any, Mapping

from nats.aio.client import Client as NatsClient
from nats.aio.msg import Msg
from nats.errors import TimeoutError as NatsTimeoutError
from nats.js.api import AckPolicy
from nats.js.client import JetStreamContext

from event_tenant_routing import TENANT_HEADER, TenantEventRoute

from consumer_runtime import (
    ConsumedEvent,
    InboxDeliveryMetadata,
    MalformedDeliveryEvidence,
)
from nats_transport import (
    AGGREGATE_ID_HEADER,
    AGGREGATE_TYPE_HEADER,
    AGGREGATE_VERSION_HEADER,
    EVENT_TYPE_HEADER,
    NATS_MESSAGE_ID_HEADER,
)

PAYLOAD_PREVIEW_BYTES = 4096


def _required_name(value: str, field_name: str) -> str:
    if not value or value != value.strip():
        raise ValueError(f"{field_name} is required and cannot contain surrounding whitespace")
    return value


class MalformedJetStreamMessage(ValueError):
    def __init__(self, failure_code: str, message: str):
        super().__init__(message)
        self.failure_code = failure_code


@dataclass
class NatsJetStreamDelivery:
    envelope: Mapping[str, Any] | None
    metadata: InboxDeliveryMetadata
    _source: NatsJetStreamPullSource = field(repr=False)
    _message: Msg = field(repr=False)
    malformed: MalformedDeliveryEvidence | None = None
    _acknowledged: bool = field(default=False, init=False, repr=False)

    def ack(self) -> None:
        if self._acknowledged:
            return
        self._source.ack_message(self._message)
        self._acknowledged = True


class NatsJetStreamPullSource:
    """Synchronous Inbox source bound to one pre-provisioned durable consumer."""

    def __init__(
        self,
        *,
        server_url: str,
        stream_name: str,
        durable_name: str,
        client_name: str = "spotwo-wms-inbox-consumer",
        connect_timeout_seconds: float = 2.0,
        fetch_timeout_seconds: float = 1.0,
        ack_timeout_seconds: float = 2.0,
        tenant_route: TenantEventRoute | None = None,
    ):
        if not server_url.strip():
            raise ValueError("server_url is required")
        if connect_timeout_seconds <= 0:
            raise ValueError("connect_timeout_seconds must be positive")
        if fetch_timeout_seconds <= 0:
            raise ValueError("fetch_timeout_seconds must be positive")
        if ack_timeout_seconds <= 0:
            raise ValueError("ack_timeout_seconds must be positive")

        self.server_url = server_url.strip()
        self.stream_name = _required_name(stream_name, "stream_name")
        self.durable_name = _required_name(durable_name, "durable_name")
        self.client_name = _required_name(client_name, "client_name")
        self.connect_timeout_seconds = connect_timeout_seconds
        self.fetch_timeout_seconds = fetch_timeout_seconds
        self.ack_timeout_seconds = ack_timeout_seconds
        self.tenant_route = tenant_route

        self._runner = asyncio.Runner()
        self._client: NatsClient | None = None
        self._jetstream: JetStreamContext | None = None
        self._subscription: JetStreamContext.PullSubscription | None = None
        self._closed = False

    def fetch_one(self) -> NatsJetStreamDelivery | None:
        if self._closed:
            raise RuntimeError("NatsJetStreamPullSource is closed")
        return self._runner.run(self._fetch_one())

    def ack_message(self, message: Msg) -> None:
        if self._closed:
            raise RuntimeError("NatsJetStreamPullSource is closed")
        self._runner.run(message.ack_sync(timeout=self.ack_timeout_seconds))

    def close(self) -> None:
        if self._closed:
            return
        try:
            self._runner.run(self._close_client())
        finally:
            self._runner.close()
            self._closed = True

    def __enter__(self) -> NatsJetStreamPullSource:
        return self

    def __exit__(self, _exc_type: Any, _exc: Any, _traceback: Any) -> None:
        self.close()

    async def _connect(self) -> JetStreamContext.PullSubscription:
        if (
            self._client is not None
            and self._client.is_connected
            and self._subscription is not None
        ):
            return self._subscription

        await self._close_client()
        client = NatsClient()
        try:
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
            jetstream = client.jetstream()
            # Binding must not silently create a consumer with accidental defaults.
            consumer_info = await jetstream.consumer_info(
                self.stream_name,
                self.durable_name,
            )
            self._validate_consumer_config(
                consumer_info.config,
                expected_durable_name=self.durable_name,
                expected_filter_subject=(
                    self.tenant_route.subject if self.tenant_route is not None else None
                ),
            )
            subscription = await jetstream.pull_subscribe_bind(
                stream=self.stream_name,
                durable=self.durable_name,
            )
        except Exception:
            if not client.is_closed:
                await client.close()
            raise

        self._client = client
        self._jetstream = jetstream
        self._subscription = subscription
        return subscription

    async def _fetch_one(self) -> NatsJetStreamDelivery | None:
        subscription = await self._connect()
        try:
            messages = await subscription.fetch(
                batch=1,
                timeout=self.fetch_timeout_seconds,
            )
        except NatsTimeoutError:
            return None
        except Exception:
            if self._client is not None and not self._client.is_connected:
                await self._close_client()
            raise

        message = messages[0]
        # JetStream metadata is transport-owned identity and must be captured before
        # any payload, envelope, event_id, or application header is trusted.
        metadata = self._delivery_metadata(message)

        try:
            envelope = self._decode_envelope(message)
            self._validate_headers(message, envelope)
            if self.tenant_route is not None:
                try:
                    self.tenant_route.validate_delivery(
                        subject=message.subject,
                        headers=message.headers or {},
                        envelope=envelope,
                    )
                except ValueError as exc:
                    raise MalformedJetStreamMessage(
                        "invalid_tenant_route", str(exc)
                    ) from exc
            try:
                ConsumedEvent.from_envelope(envelope)
            except ValueError as exc:
                raise MalformedJetStreamMessage(
                    "invalid_event_envelope",
                    str(exc),
                ) from exc
        except MalformedJetStreamMessage as exc:
            return NatsJetStreamDelivery(
                envelope=None,
                metadata=metadata,
                malformed=self._malformed_evidence(message, exc),
                _source=self,
                _message=message,
            )

        return NatsJetStreamDelivery(
            envelope=envelope,
            metadata=metadata,
            _source=self,
            _message=message,
        )

    @staticmethod
    def _delivery_metadata(message: Msg) -> InboxDeliveryMetadata:
        broker_metadata = message.metadata
        return InboxDeliveryMetadata(
            transport="nats-jetstream",
            transport_message_id=(
                f"{broker_metadata.stream}:{broker_metadata.sequence.stream}"
            ),
            subject=message.subject,
            delivery_count=broker_metadata.num_delivered,
            stream=broker_metadata.stream,
            consumer=broker_metadata.consumer,
            stream_sequence=broker_metadata.sequence.stream,
            consumer_sequence=broker_metadata.sequence.consumer,
            pending_count=broker_metadata.num_pending,
            broker_timestamp=broker_metadata.timestamp,
        )

    @staticmethod
    def _malformed_evidence(
        message: Msg,
        error: MalformedJetStreamMessage,
    ) -> MalformedDeliveryEvidence:
        payload = bytes(message.data)
        preview = payload[:PAYLOAD_PREVIEW_BYTES]
        return MalformedDeliveryEvidence(
            failure_code=error.failure_code,
            error=str(error),
            payload_sha256=sha256(payload).hexdigest(),
            payload_size=len(payload),
            payload_preview=preview,
            payload_truncated=len(preview) < len(payload),
            headers={str(key): str(value) for key, value in (message.headers or {}).items()},
        )

    async def _close_client(self) -> None:
        client = self._client
        self._client = None
        self._jetstream = None
        self._subscription = None
        if client is not None and not client.is_closed:
            await client.close()

    @staticmethod
    async def _capture_client_error(_error: Exception) -> None:
        # The runtime surfaces the final adapter exception to its supervisor.
        return None

    @staticmethod
    def _validate_consumer_config(
        config: Any,
        *,
        expected_durable_name: str | None = None,
        expected_filter_subject: str | None = None,
    ) -> None:
        if config.ack_policy != AckPolicy.EXPLICIT:
            raise ValueError("durable consumer must use explicit acknowledgement")
        if config.deliver_subject is not None:
            raise ValueError("durable consumer must be a pull consumer")
        if expected_durable_name is not None and (
            config.durable_name != expected_durable_name
        ):
            raise ValueError("consumer must be durable and match durable_name")
        if expected_filter_subject is not None and (
            config.filter_subject != expected_filter_subject
            or getattr(config, "filter_subjects", None)
        ):
            raise ValueError(
                "tenant-scoped durable must use the exact authorized filter subject"
            )

    @staticmethod
    def _decode_envelope(message: Msg) -> dict[str, Any]:
        try:
            decoded = message.data.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise MalformedJetStreamMessage(
                "invalid_message_encoding",
                "JetStream message must contain a UTF-8 JSON envelope",
            ) from exc
        try:
            envelope = json.loads(decoded)
        except json.JSONDecodeError as exc:
            raise MalformedJetStreamMessage(
                "invalid_message_json",
                "JetStream message must contain a UTF-8 JSON envelope",
            ) from exc
        if not isinstance(envelope, dict):
            raise MalformedJetStreamMessage(
                "invalid_event_envelope",
                "JetStream event envelope must be a JSON object",
            )
        return envelope

    @staticmethod
    def _validate_headers(message: Msg, envelope: Mapping[str, Any]) -> None:
        headers = message.headers or {}
        expected = {
            NATS_MESSAGE_ID_HEADER: envelope.get("event_id"),
            EVENT_TYPE_HEADER: envelope.get("type"),
            AGGREGATE_TYPE_HEADER: envelope.get("aggregate_type"),
            AGGREGATE_ID_HEADER: envelope.get("aggregate_id"),
            AGGREGATE_VERSION_HEADER: envelope.get("aggregate_version"),
        }
        for header, value in expected.items():
            actual = headers.get(header)
            if actual is None:
                raise MalformedJetStreamMessage(
                    "invalid_transport_headers",
                    f"JetStream message is missing required header {header}",
                )
            if actual != str(value):
                raise MalformedJetStreamMessage(
                    "invalid_transport_headers",
                    f"JetStream header {header} does not match envelope",
                )
