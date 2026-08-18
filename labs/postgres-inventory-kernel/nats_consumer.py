from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from typing import Any, Mapping

from nats.aio.client import Client as NatsClient
from nats.aio.msg import Msg
from nats.errors import TimeoutError as NatsTimeoutError
from nats.js.api import AckPolicy
from nats.js.client import JetStreamContext

from consumer_runtime import InboxDeliveryMetadata
from nats_transport import (
    AGGREGATE_ID_HEADER,
    AGGREGATE_TYPE_HEADER,
    AGGREGATE_VERSION_HEADER,
    EVENT_TYPE_HEADER,
    NATS_MESSAGE_ID_HEADER,
)


def _required_name(value: str, field_name: str) -> str:
    if not value or value != value.strip():
        raise ValueError(f"{field_name} is required and cannot contain surrounding whitespace")
    return value


@dataclass
class NatsJetStreamDelivery:
    envelope: Mapping[str, Any]
    metadata: InboxDeliveryMetadata
    _source: NatsJetStreamPullSource = field(repr=False)
    _message: Msg = field(repr=False)
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
        envelope = self._decode_envelope(message)
        self._validate_headers(message, envelope)
        broker_metadata = message.metadata
        return NatsJetStreamDelivery(
            envelope=envelope,
            metadata=InboxDeliveryMetadata(
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
            ),
            _source=self,
            _message=message,
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
    ) -> None:
        if config.ack_policy != AckPolicy.EXPLICIT:
            raise ValueError("durable consumer must use explicit acknowledgement")
        if config.deliver_subject is not None:
            raise ValueError("durable consumer must be a pull consumer")
        if expected_durable_name is not None and (
            config.durable_name != expected_durable_name
        ):
            raise ValueError("consumer must be durable and match durable_name")

    @staticmethod
    def _decode_envelope(message: Msg) -> dict[str, Any]:
        try:
            envelope = json.loads(message.data.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("JetStream message must contain a UTF-8 JSON envelope") from exc
        if not isinstance(envelope, dict):
            raise ValueError("JetStream event envelope must be a JSON object")
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
                raise ValueError(f"JetStream message is missing required header {header}")
            if actual != str(value):
                raise ValueError(f"JetStream header {header} does not match envelope")
