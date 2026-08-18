from __future__ import annotations

import asyncio
import json
import re
from typing import Any

import nats
from nats.aio.client import Client as NatsClient
from nats.js.client import JetStreamContext

from publisher_runtime import ClaimedEvent, PublishReceipt

NATS_MESSAGE_ID_HEADER = "Nats-Msg-Id"
SUBJECT_TOKEN = re.compile(r"^[A-Za-z0-9_-]+$")


def _validate_subject(value: str, *, field: str) -> str:
    if not value or value != value.strip():
        raise ValueError(f"{field} is required and cannot contain surrounding whitespace")

    tokens = value.split(".")
    if any(not token for token in tokens):
        raise ValueError(f"{field} must not contain empty subject tokens")
    if any(SUBJECT_TOKEN.fullmatch(token) is None for token in tokens):
        raise ValueError(f"{field} contains an unsupported NATS subject token")
    return value


class NatsJetStreamTransport:
    """Synchronous PublisherRuntime adapter backed by JetStream PUB ACKs.

    The transport owns one async client loop and is intentionally single-threaded.
    Run one transport instance per publisher worker process.
    """

    def __init__(
        self,
        *,
        server_url: str,
        stream_name: str,
        subject_prefix: str = "spotwo.wms.events",
        client_name: str = "spotwo-wms-outbox-publisher",
        connect_timeout_seconds: float = 2.0,
        publish_timeout_seconds: float = 5.0,
    ):
        if not server_url.strip():
            raise ValueError("server_url is required")
        if not stream_name.strip():
            raise ValueError("stream_name is required")
        if connect_timeout_seconds <= 0:
            raise ValueError("connect_timeout_seconds must be positive")
        if publish_timeout_seconds <= 0:
            raise ValueError("publish_timeout_seconds must be positive")

        self.server_url = server_url.strip()
        self.stream_name = stream_name.strip()
        self.subject_prefix = _validate_subject(subject_prefix, field="subject_prefix")
        self.client_name = client_name
        self.connect_timeout_seconds = connect_timeout_seconds
        self.publish_timeout_seconds = publish_timeout_seconds

        self._runner = asyncio.Runner()
        self._client: NatsClient | None = None
        self._jetstream: JetStreamContext | None = None
        self._closed = False

    def publish(self, event: ClaimedEvent) -> PublishReceipt:
        if self._closed:
            raise RuntimeError("NatsJetStreamTransport is closed")
        self._validate_envelope(event)
        return self._runner.run(self._publish(event))

    def close(self) -> None:
        if self._closed:
            return
        try:
            self._runner.run(self._close_client())
        finally:
            self._runner.close()
            self._closed = True

    def __enter__(self) -> NatsJetStreamTransport:
        return self

    def __exit__(self, _exc_type: Any, _exc: Any, _traceback: Any) -> None:
        self.close()

    def subject_for(self, event: ClaimedEvent) -> str:
        event_route = _validate_subject(event.event_type, field="event.event_type")
        return f"{self.subject_prefix}.{event_route}"

    async def _connect(self) -> JetStreamContext:
        if self._client is not None and self._client.is_connected and self._jetstream is not None:
            return self._jetstream

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
        except Exception:
            if not client.is_closed:
                await client.close()
            raise

        self._client = client
        self._jetstream = self._client.jetstream()
        return self._jetstream

    async def _publish(self, event: ClaimedEvent) -> PublishReceipt:
        jetstream = await self._connect()
        payload = json.dumps(
            event.envelope,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
        headers = {
            NATS_MESSAGE_ID_HEADER: str(event.event_id),
            "Spotwo-Event-Type": event.event_type,
            "Spotwo-Aggregate-Type": event.aggregate_type,
            "Spotwo-Aggregate-Id": event.aggregate_id,
            "Spotwo-Aggregate-Version": str(event.aggregate_version),
        }

        try:
            acknowledgement = await jetstream.publish(
                self.subject_for(event),
                payload,
                timeout=self.publish_timeout_seconds,
                stream=self.stream_name,
                headers=headers,
            )
        except Exception:
            if self._client is not None and not self._client.is_connected:
                await self._close_client()
            raise

        return PublishReceipt(
            transport_message_id=f"{acknowledgement.stream}:{acknowledgement.seq}",
            deduplicated=bool(acknowledgement.duplicate),
        )

    async def _close_client(self) -> None:
        client = self._client
        self._client = None
        self._jetstream = None
        if client is not None and not client.is_closed:
            await client.close()

    @staticmethod
    async def _capture_client_error(_error: Exception) -> None:
        # PublisherRuntime persists the final adapter exception on the Outbox row.
        # Suppress nats-py's default traceback logging for each bounded connect try.
        return None

    @staticmethod
    def _validate_envelope(event: ClaimedEvent) -> None:
        if str(event.envelope.get("event_id")) != str(event.event_id):
            raise ValueError("envelope event_id does not match ClaimedEvent.event_id")
        if event.envelope.get("type") != event.event_type:
            raise ValueError("envelope type does not match ClaimedEvent.event_type")
        if event.envelope.get("aggregate_type") != event.aggregate_type:
            raise ValueError("envelope aggregate_type does not match ClaimedEvent.aggregate_type")
        if event.envelope.get("aggregate_id") != event.aggregate_id:
            raise ValueError("envelope aggregate_id does not match ClaimedEvent.aggregate_id")
        if event.envelope.get("aggregate_version") != event.aggregate_version:
            raise ValueError("envelope aggregate_version does not match ClaimedEvent.aggregate_version")
