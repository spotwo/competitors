from __future__ import annotations

import asyncio
import json
import os
from hashlib import sha256
from uuid import uuid4

import nats
import psycopg
import pytest
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy, StorageType, StreamConfig
from nats.js.errors import NotFoundError

import conftest as lab
from consumer_runtime import (
    InboxConsumerRuntime,
    InboxDeliveryMetadata,
    MalformedDeliveryDisposition,
    MalformedDeliveryEvidence,
)
from malformed_delivery import PostgresNatsMalformedDeliveryQuarantine
from nats_consumer import NatsJetStreamPullSource
from nats_transport import (
    AGGREGATE_ID_HEADER,
    AGGREGATE_TYPE_HEADER,
    AGGREGATE_VERSION_HEADER,
    EVENT_TYPE_HEADER,
    NATS_MESSAGE_ID_HEADER,
)

NATS_URL = os.getenv("KERNEL_LAB_NATS_URL", "nats://127.0.0.1:54222")


def delivery_metadata(*, delivery_count: int = 1) -> InboxDeliveryMetadata:
    return InboxDeliveryMetadata(
        transport="nats-jetstream",
        transport_message_id="POISON:17",
        subject="spotwo.wms.poison.test",
        delivery_count=delivery_count,
        stream="POISON",
        consumer="POISON_DURABLE",
        stream_sequence=17,
        consumer_sequence=17,
        pending_count=0,
    )


def evidence(payload: bytes = b"not-json") -> MalformedDeliveryEvidence:
    preview = payload[:4096]
    return MalformedDeliveryEvidence(
        failure_code="invalid_message_json",
        error="JetStream message must contain a UTF-8 JSON envelope",
        payload_sha256=sha256(payload).hexdigest(),
        payload_size=len(payload),
        payload_preview=preview,
        payload_truncated=len(preview) < len(payload),
        headers={},
    )


def test_runtime_acknowledges_malformed_delivery_only_after_quarantine_commit():
    order: list[str] = []

    class Delivery:
        envelope = None
        metadata = delivery_metadata()
        malformed = evidence()

        def ack(self):
            order.append("transport_ack")

    class Source:
        def fetch_one(self):
            order.append("transport_fetch")
            return Delivery()

    class Quarantine:
        def capture(self, **_kwargs):
            order.append("poison_commit")
            return MalformedDeliveryDisposition(
                failure_code="invalid_message_json",
                observation_count=1,
                created=True,
            )

    result = InboxConsumerRuntime(
        source=Source(),
        store=None,
        consumer_name="availability-projector",
        handler=lambda _event, _transaction: None,
        malformed_lane=Quarantine(),
    ).run_once()

    assert order == ["transport_fetch", "poison_commit", "transport_ack"]
    assert result.event_id is None
    assert (result.received, result.malformed, result.quarantined, result.acknowledged) == (
        1,
        1,
        1,
        1,
    )


def test_runtime_does_not_acknowledge_when_malformed_quarantine_does_not_commit():
    acknowledgements: list[str] = []

    class Delivery:
        envelope = None
        metadata = delivery_metadata()
        malformed = evidence()

        def ack(self):
            acknowledgements.append("ack")

    class Source:
        def fetch_one(self):
            return Delivery()

    class UnavailableQuarantine:
        def capture(self, **_kwargs):
            raise OSError("poison database unavailable")

    runtime = InboxConsumerRuntime(
        source=Source(),
        store=None,
        consumer_name="availability-projector",
        handler=lambda _event, _transaction: None,
        malformed_lane=UnavailableQuarantine(),
    )

    with pytest.raises(OSError, match="poison database unavailable"):
        runtime.run_once()
    assert acknowledgements == []


def test_postgres_quarantine_is_delivery_identity_idempotent_without_event_id():
    quarantine = PostgresNatsMalformedDeliveryQuarantine(lab.DATABASE_URL)
    first = quarantine.capture(
        consumer_name="availability-projector",
        evidence=evidence(),
        delivery_metadata=delivery_metadata().to_dict(),
    )
    second = quarantine.capture(
        consumer_name="availability-projector",
        evidence=evidence(),
        delivery_metadata=delivery_metadata(delivery_count=2).to_dict(),
    )

    assert first == MalformedDeliveryDisposition(
        failure_code="invalid_message_json",
        observation_count=1,
        created=True,
    )
    assert second == MalformedDeliveryDisposition(
        failure_code="invalid_message_json",
        observation_count=2,
        created=False,
    )

    with lab.connect() as conn:
        row = conn.execute(
            """
            SELECT failure_code,
                   observation_count,
                   first_delivery_metadata ->> 'delivery_count',
                   last_delivery_metadata ->> 'delivery_count'
            FROM kernel_lab.nats_jetstream_consumer_poison_deliveries
            WHERE consumer_name = 'availability-projector'
              AND stream = 'POISON'
              AND durable_consumer = 'POISON_DURABLE'
              AND stream_sequence = 17
            """
        ).fetchone()
        inbox_count = conn.execute(
            """
            SELECT count(*)
            FROM kernel_lab.domain_event_inbox
            WHERE consumer_name = 'availability-projector'
            """
        ).fetchone()[0]
        columns = {
            column_name
            for (column_name,) in conn.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'kernel_lab'
                  AND table_name = 'nats_jetstream_consumer_poison_deliveries'
                """
            ).fetchall()
        }

    assert row == ("invalid_message_json", 2, "1", "2")
    assert inbox_count == 0
    assert "event_id" not in columns


def test_postgres_quarantine_fails_closed_on_delivery_identity_collision():
    quarantine = PostgresNatsMalformedDeliveryQuarantine(lab.DATABASE_URL)
    quarantine.capture(
        consumer_name="availability-projector",
        evidence=evidence(b"first poison"),
        delivery_metadata=delivery_metadata().to_dict(),
    )

    with pytest.raises(
        psycopg.errors.UniqueViolation,
        match="delivery identity belongs to different poison evidence",
    ):
        quarantine.capture(
            consumer_name="availability-projector",
            evidence=evidence(b"different poison"),
            delivery_metadata=delivery_metadata(delivery_count=2).to_dict(),
        )


class PoisonProbe:
    def __init__(self, server_url: str):
        self._runner = asyncio.Runner()
        self._client = self._runner.run(
            nats.connect(servers=[server_url], allow_reconnect=False, connect_timeout=2)
        )
        self._jetstream = self._client.jetstream()

    def provision(
        self,
        *,
        stream_name: str,
        subject_prefix: str,
        durable_name: str,
    ) -> None:
        self._runner.run(
            self._provision(
                stream_name=stream_name,
                subject_prefix=subject_prefix,
                durable_name=durable_name,
            )
        )

    def publish(self, *, subject: str, payload: bytes, headers=None) -> None:
        self._runner.run(self._jetstream.publish(subject, payload, headers=headers))

    def consumer_info(self, *, stream_name: str, durable_name: str):
        return self._runner.run(
            self._jetstream.consumer_info(stream_name, durable_name)
        )

    def close(self, stream_name: str) -> None:
        try:
            self._runner.run(self._delete_if_present(stream_name))
            self._runner.run(self._client.close())
        finally:
            self._runner.close()

    async def _provision(
        self,
        *,
        stream_name: str,
        subject_prefix: str,
        durable_name: str,
    ) -> None:
        await self._delete_if_present(stream_name)
        await self._jetstream.add_stream(
            StreamConfig(
                name=stream_name,
                subjects=[f"{subject_prefix}.>"],
                storage=StorageType.FILE,
                duplicate_window=120,
            )
        )
        await self._jetstream.add_consumer(
            stream_name,
            ConsumerConfig(
                durable_name=durable_name,
                deliver_policy=DeliverPolicy.ALL,
                ack_policy=AckPolicy.EXPLICIT,
                ack_wait=0.4,
                max_deliver=5,
                max_ack_pending=10,
                filter_subject=f"{subject_prefix}.>",
            ),
        )

    async def _delete_if_present(self, stream_name: str) -> None:
        try:
            await self._jetstream.delete_stream(stream_name)
        except NotFoundError:
            pass


def valid_envelope(*, event_id: str, ordinal: int) -> dict:
    return {
        "event_id": event_id,
        "type": "inventory.position.changed",
        "source": "spotwo.wms.inventory-kernel",
        "subject": f"inventory-position/poison-{ordinal}",
        "occurred_at": "2026-08-19T00:00:00+00:00",
        "recorded_at": "2026-08-19T00:00:00+00:00",
        "aggregate_type": "InventoryPosition",
        "aggregate_id": f"poison-{ordinal}",
        "aggregate_version": ordinal,
        "schema_version": 1,
        "data": {"ordinal": ordinal},
    }


def envelope_headers(envelope: dict) -> dict[str, str]:
    return {
        NATS_MESSAGE_ID_HEADER: str(envelope["event_id"]),
        EVENT_TYPE_HEADER: str(envelope["type"]),
        AGGREGATE_TYPE_HEADER: str(envelope["aggregate_type"]),
        AGGREGATE_ID_HEADER: str(envelope["aggregate_id"]),
        AGGREGATE_VERSION_HEADER: str(envelope["aggregate_version"]),
    }


def test_pinned_jetstream_malformed_messages_commit_poison_then_ack_without_inbox():
    suffix = uuid4().hex.upper()
    stream_name = f"WMS_POISON_{suffix}"
    durable_name = f"POISON_{suffix}"
    consumer_name = f"poison-consumer-{suffix.lower()}"
    subject_prefix = f"spotwo.wms.poison.{suffix.lower()}"
    subject = f"{subject_prefix}.inventory.position.changed"
    probe = PoisonProbe(NATS_URL)
    probe.provision(
        stream_name=stream_name,
        subject_prefix=subject_prefix,
        durable_name=durable_name,
    )
    source = NatsJetStreamPullSource(
        server_url=NATS_URL,
        stream_name=stream_name,
        durable_name=durable_name,
        client_name=f"poison-consumer-{suffix}",
        fetch_timeout_seconds=2,
    )
    runtime = InboxConsumerRuntime(
        source=source,
        store=None,
        consumer_name=consumer_name,
        handler=lambda _event, _transaction: None,
        malformed_lane=PostgresNatsMalformedDeliveryQuarantine(lab.DATABASE_URL),
    )

    try:
        probe.publish(subject=subject, payload=b"\xff\xfe\x00")

        invalid_envelope = valid_envelope(event_id="not-a-uuid", ordinal=2)
        probe.publish(
            subject=subject,
            payload=json.dumps(invalid_envelope).encode("utf-8"),
            headers=envelope_headers(invalid_envelope),
        )

        invalid_headers = valid_envelope(event_id=str(lab.new_id()), ordinal=3)
        headers = envelope_headers(invalid_headers)
        headers[AGGREGATE_VERSION_HEADER] = "999"
        probe.publish(
            subject=subject,
            payload=json.dumps(invalid_headers).encode("utf-8"),
            headers=headers,
        )

        results = [runtime.run_once(), runtime.run_once(), runtime.run_once()]

        assert [result.failure_code for result in results] == [
            "invalid_message_encoding",
            "invalid_event_envelope",
            "invalid_transport_headers",
        ]
        assert all(result.event_id is None for result in results)
        assert all(result.malformed == 1 for result in results)
        assert all(result.quarantined == 1 for result in results)
        assert all(result.acknowledged == 1 for result in results)

        with lab.connect() as conn:
            poison_rows = conn.execute(
                """
                SELECT stream_sequence, failure_code, observation_count
                FROM kernel_lab.nats_jetstream_consumer_poison_deliveries
                WHERE consumer_name = %s
                ORDER BY stream_sequence
                """,
                (consumer_name,),
            ).fetchall()
            inbox_count = conn.execute(
                """
                SELECT count(*)
                FROM kernel_lab.domain_event_inbox
                WHERE consumer_name = %s
                """,
                (consumer_name,),
            ).fetchone()[0]

        assert poison_rows == [
            (1, "invalid_message_encoding", 1),
            (2, "invalid_event_envelope", 1),
            (3, "invalid_transport_headers", 1),
        ]
        assert inbox_count == 0

        info = probe.consumer_info(
            stream_name=stream_name,
            durable_name=durable_name,
        )
        assert info.num_ack_pending == 0
        assert info.num_redelivered == 0
    finally:
        source.close()
        probe.close(stream_name)
