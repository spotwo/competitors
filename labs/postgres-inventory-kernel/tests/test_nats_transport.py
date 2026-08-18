from __future__ import annotations

import asyncio
import json
import os
from uuid import uuid4

import nats
import psycopg
import pytest
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy, StorageType, StreamConfig
from nats.js.errors import NotFoundError

import conftest as lab
from nats_transport import NATS_MESSAGE_ID_HEADER, NatsJetStreamTransport
from publisher_runtime import PostgresOutboxStore, PublisherRuntime, RetryPolicy

NATS_URL = os.getenv("KERNEL_LAB_NATS_URL", "nats://127.0.0.1:54222")
TEST_RETRY_POLICY = RetryPolicy(
    base_delay_seconds=1,
    max_delay_seconds=1,
    jitter_ratio=0,
    max_attempts=10,
)


def enqueue_event(conn: psycopg.Connection, *, dedup_key: str, ordinal: int = 1):
    return conn.execute(
        """
        SELECT kernel_lab.enqueue_domain_event(
          %s,
          %s,
          'inventory.test.event',
          %s,
          'InventoryPosition',
          %s,
          %s,
          %s::jsonb,
          clock_timestamp(),
          NULL,
          %s,
          NULL,
          %s,
          1
        )
        """,
        (
            lab.TENANT,
            dedup_key,
            f"inventory-position/test-{ordinal}",
            f"test-{ordinal}",
            ordinal,
            f'{{"ordinal": {ordinal}}}',
            f"corr-{ordinal}",
            lab.WAREHOUSE,
        ),
    ).fetchone()[0]


class JetStreamProbe:
    def __init__(self, server_url: str):
        self._runner = asyncio.Runner()
        self._client = self._runner.run(
            nats.connect(servers=[server_url], allow_reconnect=False, connect_timeout=2)
        )
        self._jetstream = self._client.jetstream()

    def reset_stream(self, *, stream_name: str, subject_prefix: str) -> None:
        self._runner.run(self._reset_stream(stream_name, subject_prefix))

    def stream_message_count(self, stream_name: str) -> int:
        info = self._runner.run(self._jetstream.stream_info(stream_name))
        return info.state.messages

    def pull_twice_with_redelivery(
        self,
        *,
        stream_name: str,
        subject_prefix: str,
        durable_name: str,
    ) -> tuple[dict, dict]:
        return self._runner.run(
            self._pull_twice_with_redelivery(
                stream_name=stream_name,
                subject_prefix=subject_prefix,
                durable_name=durable_name,
            )
        )

    def close(self, stream_name: str) -> None:
        try:
            self._runner.run(self._delete_if_present(stream_name))
            self._runner.run(self._client.close())
        finally:
            self._runner.close()

    async def _reset_stream(self, stream_name: str, subject_prefix: str) -> None:
        await self._delete_if_present(stream_name)
        await self._jetstream.add_stream(
            StreamConfig(
                name=stream_name,
                subjects=[f"{subject_prefix}.>"],
                storage=StorageType.FILE,
                duplicate_window=120,
            )
        )

    async def _delete_if_present(self, stream_name: str) -> None:
        try:
            await self._jetstream.delete_stream(stream_name)
        except NotFoundError:
            pass

    async def _pull_twice_with_redelivery(
        self,
        *,
        stream_name: str,
        subject_prefix: str,
        durable_name: str,
    ) -> tuple[dict, dict]:
        subscription = await self._jetstream.pull_subscribe(
            f"{subject_prefix}.>",
            durable=durable_name,
            stream=stream_name,
            config=ConsumerConfig(
                durable_name=durable_name,
                deliver_policy=DeliverPolicy.ALL,
                ack_policy=AckPolicy.EXPLICIT,
                ack_wait=0.4,
                max_deliver=3,
                filter_subject=f"{subject_prefix}.>",
            ),
        )

        first_message = (await subscription.fetch(batch=1, timeout=3))[0]
        first = self._snapshot(first_message)

        await asyncio.sleep(0.7)
        second_message = (await subscription.fetch(batch=1, timeout=3))[0]
        second = self._snapshot(second_message)
        await second_message.ack()
        await self._client.flush()
        return first, second

    @staticmethod
    def _snapshot(message) -> dict:
        return {
            "subject": message.subject,
            "headers": dict(message.headers or {}),
            "envelope": json.loads(message.data),
            "stream_sequence": message.metadata.sequence.stream,
            "delivered": message.metadata.num_delivered,
        }


def test_nats_pub_ack_deduplicated_retry_envelope_and_pull_redelivery():
    suffix = uuid4().hex.upper()
    stream_name = f"WMS_EVENTS_{suffix}"
    durable_name = f"PROJECTOR_{suffix}"
    subject_prefix = f"spotwo.wms.test.{suffix.lower()}"
    probe = JetStreamProbe(NATS_URL)
    probe.reset_stream(stream_name=stream_name, subject_prefix=subject_prefix)

    with lab.connect() as conn:
        event_id = enqueue_event(conn, dedup_key=f"nats:retry:{suffix}", ordinal=7)
        conn.commit()

    real_store = PostgresOutboxStore(lab.DATABASE_URL)

    class FailFirstAckStore:
        def __init__(self):
            self.failed = False

        def claim(self, **kwargs):
            return real_store.claim(**kwargs)

        def ack(self, **_kwargs):
            if not self.failed:
                self.failed = True
                raise OSError("database unavailable after JetStream PUB ACK")
            return real_store.ack(**_kwargs)

        def nack(self, **kwargs):
            return real_store.nack(**kwargs)

    receipts = []
    events = []
    transport = NatsJetStreamTransport(
        server_url=NATS_URL,
        stream_name=stream_name,
        subject_prefix=subject_prefix,
        client_name="nats-integration-publisher",
    )

    class RecordingTransport:
        def publish(self, event):
            events.append(event)
            receipt = transport.publish(event)
            receipts.append(receipt)
            return receipt

    try:
        first_runtime = PublisherRuntime(
            store=FailFirstAckStore(),
            transport=RecordingTransport(),
            worker_id="publisher-a",
            batch_size=1,
            lease_seconds=30,
            retry_policy=TEST_RETRY_POLICY,
        )
        with pytest.raises(OSError, match="after JetStream PUB ACK"):
            first_runtime.run_once()

        with lab.connect() as conn:
            published, attempts = conn.execute(
                """
                SELECT published_at IS NOT NULL, attempt_count
                FROM kernel_lab.domain_event_outbox
                WHERE event_id = %s
                """,
                (event_id,),
            ).fetchone()
            assert published is False
            assert attempts == 1
            conn.execute(
                """
                UPDATE kernel_lab.domain_event_outbox
                SET claimed_until = clock_timestamp() - interval '1 second'
                WHERE event_id = %s
                """,
                (event_id,),
            )
            conn.commit()

        second_runtime = PublisherRuntime(
            store=real_store,
            transport=RecordingTransport(),
            worker_id="publisher-b",
            batch_size=1,
            lease_seconds=30,
            retry_policy=TEST_RETRY_POLICY,
        )
        second_result = second_runtime.run_once()

        assert second_result.published == 1
        assert [event.event_id for event in events] == [event_id, event_id]
        assert receipts[0].deduplicated is False
        assert receipts[1].deduplicated is True
        assert receipts[0].transport_message_id == receipts[1].transport_message_id
        assert probe.stream_message_count(stream_name) == 1

        with lab.connect() as conn:
            published, attempts = conn.execute(
                """
                SELECT published_at IS NOT NULL, attempt_count
                FROM kernel_lab.domain_event_outbox
                WHERE event_id = %s
                """,
                (event_id,),
            ).fetchone()
            assert published is True
            assert attempts == 2

        first_delivery, redelivery = probe.pull_twice_with_redelivery(
            stream_name=stream_name,
            subject_prefix=subject_prefix,
            durable_name=durable_name,
        )

        assert first_delivery["subject"] == f"{subject_prefix}.inventory.test.event"
        assert first_delivery["headers"][NATS_MESSAGE_ID_HEADER] == str(event_id)
        assert first_delivery["headers"]["Spotwo-Aggregate-Type"] == "InventoryPosition"
        assert first_delivery["headers"]["Spotwo-Aggregate-Id"] == "test-7"
        assert first_delivery["headers"]["Spotwo-Aggregate-Version"] == "7"
        assert first_delivery["envelope"] == events[0].envelope
        assert first_delivery["delivered"] == 1
        assert redelivery["stream_sequence"] == first_delivery["stream_sequence"]
        assert redelivery["envelope"] == first_delivery["envelope"]
        assert redelivery["delivered"] == 2
    finally:
        transport.close()
        probe.close(stream_name)


def test_broker_outage_does_not_rollback_committed_inventory_transaction():
    with lab.connect() as conn:
        position_id = lab.insert_position(conn, physical_qty=0)
        transaction_id = lab.new_id()
        conn.execute(
            "SELECT kernel_lab.post_inventory_receipt(%s, %s, %s, %s, 5, 'ASN-NATS-DOWN')",
            (transaction_id, lab.TENANT, "nats:broker-down", position_id),
        )
        conn.commit()

    transport = NatsJetStreamTransport(
        server_url="nats://127.0.0.1:1",
        stream_name="WMS_EVENTS",
        connect_timeout_seconds=0.2,
        publish_timeout_seconds=0.2,
    )
    publisher = PublisherRuntime(
        store=PostgresOutboxStore(lab.DATABASE_URL),
        transport=transport,
        worker_id="publisher-broker-down",
        batch_size=10,
        lease_seconds=30,
        retry_policy=TEST_RETRY_POLICY,
    )

    try:
        result = publisher.run_once()
    finally:
        transport.close()

    assert result.claimed == 2
    assert result.published == 0
    assert result.failed == 2

    with lab.connect() as conn:
        assert conn.execute(
            "SELECT count(*) FROM kernel_lab.inventory_transactions WHERE id = %s",
            (transaction_id,),
        ).fetchone()[0] == 1
        assert conn.execute(
            "SELECT physical_qty FROM kernel_lab.inventory_positions WHERE id = %s",
            (position_id,),
        ).fetchone()[0] == 5
        rows = conn.execute(
            """
            SELECT published_at IS NULL, attempt_count, last_error IS NOT NULL
            FROM kernel_lab.domain_event_outbox
            ORDER BY event_id
            """
        ).fetchall()
        assert rows == [(True, 1, True), (True, 1, True)]
