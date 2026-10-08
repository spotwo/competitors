from __future__ import annotations

import asyncio
import os
import time
from types import SimpleNamespace
from uuid import uuid4

import nats
import psycopg
import pytest
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy, StorageType, StreamConfig
from nats.js.errors import NotFoundError

import conftest as lab
from consumer_failure import (
    ConsumerFailureRetryRuntime,
    ConsumerRetryPolicy,
    DurableConsumerFailureLane,
    PostgresConsumerFailureStore,
)
from consumer_runtime import InboxConsumerRuntime, PostgresInboxStore
from nats_consumer import NatsJetStreamPullSource
from nats_transport import (
    AGGREGATE_ID_HEADER,
    AGGREGATE_TYPE_HEADER,
    AGGREGATE_VERSION_HEADER,
    EVENT_TYPE_HEADER,
    NATS_MESSAGE_ID_HEADER,
    NatsJetStreamTransport,
)
from position_projection import InventoryPositionQuantityProjector
from position_projection_rebuild import (
    REQUIRE_EMPTY,
    PostgresPositionProjectionRebuildStore,
)
from position_projection_snapshot import InventoryPositionQuantitySnapshot
from publisher_runtime import PostgresOutboxStore, PublisherRuntime, RetryPolicy

NATS_URL = os.getenv("KERNEL_LAB_NATS_URL", "nats://127.0.0.1:54222")
NO_JITTER_RETRY = RetryPolicy(
    base_delay_seconds=1,
    max_delay_seconds=1,
    jitter_ratio=0,
    max_attempts=3,
)


def enqueue_event(conn: psycopg.Connection, *, dedup_key: str, ordinal: int):
    return conn.execute(
        """
        SELECT kernel_lab.enqueue_domain_event(
          %s,
          %s,
          'inventory.position.changed',
          %s,
          'InventoryPosition',
          %s,
          %s,
          jsonb_build_object('ordinal', %s),
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
            f"inventory-position/consumer-{ordinal}",
            f"consumer-{ordinal}",
            ordinal,
            ordinal,
            f"corr-{ordinal}",
            lab.WAREHOUSE,
        ),
    ).fetchone()[0]


def enqueue_projection_event(
    conn: psycopg.Connection,
    *,
    dedup_key: str,
    position_id,
    version: int,
    physical_delta: int = 0,
    reserved_delta: int = 0,
    allocated_delta: int = 0,
):
    return conn.execute(
        """
        SELECT kernel_lab.enqueue_domain_event(
          %s,
          %s,
          'inventory.position.changed',
          %s,
          'InventoryPosition',
          %s,
          %s,
          jsonb_build_object(
            'transaction_id', %s::uuid,
            'transaction_type', 'projection-test',
            'position_id', %s::uuid,
            'physical_delta', %s,
            'reserved_delta', %s,
            'allocated_delta', %s
          ),
          clock_timestamp(),
          NULL,
          NULL,
          NULL,
          %s,
          1
        )
        """,
        (
            lab.TENANT,
            dedup_key,
            f"inventory-position/{position_id}",
            str(position_id),
            version,
            str(lab.new_id()),
            str(position_id),
            physical_delta,
            reserved_delta,
            allocated_delta,
            lab.WAREHOUSE,
        ),
    ).fetchone()[0]


class ConsumerProbe:
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
        durable_name: str | None,
        max_deliver: int = 3,
        filter_subject: str | None = None,
    ) -> None:
        self._runner.run(
            self._provision(
                stream_name=stream_name,
                subject_prefix=subject_prefix,
                durable_name=durable_name,
                max_deliver=max_deliver,
                filter_subject=filter_subject,
            )
        )

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
        durable_name: str | None,
        max_deliver: int,
        filter_subject: str | None,
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
        if durable_name is not None:
            await self._jetstream.add_consumer(
                stream_name,
                ConsumerConfig(
                    durable_name=durable_name,
                    deliver_policy=DeliverPolicy.ALL,
                    ack_policy=AckPolicy.EXPLICIT,
                    ack_wait=0.4,
                    max_deliver=max_deliver,
                    max_ack_pending=10,
                    filter_subject=filter_subject or f"{subject_prefix}.>",
                ),
            )

    async def _delete_if_present(self, stream_name: str) -> None:
        try:
            await self._jetstream.delete_stream(stream_name)
        except NotFoundError:
            pass


def publish_one(
    *,
    stream_name: str,
    subject_prefix: str,
    suffix: str,
    ordinal: int,
):
    with lab.connect() as conn:
        event_id = enqueue_event(
            conn,
            dedup_key=f"consumer:{suffix}:{ordinal}",
            ordinal=ordinal,
        )
        conn.commit()

    publish_queued(
        stream_name=stream_name,
        subject_prefix=subject_prefix,
        suffix=suffix,
    )
    return event_id


def publish_queued(*, stream_name: str, subject_prefix: str, suffix: str) -> None:
    transport = NatsJetStreamTransport(
        server_url=NATS_URL,
        stream_name=stream_name,
        subject_prefix=subject_prefix,
        client_name=f"consumer-test-publisher-{suffix}",
    )
    try:
        result = PublisherRuntime(
            store=PostgresOutboxStore(lab.DATABASE_URL),
            transport=transport,
            worker_id=f"consumer-test-publisher-{suffix}",
            batch_size=1,
            lease_seconds=30,
            retry_policy=NO_JITTER_RETRY,
        ).run_once()
    finally:
        transport.close()

    assert result.published == 1


def effect_handler(consumer_name: str, calls: list):
    def handler(event, transaction):
        calls.append(event.event_id)
        transaction.execute(
            """
            UPDATE kernel_lab.domain_event_inbox
            SET metadata = metadata || jsonb_build_object(
              'effect_count',
              COALESCE((metadata ->> 'effect_count')::integer, 0) + 1
            )
            WHERE consumer_name = %s
              AND event_id = %s
            """,
            (consumer_name, event.event_id),
        )

    return handler


class AckFailingDelivery:
    def __init__(self, delivery):
        self.envelope = delivery.envelope
        self.metadata = delivery.metadata

    def ack(self):
        raise OSError("connection lost before JetStream ACK confirmation")


class FailFirstAckSource:
    def __init__(self, source):
        self.source = source
        self.failed = False

    def fetch_one(self):
        delivery = self.source.fetch_one()
        if delivery is None or self.failed:
            return delivery
        self.failed = True
        return AckFailingDelivery(delivery)


def test_ack_confirmation_failure_redelivers_without_reapplying_handler():
    suffix = uuid4().hex.upper()
    stream_name = f"WMS_CONSUMER_{suffix}"
    durable_name = f"AVAILABILITY_{suffix}"
    consumer_name = f"availability-projector-{suffix.lower()}"
    subject_prefix = f"spotwo.wms.consumer.{suffix.lower()}"
    probe = ConsumerProbe(NATS_URL)
    probe.provision(
        stream_name=stream_name,
        subject_prefix=subject_prefix,
        durable_name=durable_name,
    )
    source = NatsJetStreamPullSource(
        server_url=NATS_URL,
        stream_name=stream_name,
        durable_name=durable_name,
        client_name=f"inbox-consumer-{suffix}",
        fetch_timeout_seconds=2,
    )

    try:
        event_id = publish_one(
            stream_name=stream_name,
            subject_prefix=subject_prefix,
            suffix=suffix,
            ordinal=1,
        )
        calls: list = []
        runtime = InboxConsumerRuntime(
            source=FailFirstAckSource(source),
            store=PostgresInboxStore(lab.DATABASE_URL),
            consumer_name=consumer_name,
            handler=effect_handler(consumer_name, calls),
        )

        with pytest.raises(OSError, match="before JetStream ACK"):
            runtime.run_once()

        with lab.connect() as conn:
            metadata = conn.execute(
                """
                SELECT metadata
                FROM kernel_lab.domain_event_inbox
                WHERE consumer_name = %s
                  AND event_id = %s
                """,
                (consumer_name, event_id),
            ).fetchone()[0]
        assert metadata["effect_count"] == 1
        assert metadata["delivery_count"] == 1

        time.sleep(0.7)
        redelivery = runtime.run_once()

        assert redelivery.event_id == event_id
        assert redelivery.delivery_count == 2
        assert redelivery.applied == 0
        assert redelivery.duplicate == 1
        assert redelivery.acknowledged == 1
        assert calls == [event_id]
        info = probe.consumer_info(
            stream_name=stream_name,
            durable_name=durable_name,
        )
        assert info.num_ack_pending == 0
    finally:
        source.close()
        probe.close(stream_name)


def test_handler_failure_rolls_back_inbox_and_jetstream_redelivers():
    suffix = uuid4().hex.upper()
    stream_name = f"WMS_CONSUMER_{suffix}"
    durable_name = f"AUDIT_{suffix}"
    consumer_name = f"audit-projector-{suffix.lower()}"
    subject_prefix = f"spotwo.wms.consumer.{suffix.lower()}"
    probe = ConsumerProbe(NATS_URL)
    probe.provision(
        stream_name=stream_name,
        subject_prefix=subject_prefix,
        durable_name=durable_name,
    )
    source = NatsJetStreamPullSource(
        server_url=NATS_URL,
        stream_name=stream_name,
        durable_name=durable_name,
        client_name=f"inbox-consumer-{suffix}",
        fetch_timeout_seconds=2,
    )

    try:
        event_id = publish_one(
            stream_name=stream_name,
            subject_prefix=subject_prefix,
            suffix=suffix,
            ordinal=2,
        )

        def failing_handler(event, transaction):
            effect_handler(consumer_name, [])(event, transaction)
            raise RuntimeError("projection transaction failed")

        failing_runtime = InboxConsumerRuntime(
            source=source,
            store=PostgresInboxStore(lab.DATABASE_URL),
            consumer_name=consumer_name,
            handler=failing_handler,
        )
        with pytest.raises(RuntimeError, match="projection transaction failed"):
            failing_runtime.run_once()

        with lab.connect() as conn:
            assert conn.execute(
                """
                SELECT count(*)
                FROM kernel_lab.domain_event_inbox
                WHERE consumer_name = %s
                  AND event_id = %s
                """,
                (consumer_name, event_id),
            ).fetchone()[0] == 0

        time.sleep(0.7)
        calls: list = []
        successful_runtime = InboxConsumerRuntime(
            source=source,
            store=PostgresInboxStore(lab.DATABASE_URL),
            consumer_name=consumer_name,
            handler=effect_handler(consumer_name, calls),
        )
        redelivery = successful_runtime.run_once()

        assert redelivery.event_id == event_id
        assert redelivery.delivery_count == 2
        assert redelivery.applied == 1
        assert redelivery.duplicate == 0
        assert calls == [event_id]
        with lab.connect() as conn:
            metadata = conn.execute(
                """
                SELECT metadata
                FROM kernel_lab.domain_event_inbox
                WHERE consumer_name = %s
                  AND event_id = %s
                """,
                (consumer_name, event_id),
            ).fetchone()[0]
        assert metadata["effect_count"] == 1
        assert metadata["delivery_count"] == 2
    finally:
        source.close()
        probe.close(stream_name)


def test_out_of_order_versions_ack_after_durable_buffer_then_drain():
    suffix = uuid4().hex.upper()
    stream_name = f"WMS_CONSUMER_{suffix}"
    durable_name = f"POSITION_{suffix}"
    consumer_name = f"position-projector-{suffix.lower()}"
    subject_prefix = f"spotwo.wms.consumer.{suffix.lower()}"
    position_id = lab.new_id()
    probe = ConsumerProbe(NATS_URL)
    probe.provision(
        stream_name=stream_name,
        subject_prefix=subject_prefix,
        durable_name=durable_name,
    )
    source = NatsJetStreamPullSource(
        server_url=NATS_URL,
        stream_name=stream_name,
        durable_name=durable_name,
        client_name=f"position-projector-{suffix}",
        fetch_timeout_seconds=2,
    )
    runtime = InboxConsumerRuntime(
        source=source,
        store=PostgresInboxStore(lab.DATABASE_URL),
        consumer_name=consumer_name,
        handler=InventoryPositionQuantityProjector(consumer_name=consumer_name),
    )

    try:
        with lab.connect() as conn:
            second_event_id = enqueue_projection_event(
                conn,
                dedup_key=f"projection:{suffix}:2",
                position_id=position_id,
                version=2,
                reserved_delta=3,
            )
            conn.commit()
        publish_queued(
            stream_name=stream_name,
            subject_prefix=subject_prefix,
            suffix=f"{suffix}-2",
        )

        buffered = runtime.run_once()
        assert buffered.event_id == second_event_id
        assert buffered.acknowledged == 1
        with lab.connect() as conn:
            projection_version = conn.execute(
                """
                SELECT aggregate_version
                FROM kernel_lab.inventory_position_quantity_projection
                WHERE consumer_name = %s
                  AND position_id = %s
                """,
                (consumer_name, position_id),
            ).fetchone()[0]
            pending_versions = conn.execute(
                """
                SELECT aggregate_version
                FROM kernel_lab.inventory_position_projection_pending
                WHERE consumer_name = %s
                  AND position_id = %s
                """,
                (consumer_name, position_id),
            ).fetchall()
        assert projection_version == 0
        assert pending_versions == [(2,)]
        assert probe.consumer_info(
            stream_name=stream_name,
            durable_name=durable_name,
        ).num_ack_pending == 0

        with lab.connect() as conn:
            first_event_id = enqueue_projection_event(
                conn,
                dedup_key=f"projection:{suffix}:1",
                position_id=position_id,
                version=1,
                physical_delta=10,
            )
            conn.commit()
        publish_queued(
            stream_name=stream_name,
            subject_prefix=subject_prefix,
            suffix=f"{suffix}-1",
        )

        applied = runtime.run_once()
        assert applied.event_id == first_event_id
        assert applied.acknowledged == 1
        with lab.connect() as conn:
            projection = conn.execute(
                """
                SELECT aggregate_version, physical_qty, reserved_qty, allocated_qty,
                       last_event_id
                FROM kernel_lab.inventory_position_quantity_projection
                WHERE consumer_name = %s
                  AND position_id = %s
                """,
                (consumer_name, position_id),
            ).fetchone()
            pending_count = conn.execute(
                """
                SELECT count(*)
                FROM kernel_lab.inventory_position_projection_pending
                WHERE consumer_name = %s
                  AND position_id = %s
                """,
                (consumer_name, position_id),
            ).fetchone()[0]
            second_status = conn.execute(
                """
                SELECT metadata #>> '{projection,status}'
                FROM kernel_lab.domain_event_inbox
                WHERE consumer_name = %s
                  AND event_id = %s
                """,
                (consumer_name, second_event_id),
            ).fetchone()[0]
        assert projection == (2, 10, 3, 0, second_event_id)
        assert pending_count == 0
        assert second_status == "applied"
    finally:
        source.close()
        probe.close(stream_name)


def test_rebuild_fence_handoffs_before_ack_then_retries_locally_after_cancel():
    suffix = uuid4().hex.upper()
    stream_name = f"WMS_REBUILD_{suffix}"
    durable_name = f"POSITION_REBUILD_{suffix}"
    consumer_name = f"position-rebuild-{suffix.lower()}"
    subject_prefix = f"spotwo.wms.rebuild.{suffix.lower()}"
    position_id = lab.new_id()
    probe = ConsumerProbe(NATS_URL)
    probe.provision(
        stream_name=stream_name,
        subject_prefix=subject_prefix,
        durable_name=durable_name,
        max_deliver=1,
    )
    source = NatsJetStreamPullSource(
        server_url=NATS_URL,
        stream_name=stream_name,
        durable_name=durable_name,
        client_name=f"position-rebuild-{suffix}",
        fetch_timeout_seconds=2,
    )
    failure_store = PostgresConsumerFailureStore(lab.DATABASE_URL)
    consumer_retry = ConsumerRetryPolicy(
        base_delay_seconds=1,
        max_delay_seconds=1,
        jitter_ratio=0,
        max_attempts=3,
    )
    runtime = InboxConsumerRuntime(
        source=source,
        store=PostgresInboxStore(lab.DATABASE_URL),
        consumer_name=consumer_name,
        handler=InventoryPositionQuantityProjector(consumer_name=consumer_name),
        failure_lane=DurableConsumerFailureLane(
            store=failure_store,
            retry_policy=consumer_retry,
        ),
    )
    rebuild_store = PostgresPositionProjectionRebuildStore(lab.DATABASE_URL)
    rebuild_id = lab.new_id()

    try:
        with lab.connect() as conn:
            first_event_id = enqueue_projection_event(
                conn,
                dedup_key=f"rebuild:{suffix}:1",
                position_id=position_id,
                version=1,
                physical_delta=10,
            )
            conn.commit()
        publish_queued(
            stream_name=stream_name,
            subject_prefix=subject_prefix,
            suffix=f"{suffix}-1",
        )
        assert runtime.run_once().event_id == first_event_id

        rebuild_store.prepare(
            rebuild_id=rebuild_id,
            consumer_name=consumer_name,
            snapshot=InventoryPositionQuantitySnapshot.from_document(
                {
                    "position_id": str(position_id),
                    "aggregate_version": 1,
                    "physical_qty": "10",
                    "reserved_qty": "0",
                    "allocated_qty": "0",
                    "source": "authoritative-rebuild-export",
                    "reference": f"jetstream-rebuild-{suffix}",
                    "recorded_at": "2026-08-18T22:00:00+00:00",
                },
                artifact_checksum="sha256:" + ("c" * 64),
            ),
            expected_projection_version=1,
            expected_pending_count=0,
            pending_disposition=REQUIRE_EMPTY,
            operator="planner@example.com",
            reason="prove JetStream delivery recovery under rebuild fence",
        )

        with lab.connect() as conn:
            second_event_id = enqueue_projection_event(
                conn,
                dedup_key=f"rebuild:{suffix}:2",
                position_id=position_id,
                version=2,
                reserved_delta=3,
            )
            conn.commit()
        publish_queued(
            stream_name=stream_name,
            subject_prefix=subject_prefix,
            suffix=f"{suffix}-2",
        )

        deferred = runtime.run_once()
        assert deferred.event_id == second_event_id
        assert deferred.delivery_count == 1
        assert deferred.deferred == 1
        assert deferred.acknowledged == 1
        assert deferred.failure_code == "projection_rebuild_fence_active"

        with lab.connect() as conn:
            assert conn.execute(
                """
                SELECT count(*)
                FROM kernel_lab.domain_event_inbox
                WHERE consumer_name = %s AND event_id = %s
                """,
                (consumer_name, second_event_id),
            ).fetchone()[0] == 0
            failure = conn.execute(
                """
                SELECT status, attempt_count, envelope ->> 'event_id'
                FROM kernel_lab.domain_event_consumer_failures
                WHERE consumer_name = %s AND event_id = %s
                """,
                (consumer_name, second_event_id),
            ).fetchone()
        assert failure == ("deferred", 1, str(second_event_id))
        consumer_info = probe.consumer_info(
            stream_name=stream_name,
            durable_name=durable_name,
        )
        assert consumer_info.num_ack_pending == 0
        assert consumer_info.num_redelivered == 0

        rebuild_store.cancel(
            rebuild_id=rebuild_id,
            operator="operator@example.com",
            reason="complete delivery recovery proof",
        )
        with lab.connect() as conn:
            conn.execute(
                """
                UPDATE kernel_lab.domain_event_consumer_failures
                SET available_at = clock_timestamp() - interval '1 second'
                WHERE consumer_name = %s AND event_id = %s
                """,
                (consumer_name, second_event_id),
            )
            conn.commit()
        local_retry = ConsumerFailureRetryRuntime(
            store=failure_store,
            consumer_name=consumer_name,
            handler=InventoryPositionQuantityProjector(consumer_name=consumer_name),
            worker_id=f"position-retry-{suffix}",
            batch_size=1,
            lease_seconds=30,
            retry_policy=consumer_retry,
        ).run_once()

        assert (local_retry.claimed, local_retry.resolved, local_retry.applied) == (
            1,
            1,
            1,
        )
        with lab.connect() as conn:
            projection = conn.execute(
                """
                SELECT aggregate_version, physical_qty, reserved_qty, allocated_qty
                FROM kernel_lab.inventory_position_quantity_projection
                WHERE consumer_name = %s AND position_id = %s
                """,
                (consumer_name, position_id),
            ).fetchone()
            failure_status = conn.execute(
                """
                SELECT status, resolution
                FROM kernel_lab.domain_event_consumer_failures
                WHERE consumer_name = %s AND event_id = %s
                """,
                (consumer_name, second_event_id),
            ).fetchone()
        assert projection == (2, 10, 3, 0)
        assert failure_status == ("resolved", "applied")
        assert probe.consumer_info(
            stream_name=stream_name,
            durable_name=durable_name,
        ).num_ack_pending == 0
    finally:
        source.close()
        probe.close(stream_name)


def test_source_refuses_to_create_missing_durable_consumer():
    suffix = uuid4().hex.upper()
    stream_name = f"WMS_CONSUMER_{suffix}"
    durable_name = f"MISSING_{suffix}"
    subject_prefix = f"spotwo.wms.consumer.{suffix.lower()}"
    probe = ConsumerProbe(NATS_URL)
    probe.provision(
        stream_name=stream_name,
        subject_prefix=subject_prefix,
        durable_name=None,
    )
    source = NatsJetStreamPullSource(
        server_url=NATS_URL,
        stream_name=stream_name,
        durable_name=durable_name,
        fetch_timeout_seconds=0.5,
    )

    try:
        with pytest.raises(NotFoundError):
            source.fetch_one()
    finally:
        source.close()
        probe.close(stream_name)


def test_source_rejects_invalid_consumer_configuration():
    with pytest.raises(ValueError, match="explicit acknowledgement"):
        NatsJetStreamPullSource._validate_consumer_config(
            SimpleNamespace(
                ack_policy=AckPolicy.NONE,
                deliver_subject=None,
                durable_name="EXPECTED",
            )
        )

    with pytest.raises(ValueError, match="pull consumer"):
        NatsJetStreamPullSource._validate_consumer_config(
            SimpleNamespace(
                ack_policy=AckPolicy.EXPLICIT,
                deliver_subject="deliver.events",
                durable_name="EXPECTED",
            )
        )

    with pytest.raises(ValueError, match="match durable_name"):
        NatsJetStreamPullSource._validate_consumer_config(
            SimpleNamespace(
                ack_policy=AckPolicy.EXPLICIT,
                deliver_subject=None,
                durable_name=None,
            ),
            expected_durable_name="EXPECTED",
        )


def test_source_rejects_transport_header_and_envelope_mismatch():
    event_id = lab.new_id()
    envelope = {
        "event_id": str(event_id),
        "type": "inventory.position.changed",
        "aggregate_type": "InventoryPosition",
        "aggregate_id": "position-1",
        "aggregate_version": 1,
    }
    headers = {
        NATS_MESSAGE_ID_HEADER: str(event_id),
        EVENT_TYPE_HEADER: envelope["type"],
        AGGREGATE_TYPE_HEADER: envelope["aggregate_type"],
        AGGREGATE_ID_HEADER: envelope["aggregate_id"],
        AGGREGATE_VERSION_HEADER: "2",
    }

    with pytest.raises(ValueError, match=AGGREGATE_VERSION_HEADER):
        NatsJetStreamPullSource._validate_headers(
            SimpleNamespace(headers=headers),
            envelope,
        )
