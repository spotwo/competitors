from __future__ import annotations

import time
from uuid import uuid4

import pytest

import conftest as lab
import test_nats_consumer as nats_lab
import test_work_state_projection as work_lab
from consumer_runtime import InboxConsumerRuntime, PostgresInboxStore
from nats_consumer import NatsJetStreamPullSource
from nats_transport import NatsJetStreamTransport
from publisher_runtime import PostgresOutboxStore, PublisherRuntime
from work_state_projection import WarehouseWorkStateProjector


def test_warehouse_work_outbox_jetstream_inbox_projection_survives_ack_uncertainty():
    """Prove the second pipeline with real PostgreSQL and JetStream, not mocked delivery."""
    suffix = uuid4().hex.upper()
    stream_name = f"WMS_WORK_{suffix}"
    durable_name = f"WORK_{suffix}"
    consumer_name = f"work-projector-{suffix.lower()}"
    subject_prefix = f"spotwo.wms.worktest.{suffix.lower()}"

    work_id = work_lab.build_two_task_work()
    events = work_lab.load_work_events(work_id)
    assert [event.aggregate_version for event in events] == [1, 2, 3, 4]

    probe = nats_lab.ConsumerProbe(nats_lab.NATS_URL)
    probe.provision(
        stream_name=stream_name,
        subject_prefix=subject_prefix,
        durable_name=durable_name,
    )
    source = NatsJetStreamPullSource(
        server_url=nats_lab.NATS_URL,
        stream_name=stream_name,
        durable_name=durable_name,
        client_name=f"work-projector-{suffix}",
        fetch_timeout_seconds=2,
    )

    try:
        with NatsJetStreamTransport(
            server_url=nats_lab.NATS_URL,
            stream_name=stream_name,
            subject_prefix=subject_prefix,
            client_name=f"work-publisher-{suffix}",
        ) as transport:
            publish = PublisherRuntime(
                store=PostgresOutboxStore(lab.DATABASE_URL),
                transport=transport,
                worker_id=f"work-publisher-{suffix}",
                batch_size=10,
                lease_seconds=30,
                retry_policy=nats_lab.NO_JITTER_RETRY,
            ).run_once()
        assert publish.published == 4
        assert publish.failed == 0

        runtime = InboxConsumerRuntime(
            source=nats_lab.FailFirstAckSource(source),
            store=PostgresInboxStore(lab.DATABASE_URL),
            consumer_name=consumer_name,
            handler=WarehouseWorkStateProjector(consumer_name=consumer_name),
        )
        with pytest.raises(OSError, match="before JetStream ACK"):
            runtime.run_once()

        with lab.connect() as conn:
            applied_first = conn.execute(
                """
                SELECT aggregate_version
                FROM kernel_lab.warehouse_work_state_projection
                WHERE consumer_name = %s AND work_id = %s
                """,
                (consumer_name, work_id),
            ).fetchone()[0]
            receipt_count = conn.execute(
                """
                SELECT count(*)
                FROM kernel_lab.domain_event_inbox
                WHERE consumer_name = %s
                """,
                (consumer_name,),
            ).fetchone()[0]
        assert (applied_first, receipt_count) == (1, 1)

        # The first transaction committed but its broker ACK was lost.
        # Redelivery must be acknowledged without applying its projection twice.
        time.sleep(0.7)
        duplicate = runtime.run_once()
        assert duplicate.event_id == events[0].event_id
        assert duplicate.duplicate == 1
        assert duplicate.applied == 0
        assert duplicate.acknowledged == 1

        applied = [runtime.run_once() for _ in range(3)]
        assert [cycle.event_id for cycle in applied] == [
            event.event_id for event in events[1:]
        ]
        assert all(cycle.applied == 1 and cycle.acknowledged == 1 for cycle in applied)

        with lab.connect() as conn:
            projection = conn.execute(
                """
                SELECT state, aggregate_version, work_version, last_event_id
                FROM kernel_lab.warehouse_work_state_projection
                WHERE consumer_name = %s AND work_id = %s
                """,
                (consumer_name, work_id),
            ).fetchone()
            receipt_count = conn.execute(
                """
                SELECT count(*)
                FROM kernel_lab.domain_event_inbox
                WHERE consumer_name = %s
                """,
                (consumer_name,),
            ).fetchone()[0]

        assert projection == ("completed", 4, 5, events[-1].event_id)
        assert receipt_count == 4
        assert probe.consumer_info(
            stream_name=stream_name,
            durable_name=durable_name,
        ).num_ack_pending == 0
    finally:
        source.close()
        probe.close(stream_name)
