from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import nats
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy, StorageType, StreamConfig
from nats.js.errors import NotFoundError

import conftest as lab
from consumer_runtime import ConsumedEvent, InboxConsumerRuntime, PostgresInboxStore
from event_pipeline_canary import (
    CANARY_AGGREGATE_TYPE,
    CANARY_EVENT_TYPE,
    CanaryAckState,
    EventPipelineCanaryDbSnapshot,
    EventPipelineCanaryHandler,
    EventPipelineCanaryRunSnapshot,
    EventPipelineCanarySloPolicy,
    NatsJetStreamCanaryAckObserver,
    PostgresEventPipelineCanaryStore,
    render_prometheus,
)
from nats_consumer import NatsJetStreamPullSource
from nats_transport import NatsJetStreamTransport
from publisher_runtime import PostgresOutboxStore, PublisherRuntime, RetryPolicy

NATS_URL = lab.os.getenv("KERNEL_LAB_NATS_URL", "nats://127.0.0.1:54222") if hasattr(lab, "os") else "nats://127.0.0.1:54222"


def test_start_canary_enqueues_isolated_synthetic_event_without_inventory_mutation():
    identity = PostgresEventPipelineCanaryStore(lab.DATABASE_URL).start(
        tenant_id=lab.TENANT
    )

    with lab.connect() as conn:
        event = conn.execute(
            """
            SELECT event_type, aggregate_type, aggregate_id, aggregate_version,
                   subject, correlation_id, warehouse_id, schema_version, data
            FROM kernel_lab.domain_event_outbox
            WHERE event_id = %s
            """,
            (identity.event_id,),
        ).fetchone()
        run = conn.execute(
            """
            SELECT canary_id, tenant_id, event_id, started_at, projected_at
            FROM kernel_lab.event_pipeline_canary_runs
            WHERE canary_id = %s
            """,
            (identity.canary_id,),
        ).fetchone()
        inventory_positions = conn.execute(
            "SELECT count(*) FROM kernel_lab.inventory_positions"
        ).fetchone()[0]
        projection_rows = conn.execute(
            "SELECT count(*) FROM kernel_lab.inventory_position_quantity_projection"
        ).fetchone()[0]

    assert event[0] == CANARY_EVENT_TYPE
    assert event[1] == CANARY_AGGREGATE_TYPE
    assert event[2] == str(identity.canary_id)
    assert event[3] == 1
    assert event[4] == f"event-pipeline-canary/{identity.canary_id}"
    assert event[5] == str(identity.canary_id)
    assert event[6] is None
    assert event[7] == 1
    assert event[8] == {"canary_id": str(identity.canary_id), "synthetic": True}
    assert run[:3] == (identity.canary_id, lab.TENANT, identity.event_id)
    assert run[3] == identity.recorded_at
    assert run[4] is None
    assert inventory_positions == 0
    assert projection_rows == 0


def test_canary_handler_commits_only_synthetic_projection_and_transport_evidence():
    store = PostgresEventPipelineCanaryStore(lab.DATABASE_URL)
    identity = store.start(tenant_id=lab.TENANT)
    claimed = PostgresOutboxStore(lab.DATABASE_URL).claim(
        worker_id="canary-handler-test",
        limit=1,
        lease_seconds=30,
    )[0]
    assert claimed.event_id == identity.event_id
    event = ConsumedEvent.from_envelope(claimed.envelope)
    broker_timestamp = identity.recorded_at + timedelta(milliseconds=20)
    metadata = {
        "transport": "nats-jetstream",
        "transport_message_id": "WMS_EVENTS:42",
        "subject": "spotwo.wms.events.health_check.ping",
        "delivery_count": 1,
        "stream": "WMS_EVENTS",
        "consumer": "EVENT_PIPELINE_CANARY",
        "stream_sequence": 42,
        "consumer_sequence": 7,
        "pending_count": 0,
        "broker_timestamp": broker_timestamp.isoformat(),
    }

    applied = PostgresInboxStore(lab.DATABASE_URL).process_once(
        consumer_name="event_pipeline_canary",
        event=event,
        delivery_metadata=metadata,
        handler=EventPipelineCanaryHandler(consumer_name="event_pipeline_canary"),
    )
    snapshot = store.snapshot(
        canary_id=identity.canary_id,
        consumer_name="event_pipeline_canary",
    )

    assert applied is True
    assert snapshot.projected_at is not None
    assert snapshot.projected_consumer_name == "event_pipeline_canary"
    assert snapshot.receipt_consumer_name == "event_pipeline_canary"
    assert snapshot.transport_message_id == "WMS_EVENTS:42"
    assert snapshot.stream_name == "WMS_EVENTS"
    assert snapshot.durable_name == "EVENT_PIPELINE_CANARY"
    assert snapshot.stream_sequence == 42
    assert snapshot.consumer_sequence == 7
    assert snapshot.delivery_count == 1
    assert snapshot.broker_timestamp == broker_timestamp
    assert snapshot.inbox_to_projection_seconds is not None
    assert snapshot.inbox_to_projection_seconds >= 0

    with lab.connect() as conn:
        assert conn.execute(
            "SELECT count(*) FROM kernel_lab.inventory_positions"
        ).fetchone()[0] == 0
        assert conn.execute(
            "SELECT count(*) FROM kernel_lab.inventory_position_quantity_projection"
        ).fetchone()[0] == 0


def _policy_snapshot(
    *,
    broker_offset: float = 1.0,
    inbox_offset: float = 2.0,
    projection_offset: float = 3.0,
    published_offset: float = 1.5,
    ack_floor: int = 10,
) -> EventPipelineCanaryRunSnapshot:
    started = datetime(2026, 8, 19, 0, 0, tzinfo=timezone.utc)
    canary_id = uuid4()
    event_id = uuid4()
    database = EventPipelineCanaryDbSnapshot(
        canary_id=canary_id,
        tenant_id=lab.TENANT,
        event_id=event_id,
        started_at=started,
        published_at=started + timedelta(seconds=published_offset),
        projected_at=started + timedelta(seconds=projection_offset),
        projected_consumer_name="event_pipeline_canary",
        receipt_consumer_name="event_pipeline_canary",
        inbox_first_seen_at=started + timedelta(seconds=inbox_offset),
        transport_message_id="WMS_EVENTS:10",
        stream_name="WMS_EVENTS",
        durable_name="EVENT_PIPELINE_CANARY",
        stream_sequence=10,
        consumer_sequence=5,
        delivery_count=1,
        broker_timestamp=started + timedelta(seconds=broker_offset),
    )
    observed_at = started + timedelta(seconds=max(projection_offset, 4.0))
    ack = CanaryAckState(
        observed_at=observed_at,
        available=True,
        configuration_valid=True,
        ack_floor_stream_sequence=ack_floor,
        filter_subject="spotwo.wms.events.health_check.ping",
        last_ack_at=observed_at - timedelta(milliseconds=100),
    )
    return EventPipelineCanaryRunSnapshot(
        observed_at=observed_at,
        expected_stream_name="WMS_EVENTS",
        expected_durable_name="EVENT_PIPELINE_CANARY",
        expected_consumer_name="event_pipeline_canary",
        expected_filter_subject="spotwo.wms.events.health_check.ping",
        database=database,
        ack=ack,
    )


def test_slo_policy_uses_measured_stage_latencies_and_bounded_prometheus_labels():
    snapshot = _policy_snapshot(
        published_offset=2.0,
        broker_offset=1.0,
        inbox_offset=3.0,
        projection_offset=5.0,
    )
    report = EventPipelineCanarySloPolicy(
        warning_publish_seconds=2,
        critical_publish_seconds=10,
        warning_delivery_seconds=2,
        critical_delivery_seconds=10,
        warning_projection_seconds=2,
        critical_projection_seconds=10,
        warning_end_to_end_seconds=5,
        critical_end_to_end_seconds=20,
    ).evaluate(snapshot, timed_out=False)

    assert snapshot.complete is True
    assert report.status == "warning"
    assert [(alert.code, alert.severity) for alert in report.alerts] == [
        ("canary_publish_latency", "warning"),
        ("canary_delivery_latency", "warning"),
        ("canary_projection_latency", "warning"),
        ("canary_end_to_end_latency", "warning"),
    ]

    prometheus = render_prometheus(report)
    assert (
        'spotwo_wms_event_pipeline_canary_latency_seconds{stream="WMS_EVENTS",durable="EVENT_PIPELINE_CANARY",consumer="event_pipeline_canary",stage="end_to_end_projection"} 5'
        in prometheus
    )
    for forbidden in (
        str(snapshot.database.canary_id),
        str(snapshot.database.event_id),
        "transport_message_id",
        "stream_sequence",
        "canary_id",
        "event_id",
    ):
        assert forbidden not in prometheus


def test_policy_detects_cross_clock_skew_and_identifies_delivery_timeout_stage():
    skewed = _policy_snapshot(
        broker_offset=-2.0,
        inbox_offset=1.0,
        projection_offset=2.0,
        published_offset=1.0,
    )
    skew_report = EventPipelineCanarySloPolicy(
        warning_publish_seconds=100,
        critical_publish_seconds=200,
        warning_delivery_seconds=100,
        critical_delivery_seconds=200,
        warning_projection_seconds=100,
        critical_projection_seconds=200,
        warning_end_to_end_seconds=100,
        critical_end_to_end_seconds=200,
        clock_skew_tolerance_seconds=1,
    ).evaluate(skewed, timed_out=False)
    assert skew_report.status == "critical"
    assert skew_report.alerts[0].code == "canary_clock_skew_detected"

    started = datetime(2026, 8, 19, 0, 0, tzinfo=timezone.utc)
    observed = started + timedelta(seconds=30)
    waiting_db = EventPipelineCanaryDbSnapshot(
        canary_id=uuid4(),
        tenant_id=lab.TENANT,
        event_id=uuid4(),
        started_at=started,
        published_at=started + timedelta(seconds=1),
        projected_at=None,
        projected_consumer_name=None,
        receipt_consumer_name=None,
        inbox_first_seen_at=None,
        transport_message_id=None,
        stream_name=None,
        durable_name=None,
        stream_sequence=None,
        consumer_sequence=None,
        delivery_count=None,
        broker_timestamp=None,
    )
    waiting = EventPipelineCanaryRunSnapshot(
        observed_at=observed,
        expected_stream_name="WMS_EVENTS",
        expected_durable_name="EVENT_PIPELINE_CANARY",
        expected_consumer_name="event_pipeline_canary",
        expected_filter_subject="spotwo.wms.events.health_check.ping",
        database=waiting_db,
        ack=CanaryAckState(
            observed_at=observed,
            available=True,
            configuration_valid=True,
            ack_floor_stream_sequence=0,
            filter_subject="spotwo.wms.events.health_check.ping",
            last_ack_at=None,
        ),
    )
    timeout_report = EventPipelineCanarySloPolicy().evaluate(
        waiting,
        timed_out=True,
    )
    assert timeout_report.status == "critical"
    assert timeout_report.alerts[-1].code == "canary_delivery_timeout"


class CanaryNatsProbe:
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
        durable_name: str,
        subject_prefix: str,
    ) -> None:
        self._runner.run(
            self._provision(stream_name, durable_name, subject_prefix)
        )

    def close(self, stream_name: str) -> None:
        try:
            self._runner.run(self._delete_if_present(stream_name))
            self._runner.run(self._client.close())
        finally:
            self._runner.close()

    async def _provision(
        self,
        stream_name: str,
        durable_name: str,
        subject_prefix: str,
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
                ack_wait=5,
                max_deliver=3,
                max_ack_pending=10,
                filter_subject=f"{subject_prefix}.{CANARY_EVENT_TYPE}",
            ),
        )

    async def _delete_if_present(self, stream_name: str) -> None:
        try:
            await self._jetstream.delete_stream(stream_name)
        except NotFoundError:
            pass


def test_real_postgres_nats_canary_measures_publish_delivery_projection_and_ack():
    suffix = uuid4().hex.upper()
    stream_name = f"WMS_CANARY_{suffix}"
    durable_name = f"CANARY_{suffix}"
    consumer_name = f"event_pipeline_canary_{suffix.lower()}"
    subject_prefix = f"spotwo.wms.canary.{suffix.lower()}"
    nats_probe = CanaryNatsProbe(NATS_URL)
    nats_probe.provision(
        stream_name=stream_name,
        durable_name=durable_name,
        subject_prefix=subject_prefix,
    )

    canary_store = PostgresEventPipelineCanaryStore(lab.DATABASE_URL)
    identity = canary_store.start(tenant_id=lab.TENANT)
    transport = NatsJetStreamTransport(
        server_url=NATS_URL,
        stream_name=stream_name,
        subject_prefix=subject_prefix,
        client_name=f"canary-publisher-{suffix}",
    )

    try:
        publish_result = PublisherRuntime(
            store=PostgresOutboxStore(lab.DATABASE_URL),
            transport=transport,
            worker_id=f"canary-publisher-{suffix}",
            batch_size=1,
            lease_seconds=30,
            retry_policy=RetryPolicy(
                base_delay_seconds=1,
                max_delay_seconds=1,
                jitter_ratio=0,
                max_attempts=3,
            ),
        ).run_once()
        assert publish_result.claimed == 1
        assert publish_result.published == 1

        with NatsJetStreamPullSource(
            server_url=NATS_URL,
            stream_name=stream_name,
            durable_name=durable_name,
            client_name=f"canary-consumer-{suffix}",
            fetch_timeout_seconds=2,
        ) as source:
            consume_result = InboxConsumerRuntime(
                source=source,
                store=PostgresInboxStore(lab.DATABASE_URL),
                consumer_name=consumer_name,
                handler=EventPipelineCanaryHandler(consumer_name=consumer_name),
            ).run_once()

        assert consume_result.received == 1
        assert consume_result.applied == 1
        assert consume_result.acknowledged == 1
        assert consume_result.event_id == identity.event_id

        observed_at = datetime.now(timezone.utc)
        database = canary_store.snapshot(
            canary_id=identity.canary_id,
            consumer_name=consumer_name,
        )
        with NatsJetStreamCanaryAckObserver(
            server_url=NATS_URL,
            stream_name=stream_name,
            durable_name=durable_name,
            expected_filter_subject=f"{subject_prefix}.{CANARY_EVENT_TYPE}",
        ) as observer:
            ack = observer.snapshot(observed_at=observed_at)

        snapshot = EventPipelineCanaryRunSnapshot(
            observed_at=observed_at,
            expected_stream_name=stream_name,
            expected_durable_name=durable_name,
            expected_consumer_name=consumer_name,
            expected_filter_subject=f"{subject_prefix}.{CANARY_EVENT_TYPE}",
            database=database,
            ack=ack,
        )
        report = EventPipelineCanarySloPolicy(
            warning_publish_seconds=60,
            critical_publish_seconds=120,
            warning_delivery_seconds=60,
            critical_delivery_seconds=120,
            warning_projection_seconds=60,
            critical_projection_seconds=120,
            warning_end_to_end_seconds=60,
            critical_end_to_end_seconds=120,
            clock_skew_tolerance_seconds=5,
        ).evaluate(snapshot, timed_out=False)

        assert database.published_at is not None
        assert database.projected_at is not None
        assert database.broker_timestamp is not None
        assert database.inbox_first_seen_at is not None
        assert database.stream_name == stream_name
        assert database.durable_name == durable_name
        assert database.stream_sequence is not None
        assert ack.configuration_valid is True
        assert snapshot.ack_confirmed is True
        assert snapshot.complete is True
        assert report.status == "ok"
        assert report.alerts == ()
        assert database.outbox_publish_confirm_seconds is not None
        assert database.outbox_publish_confirm_seconds >= 0
        assert database.inbox_to_projection_seconds is not None
        assert database.inbox_to_projection_seconds >= 0
        assert database.end_to_end_projection_seconds is not None
        assert database.end_to_end_projection_seconds >= 0

        with lab.connect() as conn:
            assert conn.execute(
                "SELECT count(*) FROM kernel_lab.inventory_positions"
            ).fetchone()[0] == 0
            assert conn.execute(
                "SELECT count(*) FROM kernel_lab.inventory_position_quantity_projection"
            ).fetchone()[0] == 0
    finally:
        transport.close()
        nats_probe.close(stream_name)
