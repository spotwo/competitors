from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import uuid4

import nats
import pytest
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy, StorageType, StreamConfig
from nats.js.errors import NotFoundError

import conftest as lab
from nats_consumer_telemetry import (
    NatsConsumerAlertPolicy,
    NatsConsumerTelemetrySnapshot,
    NatsJetStreamConsumerTelemetryStore,
    render_prometheus,
    snapshot_from_consumer_info,
)

NATS_URL = lab.os.getenv("KERNEL_LAB_NATS_URL", "nats://127.0.0.1:54222") if hasattr(lab, "os") else "nats://127.0.0.1:54222"


def fake_info(
    *,
    observed_at: datetime,
    pending: int = 0,
    ack_pending: int = 0,
    redelivered: int = 0,
    waiting: int = 0,
    delivered_consumer: int = 10,
    delivered_stream: int = 20,
    ack_consumer: int = 10,
    ack_stream: int = 20,
    last_delivery_seconds_ago: int | None = 10,
    last_ack_seconds_ago: int | None = 5,
    max_ack_pending: int = 10,
    paused: bool = False,
    ack_policy=AckPolicy.EXPLICIT,
    deliver_subject=None,
):
    return SimpleNamespace(
        name="POSITION_PROJECTOR",
        stream_name="WMS_EVENTS",
        created=observed_at - timedelta(hours=1),
        config=SimpleNamespace(
            durable_name="POSITION_PROJECTOR",
            ack_policy=ack_policy,
            deliver_subject=deliver_subject,
            max_ack_pending=max_ack_pending,
            max_deliver=5,
            ack_wait=30.0,
        ),
        delivered=SimpleNamespace(
            consumer_seq=delivered_consumer,
            stream_seq=delivered_stream,
            last_active=(
                None
                if last_delivery_seconds_ago is None
                else observed_at - timedelta(seconds=last_delivery_seconds_ago)
            ),
        ),
        ack_floor=SimpleNamespace(
            consumer_seq=ack_consumer,
            stream_seq=ack_stream,
            last_active=(
                None
                if last_ack_seconds_ago is None
                else observed_at - timedelta(seconds=last_ack_seconds_ago)
            ),
        ),
        num_pending=pending,
        num_ack_pending=ack_pending,
        num_redelivered=redelivered,
        num_waiting=waiting,
        paused=paused,
    )


def test_snapshot_maps_consumer_info_and_uses_backlog_sensitive_activity_age():
    observed_at = datetime(2026, 8, 19, 1, 0, tzinfo=timezone.utc)
    snapshot = snapshot_from_consumer_info(
        fake_info(
            observed_at=observed_at,
            pending=25,
            ack_pending=8,
            redelivered=1,
            delivered_consumer=12,
            delivered_stream=42,
            ack_consumer=10,
            ack_stream=40,
            last_delivery_seconds_ago=600,
            last_ack_seconds_ago=400,
        ),
        stream_name="WMS_EVENTS",
        durable_name="POSITION_PROJECTOR",
        observed_at=observed_at,
    )

    assert snapshot.pending_messages == 25
    assert snapshot.ack_pending_messages == 8
    assert snapshot.redelivered_messages == 1
    assert snapshot.delivered_consumer_sequence == 12
    assert snapshot.ack_floor_consumer_sequence == 10
    assert snapshot.delivery_activity_age_seconds == 600
    assert snapshot.ack_activity_age_seconds == 400
    assert snapshot.delivery_backlog_stall_age_seconds == 600
    assert snapshot.ack_backlog_stall_age_seconds == 400
    assert snapshot.ack_pending_ratio == 0.8
    assert snapshot.configuration_valid is True

    payload = snapshot.to_dict()
    assert payload["backlog"]["total"] == 33
    assert payload["activity"]["age_seconds"]["delivery_backlog_stall"] == 600


def test_idle_consumer_does_not_alarm_on_old_activity_without_backlog():
    observed_at = datetime(2026, 8, 19, 1, 0, tzinfo=timezone.utc)
    snapshot = snapshot_from_consumer_info(
        fake_info(
            observed_at=observed_at,
            pending=0,
            ack_pending=0,
            redelivered=0,
            last_delivery_seconds_ago=7200,
            last_ack_seconds_ago=7190,
        ),
        stream_name="WMS_EVENTS",
        durable_name="POSITION_PROJECTOR",
        observed_at=observed_at,
    )

    report = NatsConsumerAlertPolicy().evaluate(snapshot)

    assert snapshot.delivery_activity_age_seconds == 7200
    assert snapshot.delivery_backlog_stall_age_seconds is None
    assert snapshot.ack_backlog_stall_age_seconds is None
    assert report.status == "ok"
    assert report.alerts == ()


def test_alert_policy_distinguishes_backlog_redelivery_capacity_and_stall():
    observed_at = datetime(2026, 8, 19, 1, 0, tzinfo=timezone.utc)
    snapshot = snapshot_from_consumer_info(
        fake_info(
            observed_at=observed_at,
            pending=100,
            ack_pending=8,
            redelivered=1,
            last_delivery_seconds_ago=300,
            last_ack_seconds_ago=300,
        ),
        stream_name="WMS_EVENTS",
        durable_name="POSITION_PROJECTOR",
        observed_at=observed_at,
    )

    report = NatsConsumerAlertPolicy().evaluate(snapshot)

    assert report.status == "warning"
    assert [(alert.code, alert.severity) for alert in report.alerts] == [
        ("nats_consumer_pending_backlog", "warning"),
        ("nats_consumer_redeliveries", "warning"),
        ("nats_consumer_ack_capacity", "warning"),
        ("nats_consumer_delivery_stalled", "warning"),
        ("nats_consumer_ack_stalled", "warning"),
    ]

    prometheus = render_prometheus(report)
    assert (
        'spotwo_wms_nats_consumer_pending_messages{stream="WMS_EVENTS",durable="POSITION_PROJECTOR"} 100'
        in prometheus
    )
    assert (
        'spotwo_wms_nats_consumer_activity_age_seconds{kind="delivery_backlog_stall",stream="WMS_EVENTS",durable="POSITION_PROJECTOR"} 300'
        in prometheus
    )
    for forbidden in (
        "event_id",
        "payload_sha256",
        "subject=",
        "worker_id",
        "client_name",
    ):
        assert forbidden not in prometheus


def test_critical_configuration_and_pressure_promote_health():
    observed_at = datetime(2026, 8, 19, 1, 0, tzinfo=timezone.utc)
    info = fake_info(
        observed_at=observed_at,
        pending=1000,
        ack_pending=10,
        redelivered=10,
        last_delivery_seconds_ago=1800,
        last_ack_seconds_ago=1800,
        paused=True,
        ack_policy=AckPolicy.NONE,
    )
    snapshot = snapshot_from_consumer_info(
        info,
        stream_name="WMS_EVENTS",
        durable_name="POSITION_PROJECTOR",
        observed_at=observed_at,
    )

    report = NatsConsumerAlertPolicy().evaluate(snapshot)

    assert snapshot.configuration_valid is False
    assert report.status == "critical"
    assert ("nats_consumer_configuration_invalid", "critical") in [
        (alert.code, alert.severity) for alert in report.alerts
    ]
    assert ("nats_consumer_pending_backlog", "critical") in [
        (alert.code, alert.severity) for alert in report.alerts
    ]
    assert ("nats_consumer_paused_with_backlog", "warning") in [
        (alert.code, alert.severity) for alert in report.alerts
    ]


def test_first_unacked_delivery_uses_delivery_time_instead_of_consumer_age():
    observed_at = datetime(2026, 8, 19, 1, 0, tzinfo=timezone.utc)
    snapshot = snapshot_from_consumer_info(
        fake_info(
            observed_at=observed_at,
            pending=0,
            ack_pending=1,
            delivered_consumer=1,
            delivered_stream=1,
            ack_consumer=0,
            ack_stream=0,
            last_delivery_seconds_ago=2,
            last_ack_seconds_ago=None,
        ),
        stream_name="WMS_EVENTS",
        durable_name="POSITION_PROJECTOR",
        observed_at=observed_at,
    )

    assert snapshot.created_age_seconds == 3600
    assert snapshot.ack_backlog_stall_age_seconds == 2
    assert NatsConsumerAlertPolicy().evaluate(snapshot).status == "ok"


def test_snapshot_rejects_wrong_identity_missing_state_and_invalid_sequences():
    observed_at = datetime(2026, 8, 19, 1, 0, tzinfo=timezone.utc)
    info = fake_info(observed_at=observed_at)

    with pytest.raises(ValueError, match="another stream"):
        snapshot_from_consumer_info(
            info,
            stream_name="OTHER",
            durable_name="POSITION_PROJECTOR",
            observed_at=observed_at,
        )

    with pytest.raises(ValueError, match="timezone"):
        snapshot_from_consumer_info(
            info,
            stream_name="WMS_EVENTS",
            durable_name="POSITION_PROJECTOR",
            observed_at=datetime(2026, 8, 19, 1, 0),
        )

    bad = fake_info(
        observed_at=observed_at,
        delivered_consumer=2,
        delivered_stream=2,
        ack_consumer=3,
        ack_stream=3,
    )
    with pytest.raises(ValueError, match="ack floor"):
        snapshot_from_consumer_info(
            bad,
            stream_name="WMS_EVENTS",
            durable_name="POSITION_PROJECTOR",
            observed_at=observed_at,
        )


def test_alert_policy_inputs_are_bounded_and_ordered():
    with pytest.raises(ValueError, match="pending message thresholds"):
        NatsConsumerAlertPolicy(warning_pending_messages=1000, critical_pending_messages=100)
    with pytest.raises(ValueError, match="redelivery thresholds"):
        NatsConsumerAlertPolicy(warning_redelivered_messages=0)
    with pytest.raises(ValueError, match="stalled activity thresholds"):
        NatsConsumerAlertPolicy(warning_stalled_seconds=0)
    with pytest.raises(ValueError, match="ACK capacity"):
        NatsConsumerAlertPolicy(warning_ack_capacity_ratio=0.9, critical_ack_capacity_ratio=0.8)


class NatsTelemetryProbe:
    def __init__(self, server_url: str):
        self._runner = asyncio.Runner()
        self._client = self._runner.run(
            nats.connect(servers=[server_url], allow_reconnect=False, connect_timeout=2)
        )
        self._jetstream = self._client.jetstream()

    def provision(self, *, stream_name: str, durable_name: str, subject: str) -> None:
        self._runner.run(self._provision(stream_name, durable_name, subject))

    def publish(self, subject: str, count: int) -> None:
        self._runner.run(self._publish(subject, count))

    def fetch_and_ack_one(self, *, stream_name: str, durable_name: str) -> None:
        self._runner.run(self._fetch_and_ack_one(stream_name, durable_name))

    def close(self, stream_name: str) -> None:
        try:
            self._runner.run(self._delete_if_present(stream_name))
            self._runner.run(self._client.close())
        finally:
            self._runner.close()

    async def _provision(self, stream_name: str, durable_name: str, subject: str) -> None:
        await self._delete_if_present(stream_name)
        await self._jetstream.add_stream(
            StreamConfig(
                name=stream_name,
                subjects=[subject],
                storage=StorageType.FILE,
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
                filter_subject=subject,
            ),
        )

    async def _publish(self, subject: str, count: int) -> None:
        for ordinal in range(count):
            await self._jetstream.publish(subject, f"health-{ordinal}".encode())

    async def _fetch_and_ack_one(self, stream_name: str, durable_name: str) -> None:
        subscription = await self._jetstream.pull_subscribe_bind(
            stream=stream_name,
            durable=durable_name,
        )
        messages = await subscription.fetch(batch=1, timeout=2)
        await messages[0].ack_sync(timeout=2)

    async def _delete_if_present(self, stream_name: str) -> None:
        try:
            await self._jetstream.delete_stream(stream_name)
        except NotFoundError:
            pass


def test_read_only_store_observes_real_jetstream_backlog_and_progress():
    suffix = uuid4().hex.upper()
    stream_name = f"WMS_HEALTH_{suffix}"
    durable_name = f"HEALTH_{suffix}"
    subject = f"spotwo.wms.health.{suffix.lower()}"
    probe = NatsTelemetryProbe(NATS_URL)
    probe.provision(
        stream_name=stream_name,
        durable_name=durable_name,
        subject=subject,
    )

    try:
        probe.publish(subject, 3)
        store = NatsJetStreamConsumerTelemetryStore(NATS_URL)
        before = store.snapshot(stream_name=stream_name, durable_name=durable_name)

        assert before.stream_name == stream_name
        assert before.durable_name == durable_name
        assert before.configuration_valid is True
        assert before.pending_messages == 3
        assert before.ack_pending_messages == 0
        assert before.delivered_consumer_sequence == 0
        assert before.ack_floor_consumer_sequence == 0
        assert before.max_ack_pending == 10

        pressure = NatsConsumerAlertPolicy(
            warning_pending_messages=2,
            critical_pending_messages=3,
        ).evaluate(before)
        assert pressure.status == "critical"
        assert pressure.alerts[0].code == "nats_consumer_pending_backlog"

        probe.fetch_and_ack_one(stream_name=stream_name, durable_name=durable_name)
        after = store.snapshot(stream_name=stream_name, durable_name=durable_name)

        assert after.pending_messages == 2
        assert after.ack_pending_messages == 0
        assert after.delivered_consumer_sequence >= 1
        assert after.ack_floor_consumer_sequence >= 1
        assert after.last_delivery_at is not None
        assert after.last_ack_at is not None
    finally:
        probe.close(stream_name)
