from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import uuid4

import nats
from nats.js.api import StorageType, StreamConfig

from nats_stream_telemetry import (
    NatsJetStreamStreamTelemetryStore,
    NatsStreamAlertPolicy,
    render_prometheus,
    snapshot_from_stream_info,
)

NATS_URL = os.getenv("KERNEL_LAB_NATS_URL", "nats://127.0.0.1:54222")


def test_snapshot_and_policy_cover_capacity_loss_and_replica_health():
    observed_at = datetime(2026, 8, 19, 0, 45, tzinfo=timezone.utc)
    info = SimpleNamespace(
        config=SimpleNamespace(
            name="WMS_EVENTS",
            max_msgs=100,
            max_bytes=1000,
            max_age=3600.0,
            num_replicas=3,
            storage=SimpleNamespace(value="file"),
            retention=SimpleNamespace(value="limits"),
            discard=SimpleNamespace(value="old"),
        ),
        state=SimpleNamespace(
            messages=95,
            bytes=900,
            first_seq=7,
            last_seq=101,
            first_ts=observed_at - timedelta(minutes=30),
            last_ts=observed_at - timedelta(seconds=5),
            consumer_count=2,
            num_deleted=6,
            lost=SimpleNamespace(msgs=[44, 45]),
        ),
        cluster=SimpleNamespace(
            leader="S1",
            replicas=[
                SimpleNamespace(current=True, offline=False, lag=0),
                SimpleNamespace(current=False, offline=False, lag=2500),
            ],
        ),
    )

    snapshot = snapshot_from_stream_info(
        info,
        stream_name="WMS_EVENTS",
        observed_at=observed_at,
    )
    report = NatsStreamAlertPolicy().evaluate(snapshot)

    assert snapshot.message_capacity_ratio == 0.95
    assert snapshot.byte_capacity_ratio == 0.9
    assert snapshot.oldest_message_age_seconds == 1800
    assert snapshot.lost_messages == 2
    assert snapshot.not_current_followers == 1
    assert snapshot.max_replica_lag_messages == 2500
    assert report.status == "critical"
    assert [(alert.code, alert.severity) for alert in report.alerts] == [
        ("nats_stream_lost_messages", "critical"),
        ("nats_stream_followers_not_current", "warning"),
        ("nats_stream_replica_lag", "warning"),
        ("nats_stream_message_capacity", "critical"),
        ("nats_stream_byte_capacity", "warning"),
    ]

    prometheus = render_prometheus(report)
    assert 'spotwo_wms_nats_stream_messages{stream="WMS_EVENTS"} 95' in prometheus
    assert 'spotwo_wms_nats_stream_capacity_ratio{stream="WMS_EVENTS",kind="messages"} 0.95' in prometheus
    for forbidden in ("S1", "subject", "first_seq=", "payload", "event_id"):
        assert forbidden not in prometheus


def test_empty_standalone_stream_is_healthy():
    observed_at = datetime(2026, 8, 19, 0, 45, tzinfo=timezone.utc)
    info = SimpleNamespace(
        config=SimpleNamespace(
            name="EMPTY",
            max_msgs=-1,
            max_bytes=-1,
            max_age=0.0,
            num_replicas=1,
            storage=SimpleNamespace(value="file"),
            retention=SimpleNamespace(value="limits"),
            discard=SimpleNamespace(value="old"),
        ),
        state=SimpleNamespace(
            messages=0,
            bytes=0,
            first_seq=0,
            last_seq=0,
            first_ts=None,
            last_ts=None,
            consumer_count=0,
            num_deleted=0,
            lost=None,
        ),
        cluster=None,
    )

    snapshot = snapshot_from_stream_info(info, stream_name="EMPTY", observed_at=observed_at)
    report = NatsStreamAlertPolicy().evaluate(snapshot)

    assert snapshot.message_capacity_ratio is None
    assert snapshot.byte_capacity_ratio is None
    assert snapshot.cluster_leader_present is None
    assert report.status == "ok"
    assert report.alerts == ()


def test_live_stream_info_is_read_only_and_warns_on_finite_message_capacity():
    suffix = uuid4().hex.upper()
    stream_name = f"WMS_STREAM_HEALTH_{suffix}"
    subject = f"spotwo.wms.stream-health.{suffix.lower()}"

    async def provision_and_publish():
        client = await nats.connect(servers=[NATS_URL], allow_reconnect=False, connect_timeout=2)
        js = client.jetstream()
        try:
            await js.add_stream(
                StreamConfig(
                    name=stream_name,
                    subjects=[subject],
                    storage=StorageType.FILE,
                    max_msgs=10,
                )
            )
            for ordinal in range(8):
                await js.publish(subject, f"event-{ordinal}".encode())
            return await js.stream_info(stream_name)
        finally:
            await client.close()

    async def read_and_delete():
        client = await nats.connect(servers=[NATS_URL], allow_reconnect=False, connect_timeout=2)
        js = client.jetstream()
        try:
            info = await js.stream_info(stream_name)
            await js.delete_stream(stream_name)
            return info
        finally:
            await client.close()

    with asyncio.Runner() as runner:
        before = runner.run(provision_and_publish())

    try:
        snapshot = NatsJetStreamStreamTelemetryStore(NATS_URL).snapshot(stream_name=stream_name)
        report = NatsStreamAlertPolicy().evaluate(snapshot)

        assert snapshot.messages == 8
        assert snapshot.max_messages == 10
        assert snapshot.message_capacity_ratio == 0.8
        assert snapshot.lost_messages == 0
        assert report.status == "warning"
        assert [(alert.code, alert.severity) for alert in report.alerts] == [
            ("nats_stream_message_capacity", "warning"),
        ]
    finally:
        with asyncio.Runner() as runner:
            after = runner.run(read_and_delete())

    assert after.state.messages == before.state.messages
    assert after.state.bytes == before.state.bytes
    assert after.state.first_seq == before.state.first_seq
    assert after.state.last_seq == before.state.last_seq
