from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import nats
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy, StorageType, StreamConfig

import conftest as lab
from event_pipeline_telemetry import EventPipelineHealthCollector, render_prometheus

NATS_URL = os.getenv("KERNEL_LAB_NATS_URL", "nats://127.0.0.1:54222")


class FakeSnapshot:
    def __init__(self, marker: str):
        self.marker = marker

    def to_dict(self) -> dict:
        return {"marker": self.marker}


class FakeStore:
    def __init__(self, marker: str):
        self.marker = marker

    def snapshot(self, **_kwargs):
        return FakeSnapshot(self.marker)


class RaisingStore:
    def snapshot(self, **_kwargs):
        raise ConnectionError("intentionally unavailable")


class FakePolicy:
    def __init__(self, status: str = "ok", *alerts: SimpleNamespace):
        self.status = status
        self.alerts = alerts

    def evaluate(self, snapshot: FakeSnapshot):
        return SimpleNamespace(
            status=self.status,
            alerts=self.alerts,
            snapshot=snapshot,
        )


def fake_alert(code: str, severity: str, observed_value=1, threshold=1):
    return SimpleNamespace(
        code=code,
        severity=severity,
        observed_value=observed_value,
        threshold=threshold,
    )


def fake_collector() -> EventPipelineHealthCollector:
    collector = EventPipelineHealthCollector("postgresql://unused", "nats://unused")
    collector.outbox_store = FakeStore("outbox")
    collector.stream_store = FakeStore("nats_stream")
    collector.consumer_store = FakeStore("nats_consumer")
    collector.malformed_store = FakeStore("malformed_delivery")
    collector.consumer_failure_store = FakeStore("consumer_failure")
    collector.projection_gap_store = FakeStore("projection_gap")
    collector.outbox_policy = FakePolicy()
    collector.stream_policy = FakePolicy()
    collector.consumer_policy = FakePolicy()
    collector.malformed_policy = FakePolicy()
    collector.consumer_failure_policy = FakePolicy()
    collector.projection_gap_policy = FakePolicy()
    return collector


def collect_fake(collector: EventPipelineHealthCollector):
    return collector.collect(
        stream_name="WMS_EVENTS",
        durable_name="POSITION_PROJECTOR",
        consumer_name="position_projection",
        observed_at=datetime(2026, 8, 19, 0, 0, tzinfo=timezone.utc),
    )


def test_pipeline_is_healthy_only_when_every_component_is_healthy():
    report = collect_fake(fake_collector())

    assert report.status == "ok"
    assert report.root_cause_candidate is None
    assert report.degraded_components == ()
    assert [component.component for component in report.components] == [
        "outbox",
        "nats_stream",
        "nats_consumer",
        "malformed_delivery",
        "consumer_failure",
        "projection_gap",
    ]

    payload = report.to_dict()
    assert payload["collection"] == {"read_only": True, "atomic": False}
    assert payload["components"]["outbox"]["telemetry"] == {"marker": "outbox"}


def test_root_cause_candidate_prefers_severity_before_topology_order():
    collector = fake_collector()
    collector.outbox_policy = FakePolicy(
        "warning",
        fake_alert("outbox_ready_age_exceeded", "warning"),
    )
    collector.projection_gap_policy = FakePolicy(
        "critical",
        fake_alert("projection_gap_age_exceeded", "critical"),
    )

    report = collect_fake(collector)

    assert report.status == "critical"
    assert report.degraded_components == ("outbox", "projection_gap")
    assert report.root_cause_candidate is not None
    assert report.root_cause_candidate.component == "projection_gap"
    assert report.root_cause_candidate.code == "projection_gap_age_exceeded"


def test_root_cause_candidate_is_upstream_first_when_severity_ties():
    collector = fake_collector()
    collector.outbox_policy = FakePolicy(
        "critical",
        fake_alert("outbox_ready_age_exceeded", "critical"),
    )
    collector.consumer_policy = FakePolicy(
        "critical",
        fake_alert("nats_consumer_delivery_stalled", "critical"),
    )
    collector.projection_gap_policy = FakePolicy(
        "critical",
        fake_alert("projection_gap_age_exceeded", "critical"),
    )

    report = collect_fake(collector)

    assert report.status == "critical"
    assert report.root_cause_candidate is not None
    assert report.root_cause_candidate.component == "outbox"
    assert report.root_cause_candidate.code == "outbox_ready_age_exceeded"


def test_component_collection_failure_is_explicit_critical_health():
    collector = fake_collector()
    collector.stream_store = RaisingStore()

    report = collect_fake(collector)
    stream = next(component for component in report.components if component.component == "nats_stream")

    assert report.status == "critical"
    assert stream.available is False
    assert stream.error_code == "nats_stream_telemetry_unavailable"
    assert stream.telemetry is None
    assert stream.alerts[0].synthetic is True
    assert report.root_cause_candidate == stream.alerts[0]


def test_prometheus_exports_only_bounded_pipeline_identity_and_codes():
    collector = fake_collector()
    collector.consumer_policy = FakePolicy(
        "warning",
        fake_alert(
            "nats_consumer_pending_backlog",
            "warning",
            observed_value="raw-sensitive-value",
            threshold=100,
        ),
    )
    report = collect_fake(collector)

    prometheus = render_prometheus(report)

    assert (
        'spotwo_wms_event_pipeline_health_status{stream="WMS_EVENTS",durable="POSITION_PROJECTOR",consumer="position_projection"} 1'
        in prometheus
    )
    assert (
        'component="nats_consumer",code="nats_consumer_pending_backlog",severity="warning"'
        in prometheus
    )
    assert "raw-sensitive-value" not in prometheus
    for forbidden in ("event_id", "payload", "subject=", "peer=", "stream_sequence"):
        assert forbidden not in prometheus


def test_strict_work_state_health_omits_inapplicable_position_gap_component():
    collector = fake_collector()
    collector.projection_gap_store = RaisingStore()

    report = collector.collect(
        stream_name="WMS_EVENTS",
        durable_name="WORK_STATE_PROJECTOR",
        consumer_name="warehouse_work_state_projection",
        projection_gap_monitor="none",
        observed_at=datetime(2026, 8, 19, 0, 0, tzinfo=timezone.utc),
    )

    assert report.status == "ok"
    assert report.degraded_components == ()
    assert [component.component for component in report.components] == [
        "outbox",
        "nats_stream",
        "nats_consumer",
        "malformed_delivery",
        "consumer_failure",
    ]
    assert "projection_gap" not in report.to_dict()["components"]
    assert "projection_gap" not in render_prometheus(report)


def test_unknown_projection_gap_monitor_fails_closed():
    collector = fake_collector()
    try:
        collector.collect(
            stream_name="WMS_EVENTS",
            durable_name="WORK_STATE_PROJECTOR",
            consumer_name="warehouse_work_state_projection",
            projection_gap_monitor="invented",
        )
    except ValueError as exc:
        assert "projection_gap_monitor" in str(exc)
    else:
        raise AssertionError("unknown projection monitor must be rejected")

class LivePipelineProbe:
    def __init__(self, server_url: str):
        self._runner = asyncio.Runner()
        self._client = self._runner.run(
            nats.connect(servers=[server_url], allow_reconnect=False, connect_timeout=2)
        )
        self._jetstream = self._client.jetstream()

    def provision(self, *, stream_name: str, durable_name: str, subject: str) -> None:
        self._runner.run(self._provision(stream_name, durable_name, subject))

    def state(self, *, stream_name: str, durable_name: str):
        return self._runner.run(self._state(stream_name, durable_name))

    def close(self, stream_name: str) -> None:
        try:
            self._runner.run(self._jetstream.delete_stream(stream_name))
            self._runner.run(self._client.close())
        finally:
            self._runner.close()

    async def _provision(self, stream_name: str, durable_name: str, subject: str) -> None:
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

    async def _state(self, stream_name: str, durable_name: str):
        stream = await self._jetstream.stream_info(stream_name)
        consumer = await self._jetstream.consumer_info(stream_name, durable_name)
        return (
            stream.state.messages,
            stream.state.bytes,
            stream.state.first_seq,
            stream.state.last_seq,
            consumer.delivered.consumer_seq,
            consumer.delivered.stream_seq,
            consumer.ack_floor.consumer_seq,
            consumer.ack_floor.stream_seq,
        )


def pipeline_database_counts() -> tuple[int, ...]:
    with lab.connect() as conn:
        return tuple(
            conn.execute(f"SELECT count(*) FROM kernel_lab.{table}").fetchone()[0]
            for table in (
                "domain_event_outbox",
                "domain_event_inbox",
                "domain_event_consumer_failures",
                "nats_jetstream_consumer_poison_deliveries",
                "inventory_position_projection_pending",
            )
        )


def test_live_pipeline_collection_is_read_only_across_postgres_and_jetstream():
    suffix = uuid4().hex.upper()
    stream_name = f"WMS_PIPELINE_{suffix}"
    durable_name = f"PIPELINE_{suffix}"
    consumer_name = f"position_projection_{suffix.lower()}"
    subject = f"spotwo.wms.pipeline.{suffix.lower()}"
    probe = LivePipelineProbe(NATS_URL)
    probe.provision(
        stream_name=stream_name,
        durable_name=durable_name,
        subject=subject,
    )

    try:
        broker_before = probe.state(stream_name=stream_name, durable_name=durable_name)
        database_before = pipeline_database_counts()

        report = EventPipelineHealthCollector(lab.DATABASE_URL, NATS_URL).collect(
            stream_name=stream_name,
            durable_name=durable_name,
            consumer_name=consumer_name,
        )

        broker_after = probe.state(stream_name=stream_name, durable_name=durable_name)
        database_after = pipeline_database_counts()

        assert report.status == "ok"
        assert report.root_cause_candidate is None
        assert all(component.available for component in report.components)
        assert broker_after == broker_before
        assert database_after == database_before
    finally:
        probe.close(stream_name)
