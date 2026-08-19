from __future__ import annotations

import asyncio
import os
from types import SimpleNamespace
from uuid import uuid4

import nats
from nats.js.api import (
    AckPolicy,
    ConsumerConfig,
    DeliverPolicy,
    RetentionPolicy,
    StorageType,
    StreamConfig,
)
from nats.js.errors import NotFoundError

import conftest as lab
from event_pipeline_readiness import EventPipelineDeploymentRegistry
from event_pipeline_topology import (
    ConsumerTopologyExpectation,
    EventPipelineTopologyExpectation,
    NatsJetStreamTopologyInspector,
    StreamTopologyExpectation,
    evaluate_topology,
    render_prometheus,
)
from event_pipeline_topology_config import DeploymentTopologyConfig

NATS_URL = os.getenv("KERNEL_LAB_NATS_URL", "nats://127.0.0.1:54222")


def expectation(
    *,
    stream_name: str = "WMS_EVENTS",
    subject_prefix: str = "spotwo.wms.events",
    business_durable: str = "POSITION_PROJECTOR",
    canary_durable: str = "EVENT_PIPELINE_CANARY",
) -> EventPipelineTopologyExpectation:
    return EventPipelineTopologyExpectation(
        stream_name=stream_name,
        stream=StreamTopologyExpectation(
            subjects=(f"{subject_prefix}.>",),
            storage="file",
            retention="limits",
            replicas=1,
            duplicate_window_seconds=120,
        ),
        business_consumer=ConsumerTopologyExpectation(
            durable_name=business_durable,
            delivery_mode="pull",
            deliver_policy="all",
            ack_policy="explicit",
            filter_subject=f"{subject_prefix}.inventory.position.changed",
            max_ack_pending=10,
            max_deliver=3,
        ),
        canary_consumer=ConsumerTopologyExpectation(
            durable_name=canary_durable,
            delivery_mode="pull",
            deliver_policy="all",
            ack_policy="explicit",
            filter_subject=f"{subject_prefix}.health_check.ping",
            max_ack_pending=10,
            max_deliver=3,
        ),
    )


def stream_info(**overrides):
    values = {
        "subjects": ["spotwo.wms.events.>"],
        "storage": "file",
        "retention": "limits",
        "num_replicas": 1,
        "duplicate_window": 120.0,
    }
    values.update(overrides)
    return SimpleNamespace(config=SimpleNamespace(**values))


def consumer_info(*, canary: bool = False, **overrides):
    values = {
        "deliver_subject": None,
        "deliver_policy": "all",
        "ack_policy": "explicit",
        "filter_subject": (
            "spotwo.wms.events.health_check.ping"
            if canary
            else "spotwo.wms.events.inventory.position.changed"
        ),
        "max_ack_pending": 10,
        "max_deliver": 3,
    }
    values.update(overrides)
    return SimpleNamespace(config=SimpleNamespace(**values))


def test_exact_topology_match_is_healthy_and_prometheus_labels_are_bounded():
    report = evaluate_topology(
        expectation(),
        stream_info=stream_info(),
        business_consumer_info=consumer_info(),
        canary_consumer_info=consumer_info(canary=True),
    )

    assert report.status == "ok"
    assert report.drift == ()
    assert all(resource.exists for resource in report.resources)

    prometheus = render_prometheus(report, pipeline_id="inventory-position-projection")
    assert (
        'spotwo_wms_event_pipeline_topology_match{pipeline="inventory-position-projection"} 1'
        in prometheus
    )
    for forbidden in (
        "WMS_EVENTS",
        "POSITION_PROJECTOR",
        "EVENT_PIPELINE_CANARY",
        "spotwo.wms.events.>",
        "inventory.position.changed",
        "health_check.ping",
    ):
        assert forbidden not in prometheus


def test_drift_reports_exact_resource_fields_without_suppressing_other_mismatches():
    report = evaluate_topology(
        expectation(),
        stream_info=stream_info(
            subjects=["spotwo.wms.events.inventory.>"],
            num_replicas=3,
        ),
        business_consumer_info=consumer_info(
            deliver_subject="deliver.position",
            ack_policy="all",
            max_ack_pending=100,
        ),
        canary_consumer_info=None,
    )

    assert report.status == "critical"
    assert [(item.resource, item.field, item.code) for item in report.drift] == [
        ("stream", "subjects", "stream_subjects_mismatch"),
        ("stream", "replicas", "stream_replicas_mismatch"),
        (
            "business_consumer",
            "delivery_mode",
            "business_consumer_delivery_mode_mismatch",
        ),
        (
            "business_consumer",
            "ack_policy",
            "business_consumer_ack_policy_mismatch",
        ),
        (
            "business_consumer",
            "max_ack_pending",
            "business_consumer_max_ack_pending_mismatch",
        ),
        ("canary_consumer", "exists", "canary_consumer_missing"),
    ]


def deployment_spec():
    registry_data = {
        "version": 1,
        "pipelines": [
            {
                "id": "inventory-position-projection",
                "enabled": True,
                "runtime": {
                    "database_url_env": "DATABASE_URL",
                    "nats_url_env": "NATS_URL",
                    "watchdog_state_path_env": "WATCHDOG_STATE_PATH",
                },
                "transport": {
                    "stream": "WMS_EVENTS",
                    "subject_prefix": "spotwo.wms.events",
                },
                "consumer": {
                    "durable": "POSITION_PROJECTOR",
                    "inbox_consumer_name": "position_projection",
                    "malformed_lookback_seconds": 300,
                },
                "canary": {
                    "durable": "EVENT_PIPELINE_CANARY",
                    "consumer_name": "event_pipeline_canary",
                    "cadence_seconds": 60,
                    "timeout_seconds": 30,
                },
                "slo": {
                    "availability_target": 0.999,
                    "latency_target_ratio": 0.99,
                    "latency_target_seconds": 5,
                    "stale_after_seconds": 180,
                    "warning_consecutive_failures": 2,
                    "critical_consecutive_failures": 3,
                    "warning_slow_burn_rate": 3.0,
                    "critical_fast_burn_rate": 14.4,
                },
                "watchdog": {
                    "start_grace_seconds": 30,
                    "warning_missing_executions": 1,
                    "critical_missing_executions": 2,
                    "warning_scheduler_lag_seconds": 15,
                    "critical_scheduler_lag_seconds": 60,
                    "execution_timeout_seconds": 45,
                },
            }
        ],
    }
    return EventPipelineDeploymentRegistry.from_mapping(registry_data).pipelines[0]


def topology_mapping():
    return {
        "stream": {
            "subjects": ["spotwo.wms.events.>"],
            "storage": "file",
            "retention": "limits",
            "replicas": 1,
            "duplicate_window_seconds": 120,
        },
        "business_consumer": {
            "delivery_mode": "pull",
            "deliver_policy": "all",
            "ack_policy": "explicit",
            "filter_subject": "spotwo.wms.events.inventory.position.changed",
            "max_ack_pending": 10,
            "max_deliver": 3,
        },
        "canary_consumer": {
            "delivery_mode": "pull",
            "deliver_policy": "all",
            "ack_policy": "explicit",
            "filter_subject": "spotwo.wms.events.health_check.ping",
            "max_ack_pending": 10,
            "max_deliver": 3,
        },
    }


def test_topology_config_binds_subject_contract_to_deployment_identity():
    spec = deployment_spec()
    topology = topology_mapping()

    parsed = DeploymentTopologyConfig.from_mapping(topology).expectation(spec)
    assert parsed.stream_name == "WMS_EVENTS"
    assert parsed.business_consumer.durable_name == "POSITION_PROJECTOR"
    assert parsed.business_consumer.filter_subject.endswith("inventory.position.changed")
    assert parsed.canary_consumer.durable_name == "EVENT_PIPELINE_CANARY"

    topology["canary_consumer"]["filter_subject"] = "spotwo.wms.events.>"
    try:
        DeploymentTopologyConfig.from_mapping(topology).expectation(spec)
    except ValueError as exc:
        assert "exact synthetic canary subject" in str(exc)
    else:
        raise AssertionError("invalid canary filter must be rejected")


def test_topology_config_rejects_business_filter_that_matches_canary_subject():
    spec = deployment_spec()
    topology = topology_mapping()
    topology["business_consumer"]["filter_subject"] = "spotwo.wms.events.>"

    try:
        DeploymentTopologyConfig.from_mapping(topology).validate(spec)
    except ValueError as exc:
        assert "exclude the synthetic canary subject" in str(exc)
    else:
        raise AssertionError("business consumer must not receive canary traffic")


class LiveTopologyProbe:
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
        business_durable: str,
        canary_durable: str,
    ) -> None:
        self._runner.run(
            self._provision(
                stream_name=stream_name,
                subject_prefix=subject_prefix,
                business_durable=business_durable,
                canary_durable=canary_durable,
            )
        )

    def state(self, stream_name: str, business_durable: str, canary_durable: str):
        return self._runner.run(
            self._state(stream_name, business_durable, canary_durable)
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
        business_durable: str,
        canary_durable: str,
    ) -> None:
        await self._delete_if_present(stream_name)
        await self._jetstream.add_stream(
            StreamConfig(
                name=stream_name,
                subjects=[f"{subject_prefix}.>"],
                storage=StorageType.FILE,
                retention=RetentionPolicy.LIMITS,
                num_replicas=1,
                duplicate_window=120,
            )
        )
        await self._jetstream.add_consumer(
            stream_name,
            ConsumerConfig(
                durable_name=business_durable,
                deliver_policy=DeliverPolicy.ALL,
                ack_policy=AckPolicy.EXPLICIT,
                max_deliver=3,
                max_ack_pending=10,
                filter_subject=f"{subject_prefix}.inventory.position.changed",
            ),
        )
        await self._jetstream.add_consumer(
            stream_name,
            ConsumerConfig(
                durable_name=canary_durable,
                deliver_policy=DeliverPolicy.ALL,
                ack_policy=AckPolicy.EXPLICIT,
                max_deliver=3,
                max_ack_pending=10,
                filter_subject=f"{subject_prefix}.health_check.ping",
            ),
        )

    async def _state(self, stream_name: str, business_durable: str, canary_durable: str):
        stream = await self._jetstream.stream_info(stream_name)
        business = await self._jetstream.consumer_info(stream_name, business_durable)
        canary = await self._jetstream.consumer_info(stream_name, canary_durable)
        return (
            stream.state.messages,
            stream.state.bytes,
            business.num_pending,
            business.num_ack_pending,
            canary.num_pending,
            canary.num_ack_pending,
        )

    async def _delete_if_present(self, stream_name: str) -> None:
        try:
            await self._jetstream.delete_stream(stream_name)
        except NotFoundError:
            pass


def test_live_topology_inspection_matches_preprovisioned_resources_without_mutation():
    suffix = uuid4().hex.upper()
    stream_name = f"WMS_TOPOLOGY_{suffix}"
    subject_prefix = f"spotwo.wms.topology.{suffix.lower()}"
    business_durable = f"POSITION_{suffix}"
    canary_durable = f"CANARY_{suffix}"
    probe = LiveTopologyProbe(NATS_URL)
    probe.provision(
        stream_name=stream_name,
        subject_prefix=subject_prefix,
        business_durable=business_durable,
        canary_durable=canary_durable,
    )
    expected = expectation(
        stream_name=stream_name,
        subject_prefix=subject_prefix,
        business_durable=business_durable,
        canary_durable=canary_durable,
    )

    try:
        before = probe.state(stream_name, business_durable, canary_durable)
        report = NatsJetStreamTopologyInspector(NATS_URL).inspect(expected)
        after = probe.state(stream_name, business_durable, canary_durable)

        assert report.status == "ok"
        assert report.drift == ()
        assert before == after
    finally:
        probe.close(stream_name)
