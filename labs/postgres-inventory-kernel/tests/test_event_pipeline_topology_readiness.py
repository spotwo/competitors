from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import yaml

from event_pipeline_readiness import (
    EventPipelineReadinessReport,
    ReadinessSignal,
    load_registry,
)
from event_pipeline_topology_config import pipeline_topology_mapping
from event_pipeline_topology_readiness import (
    TopologyAwareEventPipelineReadinessCollector,
)

ROOT = Path(__file__).resolve().parents[3]
REGISTRY = ROOT / "operations" / "event-pipelines" / "registry.yml"


def configured_pipeline():
    registry = load_registry(REGISTRY)
    with REGISTRY.open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)
    spec = registry.get("inventory-position-projection")
    topology = pipeline_topology_mapping(raw, spec.pipeline_id)
    return spec, topology


class HealthyBaseCollector:
    def collect(self, spec, *, observed_at, **_kwargs):
        signals = tuple(
            ReadinessSignal(
                name=name,
                status="ok",
                available=True,
                code=None,
                details={},
            )
            for name in (
                "configuration",
                "pipeline_health",
                "watchdog",
                "canary_sli",
            )
        )
        return EventPipelineReadinessReport(
            observed_at=observed_at,
            pipeline_id=spec.pipeline_id,
            status="ready",
            signals=signals,
            config=spec.public_config(),
        )


class DriftingInspector:
    def __init__(self, _nats_url):
        pass

    def inspect(self, _expectation, *, observed_at):
        return SimpleNamespace(
            status="critical",
            to_dict=lambda: {
                "status": "critical",
                "observed_at": observed_at.isoformat(),
                "drift_count": 1,
                "drift": [
                    {
                        "resource": "business_consumer",
                        "field": "ack_policy",
                        "code": "business_consumer_ack_policy_mismatch",
                    }
                ],
            },
        )


def test_topology_drift_makes_otherwise_healthy_deployment_not_ready(monkeypatch):
    spec, topology = configured_pipeline()
    observed_at = datetime(2026, 8, 19, 8, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(
        "event_pipeline_topology_readiness.NatsJetStreamTopologyInspector",
        DriftingInspector,
    )
    collector = TopologyAwareEventPipelineReadinessCollector()
    collector.base = HealthyBaseCollector()

    report = collector.collect(
        spec,
        topology=topology,
        database_url="postgresql://not-used",
        nats_url="nats://not-used",
        watchdog_state={},
        observed_at=observed_at,
    )

    assert report.status == "not_ready"
    assert [signal.name for signal in report.signals] == [
        "configuration",
        "topology",
        "pipeline_health",
        "watchdog",
        "canary_sli",
    ]
    topology_signal = report.signals[1]
    assert topology_signal.status == "critical"
    assert topology_signal.available is True
    assert topology_signal.code == "topology_drift"
    assert report.root_cause_candidate == topology_signal


def test_missing_nats_endpoint_fails_topology_signal_closed():
    spec, topology = configured_pipeline()
    signal = TopologyAwareEventPipelineReadinessCollector._topology_signal(
        spec,
        topology=topology,
        nats_url=None,
        observed_at=datetime(2026, 8, 19, 8, 0, tzinfo=timezone.utc),
    )

    assert signal.status == "critical"
    assert signal.available is False
    assert signal.code == "topology_nats_url_missing"
