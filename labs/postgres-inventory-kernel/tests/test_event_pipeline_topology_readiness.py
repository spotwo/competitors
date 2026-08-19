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


class DriftingContractInspector:
    def __init__(self, _nats_url):
        pass

    def inspect(
        self,
        _spec,
        *,
        target_topology,
        migration_mapping,
        observed_at,
    ):
        assert target_topology
        assert migration_mapping is None
        return SimpleNamespace(
            status="critical",
            code="topology_drift",
            to_dict=lambda: {
                "status": "critical",
                "code": "topology_drift",
                "observed_at": observed_at.isoformat(),
                "matched": "neither",
            },
        )


class SourceMigrationContractInspector:
    def __init__(self, _nats_url):
        pass

    def inspect(
        self,
        _spec,
        *,
        target_topology,
        migration_mapping,
        observed_at,
    ):
        assert target_topology
        assert migration_mapping is not None
        return SimpleNamespace(
            status="ok",
            code="topology_migration_source_accepted",
            to_dict=lambda: {
                "status": "ok",
                "code": "topology_migration_source_accepted",
                "observed_at": observed_at.isoformat(),
                "matched": "source",
            },
        )


def test_topology_drift_makes_otherwise_healthy_deployment_not_ready(monkeypatch):
    spec, topology = configured_pipeline()
    observed_at = datetime(2026, 8, 19, 8, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(
        "event_pipeline_topology_readiness.TopologyContractInspector",
        DriftingContractInspector,
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


def test_active_source_migration_can_keep_readiness_ready(monkeypatch):
    spec, topology = configured_pipeline()
    observed_at = datetime(2026, 8, 19, 8, 0, tzinfo=timezone.utc)
    migration = {
        "id": "replicas-1-to-3",
        "valid_until": "2026-08-19T12:00:00Z",
        "from_overrides": {"stream": {"replicas": 1}},
    }
    monkeypatch.setattr(
        "event_pipeline_topology_readiness.TopologyContractInspector",
        SourceMigrationContractInspector,
    )
    collector = TopologyAwareEventPipelineReadinessCollector()
    collector.base = HealthyBaseCollector()

    report = collector.collect(
        spec,
        topology=topology,
        topology_migration=migration,
        database_url="postgresql://not-used",
        nats_url="nats://not-used",
        watchdog_state={},
        observed_at=observed_at,
    )

    assert report.status == "ready"
    assert report.signals[1].status == "ok"
    assert report.signals[1].code == "topology_migration_source_accepted"
    assert report.config["topology_migration"] == migration


def test_missing_nats_endpoint_fails_topology_signal_closed():
    spec, topology = configured_pipeline()
    signal = TopologyAwareEventPipelineReadinessCollector._topology_signal(
        spec,
        topology=topology,
        topology_migration=None,
        nats_url=None,
        observed_at=datetime(2026, 8, 19, 8, 0, tzinfo=timezone.utc),
    )

    assert signal.status == "critical"
    assert signal.available is False
    assert signal.code == "topology_nats_url_missing"
