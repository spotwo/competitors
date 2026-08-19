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


def healthy_provisioning_signal(*_args, **_kwargs):
    return ReadinessSignal(
        name="topology_provisioning",
        status="ok",
        available=True,
        code=None,
        details={},
    )


def test_topology_drift_makes_otherwise_healthy_deployment_not_ready(monkeypatch):
    spec, topology = configured_pipeline()
    observed_at = datetime(2026, 8, 19, 8, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(
        "event_pipeline_topology_readiness.TopologyContractInspector",
        DriftingContractInspector,
    )
    monkeypatch.setattr(
        TopologyAwareEventPipelineReadinessCollector,
        "_provisioning_signal",
        staticmethod(healthy_provisioning_signal),
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
        "topology_provisioning",
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
    monkeypatch.setattr(
        TopologyAwareEventPipelineReadinessCollector,
        "_provisioning_signal",
        staticmethod(healthy_provisioning_signal),
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
    assert report.signals[1].code is None
    assert report.signals[1].details["code"] == "topology_migration_source_accepted"
    assert report.signals[2].status == "ok"
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


def test_missing_database_fails_provisioning_health_signal_closed():
    spec, _topology = configured_pipeline()
    signal = TopologyAwareEventPipelineReadinessCollector._provisioning_signal(
        spec,
        database_url=None,
        observed_at=datetime(2026, 8, 19, 8, 0, tzinfo=timezone.utc),
    )

    assert signal.status == "critical"
    assert signal.available is False
    assert signal.code == "topology_provisioning_database_url_missing"


def test_historical_failed_rollout_does_not_block_current_readiness(monkeypatch):
    spec, topology = configured_pipeline()
    observed_at = datetime(2026, 8, 19, 8, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(
        "event_pipeline_topology_readiness.TopologyContractInspector",
        SourceMigrationContractInspector,
    )

    class HistoricalFailureStore:
        def __init__(self, _database_url):
            pass

        def snapshot(self, *, pipeline_id, observed_at):
            assert pipeline_id == spec.pipeline_id
            return SimpleNamespace(
                pipeline_id=pipeline_id,
                observed_at=observed_at,
            )

    class HistoricalFailurePolicy:
        def evaluate(self, _snapshot):
            return SimpleNamespace(
                current_status="ok",
                current_code=None,
                status="critical",
                to_dict=lambda: {
                    "status": "critical",
                    "current_execution": {"status": "ok", "code": None},
                    "latest_terminal_attention": {
                        "status": "critical",
                        "code": "topology_provisioning_latest_failed",
                    },
                },
            )

    monkeypatch.setattr(
        "event_pipeline_topology_readiness.PostgresTopologyProvisioningHealthStore",
        HistoricalFailureStore,
    )
    monkeypatch.setattr(
        "event_pipeline_topology_readiness.TopologyProvisioningHealthPolicy",
        HistoricalFailurePolicy,
    )
    collector = TopologyAwareEventPipelineReadinessCollector()
    collector.base = HealthyBaseCollector()
    migration = {
        "id": "replicas-1-to-3",
        "valid_until": "2026-08-19T12:00:00Z",
        "from_overrides": {"stream": {"replicas": 1}},
    }

    report = collector.collect(
        spec,
        topology=topology,
        topology_migration=migration,
        database_url="postgresql://not-used",
        nats_url="nats://not-used",
        watchdog_state={},
        observed_at=observed_at,
    )

    provisioning = report.signals[2]
    assert provisioning.status == "ok"
    assert provisioning.code is None
    assert provisioning.details["status"] == "critical"
    assert report.status == "ready"
