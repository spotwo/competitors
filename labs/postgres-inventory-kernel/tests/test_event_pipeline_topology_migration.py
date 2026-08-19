from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path

import yaml

from event_pipeline_readiness import load_registry
from event_pipeline_topology import (
    EventPipelineTopologyReport,
    TopologyDrift,
    TopologyResourceSnapshot,
)
from event_pipeline_topology_config import pipeline_topology_mapping
from event_pipeline_topology_migration import (
    TopologyContractInspector,
    TopologyMigrationConfig,
    render_prometheus,
)

ROOT = Path(__file__).resolve().parents[3]
REGISTRY = ROOT / "operations" / "event-pipelines" / "registry.yml"


def configured():
    registry = load_registry(REGISTRY)
    with REGISTRY.open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)
    spec = registry.get("inventory-position-projection")
    topology = deepcopy(pipeline_topology_mapping(raw, spec.pipeline_id))
    return spec, topology


def resources(*, replicas: int) -> tuple[TopologyResourceSnapshot, ...]:
    return (
        TopologyResourceSnapshot(
            resource="stream",
            name="WMS_EVENTS",
            exists=True,
            fields={
                "subjects": ("spotwo.wms.events.>",),
                "storage": "file",
                "retention": "limits",
                "replicas": replicas,
                "duplicate_window_seconds": 120.0,
            },
        ),
        TopologyResourceSnapshot(
            resource="business_consumer",
            name="POSITION_PROJECTOR",
            exists=True,
            fields={
                "delivery_mode": "pull",
                "deliver_policy": "all",
                "ack_policy": "explicit",
                "filter_subject": "spotwo.wms.events.inventory.position.changed",
                "max_ack_pending": 10,
                "max_deliver": 3,
            },
        ),
        TopologyResourceSnapshot(
            resource="canary_consumer",
            name="EVENT_PIPELINE_CANARY",
            exists=True,
            fields={
                "delivery_mode": "pull",
                "deliver_policy": "all",
                "ack_policy": "explicit",
                "filter_subject": "spotwo.wms.events.health_check.ping",
                "max_ack_pending": 10,
                "max_deliver": 3,
            },
        ),
    )


def target_report(*, observed_at: datetime, target_replicas: int, actual_replicas: int):
    actual = resources(replicas=actual_replicas)
    drift = ()
    if target_replicas != actual_replicas:
        drift = (
            TopologyDrift(
                resource="stream",
                field="replicas",
                code="stream_replicas_mismatch",
                expected=target_replicas,
                actual=actual_replicas,
            ),
        )
    return EventPipelineTopologyReport(
        observed_at=observed_at,
        stream_name="WMS_EVENTS",
        status="critical" if drift else "ok",
        resources=actual,
        drift=drift,
    )


class FixedInspector:
    def __init__(self, report):
        self.report = report

    def inspect(self, _expectation, *, observed_at=None):
        assert observed_at == self.report.observed_at
        return self.report


def migration(valid_until: str = "2026-08-19T12:00:00Z"):
    return {
        "id": "replicas-1-to-3",
        "valid_until": valid_until,
        "from_overrides": {"stream": {"replicas": 1}},
    }


def inspect(*, observed_at: datetime, actual_replicas: int):
    spec, target = configured()
    target["stream"]["replicas"] = 3
    report = target_report(
        observed_at=observed_at,
        target_replicas=3,
        actual_replicas=actual_replicas,
    )
    inspector = TopologyContractInspector("nats://unused")
    inspector.inspector = FixedInspector(report)
    return inspector.inspect(
        spec,
        target_topology=target,
        migration_mapping=migration(),
        observed_at=observed_at,
    )


def test_active_migration_accepts_old_source_without_hiding_target_mismatch():
    observed_at = datetime(2026, 8, 19, 11, 0, tzinfo=timezone.utc)
    report = inspect(observed_at=observed_at, actual_replicas=1)

    assert report.status == "ok"
    assert report.code == "topology_migration_source_accepted"
    assert report.phase == "active"
    assert report.matched == "source"
    assert report.target.status == "critical"
    assert report.source_drift == ()


def test_active_migration_accepts_target_and_reports_completion_state():
    observed_at = datetime(2026, 8, 19, 11, 0, tzinfo=timezone.utc)
    report = inspect(observed_at=observed_at, actual_replicas=3)

    assert report.status == "ok"
    assert report.code is None
    assert report.phase == "active"
    assert report.matched == "target"


def test_expired_migration_fails_closed_when_source_is_still_deployed():
    observed_at = datetime(2026, 8, 19, 12, 0, 1, tzinfo=timezone.utc)
    report = inspect(observed_at=observed_at, actual_replicas=1)

    assert report.status == "critical"
    assert report.code == "topology_migration_expired"
    assert report.phase == "expired"
    assert report.matched == "source"


def test_expired_migration_requires_target_but_warns_until_contract_is_removed():
    observed_at = datetime(2026, 8, 19, 12, 0, 1, tzinfo=timezone.utc)
    report = inspect(observed_at=observed_at, actual_replicas=3)

    assert report.status == "warning"
    assert report.code == "topology_migration_contract_expired"
    assert report.phase == "expired"
    assert report.matched == "target"


def test_migration_override_must_describe_a_distinct_valid_source_topology():
    spec, target = configured()
    unchanged = {
        "id": "noop",
        "valid_until": "2026-08-19T12:00:00Z",
        "from_overrides": {"stream": {"replicas": target["stream"]["replicas"]}},
    }

    try:
        TopologyMigrationConfig.from_mapping(unchanged).source_topology(target, spec)
    except ValueError as exc:
        assert "must change topology" in str(exc)
    else:
        raise AssertionError("no-op migration must be rejected")


def test_prometheus_migration_labels_are_bounded_and_do_not_export_contract_values():
    observed_at = datetime(2026, 8, 19, 11, 0, tzinfo=timezone.utc)
    report = inspect(observed_at=observed_at, actual_replicas=1)
    prometheus = render_prometheus(report, pipeline_id="inventory-position-projection")

    assert 'state="source"' in prometheus
    assert "replicas-1-to-3" not in prometheus
    assert "2026-08-19" not in prometheus
    assert "WMS_EVENTS" not in prometheus
    assert "POSITION_PROJECTOR" not in prometheus
    assert "spotwo.wms.events" not in prometheus
