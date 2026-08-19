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
from event_pipeline_topology_migration import TopologyContractInspector
from event_pipeline_topology_preflight import EventPipelineTopologyPreflightPlanner

ROOT = Path(__file__).resolve().parents[3]
REGISTRY = ROOT / "operations" / "event-pipelines" / "registry.yml"


def configured():
    registry = load_registry(REGISTRY)
    with REGISTRY.open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)
    spec = registry.get("inventory-position-projection")
    topology = deepcopy(pipeline_topology_mapping(raw, spec.pipeline_id))
    return spec, topology


def migration(valid_until: str = "2026-08-19T12:00:00Z"):
    return {
        "id": "replicas-1-to-3",
        "valid_until": valid_until,
        "from_overrides": {"stream": {"replicas": 1}},
    }


def resources(*, replicas: int, business_max_deliver: int = 3):
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
                "max_deliver": business_max_deliver,
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


def target_report(
    *,
    observed_at: datetime,
    target_replicas: int,
    actual_replicas: int,
    target_business_max_deliver: int = 3,
    actual_business_max_deliver: int = 3,
):
    actual = resources(
        replicas=actual_replicas,
        business_max_deliver=actual_business_max_deliver,
    )
    drift = []
    if target_replicas != actual_replicas:
        drift.append(
            TopologyDrift(
                resource="stream",
                field="replicas",
                code="stream_replicas_mismatch",
                expected=target_replicas,
                actual=actual_replicas,
            )
        )
    if target_business_max_deliver != actual_business_max_deliver:
        drift.append(
            TopologyDrift(
                resource="business_consumer",
                field="max_deliver",
                code="business_consumer_max_deliver_mismatch",
                expected=target_business_max_deliver,
                actual=actual_business_max_deliver,
            )
        )
    return EventPipelineTopologyReport(
        observed_at=observed_at,
        stream_name="WMS_EVENTS",
        status="critical" if drift else "ok",
        resources=actual,
        drift=tuple(drift),
    )


class FixedInspector:
    def __init__(self, report):
        self.report = report

    def inspect(self, _expectation, *, observed_at=None):
        assert observed_at == self.report.observed_at
        return self.report


def planner_for(report):
    contract = TopologyContractInspector("nats://unused")
    contract.inspector = FixedInspector(report)
    return EventPipelineTopologyPreflightPlanner(
        "nats://unused",
        contract_inspector=contract,
    )


def test_active_source_builds_single_resource_safe_to_apply_plan():
    observed_at = datetime(2026, 8, 19, 11, 0, tzinfo=timezone.utc)
    spec, target = configured()
    target["stream"]["replicas"] = 3
    report = target_report(
        observed_at=observed_at,
        target_replicas=3,
        actual_replicas=1,
    )

    preflight = planner_for(report).plan(
        spec,
        target_topology=target,
        migration_mapping=migration(),
        observed_at=observed_at,
    )

    assert preflight.status == "ok"
    assert preflight.decision == "safe_to_apply"
    assert preflight.code == "topology_migration_ready"
    assert [(change.resource, change.field, change.current, change.target) for change in preflight.changes] == [
        ("stream", "replicas", 1, 3)
    ]
    assert [step.code for step in preflight.steps] == [
        "apply_target_resource",
        "verify_exact_target",
        "verify_event_pipeline_readiness",
        "remove_topology_migration_contract",
    ]
    assert preflight.steps[0].requires_atomic_resource_update is True
    assert preflight.migration_deadline_remaining_seconds == 3600
    assert preflight.to_dict()["execution"] == {
        "dry_run": True,
        "mutates": False,
        "external_provisioner_required": True,
    }


def test_active_target_is_complete_and_only_needs_verification_and_cleanup():
    observed_at = datetime(2026, 8, 19, 11, 0, tzinfo=timezone.utc)
    spec, target = configured()
    target["stream"]["replicas"] = 3
    report = target_report(
        observed_at=observed_at,
        target_replicas=3,
        actual_replicas=3,
    )

    preflight = planner_for(report).plan(
        spec,
        target_topology=target,
        migration_mapping=migration(),
        observed_at=observed_at,
    )

    assert preflight.status == "ok"
    assert preflight.decision == "no_changes"
    assert preflight.code == "topology_migration_target_reached"
    assert preflight.changes == ()
    assert [step.code for step in preflight.steps] == [
        "verify_exact_target",
        "verify_event_pipeline_readiness",
        "remove_topology_migration_contract",
    ]


def test_expired_source_is_blocked():
    observed_at = datetime(2026, 8, 19, 12, 0, 1, tzinfo=timezone.utc)
    spec, target = configured()
    target["stream"]["replicas"] = 3
    report = target_report(
        observed_at=observed_at,
        target_replicas=3,
        actual_replicas=1,
    )

    preflight = planner_for(report).plan(
        spec,
        target_topology=target,
        migration_mapping=migration(),
        observed_at=observed_at,
    )

    assert preflight.status == "critical"
    assert preflight.decision == "blocked"
    assert preflight.code == "topology_migration_expired"
    assert preflight.steps == ()


def test_expired_target_warns_that_migration_contract_needs_cleanup():
    observed_at = datetime(2026, 8, 19, 12, 0, 1, tzinfo=timezone.utc)
    spec, target = configured()
    target["stream"]["replicas"] = 3
    report = target_report(
        observed_at=observed_at,
        target_replicas=3,
        actual_replicas=3,
    )

    preflight = planner_for(report).plan(
        spec,
        target_topology=target,
        migration_mapping=migration(),
        observed_at=observed_at,
    )

    assert preflight.status == "warning"
    assert preflight.decision == "no_changes"
    assert preflight.code == "topology_migration_cleanup_required"


def test_drift_without_reviewed_migration_is_blocked_but_delta_is_visible():
    observed_at = datetime(2026, 8, 19, 11, 0, tzinfo=timezone.utc)
    spec, target = configured()
    target["stream"]["replicas"] = 3
    report = target_report(
        observed_at=observed_at,
        target_replicas=3,
        actual_replicas=1,
    )

    preflight = planner_for(report).plan(
        spec,
        target_topology=target,
        migration_mapping=None,
        observed_at=observed_at,
    )

    assert preflight.status == "critical"
    assert preflight.decision == "blocked"
    assert preflight.code == "topology_migration_required"
    assert preflight.changes[0].current == 1
    assert preflight.changes[0].target == 3


def test_active_migration_blocks_unexpected_third_state():
    observed_at = datetime(2026, 8, 19, 11, 0, tzinfo=timezone.utc)
    spec, target = configured()
    target["stream"]["replicas"] = 3
    report = target_report(
        observed_at=observed_at,
        target_replicas=3,
        actual_replicas=2,
    )

    preflight = planner_for(report).plan(
        spec,
        target_topology=target,
        migration_mapping=migration(),
        observed_at=observed_at,
    )

    assert preflight.status == "critical"
    assert preflight.decision == "blocked"
    assert preflight.code == "topology_live_state_unexpected"


def test_multi_resource_transition_is_blocked_to_avoid_hybrid_intermediate_state():
    observed_at = datetime(2026, 8, 19, 11, 0, tzinfo=timezone.utc)
    spec, target = configured()
    target["stream"]["replicas"] = 3
    target["business_consumer"]["max_deliver"] = 5
    migration_mapping = {
        "id": "replicas-and-delivery",
        "valid_until": "2026-08-19T12:00:00Z",
        "from_overrides": {
            "stream": {"replicas": 1},
            "business_consumer": {"max_deliver": 3},
        },
    }
    report = target_report(
        observed_at=observed_at,
        target_replicas=3,
        actual_replicas=1,
        target_business_max_deliver=5,
        actual_business_max_deliver=3,
    )

    preflight = planner_for(report).plan(
        spec,
        target_topology=target,
        migration_mapping=migration_mapping,
        observed_at=observed_at,
    )

    assert preflight.status == "critical"
    assert preflight.decision == "blocked"
    assert preflight.code == "topology_migration_requires_single_resource"
    assert {change.resource for change in preflight.changes} == {
        "stream",
        "business_consumer",
    }
