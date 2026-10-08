from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from event_pipeline_readiness import (
    EventPipelineDeploymentRegistry,
    EventPipelineDeploymentSpec,
    EventPipelineReadinessCollector,
    EventPipelineReadinessReport,
    ReadinessSignal,
    load_registry,
    readiness_status,
    render_prometheus,
)

ROOT = Path(__file__).resolve().parents[3]
REGISTRY_PATH = ROOT / "operations" / "event-pipelines" / "registry.yml"
BASE = datetime(2026, 8, 19, 1, 0, tzinfo=timezone.utc)


def spec_mapping() -> dict:
    return {
        "id": "inventory-position-projection",
        "enabled": True,
        "runtime": {
            "database_url_env": "TEST_DATABASE_URL",
            "nats_url_env": "TEST_NATS_URL",
            "watchdog_state_path_env": "TEST_WATCHDOG_STATE_PATH",
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


def spec() -> EventPipelineDeploymentSpec:
    return EventPipelineDeploymentSpec.from_mapping(spec_mapping())


def watchdog_state(
    *,
    stream: str = "WMS_EVENTS",
    durable: str = "EVENT_PIPELINE_CANARY",
    consumer: str = "event_pipeline_canary",
    cadence_seconds: float = 60,
) -> dict:
    return {
        "scope": {
            "stream": stream,
            "durable": durable,
            "consumer": consumer,
        },
        "expected_execution_at": BASE.isoformat(),
        "cadence_seconds": cadence_seconds,
        "receipts": [
            {
                "execution_id": "slot-1",
                "scheduled_at": BASE.isoformat(),
                "started_at": (BASE + timedelta(seconds=1)).isoformat(),
                "finished_at": (BASE + timedelta(seconds=2)).isoformat(),
                "outcome": "succeeded",
                "probe_status": "ok",
                "exit_code": 0,
            }
        ],
    }


def signal(name: str, status: str, code: str | None = None) -> ReadinessSignal:
    return ReadinessSignal(
        name=name,
        status=status,
        available=True,
        code=code,
        details={},
    )


def test_repository_registry_loads_and_exposes_one_source_of_truth():
    registry = load_registry(REGISTRY_PATH)
    deployment = registry.get("inventory-position-projection")

    assert registry.version == 1
    assert deployment.transport.stream == "WMS_EVENTS"
    assert deployment.consumer.durable == "POSITION_PROJECTOR"
    assert deployment.consumer.inbox_consumer_name == "position_projection"
    assert deployment.consumer.projection_gap_monitor == "inventory_position"
    assert deployment.canary.durable == "EVENT_PIPELINE_CANARY"
    assert deployment.canary.cadence_seconds == 60
    assert deployment.slo.availability_target == pytest.approx(0.999)
    assert deployment.watchdog.execution_timeout_seconds == 45



def test_registry_rejects_unknown_projection_gap_monitor():
    item = spec_mapping()
    item["consumer"]["projection_gap_monitor"] = "unsupported"
    with pytest.raises(ValueError, match="projection_gap_monitor"):
        EventPipelineDeploymentSpec.from_mapping(item)

def test_registry_rejects_duplicate_pipeline_ids():
    item = spec_mapping()
    with pytest.raises(ValueError, match="ids must be unique"):
        EventPipelineDeploymentRegistry.from_mapping(
            {"version": 1, "pipelines": [item, item]}
        )


def test_spec_requires_dedicated_canary_identity():
    item = spec_mapping()
    item["canary"]["durable"] = item["consumer"]["durable"]
    with pytest.raises(ValueError, match="canary durable must be separate"):
        EventPipelineDeploymentSpec.from_mapping(item)


def test_spec_requires_sli_freshness_to_cover_canary_cadence():
    item = spec_mapping()
    item["slo"]["stale_after_seconds"] = 30
    with pytest.raises(ValueError, match="cover at least one canary cadence"):
        EventPipelineDeploymentSpec.from_mapping(item)


def test_spec_requires_external_watchdog_timeout_beyond_canary_timeout():
    item = spec_mapping()
    item["watchdog"]["execution_timeout_seconds"] = 30
    with pytest.raises(ValueError, match="must exceed canary timeout"):
        EventPipelineDeploymentSpec.from_mapping(item)


def test_readiness_status_maps_warning_and_critical_fail_closed():
    assert readiness_status((signal("configuration", "ok"),)) == "ready"
    assert (
        readiness_status(
            (
                signal("configuration", "ok"),
                signal("pipeline_health", "warning", "pipeline_health_degraded"),
            )
        )
        == "degraded"
    )
    assert (
        readiness_status(
            (
                signal("configuration", "ok"),
                signal("watchdog", "critical", "watchdog_critical"),
            )
        )
        == "not_ready"
    )


def test_root_cause_prefers_worst_severity_then_canonical_signal_order():
    signals = (
        signal("configuration", "ok"),
        signal("pipeline_health", "critical", "pipeline_health_critical"),
        signal("watchdog", "critical", "watchdog_critical"),
        signal("canary_sli", "warning", "canary_sli_degraded"),
    )
    report = EventPipelineReadinessReport(
        observed_at=BASE,
        pipeline_id="inventory-position-projection",
        status="not_ready",
        signals=signals,
        config={},
    )

    assert report.root_cause_candidate is not None
    assert report.root_cause_candidate.name == "pipeline_health"


def test_missing_runtime_dependencies_are_visible_instead_of_crashing():
    report = EventPipelineReadinessCollector().collect(
        spec(),
        database_url=None,
        nats_url=None,
        watchdog_state=None,
        observed_at=BASE + timedelta(seconds=5),
    )

    assert report.status == "not_ready"
    by_name = {item.name: item for item in report.signals}
    assert by_name["configuration"].status == "ok"
    assert by_name["pipeline_health"].code == "pipeline_runtime_endpoint_missing"
    assert by_name["watchdog"].code == "watchdog_state_unavailable"
    assert by_name["canary_sli"].code == "canary_sli_database_url_missing"


def test_disabled_deployment_short_circuits_without_runtime_collection():
    item = spec_mapping()
    item["enabled"] = False
    report = EventPipelineReadinessCollector().collect(
        EventPipelineDeploymentSpec.from_mapping(item),
        database_url="unused",
        nats_url="unused",
        watchdog_state=watchdog_state(),
        observed_at=BASE + timedelta(seconds=5),
    )

    assert report.status == "not_ready"
    assert len(report.signals) == 1
    assert report.signals[0].code == "deployment_disabled"


def test_external_watchdog_signal_can_be_healthy_independently_of_runtime_endpoints():
    report = EventPipelineReadinessCollector().collect(
        spec(),
        database_url=None,
        nats_url=None,
        watchdog_state=watchdog_state(),
        observed_at=BASE + timedelta(seconds=5),
    )
    by_name = {item.name: item for item in report.signals}

    assert by_name["watchdog"].status == "ok"
    assert by_name["watchdog"].available is True
    assert report.status == "not_ready"


def test_watchdog_scope_drift_is_not_silently_accepted():
    report = EventPipelineReadinessCollector().collect(
        spec(),
        database_url=None,
        nats_url=None,
        watchdog_state=watchdog_state(durable="WRONG_DURABLE"),
        observed_at=BASE + timedelta(seconds=5),
    )
    watchdog = {item.name: item for item in report.signals}["watchdog"]

    assert watchdog.status == "critical"
    assert watchdog.available is True
    assert watchdog.code == "watchdog_scope_mismatch"


def test_watchdog_cadence_drift_is_not_silently_accepted():
    report = EventPipelineReadinessCollector().collect(
        spec(),
        database_url=None,
        nats_url=None,
        watchdog_state=watchdog_state(cadence_seconds=30),
        observed_at=BASE + timedelta(seconds=5),
    )
    watchdog = {item.name: item for item in report.signals}["watchdog"]

    assert watchdog.status == "critical"
    assert watchdog.code == "watchdog_cadence_mismatch"


def test_prometheus_uses_only_bounded_readiness_dimensions():
    report = EventPipelineReadinessCollector().collect(
        spec(),
        database_url=None,
        nats_url=None,
        watchdog_state=watchdog_state(),
        observed_at=BASE + timedelta(seconds=5),
    )
    rendered = render_prometheus(report)

    assert 'pipeline="inventory-position-projection"' in rendered
    assert 'signal="watchdog"' in rendered
    assert 'code="pipeline_runtime_endpoint_missing"' in rendered
    assert "slot-1" not in rendered
    assert "POSITION_PROJECTOR" not in rendered
    assert "EVENT_PIPELINE_CANARY" not in rendered


def test_readiness_cli_fails_closed_when_runtime_environment_is_absent():
    env = os.environ.copy()
    env.pop("KERNEL_LAB_DATABASE_URL", None)
    env.pop("KERNEL_LAB_NATS_URL", None)
    env.pop("KERNEL_LAB_CANARY_WATCHDOG_STATE_PATH", None)
    result = subprocess.run(
        [
            str(ROOT / "bin" / "inspect-kernel-event-pipeline-readiness"),
            "--pipeline",
            "inventory-position-projection",
            "--check",
        ],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 2
    payload = json.loads(result.stdout)
    assert payload["status"] == "not_ready"
    assert payload["signals"]["pipeline_health"]["available"] is False
