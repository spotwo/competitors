from __future__ import annotations

from dataclasses import replace
from pathlib import Path
import subprocess
import sys

import pytest

from event_pipeline_readiness import EventPipelineDeploymentRegistry, load_registry


ROOT = Path(__file__).resolve().parents[3]
REGISTRY_PATH = ROOT / "operations" / "event-pipelines" / "registry.yml"
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from validate_event_pipeline_deployments import (  # noqa: E402
    validate_global_consumer_identities,
)


def test_registry_configures_second_real_business_pipeline_without_enabling_it():
    registry = load_registry(REGISTRY_PATH)
    position = registry.get("inventory-position-projection")
    work = registry.get("warehouse-work-state-projection")

    assert len(registry.pipelines) == 2
    assert position.enabled is True
    assert work.enabled is False
    assert work.transport.stream == position.transport.stream == "WMS_EVENTS"
    assert work.consumer.durable == "WORK_STATE_PROJECTOR"
    assert work.consumer.inbox_consumer_name == "warehouse_work_state_projection"
    assert work.canary.durable == "WORK_EVENT_PIPELINE_CANARY"
    assert work.canary.consumer_name == "warehouse_work_event_pipeline_canary"


def test_registry_requires_global_consumer_and_durable_identity_separation():
    registry = load_registry(REGISTRY_PATH)
    first, second = registry.pipelines

    duplicate_durable = replace(
        second,
        consumer=replace(second.consumer, durable=first.consumer.durable),
    )
    with pytest.raises(ValueError, match="durable identities must be unique"):
        validate_global_consumer_identities(
            EventPipelineDeploymentRegistry(
                version=registry.version,
                pipelines=(first, duplicate_durable),
            )
        )

    duplicate_inbox = replace(
        second,
        consumer=replace(
            second.consumer,
            inbox_consumer_name=first.consumer.inbox_consumer_name,
        ),
    )
    with pytest.raises(ValueError, match="Inbox consumer identities must be unique"):
        validate_global_consumer_identities(
            EventPipelineDeploymentRegistry(
                version=registry.version,
                pipelines=(first, duplicate_inbox),
            )
        )


def test_repository_deployment_validator_accepts_two_configured_one_enabled():
    result = subprocess.run(
        [sys.executable, str(SCRIPTS / "validate_event_pipeline_deployments.py")],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "2 configured, 1 enabled" in result.stdout
