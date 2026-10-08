from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from event_pipeline_readiness import EventPipelineDeploymentRegistry, load_registry
from event_pipeline_registry_validation import validate_global_consumer_identities


ROOT = Path(__file__).resolve().parents[3]
REGISTRY_PATH = ROOT / "operations" / "event-pipelines" / "registry.yml"


def test_registry_enables_second_proven_business_pipeline():
    registry = load_registry(REGISTRY_PATH)
    position = registry.get("inventory-position-projection")
    work = registry.get("warehouse-work-state-projection")

    assert len(registry.pipelines) == 3
    assert position.enabled is True
    assert position.consumer.projection_gap_monitor == "inventory_position"
    assert work.enabled is True
    assert work.consumer.projection_gap_monitor == "none"
    assert work.transport.stream == position.transport.stream == "WMS_EVENTS"
    assert work.consumer.durable == "WORK_STATE_PROJECTOR"
    assert work.consumer.inbox_consumer_name == "warehouse_work_state_projection"
    assert work.canary.durable == "WORK_EVENT_PIPELINE_CANARY"
    assert work.canary.consumer_name == "warehouse_work_event_pipeline_canary"


def test_registry_requires_global_consumer_and_durable_identity_separation():
    registry = load_registry(REGISTRY_PATH)
    first, second = registry.pipelines[:2]

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
