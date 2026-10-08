"""Tenant deployment registry, dry-run planning and fail-closed readiness."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

import pytest
import yaml

from event_pipeline_readiness import load_registry
from tenant_event_deployment import (
    TenantDeployment, TenantReadinessGate, TenantTopologyPlan,
    load_tenant_deployments,
)

ROOT = Path(__file__).resolve().parents[3]
REGISTRY_PATH = ROOT / "operations/event-pipelines/registry.yml"
TENANT = UUID("00000000-0000-0000-0000-000000000001")


def raw_and_bindings():
    with REGISTRY_PATH.open("r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)
    reg = load_registry(REGISTRY_PATH)
    return raw, reg, load_tenant_deployments(raw, reg)


def test_disabled_pilot_binding_is_exact_without_touching_three_shared_pipelines():
    raw, reg, bindings = raw_and_bindings()
    assert len(reg.pipelines) == 3
    assert len(bindings) == 1
    bound = bindings[0]
    assert bound.deployment.enabled is False
    assert bound.deployment.tenant_id == TENANT
    assert bound.stream == "WMS_EVENTS"
    assert bound.route.subject == (
        "spotwo.wms.events.tenants."
        "00000000-0000-0000-0000-000000000001.inventory.transaction.posted"
    )
    assert bound.canary_subject == (
        "spotwo.wms.events.tenants."
        "00000000-0000-0000-0000-000000000001.health_check.ping"
    )
    assert all(not x.deployment.enabled for x in bindings)


def test_registry_rejects_duplicate_tenant_binding_and_global_durable_collision():
    raw, reg, _ = raw_and_bindings()
    entry = dict(raw["tenant_deployments"][0])
    with pytest.raises(ValueError, match="ids must be unique"):
        load_tenant_deployments(
            {**raw, "tenant_deployments": [entry, dict(entry)]}, reg
        )
    entry["business_durable"] = "INVENTORY_TRANSACTION_INDEX_PROJECTOR"
    with pytest.raises(ValueError, match="durables must be globally unique"):
        load_tenant_deployments({**raw, "tenant_deployments": [entry]}, reg)


def test_registry_rejects_unsupported_domain_noncanonical_identity_and_secret_like_env():
    raw, reg, _ = raw_and_bindings()
    entry = dict(raw["tenant_deployments"][0])
    with pytest.raises(ValueError, match="supports only transaction index"):
        load_tenant_deployments(
            {**raw, "tenant_deployments": [{**entry, "pipeline_id": "warehouse-work-state-projection"}]},
            reg,
        )
    with pytest.raises(ValueError, match="canonical lowercase UUID"):
        TenantDeployment.from_mapping({**entry, "tenant_id": "NOT-UUID"})
    with pytest.raises(ValueError, match="uppercase environment variable"):
        TenantDeployment.from_mapping({**entry, "publisher_nats_url_env": "nats://secret@example"})
    with pytest.raises(ValueError, match="credential env names must be distinct"):
        TenantDeployment.from_mapping({
            **entry, "consumer_nats_url_env": entry["publisher_nats_url_env"]
        })


def test_readiness_fails_closed_on_disabled_missing_unscoped_and_stale_evidence():
    _, _, bindings = raw_and_bindings()
    binding = bindings[0]
    gate = TenantReadinessGate()
    now = datetime.now(timezone.utc)
    plan = TenantTopologyPlan("ready", None, (), binding.stream)
    off = gate.assess(
        binding, topology=plan, acl_proof=None, canary_proof=None, observed_at=now,
    )
    assert off.status == "not_ready"
    assert "deployment_disabled" in off.codes
    assert "tenant_canary_unverified" in off.codes
    active = replace(binding, deployment=replace(binding.deployment, enabled=True))
    acl = {
        "verified": True, "tenant_id": str(TENANT),
        "stream": binding.stream, "durable": binding.deployment.business_durable,
        "observed_at": now.isoformat(),
    }
    canary = {
        "verified": True, "tenant_id": str(TENANT),
        "stream": binding.stream, "durable": binding.deployment.canary_durable,
        "subject": binding.canary_subject, "observed_at": now.isoformat(),
    }
    assert gate.assess(
        active, topology=plan, acl_proof=acl, canary_proof=canary,
        observed_at=now,
    ).status == "ready"
    for bad_acl in (
        {**acl, "tenant_id": str(UUID(int=2))},
        {**acl, "durable": "BROAD_DURABLE"},
        {**acl, "observed_at": (now - timedelta(minutes=3)).isoformat()},
        {**acl, "verified": False},
    ):
        report = gate.assess(
            active, topology=plan, acl_proof=bad_acl, canary_proof=canary,
            observed_at=now,
        )
        assert report.status == "not_ready"
        assert "tenant_broker_acl_unverified" in report.codes
    assert "tenant_canary_unverified" in gate.assess(
        active, topology=plan, acl_proof=acl,
        canary_proof={**canary, "subject": binding.route.subject},
        observed_at=now,
    ).codes
    assert "tenant_topology_not_ready" in gate.assess(
        active,
        topology=TenantTopologyPlan("provisionable", "missing", ("ONE",), binding.stream),
        acl_proof=acl, canary_proof=canary, observed_at=now,
    ).codes
