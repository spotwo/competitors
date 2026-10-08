"""Real authenticated JetStream provisioning and fail-closed tenant readiness."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from pathlib import Path

import nats
import pytest
from nats.js.api import StorageType, StreamConfig
from nats.js.errors import NotFoundError

import test_nats_authenticated_acl as auth_lab
from event_pipeline_readiness import load_registry
from tenant_deployment_probe import TenantDeploymentProbe
from tenant_event_deployment import (
    TenantDeploymentBinding,
    TenantReadinessGate,
    TenantTopologyProvisioner,
    load_tenant_deployments,
)
import yaml


ROOT = Path(__file__).resolve().parents[3]
REGISTRY_PATH = ROOT / "operations/event-pipelines/registry.yml"


async def reset_stream(*, create: bool):
    client = await nats.connect(
        servers=[auth_lab.ADMIN_URL], allow_reconnect=False, connect_timeout=2,
    )
    try:
        js = client.jetstream()
        try:
            await js.delete_stream(auth_lab.STREAM)
        except NotFoundError:
            pass
        if create:
            await js.add_stream(StreamConfig(
                name=auth_lab.STREAM,
                subjects=[f"{auth_lab.PREFIX}.>"],
                storage=StorageType.FILE,
            ))
    finally:
        await client.close()


def lab_binding() -> TenantDeploymentBinding:
    with REGISTRY_PATH.open("r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)
    registry = load_registry(REGISTRY_PATH)
    pilot = load_tenant_deployments(raw, registry)[0]
    transport = replace(
        pilot.parent.transport,
        stream=auth_lab.STREAM,
        subject_prefix=auth_lab.PREFIX,
    )
    return replace(
        pilot,
        parent=replace(pilot.parent, transport=transport),
        deployment=replace(
            pilot.deployment, enabled=True,
            business_durable=auth_lab.DURABLE_A,
            canary_durable="TENANT_A_CANARY",
        ),
    )


def test_provisioner_requires_stream_and_explicit_operator_approval():
    binding = lab_binding()
    provisioner = TenantTopologyProvisioner(auth_lab.ADMIN_URL)
    with asyncio.Runner() as runner:
        runner.run(reset_stream(create=False))

    assert provisioner.plan(binding).code == "stream_missing"
    with pytest.raises(ValueError, match="operator_id"):
        provisioner.provision(binding, operator_id="", approval_id="approved")
    with pytest.raises(ValueError, match="approval_id"):
        provisioner.provision(binding, operator_id="operator", approval_id="")


def test_authenticated_jetstream_plan_apply_idempotence_and_readiness_fail_closed():
    binding = lab_binding()
    provisioner = TenantTopologyProvisioner(auth_lab.ADMIN_URL)
    with asyncio.Runner() as runner:
        runner.run(reset_stream(create=True))
    try:
        preview = provisioner.plan(binding)
        assert preview.status == "provisionable"
        assert set(preview.missing_durables) == {
            auth_lab.DURABLE_A, "TENANT_A_CANARY",
        }
        assert set(provisioner.plan(binding).missing_durables) == {
            auth_lab.DURABLE_A, "TENANT_A_CANARY",
        }
        applied = provisioner.provision(
            binding, operator_id="lab-provisioner", approval_id="change-123",
        )
        assert applied.status == "ready"
        assert applied.missing_durables == ()
        assert provisioner.provision(
            binding, operator_id="lab-provisioner", approval_id="change-123",
        ).status == "ready"

        # Real tenant publisher and consumer identities. They can only access
        # allowed broker API/subjects; the fixture intentionally lacks a
        # separately authorized tenant A canary credential and heartbeat grant.
        # The gate MUST NOT claim readiness with only topology + ACL success.
        acl_proof, canary_proof = TenantDeploymentProbe(
            publisher_nats_url=auth_lab.user_url("lab_tenant_a_pub", "lab-a-pub"),
            consumer_nats_url=auth_lab.user_url("lab_tenant_a_sub", "lab-a-sub"),
            canary_consumer_nats_url=auth_lab.user_url("lab_tenant_b_sub", "lab-b-sub"),
        ).run(binding)
        assert acl_proof["verified"] is True
        assert canary_proof["verified"] is False
        report = TenantReadinessGate().assess(
            binding, topology=provisioner.plan(binding),
            acl_proof=acl_proof, canary_proof=canary_proof,
        )
        assert report.status == "not_ready"
        assert report.codes == ("tenant_canary_unverified",)
    finally:
        with asyncio.Runner() as runner:
            runner.run(reset_stream(create=False))
