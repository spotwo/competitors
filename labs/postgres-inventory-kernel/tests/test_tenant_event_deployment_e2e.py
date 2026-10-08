"""Real authenticated JetStream provisioning and fail-closed tenant readiness."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from pathlib import Path
from datetime import datetime, timedelta, timezone

import psycopg
import conftest as lab
from tenant_deployment_activation import TenantActivationController

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

        # Real tenant-scoped heartbeat now succeeds via a dedicated canary
        # credential, separate from business consumer and publisher identity.
        granted_acl, granted_canary = TenantDeploymentProbe(
            publisher_nats_url=auth_lab.user_url("lab_tenant_a_pub", "lab-a-pub"),
            consumer_nats_url=auth_lab.user_url("lab_tenant_a_sub", "lab-a-sub"),
            canary_consumer_nats_url=auth_lab.user_url("lab_tenant_a_canary", "lab-a-canary"),
        ).run(binding)
        assert granted_acl["verified"] is True
        assert granted_canary["verified"] is True
        assert TenantReadinessGate().assess(
            binding, topology=provisioner.plan(binding),
            acl_proof=granted_acl, canary_proof=granted_canary,
        ).status == "ready"

        # Wrong canary credentials remain fail-closed even though business
        # topology and broker ACL for the other roles are healthy.
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


def controller(*, canary_user="lab_tenant_a_canary", canary_password="lab-a-canary"):
    return TenantActivationController(
        database_url=lab.DATABASE_URL,
        provisioner_nats_url=auth_lab.ADMIN_URL,
        publisher_nats_url=auth_lab.user_url("lab_tenant_a_pub", "lab-a-pub"),
        consumer_nats_url=auth_lab.user_url("lab_tenant_a_sub", "lab-a-sub"),
        canary_consumer_nats_url=auth_lab.user_url(canary_user, canary_password),
    )


def test_positive_canary_activates_with_immutable_audit_and_idempotent_approval():
    binding = lab_binding()
    with asyncio.Runner() as runner:
        runner.run(reset_stream(create=True))
    try:
        assert TenantTopologyProvisioner(auth_lab.ADMIN_URL).provision(
            binding, operator_id="topology-owner", approval_id="topology-ticket"
        ).status == "ready"

        inactive = replace(binding, deployment=replace(binding.deployment, enabled=False))
        with pytest.raises(ValueError, match="disabled in registry"):
            controller().activate(
                inactive, operator_id="reviewer", approval_id="ACT-100"
            )

        receipt = controller().activate(
            binding, operator_id="reviewer", approval_id="ACT-100"
        )
        assert receipt.status == "activated"
        assert receipt.tenant_id == binding.deployment.tenant_id
        assert controller().activate(
            binding, operator_id="reviewer", approval_id="ACT-100"
        ).receipt_id == receipt.receipt_id

        with lab.connect() as conn:
            audit = conn.execute(
                """
                SELECT operator_id, approval_id, stream_name,
                       business_subject, canary_subject
                FROM kernel_lab.tenant_deployment_activation_journal
                WHERE receipt_id = %s
                """, (receipt.receipt_id,)
            ).fetchone()
            assert audit == (
                "reviewer", "ACT-100", binding.stream,
                binding.route.subject, binding.canary_subject,
            )
            state = conn.execute(
                "SELECT tenant_id, activation_receipt_id FROM "
                "kernel_lab.tenant_deployment_activation_state "
                "WHERE deployment_id = %s", (binding.deployment.deployment_id,)
            ).fetchone()
            assert state == (binding.deployment.tenant_id, receipt.receipt_id)

        with pytest.raises(psycopg.errors.UniqueViolation):
            controller().activate(
                binding, operator_id="reviewer", approval_id="ACT-101"
            )
        with pytest.raises(psycopg.errors.UniqueViolation):
            controller().activate(
                binding, operator_id="different-reviewer", approval_id="ACT-100"
            )
        with lab.connect() as conn:
            with pytest.raises(psycopg.errors.CheckViolation, match="append-only"):
                conn.execute(
                    "DELETE FROM kernel_lab.tenant_deployment_activation_journal "
                    "WHERE receipt_id = %s", (receipt.receipt_id,)
                )
    finally:
        with asyncio.Runner() as runner:
            runner.run(reset_stream(create=False))


def test_activation_denied_without_canary_rights_and_never_writes_journal():
    binding = lab_binding()
    with asyncio.Runner() as runner:
        runner.run(reset_stream(create=True))
    try:
        TenantTopologyProvisioner(auth_lab.ADMIN_URL).provision(
            binding, operator_id="provisioner", approval_id="topology-review"
        )
        with pytest.raises(ValueError, match="tenant_canary_unverified"):
            controller(
                canary_user="lab_tenant_b_sub", canary_password="lab-b-sub"
            ).activate(binding, operator_id="reviewer", approval_id="ACT-DENIED")
        with lab.connect() as conn:
            assert conn.execute(
                "SELECT count(*) FROM kernel_lab.tenant_deployment_activation_journal"
            ).fetchone()[0] == 0
    finally:
        with asyncio.Runner() as runner:
            runner.run(reset_stream(create=False))


def test_journal_rejects_stale_evidence_without_state_change():
    binding = lab_binding()
    old = datetime.now(timezone.utc) - timedelta(minutes=10)
    with lab.connect() as conn:
        with pytest.raises(psycopg.errors.CheckViolation):
            conn.execute(
                """
                SELECT kernel_lab.record_tenant_deployment_activation(
                  %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                )
                """,
                (
                    binding.deployment.deployment_id,
                    binding.deployment.tenant_id,
                    "reviewer", "STALE-APPROVAL", binding.stream,
                    binding.deployment.business_durable,
                    binding.deployment.canary_durable,
                    binding.route.subject, binding.canary_subject, old, old,
                ),
            )
    with lab.connect() as conn:
        assert conn.execute(
            "SELECT count(*) FROM kernel_lab.tenant_deployment_activation_state"
        ).fetchone()[0] == 0
