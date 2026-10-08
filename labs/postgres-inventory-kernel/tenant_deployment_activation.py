"""Explicit, audited tenant pilot activation after live broker and canary proof.

Controlled API only. The signed/off-platform approval decision and DB role
grants are external prerequisites, never inferred from approval_id text.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

import psycopg

from tenant_deployment_probe import TenantDeploymentProbe
from tenant_event_deployment import (
    TenantDeploymentBinding, TenantReadinessGate, TenantTopologyProvisioner,
)


@dataclass(frozen=True)
class TenantActivationReceipt:
    deployment_id: str
    tenant_id: UUID
    receipt_id: UUID
    status: str

    def to_dict(self) -> dict:
        return {
            "deployment_id": self.deployment_id,
            "tenant_id": str(self.tenant_id),
            "receipt_id": str(self.receipt_id),
            "status": self.status,
        }


class PostgresTenantActivationJournal:
    def __init__(self, database_url: str):
        if not isinstance(database_url, str) or not database_url.strip():
            raise ValueError("database_url is required")
        self.database_url = database_url

    def record(
        self,
        binding: TenantDeploymentBinding,
        *,
        operator_id: str,
        approval_id: str,
        acl_verified_at: datetime,
        canary_verified_at: datetime,
    ) -> TenantActivationReceipt:
        deployment = binding.deployment
        with psycopg.connect(self.database_url) as conn:
            receipt_id = conn.execute(
                """
                SELECT kernel_lab.record_tenant_deployment_activation(
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                )
                """,
                (
                    deployment.deployment_id, deployment.tenant_id,
                    operator_id, approval_id, binding.stream,
                    deployment.business_durable, deployment.canary_durable,
                    binding.route.subject, binding.canary_subject,
                    acl_verified_at, canary_verified_at,
                ),
            ).fetchone()[0]
            conn.commit()
        return TenantActivationReceipt(
            deployment_id=deployment.deployment_id,
            tenant_id=deployment.tenant_id,
            receipt_id=receipt_id,
            status="activated",
        )


class TenantActivationController:
    """Only this path records activation: exact topology and live proofs first."""

    def __init__(
        self, *,
        database_url: str,
        provisioner_nats_url: str,
        publisher_nats_url: str,
        consumer_nats_url: str,
        canary_consumer_nats_url: str,
    ):
        self.journal = PostgresTenantActivationJournal(database_url)
        self.provisioner = TenantTopologyProvisioner(provisioner_nats_url)
        self.probe = TenantDeploymentProbe(
            publisher_nats_url=publisher_nats_url,
            consumer_nats_url=consumer_nats_url,
            canary_consumer_nats_url=canary_consumer_nats_url,
        )

    def activate(
        self, binding: TenantDeploymentBinding, *,
        operator_id: str, approval_id: str,
    ) -> TenantActivationReceipt:
        if not binding.deployment.enabled:
            raise ValueError("tenant deployment is disabled in registry")
        if not operator_id or operator_id != operator_id.strip():
            raise ValueError("operator_id is required")
        if not approval_id or approval_id != approval_id.strip():
            raise ValueError("approval_id is required")

        topology = self.provisioner.plan(binding)
        if topology.status != "ready":
            raise ValueError(f"tenant topology not ready: {topology.code}")
        acl_proof, canary_proof = self.probe.run(binding)
        readiness = TenantReadinessGate().assess(
            binding, topology=topology,
            acl_proof=acl_proof, canary_proof=canary_proof,
        )
        if readiness.status != "ready":
            raise ValueError(
                "tenant activation refused: " + ",".join(readiness.codes)
            )
        return self.journal.record(
            binding,
            operator_id=operator_id, approval_id=approval_id,
            acl_verified_at=datetime.fromisoformat(acl_proof["observed_at"]),
            canary_verified_at=datetime.fromisoformat(canary_proof["observed_at"]),
        )
