"""Activation-fenced explicit tenant cutover, health checks and safe rollback.

This is a bounded event-owner migration, not a live publisher orchestration
agent. Operators remain responsible for actual worker scheduling and RBAC.
"""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID
from typing import Any

import psycopg

from tenant_deployment_probe import TenantDeploymentProbe
from tenant_event_deployment import (
    TenantDeploymentBinding, TenantReadinessGate, TenantTopologyProvisioner,
)


@dataclass(frozen=True)
class TenantRolloutSnapshot:
    phase: str
    deployment_id: str
    tenant_id: UUID
    tenant_pending: int
    tenant_attempted_unpublished: int
    published_unprojected: int
    oldest_projection_lag_seconds: float

    @property
    def safe_for_next_wave(self) -> bool:
        return (
            self.phase == "active"
            and self.tenant_pending == 0
            and self.published_unprojected == 0
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "phase": self.phase,
            "deployment_id": self.deployment_id,
            "tenant_id": str(self.tenant_id),
            "tenant_pending": self.tenant_pending,
            "tenant_attempted_unpublished": self.tenant_attempted_unpublished,
            "published_unprojected": self.published_unprojected,
            "oldest_projection_lag_seconds": self.oldest_projection_lag_seconds,
            "safe_for_next_wave": self.safe_for_next_wave,
        }


class PostgresTenantRolloutStore:
    def __init__(self, database_url: str):
        if not isinstance(database_url, str) or not database_url.strip():
            raise ValueError("database_url is required")
        self.database_url = database_url

    def _conn(self) -> psycopg.Connection:
        conn = psycopg.connect(self.database_url)
        conn.execute("SET statement_timeout='8s'")
        conn.execute("SET lock_timeout='5s'")
        return conn

    def start(
        self, binding: TenantDeploymentBinding,
        *, activation_receipt: UUID, operator_id: str, change_ref: str,
    ) -> UUID:
        with self._conn() as conn:
            generation = conn.execute(
                "SELECT kernel_lab.begin_tenant_rollout(%s,%s,%s,%s,%s)",
                (binding.deployment.deployment_id, binding.deployment.tenant_id,
                 activation_receipt, operator_id, change_ref),
            ).fetchone()[0]
            conn.commit()
        return generation

    def stage(
        self, binding: TenantDeploymentBinding, *,
        event_ids: tuple[UUID, ...], operator_id: str, change_ref: str,
    ) -> int:
        with self._conn() as conn:
            changed = conn.execute(
                "SELECT kernel_lab.stage_tenant_rollout_batch(%s,%s,%s,%s,%s)",
                (binding.deployment.deployment_id, binding.deployment.tenant_id,
                 list(event_ids), operator_id, change_ref),
            ).fetchone()[0]
            conn.commit()
        return changed

    def pause(
        self, binding: TenantDeploymentBinding, *,
        operator_id: str, change_ref: str,
    ) -> bool:
        with self._conn() as conn:
            paused = conn.execute(
                "SELECT kernel_lab.pause_tenant_rollout(%s,%s,%s,%s)",
                (binding.deployment.deployment_id, binding.deployment.tenant_id,
                 operator_id, change_ref),
            ).fetchone()[0]
            conn.commit()
        return paused

    def rollback(
        self, binding: TenantDeploymentBinding, *,
        event_ids: tuple[UUID, ...], operator_id: str, change_ref: str,
    ) -> int:
        with self._conn() as conn:
            reverted = conn.execute(
                "SELECT kernel_lab.rollback_tenant_rollout_batch(%s,%s,%s,%s,%s)",
                (binding.deployment.deployment_id, binding.deployment.tenant_id,
                 list(event_ids), operator_id, change_ref),
            ).fetchone()[0]
            conn.commit()
        return reverted

    def snapshot(self, binding: TenantDeploymentBinding) -> TenantRolloutSnapshot:
        d = binding.deployment
        with self._conn() as conn:
            phase = conn.execute(
                "SELECT phase FROM kernel_lab.tenant_deployment_rollout_state "
                "WHERE deployment_id=%s AND tenant_id=%s",
                (d.deployment_id, d.tenant_id),
            ).fetchone()
            if phase is None:
                return TenantRolloutSnapshot(
                    "not_started", d.deployment_id, d.tenant_id, 0, 0, 0, 0.0
                )
            rows = conn.execute(
                """
                SELECT
                    count(*) FILTER (WHERE o.published_at IS NULL) AS pending,
                    count(*) FILTER (
                        WHERE o.published_at IS NULL AND o.attempt_count > 0
                    ) AS attempted,
                    count(*) FILTER (
                        WHERE o.published_at IS NOT NULL AND p.event_id IS NULL
                    ) AS unprojected,
                    COALESCE(max(
                        extract(epoch FROM (clock_timestamp() - o.published_at))
                    ) FILTER (
                        WHERE o.published_at IS NOT NULL AND p.event_id IS NULL
                    ), 0) AS longest_lag
                FROM kernel_lab.domain_event_outbox o
                JOIN kernel_lab.tenant_deployment_rollout_journal j
                  ON j.event_id = o.event_id
                 AND j.action = 'staged'
                 AND j.deployment_id = %s
                LEFT JOIN kernel_lab.inventory_transaction_index_projection p
                  ON p.event_id = o.event_id
                 AND p.tenant_id = %s
                 AND p.consumer_name = %s
                WHERE o.tenant_id = %s AND o.delivery_route = 'tenant'
                """,
                (d.deployment_id, d.tenant_id,
                 d.inbox_consumer_name, d.tenant_id),
            ).fetchone()
        return TenantRolloutSnapshot(
            phase[0], d.deployment_id, d.tenant_id,
            int(rows[0]), int(rows[1]), int(rows[2]), float(rows[3]),
        )


class TenantRolloutController:
    def __init__(
        self, *, database_url: str, provisioner_nats_url: str,
        publisher_nats_url: str, consumer_nats_url: str,
        canary_consumer_nats_url: str,
    ):
        self.store = PostgresTenantRolloutStore(database_url)
        self.provisioner = TenantTopologyProvisioner(provisioner_nats_url)
        self.probe = TenantDeploymentProbe(
            publisher_nats_url=publisher_nats_url,
            consumer_nats_url=consumer_nats_url,
            canary_consumer_nats_url=canary_consumer_nats_url,
        )

    def _require_readiness(self, binding: TenantDeploymentBinding) -> None:
        if not binding.deployment.enabled:
            raise ValueError("rollout is disabled in registry")
        plan = self.provisioner.plan(binding)
        acl, canary = self.probe.run(binding) if plan.status == "ready" else (None, None)
        report = TenantReadinessGate().assess(
            binding, topology=plan, acl_proof=acl, canary_proof=canary
        )
        if report.status != "ready":
            raise ValueError("tenant rollout preflight failed: " + ",".join(report.codes))

    def start(
        self, binding: TenantDeploymentBinding, *,
        activation_receipt: UUID, operator_id: str, change_ref: str,
    ) -> UUID:
        self._require_readiness(binding)
        return self.store.start(
            binding, activation_receipt=activation_receipt,
            operator_id=operator_id, change_ref=change_ref
        )

    def stage(
        self, binding: TenantDeploymentBinding, *, event_ids: tuple[UUID, ...],
        operator_id: str, change_ref: str,
    ) -> int:
        self._require_readiness(binding)
        if not self.store.snapshot(binding).safe_for_next_wave:
            raise ValueError("tenant rollout has pending deliveries or projection lag")
        # Database transaction is the authoritative atomic lease fence; this
        # preflight does not replace server-side checked UPDATE predicates.
        return self.store.stage(
            binding, event_ids=event_ids, operator_id=operator_id,
            change_ref=change_ref,
        )
