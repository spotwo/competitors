from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Literal, Mapping
from uuid import UUID

from event_pipeline_topology_crash_safe_provisioner import (
    CrashSafeAuthorizedEventPipelineTopologyProvisioner,
    CrashSafeTopologyProvisioningReport,
)
from event_pipeline_topology_migration import TopologyMigrationConfig
from event_pipeline_topology_preflight import EventPipelineTopologyPreflightReport
from event_pipeline_topology_provisioning_health import (
    PostgresTopologyProvisioningHealthStore,
    TopologyProvisioningHealthSnapshot,
)
from event_pipeline_topology_provisioning_journal import (
    PostgresTopologyProvisioningJournal,
    TopologyProvisioningJournalError,
    TopologyProvisioningJournalRun,
)

ControllerStatus = Literal["idle", "deferred", "reconciled", "blocked", "failed"]


def _required_name(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise ValueError(f"{field} must be a normalized non-empty string")
    return value


@dataclass(frozen=True)
class TopologyReconciliationControllerReport:
    observed_at: datetime
    pipeline_id: str
    status: ControllerStatus
    code: str
    run_id: UUID | None = None
    state_before: str | None = None
    provisioning: CrashSafeTopologyProvisioningReport | None = None

    @property
    def successful(self) -> bool:
        return self.status in {"idle", "deferred", "reconciled"}

    def to_dict(self) -> dict[str, Any]:
        return {
            "observed_at": self.observed_at.isoformat(),
            "pipeline": self.pipeline_id,
            "status": self.status,
            "code": self.code,
            "recovery_only": True,
            "starts_new_migrations": False,
            "run_id": str(self.run_id) if self.run_id else None,
            "state_before": self.state_before,
            "provisioning": (
                self.provisioning.to_dict() if self.provisioning is not None else None
            ),
        }


RecoveryExecutor = Callable[..., CrashSafeTopologyProvisioningReport]


class UnattendedTopologyReconciliationController:
    """Recover expired journaled topology runs without ever starting a new rollout."""

    def __init__(
        self,
        *,
        database_url: str,
        nats_url: str,
        owner_id: str,
        lease_seconds: float,
        health_store: PostgresTopologyProvisioningHealthStore | None = None,
        journal: PostgresTopologyProvisioningJournal | None = None,
        provisioner: CrashSafeAuthorizedEventPipelineTopologyProvisioner | None = None,
        recovery_executor: RecoveryExecutor | None = None,
    ):
        self.database_url = _required_name(database_url, "database_url")
        self.nats_url = _required_name(nats_url, "nats_url")
        self.owner_id = _required_name(owner_id, "owner_id")
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        self.lease_seconds = float(lease_seconds)
        self.health_store = health_store or PostgresTopologyProvisioningHealthStore(
            self.database_url
        )
        self.journal = journal or PostgresTopologyProvisioningJournal(
            self.database_url,
            owner_id=self.owner_id,
            lease_seconds=self.lease_seconds,
        )
        self.provisioner = provisioner or CrashSafeAuthorizedEventPipelineTopologyProvisioner(
            self.nats_url,
            journal=self.journal,
        )
        self.recovery_executor = recovery_executor or self._recover_claimed_run

    @staticmethod
    def _report(
        *,
        observed_at: datetime,
        pipeline_id: str,
        status: ControllerStatus,
        code: str,
        run_id: UUID | None = None,
        state_before: str | None = None,
        provisioning: CrashSafeTopologyProvisioningReport | None = None,
    ) -> TopologyReconciliationControllerReport:
        return TopologyReconciliationControllerReport(
            observed_at=observed_at,
            pipeline_id=pipeline_id,
            status=status,
            code=code,
            run_id=run_id,
            state_before=state_before,
            provisioning=provisioning,
        )

    def reconcile_once(
        self,
        spec: Any,
        *,
        target_topology: Mapping[str, Any],
        migration_mapping: Mapping[str, Any] | None,
        readiness_verifier: Any,
        canary_verifier: Any,
        observed_at: datetime | None = None,
    ) -> TopologyReconciliationControllerReport:
        observed_at = observed_at or datetime.now(timezone.utc)
        pipeline_id = _required_name(spec.pipeline_id, "pipeline_id")

        try:
            snapshot = self.health_store.snapshot(
                pipeline_id=pipeline_id,
                observed_at=observed_at,
            )
        except Exception:
            return self._report(
                observed_at=observed_at,
                pipeline_id=pipeline_id,
                status="failed",
                code="topology_reconciler_journal_health_unavailable",
            )

        active = snapshot.active
        if active is None:
            return self._report(
                observed_at=observed_at,
                pipeline_id=pipeline_id,
                status="idle",
                code="topology_reconciler_no_active_execution",
            )

        if not active.lease_expired:
            return self._report(
                observed_at=observed_at,
                pipeline_id=pipeline_id,
                status="deferred",
                code="topology_reconciler_live_lease",
                run_id=active.run_id,
                state_before=active.state,
            )

        if migration_mapping is None:
            return self._report(
                observed_at=observed_at,
                pipeline_id=pipeline_id,
                status="blocked",
                code="topology_reconciler_migration_contract_missing",
                run_id=active.run_id,
                state_before=active.state,
            )

        try:
            migration = TopologyMigrationConfig.from_mapping(migration_mapping)
        except ValueError:
            return self._report(
                observed_at=observed_at,
                pipeline_id=pipeline_id,
                status="blocked",
                code="topology_reconciler_migration_contract_invalid",
                run_id=active.run_id,
                state_before=active.state,
            )

        if migration.migration_id != active.migration_id:
            return self._report(
                observed_at=observed_at,
                pipeline_id=pipeline_id,
                status="blocked",
                code="topology_reconciler_migration_contract_mismatch",
                run_id=active.run_id,
                state_before=active.state,
            )

        try:
            run = self.journal.claim_active(
                pipeline_id=pipeline_id,
                migration_id=active.migration_id,
            )
        except TopologyProvisioningJournalError as exc:
            if exc.code == "topology_provisioning_lease_held":
                return self._report(
                    observed_at=observed_at,
                    pipeline_id=pipeline_id,
                    status="deferred",
                    code="topology_reconciler_lease_recovered_by_other_owner",
                    run_id=active.run_id,
                    state_before=active.state,
                )
            return self._report(
                observed_at=observed_at,
                pipeline_id=pipeline_id,
                status="blocked",
                code=exc.code,
                run_id=active.run_id,
                state_before=active.state,
            )
        except Exception:
            return self._report(
                observed_at=observed_at,
                pipeline_id=pipeline_id,
                status="failed",
                code="topology_reconciler_claim_unavailable",
                run_id=active.run_id,
                state_before=active.state,
            )

        if run is None:
            return self._report(
                observed_at=observed_at,
                pipeline_id=pipeline_id,
                status="idle",
                code="topology_reconciler_execution_completed_before_claim",
                run_id=active.run_id,
                state_before=active.state,
            )

        try:
            provisioning = self.recovery_executor(
                run=run,
                observed_at=observed_at,
                spec=spec,
                target_topology=target_topology,
                migration_mapping=migration_mapping,
                readiness_verifier=readiness_verifier,
                canary_verifier=canary_verifier,
            )
        except Exception:
            return self._report(
                observed_at=observed_at,
                pipeline_id=pipeline_id,
                status="failed",
                code="topology_reconciler_recovery_unavailable",
                run_id=run.run_id,
                state_before=active.state,
            )

        if provisioning.status in {"succeeded", "rolled_back"}:
            status: ControllerStatus = "reconciled"
        elif provisioning.status == "blocked":
            status = "blocked"
        else:
            status = "failed"
        return self._report(
            observed_at=observed_at,
            pipeline_id=pipeline_id,
            status=status,
            code=provisioning.code,
            run_id=run.run_id,
            state_before=active.state,
            provisioning=provisioning,
        )

    def _recover_claimed_run(
        self,
        *,
        run: TopologyProvisioningJournalRun,
        observed_at: datetime,
        spec: Any,
        target_topology: Mapping[str, Any],
        migration_mapping: Mapping[str, Any],
        readiness_verifier: Any,
        canary_verifier: Any,
    ) -> CrashSafeTopologyProvisioningReport:
        # The controller intentionally bypasses the normal execute entry point.
        # It has already claimed one existing expired run and must not enter the
        # code path that can create a new rollout when no active run exists.
        preflight: EventPipelineTopologyPreflightReport = (
            self.provisioner.preflight_planner.plan(
                spec,
                target_topology=target_topology,
                migration_mapping=migration_mapping,
                observed_at=observed_at,
            )
        )
        return self.provisioner._reconcile(
            run,
            observed_at=observed_at,
            spec=spec,
            target_topology=target_topology,
            migration_mapping=migration_mapping,
            migration_id=run.migration_id,
            preflight=preflight,
            readiness_verifier=readiness_verifier,
            canary_verifier=canary_verifier,
        )
