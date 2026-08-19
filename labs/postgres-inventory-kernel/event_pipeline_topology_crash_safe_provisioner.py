from __future__ import annotations

import asyncio
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping

from event_pipeline_topology_migration import TopologyContractInspector, TopologyMigrationConfig
from event_pipeline_topology_preflight import (
    EventPipelineTopologyPreflightPlanner,
    EventPipelineTopologyPreflightReport,
)
from event_pipeline_topology_provisioner import (
    SUPPORTED_MUTATION_FIELDS,
    AuthorizedTopologyProvisioningReport,
    MutationReceipt,
    NatsJetStreamTopologyMutator,
    RollbackReceipt,
    TopologyMutationError,
)
from event_pipeline_topology_provisioning_journal import (
    PostgresTopologyProvisioningJournal,
    TopologyProvisioningJournalError,
    TopologyProvisioningJournalRun,
)


def _required_name(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise ValueError(f"{field} must be a normalized non-empty string")
    return value


def _status(report: Any) -> str | None:
    return getattr(report, "status", None)


@dataclass(frozen=True)
class CrashSafeTopologyProvisioningReport:
    result: AuthorizedTopologyProvisioningReport
    journal: TopologyProvisioningJournalRun | None

    @property
    def status(self) -> str:
        return self.result.status

    @property
    def code(self) -> str:
        return self.result.code

    @property
    def mutation(self) -> MutationReceipt | None:
        return self.result.mutation

    @property
    def rollback(self) -> RollbackReceipt:
        return self.result.rollback

    def to_dict(self) -> dict[str, Any]:
        value = self.result.to_dict()
        value["execution_journal"] = self.journal.to_dict() if self.journal else None
        value["crash_safe_reconciliation"] = self.journal is not None
        return value


class RecoverableNatsJetStreamTopologyMutator(NatsJetStreamTopologyMutator):
    """Extend the guarded mutator with snapshot-based rollback after process restart."""

    def rollback_to_snapshot(self, *, spec: Any, resource: str, source_snapshot: Any) -> None:
        runner = asyncio.Runner()
        try:
            runner.run(
                self._rollback_to_snapshot(
                    spec=spec,
                    resource=resource,
                    source_snapshot=source_snapshot,
                )
            )
        finally:
            runner.close()

    async def _rollback_to_snapshot(self, *, spec: Any, resource: str, source_snapshot: Any) -> None:
        client = None
        try:
            client = await self._connect()
            jetstream = client.jetstream()
            info = await self._read_info(jetstream, spec, resource)
            if info is None:
                raise TopologyMutationError("topology_recovery_resource_missing")
            config = deepcopy(info.config)
            fields = source_snapshot.fields
            if resource == "stream":
                config.num_replicas = int(fields["replicas"])
                config.duplicate_window = float(fields["duplicate_window_seconds"])
            else:
                config.max_ack_pending = int(fields["max_ack_pending"])
                config.max_deliver = int(fields["max_deliver"])
            await self._write_config(jetstream, spec, resource, config)
        finally:
            if client is not None and not client.is_closed:
                try:
                    await client.close()
                except Exception:
                    pass


class CrashSafeAuthorizedEventPipelineTopologyProvisioner:
    """Execute or reconcile one journaled topology migration under a PostgreSQL lease."""

    def __init__(
        self,
        server_url: str,
        *,
        journal: PostgresTopologyProvisioningJournal,
        preflight_planner: EventPipelineTopologyPreflightPlanner | None = None,
        contract_inspector: TopologyContractInspector | None = None,
        mutator: RecoverableNatsJetStreamTopologyMutator | None = None,
    ):
        self.server_url = _required_name(server_url, "server_url")
        self.journal = journal
        self.preflight_planner = preflight_planner or EventPipelineTopologyPreflightPlanner(
            server_url
        )
        self.contract_inspector = contract_inspector or TopologyContractInspector(server_url)
        self.mutator = mutator or RecoverableNatsJetStreamTopologyMutator(server_url)

    @staticmethod
    def _terminal(
        *,
        observed_at: datetime,
        spec: Any,
        status: str,
        code: str,
        migration_id: str,
        preflight: EventPipelineTopologyPreflightReport,
        journal: TopologyProvisioningJournalRun | None = None,
        readiness_before: Any | None = None,
        mutation: MutationReceipt | None = None,
        canary_after: Any | None = None,
        readiness_after: Any | None = None,
        rollback: RollbackReceipt | None = None,
    ) -> CrashSafeTopologyProvisioningReport:
        base = AuthorizedTopologyProvisioningReport(
            observed_at=observed_at,
            pipeline_id=spec.pipeline_id,
            status=status,
            code=code,
            authorized_migration_id=migration_id,
            preflight=preflight,
            readiness_before=readiness_before,
            mutation=mutation,
            canary_after=canary_after,
            readiness_after=readiness_after,
            rollback=rollback or RollbackReceipt(False, False, False, None),
        )
        return CrashSafeTopologyProvisioningReport(base, journal)

    def _journal_error(
        self,
        *,
        exc: TopologyProvisioningJournalError,
        observed_at: datetime,
        spec: Any,
        migration_id: str,
        preflight: EventPipelineTopologyPreflightReport,
        journal: TopologyProvisioningJournalRun | None = None,
        readiness_before: Any | None = None,
        mutation: MutationReceipt | None = None,
    ) -> CrashSafeTopologyProvisioningReport:
        status = "blocked" if exc.code in {
            "topology_provisioning_lease_held",
            "topology_provisioning_active_migration_conflict",
            "topology_provisioning_intent_mismatch",
            "topology_provisioning_start_race",
        } else "failed"
        return self._terminal(
            observed_at=observed_at,
            spec=spec,
            status=status,
            code=exc.code,
            migration_id=migration_id,
            preflight=preflight,
            journal=journal,
            readiness_before=readiness_before,
            mutation=mutation,
        )

    def execute(
        self,
        spec: Any,
        *,
        target_topology: Mapping[str, Any],
        migration_mapping: Mapping[str, Any] | None,
        authorized_migration_id: str,
        readiness_verifier: Any,
        canary_verifier: Any,
        observed_at: datetime | None = None,
    ) -> CrashSafeTopologyProvisioningReport:
        observed_at = observed_at or datetime.now(timezone.utc)
        migration_id = _required_name(authorized_migration_id, "authorized_migration_id")
        preflight = self.preflight_planner.plan(
            spec,
            target_topology=target_topology,
            migration_mapping=migration_mapping,
            observed_at=observed_at,
        )

        if migration_mapping is None:
            return self._terminal(
                observed_at=observed_at,
                spec=spec,
                status="blocked",
                code="topology_migration_required",
                migration_id=migration_id,
                preflight=preflight,
            )

        migration = TopologyMigrationConfig.from_mapping(migration_mapping)
        if migration.migration_id != migration_id:
            return self._terminal(
                observed_at=observed_at,
                spec=spec,
                status="blocked",
                code="topology_migration_authorization_mismatch",
                migration_id=migration_id,
                preflight=preflight,
            )

        try:
            active = self.journal.claim_active(
                pipeline_id=spec.pipeline_id,
                migration_id=migration_id,
            )
        except TopologyProvisioningJournalError as exc:
            return self._journal_error(
                exc=exc,
                observed_at=observed_at,
                spec=spec,
                migration_id=migration_id,
                preflight=preflight,
            )

        if active is not None:
            return self._reconcile(
                active,
                observed_at=observed_at,
                spec=spec,
                target_topology=target_topology,
                migration_mapping=migration_mapping,
                migration_id=migration_id,
                preflight=preflight,
                readiness_verifier=readiness_verifier,
                canary_verifier=canary_verifier,
            )

        if preflight.decision != "safe_to_apply":
            return self._terminal(
                observed_at=observed_at,
                spec=spec,
                status="blocked",
                code=preflight.code,
                migration_id=migration_id,
                preflight=preflight,
            )

        resources = tuple(dict.fromkeys(change.resource for change in preflight.changes))
        if len(resources) != 1:
            raise RuntimeError("safe preflight must contain exactly one resource")
        resource = resources[0]
        if any(
            change.field not in SUPPORTED_MUTATION_FIELDS.get(resource, frozenset())
            for change in preflight.changes
        ):
            return self._terminal(
                observed_at=observed_at,
                spec=spec,
                status="blocked",
                code="topology_mutation_not_supported",
                migration_id=migration_id,
                preflight=preflight,
            )

        readiness_before = readiness_verifier()
        if _status(readiness_before) != "ready":
            return self._terminal(
                observed_at=observed_at,
                spec=spec,
                status="blocked",
                code="topology_preapply_readiness_not_ready",
                migration_id=migration_id,
                preflight=preflight,
                readiness_before=readiness_before,
            )

        source_snapshot = next(
            item for item in preflight.contract.target.resources if item.resource == resource
        )
        try:
            run = self.journal.start(
                pipeline_id=spec.pipeline_id,
                migration_id=migration_id,
                resource=resource,
                source_snapshot=source_snapshot,
                changes=preflight.changes,
            )
        except TopologyProvisioningJournalError as exc:
            return self._journal_error(
                exc=exc,
                observed_at=observed_at,
                spec=spec,
                migration_id=migration_id,
                preflight=preflight,
                readiness_before=readiness_before,
            )

        if run.recovered or run.state != "prepared":
            return self._reconcile(
                run,
                observed_at=observed_at,
                spec=spec,
                target_topology=target_topology,
                migration_mapping=migration_mapping,
                migration_id=migration_id,
                preflight=preflight,
                readiness_verifier=readiness_verifier,
                canary_verifier=canary_verifier,
                readiness_before=readiness_before,
            )

        return self._apply_new(
            run,
            observed_at=observed_at,
            spec=spec,
            target_topology=target_topology,
            migration_mapping=migration_mapping,
            migration_id=migration_id,
            preflight=preflight,
            readiness_before=readiness_before,
            readiness_verifier=readiness_verifier,
            canary_verifier=canary_verifier,
        )

    def _apply_new(
        self,
        run: TopologyProvisioningJournalRun,
        *,
        observed_at: datetime,
        spec: Any,
        target_topology: Mapping[str, Any],
        migration_mapping: Mapping[str, Any],
        migration_id: str,
        preflight: EventPipelineTopologyPreflightReport,
        readiness_before: Any,
        readiness_verifier: Any,
        canary_verifier: Any,
    ) -> CrashSafeTopologyProvisioningReport:
        source_config = None
        mutation = None
        canary_after = None
        readiness_after = None
        try:
            run = self.journal.transition(run.run_id, "applying")
            actual_source, source_config = self.mutator.apply(
                spec=spec,
                resource=run.resource,
                changes=preflight.changes,
                expected_source=run.source_snapshot,
            )
            mutation = MutationReceipt(
                resource=run.resource,
                fields=run.changed_fields,
                source=actual_source,
                target_verified=False,
            )
            run = self.journal.transition(run.run_id, "applied")

            contract_after = self.contract_inspector.inspect(
                spec,
                target_topology=target_topology,
                migration_mapping=migration_mapping,
            )
            if contract_after.matched != "target" or contract_after.target.status != "ok":
                raise RuntimeError("topology_target_verification_failed")
            mutation = MutationReceipt(
                resource=run.resource,
                fields=run.changed_fields,
                source=actual_source,
                target_verified=True,
            )
            run = self.journal.transition(run.run_id, "target_verified")

            canary_after = canary_verifier()
            if _status(canary_after) != "ok":
                raise RuntimeError("topology_postapply_canary_not_ok")
            run = self.journal.transition(run.run_id, "canary_verified")

            readiness_after = readiness_verifier()
            if _status(readiness_after) != "ready":
                raise RuntimeError("topology_postapply_readiness_not_ready")
            run = self.journal.transition(run.run_id, "readiness_verified")
            run = self.journal.transition(
                run.run_id,
                "completed",
                code="topology_target_applied_and_verified",
            )
            return self._terminal(
                observed_at=observed_at,
                spec=spec,
                status="succeeded",
                code="topology_target_applied_and_verified",
                migration_id=migration_id,
                preflight=preflight,
                journal=run,
                readiness_before=readiness_before,
                mutation=mutation,
                canary_after=canary_after,
                readiness_after=readiness_after,
            )
        except TopologyProvisioningJournalError as exc:
            return self._journal_error(
                exc=exc,
                observed_at=observed_at,
                spec=spec,
                migration_id=migration_id,
                preflight=preflight,
                journal=run,
                readiness_before=readiness_before,
                mutation=mutation,
            )
        except Exception as exc:
            failure_code = self._failure_code(exc)
            if source_config is None:
                try:
                    run = self.journal.transition(run.run_id, "failed", code=failure_code)
                except TopologyProvisioningJournalError as journal_exc:
                    return self._journal_error(
                        exc=journal_exc,
                        observed_at=observed_at,
                        spec=spec,
                        migration_id=migration_id,
                        preflight=preflight,
                        journal=run,
                        readiness_before=readiness_before,
                        mutation=mutation,
                    )
                return self._terminal(
                    observed_at=observed_at,
                    spec=spec,
                    status="failed",
                    code=failure_code,
                    migration_id=migration_id,
                    preflight=preflight,
                    journal=run,
                    readiness_before=readiness_before,
                    mutation=mutation,
                    canary_after=canary_after,
                    readiness_after=readiness_after,
                )
            return self._rollback_in_process(
                run,
                source_config=source_config,
                failure_code=failure_code,
                observed_at=observed_at,
                spec=spec,
                target_topology=target_topology,
                migration_mapping=migration_mapping,
                migration_id=migration_id,
                preflight=preflight,
                readiness_before=readiness_before,
                mutation=mutation,
                canary_after=canary_after,
                readiness_after=readiness_after,
            )

    @staticmethod
    def _failure_code(exc: Exception) -> str:
        if isinstance(exc, TopologyMutationError):
            value = exc.code
        else:
            value = str(exc)
        if value in {
            "topology_source_changed_before_apply",
            "topology_source_resource_missing",
            "topology_target_verification_failed",
            "topology_postapply_canary_not_ok",
            "topology_postapply_readiness_not_ready",
        }:
            return value
        return "topology_mutation_failed"

    def _rollback_in_process(
        self,
        run: TopologyProvisioningJournalRun,
        *,
        source_config: Any,
        failure_code: str,
        observed_at: datetime,
        spec: Any,
        target_topology: Mapping[str, Any],
        migration_mapping: Mapping[str, Any],
        migration_id: str,
        preflight: EventPipelineTopologyPreflightReport,
        readiness_before: Any,
        mutation: MutationReceipt | None,
        canary_after: Any | None,
        readiness_after: Any | None,
    ) -> CrashSafeTopologyProvisioningReport:
        try:
            run = self.journal.transition(run.run_id, "rollback_started", code=failure_code)
        except TopologyProvisioningJournalError as exc:
            return self._journal_error(
                exc=exc,
                observed_at=observed_at,
                spec=spec,
                migration_id=migration_id,
                preflight=preflight,
                journal=run,
                readiness_before=readiness_before,
                mutation=mutation,
            )
        try:
            self.mutator.rollback(
                spec=spec,
                resource=run.resource,
                source_config=source_config,
            )
        except Exception:
            return self._finish_rollback_failure(
                run,
                code="topology_rollback_failed",
                observed_at=observed_at,
                spec=spec,
                migration_id=migration_id,
                preflight=preflight,
                readiness_before=readiness_before,
                mutation=mutation,
                canary_after=canary_after,
                readiness_after=readiness_after,
            )
        return self._verify_source_and_finish_rollback(
            run,
            failure_code=failure_code,
            observed_at=observed_at,
            spec=spec,
            target_topology=target_topology,
            migration_mapping=migration_mapping,
            migration_id=migration_id,
            preflight=preflight,
            readiness_before=readiness_before,
            mutation=mutation,
            canary_after=canary_after,
            readiness_after=readiness_after,
        )

    def _finish_rollback_failure(self, run: TopologyProvisioningJournalRun, **kwargs: Any) -> CrashSafeTopologyProvisioningReport:
        code = kwargs.pop("code")
        try:
            run = self.journal.transition(run.run_id, "failed", code=code)
        except TopologyProvisioningJournalError:
            pass
        return self._terminal(
            journal=run,
            status="failed",
            code=code,
            rollback=RollbackReceipt(True, False, False, code),
            **kwargs,
        )

    def _verify_source_and_finish_rollback(
        self,
        run: TopologyProvisioningJournalRun,
        *,
        failure_code: str,
        observed_at: datetime,
        spec: Any,
        target_topology: Mapping[str, Any],
        migration_mapping: Mapping[str, Any],
        migration_id: str,
        preflight: EventPipelineTopologyPreflightReport,
        readiness_before: Any | None = None,
        mutation: MutationReceipt | None = None,
        canary_after: Any | None = None,
        readiness_after: Any | None = None,
    ) -> CrashSafeTopologyProvisioningReport:
        try:
            contract = self.contract_inspector.inspect(
                spec,
                target_topology=target_topology,
                migration_mapping=migration_mapping,
            )
            source_verified = contract.matched == "source"
        except Exception:
            source_verified = False
        if not source_verified:
            return self._finish_rollback_failure(
                run,
                code="topology_rollback_verification_failed",
                observed_at=observed_at,
                spec=spec,
                migration_id=migration_id,
                preflight=preflight,
                readiness_before=readiness_before,
                mutation=mutation,
                canary_after=canary_after,
                readiness_after=readiness_after,
            )
        try:
            if run.state != "source_verified":
                run = self.journal.transition(run.run_id, "source_verified")
            run = self.journal.transition(run.run_id, "rolled_back", code=failure_code)
        except TopologyProvisioningJournalError as exc:
            return self._journal_error(
                exc=exc,
                observed_at=observed_at,
                spec=spec,
                migration_id=migration_id,
                preflight=preflight,
                journal=run,
                readiness_before=readiness_before,
                mutation=mutation,
            )
        return self._terminal(
            observed_at=observed_at,
            spec=spec,
            status="rolled_back",
            code=failure_code,
            migration_id=migration_id,
            preflight=preflight,
            journal=run,
            readiness_before=readiness_before,
            mutation=mutation,
            canary_after=canary_after,
            readiness_after=readiness_after,
            rollback=RollbackReceipt(True, True, True, None),
        )

    def _reconcile(
        self,
        run: TopologyProvisioningJournalRun,
        *,
        observed_at: datetime,
        spec: Any,
        target_topology: Mapping[str, Any],
        migration_mapping: Mapping[str, Any],
        migration_id: str,
        preflight: EventPipelineTopologyPreflightReport,
        readiness_verifier: Any,
        canary_verifier: Any,
        readiness_before: Any | None = None,
    ) -> CrashSafeTopologyProvisioningReport:
        try:
            contract = self.contract_inspector.inspect(
                spec,
                target_topology=target_topology,
                migration_mapping=migration_mapping,
            )
        except Exception:
            return self._terminal(
                observed_at=observed_at,
                spec=spec,
                status="failed",
                code="topology_recovery_inspection_unavailable",
                migration_id=migration_id,
                preflight=preflight,
                journal=run,
                readiness_before=readiness_before,
            )

        mutation = MutationReceipt(
            resource=run.resource,
            fields=run.changed_fields,
            source=run.source_snapshot,
            target_verified=contract.matched == "target" and contract.target.status == "ok",
        )

        if contract.matched == "source":
            try:
                if run.state == "source_verified":
                    pass
                elif run.state == "rollback_started":
                    run = self.journal.transition(run.run_id, "source_verified", code="topology_recovery_source_live")
                else:
                    run = self.journal.transition(run.run_id, "rollback_started", code="topology_recovery_source_live")
                    run = self.journal.transition(run.run_id, "source_verified", code="topology_recovery_source_live")
                run = self.journal.transition(run.run_id, "rolled_back", code="topology_recovery_source_live")
            except TopologyProvisioningJournalError as exc:
                return self._journal_error(
                    exc=exc,
                    observed_at=observed_at,
                    spec=spec,
                    migration_id=migration_id,
                    preflight=preflight,
                    journal=run,
                    readiness_before=readiness_before,
                    mutation=mutation,
                )
            return self._terminal(
                observed_at=observed_at,
                spec=spec,
                status="rolled_back",
                code="topology_recovery_source_live",
                migration_id=migration_id,
                preflight=preflight,
                journal=run,
                readiness_before=readiness_before,
                mutation=mutation,
                rollback=RollbackReceipt(True, True, True, None),
            )

        if contract.matched != "target" or contract.target.status != "ok":
            try:
                run = self.journal.transition(
                    run.run_id,
                    "manual_intervention",
                    code="topology_recovery_manual_intervention_required",
                )
            except TopologyProvisioningJournalError as exc:
                return self._journal_error(
                    exc=exc,
                    observed_at=observed_at,
                    spec=spec,
                    migration_id=migration_id,
                    preflight=preflight,
                    journal=run,
                    readiness_before=readiness_before,
                    mutation=mutation,
                )
            return self._terminal(
                observed_at=observed_at,
                spec=spec,
                status="failed",
                code="topology_recovery_manual_intervention_required",
                migration_id=migration_id,
                preflight=preflight,
                journal=run,
                readiness_before=readiness_before,
                mutation=mutation,
            )

        if run.state == "source_verified":
            try:
                run = self.journal.transition(
                    run.run_id,
                    "manual_intervention",
                    code="topology_recovery_state_live_contradiction",
                )
            except TopologyProvisioningJournalError as exc:
                return self._journal_error(
                    exc=exc,
                    observed_at=observed_at,
                    spec=spec,
                    migration_id=migration_id,
                    preflight=preflight,
                    journal=run,
                    readiness_before=readiness_before,
                    mutation=mutation,
                )
            return self._terminal(
                observed_at=observed_at,
                spec=spec,
                status="failed",
                code="topology_recovery_state_live_contradiction",
                migration_id=migration_id,
                preflight=preflight,
                journal=run,
                readiness_before=readiness_before,
                mutation=mutation,
            )

        if run.state == "rollback_started":
            try:
                self.mutator.rollback_to_snapshot(
                    spec=spec,
                    resource=run.resource,
                    source_snapshot=run.source_snapshot,
                )
            except Exception:
                return self._finish_rollback_failure(
                    run,
                    code="topology_recovery_rollback_failed",
                    observed_at=observed_at,
                    spec=spec,
                    migration_id=migration_id,
                    preflight=preflight,
                    readiness_before=readiness_before,
                    mutation=mutation,
                )
            return self._verify_source_and_finish_rollback(
                run,
                failure_code="topology_recovery_rollback_completed",
                observed_at=observed_at,
                spec=spec,
                target_topology=target_topology,
                migration_mapping=migration_mapping,
                migration_id=migration_id,
                preflight=preflight,
                readiness_before=readiness_before,
                mutation=mutation,
            )

        try:
            if run.state in {"prepared", "applying", "applied"}:
                run = self.journal.transition(run.run_id, "target_verified", code="topology_recovery_target_live")
            canary_after = None
            if run.state == "target_verified":
                canary_after = canary_verifier()
                if _status(canary_after) != "ok":
                    run = self.journal.transition(run.run_id, "rollback_started", code="topology_postapply_canary_not_ok")
                    self.mutator.rollback_to_snapshot(
                        spec=spec,
                        resource=run.resource,
                        source_snapshot=run.source_snapshot,
                    )
                    return self._verify_source_and_finish_rollback(
                        run,
                        failure_code="topology_postapply_canary_not_ok",
                        observed_at=observed_at,
                        spec=spec,
                        target_topology=target_topology,
                        migration_mapping=migration_mapping,
                        migration_id=migration_id,
                        preflight=preflight,
                        readiness_before=readiness_before,
                        mutation=mutation,
                        canary_after=canary_after,
                    )
                run = self.journal.transition(run.run_id, "canary_verified", code="topology_recovery_canary_ok")

            readiness_after = None
            if run.state == "canary_verified":
                readiness_after = readiness_verifier()
                if _status(readiness_after) != "ready":
                    run = self.journal.transition(run.run_id, "rollback_started", code="topology_postapply_readiness_not_ready")
                    self.mutator.rollback_to_snapshot(
                        spec=spec,
                        resource=run.resource,
                        source_snapshot=run.source_snapshot,
                    )
                    return self._verify_source_and_finish_rollback(
                        run,
                        failure_code="topology_postapply_readiness_not_ready",
                        observed_at=observed_at,
                        spec=spec,
                        target_topology=target_topology,
                        migration_mapping=migration_mapping,
                        migration_id=migration_id,
                        preflight=preflight,
                        readiness_before=readiness_before,
                        mutation=mutation,
                        canary_after=canary_after,
                        readiness_after=readiness_after,
                    )
                run = self.journal.transition(run.run_id, "readiness_verified", code="topology_recovery_readiness_ready")

            if run.state != "readiness_verified":
                raise RuntimeError("topology_recovery_state_unexpected")
            run = self.journal.transition(
                run.run_id,
                "completed",
                code="topology_target_recovered_and_verified",
            )
        except TopologyProvisioningJournalError as exc:
            return self._journal_error(
                exc=exc,
                observed_at=observed_at,
                spec=spec,
                migration_id=migration_id,
                preflight=preflight,
                journal=run,
                readiness_before=readiness_before,
                mutation=mutation,
            )
        except Exception:
            try:
                run = self.journal.transition(
                    run.run_id,
                    "manual_intervention",
                    code="topology_recovery_failed",
                )
            except TopologyProvisioningJournalError:
                pass
            return self._terminal(
                observed_at=observed_at,
                spec=spec,
                status="failed",
                code="topology_recovery_failed",
                migration_id=migration_id,
                preflight=preflight,
                journal=run,
                readiness_before=readiness_before,
                mutation=mutation,
            )

        return self._terminal(
            observed_at=observed_at,
            spec=spec,
            status="succeeded",
            code="topology_target_recovered_and_verified",
            migration_id=migration_id,
            preflight=preflight,
            journal=run,
            readiness_before=readiness_before,
            mutation=mutation,
            canary_after=canary_after,
            readiness_after=readiness_after,
        )
