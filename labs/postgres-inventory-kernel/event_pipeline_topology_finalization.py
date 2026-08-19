from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal, Mapping

from event_pipeline_topology_manual_intervention import PostgresTopologyManualInterventionStore
from event_pipeline_topology_migration import TopologyContractInspector, TopologyMigrationConfig
from event_pipeline_topology_provisioning_health import PostgresTopologyProvisioningHealthStore

FinalizationStatus = Literal["finalized", "ready", "blocked"]
FinalizationLineage = Literal["none", "completed", "resolved_target"]


@dataclass(frozen=True)
class TopologyMigrationFinalizationReport:
    observed_at: datetime
    pipeline_id: str
    status: FinalizationStatus
    code: str
    safe_to_remove_topology_migration: bool
    migration_id: str | None
    provisioning_state: str | None
    lineage: FinalizationLineage
    live_match: str | None
    readiness_status: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "observed_at": self.observed_at.isoformat(),
            "pipeline": self.pipeline_id,
            "status": self.status,
            "code": self.code,
            "safe_to_remove_topology_migration": self.safe_to_remove_topology_migration,
            "migration_id": self.migration_id,
            "provisioning_state": self.provisioning_state,
            "lineage": self.lineage,
            "live_match": self.live_match,
            "readiness_status": self.readiness_status,
            "registry_change": {
                "operation": "remove_topology_migration",
                "pipeline": self.pipeline_id,
                "safe": self.safe_to_remove_topology_migration,
            },
            "collection": {"read_only": True},
        }


class TopologyMigrationFinalizationInspector:
    """Prove when a bounded topology migration contract can be removed from Git."""

    def __init__(
        self,
        *,
        database_url: str | None,
        nats_url: str | None,
        contract_inspector: Any | None = None,
        readiness_collector: Any | None = None,
    ):
        self.database_url = database_url
        self.nats_url = nats_url
        self.contract_inspector = contract_inspector
        self.readiness_collector = readiness_collector

    @staticmethod
    def _report(
        *,
        observed_at: datetime,
        pipeline_id: str,
        status: FinalizationStatus,
        code: str,
        safe: bool,
        migration_id: str | None,
        provisioning_state: str | None = None,
        lineage: FinalizationLineage = "none",
        live_match: str | None = None,
        readiness_status: str | None = None,
    ) -> TopologyMigrationFinalizationReport:
        return TopologyMigrationFinalizationReport(
            observed_at=observed_at,
            pipeline_id=pipeline_id,
            status=status,
            code=code,
            safe_to_remove_topology_migration=safe,
            migration_id=migration_id,
            provisioning_state=provisioning_state,
            lineage=lineage,
            live_match=live_match,
            readiness_status=readiness_status,
        )

    def inspect(
        self,
        spec: Any,
        *,
        target_topology: Mapping[str, Any],
        migration_mapping: Mapping[str, Any] | None,
        watchdog_state: Mapping[str, Any] | None,
        observed_at: datetime | None = None,
    ) -> TopologyMigrationFinalizationReport:
        observed_at = observed_at or datetime.now(timezone.utc)
        if migration_mapping is None:
            return self._report(
                observed_at=observed_at,
                pipeline_id=spec.pipeline_id,
                status="finalized",
                code="topology_migration_not_present",
                safe=True,
                migration_id=None,
            )

        migration = TopologyMigrationConfig.from_mapping(migration_mapping)
        migration_id = migration.migration_id
        if not self.database_url or not self.nats_url:
            return self._report(
                observed_at=observed_at,
                pipeline_id=spec.pipeline_id,
                status="blocked",
                code="topology_migration_finalization_runtime_missing",
                safe=False,
                migration_id=migration_id,
            )

        try:
            provisioning = PostgresTopologyProvisioningHealthStore(self.database_url).snapshot(
                pipeline_id=spec.pipeline_id,
                observed_at=observed_at,
            )
        except Exception:
            return self._report(
                observed_at=observed_at,
                pipeline_id=spec.pipeline_id,
                status="blocked",
                code="topology_migration_finalization_history_unavailable",
                safe=False,
                migration_id=migration_id,
            )

        if provisioning.active is not None:
            return self._report(
                observed_at=observed_at,
                pipeline_id=spec.pipeline_id,
                status="blocked",
                code="topology_migration_provisioning_active",
                safe=False,
                migration_id=migration_id,
                provisioning_state=provisioning.active.state,
            )

        terminal = provisioning.latest_terminal
        if terminal is None:
            return self._report(
                observed_at=observed_at,
                pipeline_id=spec.pipeline_id,
                status="blocked",
                code="topology_migration_finalization_history_missing",
                safe=False,
                migration_id=migration_id,
            )
        if terminal.migration_id != migration_id:
            return self._report(
                observed_at=observed_at,
                pipeline_id=spec.pipeline_id,
                status="blocked",
                code="topology_migration_finalization_history_mismatch",
                safe=False,
                migration_id=migration_id,
                provisioning_state=terminal.state,
            )

        lineage: FinalizationLineage = "none"
        if terminal.state == "completed":
            lineage = "completed"
        elif terminal.state == "manual_intervention":
            try:
                intervention = PostgresTopologyManualInterventionStore(
                    self.database_url
                ).latest_for_run(terminal.run_id)
            except Exception:
                intervention = None
            if intervention is None:
                return self._report(
                    observed_at=observed_at,
                    pipeline_id=spec.pipeline_id,
                    status="blocked",
                    code="topology_migration_manual_intervention_resolution_missing",
                    safe=False,
                    migration_id=migration_id,
                    provisioning_state=terminal.state,
                )
            if intervention.state != "resolved_target":
                return self._report(
                    observed_at=observed_at,
                    pipeline_id=spec.pipeline_id,
                    status="blocked",
                    code="topology_migration_manual_intervention_not_target",
                    safe=False,
                    migration_id=migration_id,
                    provisioning_state=terminal.state,
                )
            lineage = "resolved_target"
        else:
            return self._report(
                observed_at=observed_at,
                pipeline_id=spec.pipeline_id,
                status="blocked",
                code="topology_migration_finalization_history_not_successful",
                safe=False,
                migration_id=migration_id,
                provisioning_state=terminal.state,
            )

        try:
            contract_inspector = self.contract_inspector or TopologyContractInspector(
                self.nats_url
            )
            contract = contract_inspector.inspect(
                spec,
                target_topology=target_topology,
                migration_mapping=migration_mapping,
                observed_at=observed_at,
            )
        except Exception:
            return self._report(
                observed_at=observed_at,
                pipeline_id=spec.pipeline_id,
                status="blocked",
                code="topology_migration_finalization_topology_unavailable",
                safe=False,
                migration_id=migration_id,
                provisioning_state=terminal.state,
                lineage=lineage,
            )

        if contract.matched != "target" or contract.target.status != "ok":
            code = (
                "topology_migration_source_still_live"
                if contract.matched == "source"
                else "topology_migration_live_topology_not_target"
            )
            return self._report(
                observed_at=observed_at,
                pipeline_id=spec.pipeline_id,
                status="blocked",
                code=code,
                safe=False,
                migration_id=migration_id,
                provisioning_state=terminal.state,
                lineage=lineage,
                live_match=contract.matched,
            )

        try:
            if self.readiness_collector is None:
                from event_pipeline_topology_readiness import (
                    TopologyAwareEventPipelineReadinessCollector,
                )

                readiness_collector = TopologyAwareEventPipelineReadinessCollector()
            else:
                readiness_collector = self.readiness_collector
            # Evaluate the steady state that will exist after the compatibility stanza
            # is removed. This avoids an expired migration contract blocking its own
            # cleanup when the exact target is already live.
            readiness = readiness_collector.collect(
                spec,
                topology=target_topology,
                topology_migration=None,
                database_url=self.database_url,
                nats_url=self.nats_url,
                watchdog_state=watchdog_state,
                observed_at=observed_at,
            )
        except Exception:
            return self._report(
                observed_at=observed_at,
                pipeline_id=spec.pipeline_id,
                status="blocked",
                code="topology_migration_finalization_readiness_unavailable",
                safe=False,
                migration_id=migration_id,
                provisioning_state=terminal.state,
                lineage=lineage,
                live_match="target",
            )

        if readiness.status != "ready":
            return self._report(
                observed_at=observed_at,
                pipeline_id=spec.pipeline_id,
                status="blocked",
                code="topology_migration_finalization_readiness_not_ready",
                safe=False,
                migration_id=migration_id,
                provisioning_state=terminal.state,
                lineage=lineage,
                live_match="target",
                readiness_status=readiness.status,
            )

        return self._report(
            observed_at=observed_at,
            pipeline_id=spec.pipeline_id,
            status="ready",
            code="topology_migration_finalization_ready",
            safe=True,
            migration_id=migration_id,
            provisioning_state=terminal.state,
            lineage=lineage,
            live_match="target",
            readiness_status="ready",
        )


def render_prometheus(report: TopologyMigrationFinalizationReport) -> str:
    def escape(value: str) -> str:
        return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")

    pipeline = escape(report.pipeline_id)
    lines = [
        "# HELP spotwo_wms_event_pipeline_topology_migration_removal_safe Whether the migration compatibility stanza is safe to remove.",
        "# TYPE spotwo_wms_event_pipeline_topology_migration_removal_safe gauge",
        f'spotwo_wms_event_pipeline_topology_migration_removal_safe{{pipeline="{pipeline}"}} {1 if report.safe_to_remove_topology_migration else 0}',
        "# HELP spotwo_wms_event_pipeline_topology_migration_finalization_state Current bounded finalization state.",
        "# TYPE spotwo_wms_event_pipeline_topology_migration_finalization_state gauge",
        f'spotwo_wms_event_pipeline_topology_migration_finalization_state{{pipeline="{pipeline}",state="{report.status}"}} 1',
    ]
    if report.status == "blocked":
        lines.extend(
            [
                "# HELP spotwo_wms_event_pipeline_topology_migration_finalization_blocked Current bounded finalization blocker.",
                "# TYPE spotwo_wms_event_pipeline_topology_migration_finalization_blocked gauge",
                f'spotwo_wms_event_pipeline_topology_migration_finalization_blocked{{pipeline="{pipeline}",code="{escape(report.code)}"}} 1',
            ]
        )
    return "\n".join(lines) + "\n"
