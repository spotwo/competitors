from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal, Mapping

from event_pipeline_topology_migration import (
    TopologyContractInspector,
    TopologyContractReport,
    TopologyMigrationConfig,
)

PreflightStatus = Literal["ok", "warning", "critical"]
PreflightDecision = Literal["safe_to_apply", "no_changes", "blocked"]


@dataclass(frozen=True)
class TopologyPlanChange:
    resource: str
    field: str
    current: Any
    target: Any

    def to_dict(self) -> dict[str, Any]:
        return {
            "resource": self.resource,
            "field": self.field,
            "current": self.current,
            "target": self.target,
        }


@dataclass(frozen=True)
class TopologyPlanStep:
    order: int
    code: str
    resource: str | None = None
    fields: tuple[str, ...] = ()
    requires_atomic_resource_update: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "order": self.order,
            "code": self.code,
            "resource": self.resource,
            "fields": list(self.fields),
            "requires_atomic_resource_update": self.requires_atomic_resource_update,
        }


@dataclass(frozen=True)
class EventPipelineTopologyPreflightReport:
    observed_at: datetime
    pipeline_id: str
    status: PreflightStatus
    decision: PreflightDecision
    code: str
    contract: TopologyContractReport
    changes: tuple[TopologyPlanChange, ...]
    steps: tuple[TopologyPlanStep, ...]

    def __post_init__(self) -> None:
        if self.observed_at.tzinfo is None or self.observed_at.utcoffset() is None:
            raise ValueError("observed_at must include a timezone")
        if not self.pipeline_id:
            raise ValueError("pipeline_id is required")
        if self.decision == "blocked" and self.status != "critical":
            raise ValueError("blocked preflight must be critical")
        if self.decision == "safe_to_apply" and self.status != "ok":
            raise ValueError("safe-to-apply preflight must be healthy")
        if self.decision == "safe_to_apply" and not self.changes:
            raise ValueError("safe-to-apply preflight requires at least one change")

    @property
    def migration_deadline_remaining_seconds(self) -> float | None:
        if self.contract.migration is None:
            return None
        return max(
            0.0,
            (self.contract.migration.valid_until - self.observed_at).total_seconds(),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "observed_at": self.observed_at.isoformat(),
            "pipeline": self.pipeline_id,
            "status": self.status,
            "decision": self.decision,
            "code": self.code,
            "execution": {
                "dry_run": True,
                "mutates": False,
                "external_provisioner_required": self.decision == "safe_to_apply",
            },
            "migration_deadline_remaining_seconds": self.migration_deadline_remaining_seconds,
            "changes": [change.to_dict() for change in self.changes],
            "steps": [step.to_dict() for step in self.steps],
            "contract": self.contract.to_dict(),
        }


def _changes(contract: TopologyContractReport) -> tuple[TopologyPlanChange, ...]:
    return tuple(
        TopologyPlanChange(
            resource=item.resource,
            field=item.field,
            current=item.actual,
            target=item.expected,
        )
        for item in contract.target.drift
    )


def _apply_steps(changes: tuple[TopologyPlanChange, ...]) -> tuple[TopologyPlanStep, ...]:
    resources = tuple(dict.fromkeys(change.resource for change in changes))
    if len(resources) != 1:
        raise ValueError("rollout plan requires exactly one changed topology resource")
    resource = resources[0]
    fields = tuple(change.field for change in changes)
    return (
        TopologyPlanStep(
            order=1,
            code="apply_target_resource",
            resource=resource,
            fields=fields,
            requires_atomic_resource_update=True,
        ),
        TopologyPlanStep(order=2, code="verify_exact_target"),
        TopologyPlanStep(order=3, code="verify_event_pipeline_readiness"),
        TopologyPlanStep(order=4, code="remove_topology_migration_contract"),
    )


def _completion_steps() -> tuple[TopologyPlanStep, ...]:
    return (
        TopologyPlanStep(order=1, code="verify_exact_target"),
        TopologyPlanStep(order=2, code="verify_event_pipeline_readiness"),
        TopologyPlanStep(order=3, code="remove_topology_migration_contract"),
    )


class EventPipelineTopologyPreflightPlanner:
    """Build a deterministic dry-run rollout plan from one live topology snapshot."""

    def __init__(
        self,
        server_url: str,
        *,
        contract_inspector: TopologyContractInspector | None = None,
    ):
        self.contract_inspector = contract_inspector or TopologyContractInspector(server_url)

    def plan(
        self,
        spec: Any,
        *,
        target_topology: Mapping[str, Any],
        migration_mapping: Mapping[str, Any] | None,
        observed_at: datetime | None = None,
    ) -> EventPipelineTopologyPreflightReport:
        observed_at = observed_at or datetime.now(timezone.utc)
        contract = self.contract_inspector.inspect(
            spec,
            target_topology=target_topology,
            migration_mapping=migration_mapping,
            observed_at=observed_at,
        )
        changes = _changes(contract)

        if migration_mapping is None:
            if contract.matched == "target":
                return EventPipelineTopologyPreflightReport(
                    observed_at=observed_at,
                    pipeline_id=spec.pipeline_id,
                    status="ok",
                    decision="no_changes",
                    code="topology_target_already_deployed",
                    contract=contract,
                    changes=(),
                    steps=(),
                )
            return EventPipelineTopologyPreflightReport(
                observed_at=observed_at,
                pipeline_id=spec.pipeline_id,
                status="critical",
                decision="blocked",
                code="topology_migration_required",
                contract=contract,
                changes=changes,
                steps=(),
            )

        migration = TopologyMigrationConfig.from_mapping(migration_mapping)
        migration.source_topology(target_topology, spec)

        if contract.phase == "expired":
            if contract.matched == "target":
                return EventPipelineTopologyPreflightReport(
                    observed_at=observed_at,
                    pipeline_id=spec.pipeline_id,
                    status="warning",
                    decision="no_changes",
                    code="topology_migration_cleanup_required",
                    contract=contract,
                    changes=(),
                    steps=_completion_steps(),
                )
            return EventPipelineTopologyPreflightReport(
                observed_at=observed_at,
                pipeline_id=spec.pipeline_id,
                status="critical",
                decision="blocked",
                code=(
                    "topology_migration_expired"
                    if contract.matched == "source"
                    else "topology_live_state_unexpected"
                ),
                contract=contract,
                changes=changes,
                steps=(),
            )

        if contract.matched == "target":
            return EventPipelineTopologyPreflightReport(
                observed_at=observed_at,
                pipeline_id=spec.pipeline_id,
                status="ok",
                decision="no_changes",
                code="topology_migration_target_reached",
                contract=contract,
                changes=(),
                steps=_completion_steps(),
            )

        if contract.matched != "source":
            return EventPipelineTopologyPreflightReport(
                observed_at=observed_at,
                pipeline_id=spec.pipeline_id,
                status="critical",
                decision="blocked",
                code="topology_live_state_unexpected",
                contract=contract,
                changes=changes,
                steps=(),
            )

        resources = {change.resource for change in changes}
        if len(resources) != 1:
            return EventPipelineTopologyPreflightReport(
                observed_at=observed_at,
                pipeline_id=spec.pipeline_id,
                status="critical",
                decision="blocked",
                code="topology_migration_requires_single_resource",
                contract=contract,
                changes=changes,
                steps=(),
            )

        return EventPipelineTopologyPreflightReport(
            observed_at=observed_at,
            pipeline_id=spec.pipeline_id,
            status="ok",
            decision="safe_to_apply",
            code="topology_migration_ready",
            contract=contract,
            changes=changes,
            steps=_apply_steps(changes),
        )
