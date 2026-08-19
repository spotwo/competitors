from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal, Mapping

from event_pipeline_topology import (
    EventPipelineTopologyExpectation,
    EventPipelineTopologyReport,
    NatsJetStreamTopologyInspector,
    TopologyDrift,
    TopologyResourceSnapshot,
)
from event_pipeline_topology_config import DeploymentTopologyConfig

ContractStatus = Literal["ok", "warning", "critical"]
MigrationPhase = Literal["active", "expired"]
MatchedTopology = Literal["target", "source", "neither"]


def _required_name(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise ValueError(f"{field} must be a normalized non-empty string")
    return value


def _parse_timestamp(value: Any, field: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be an RFC3339 string")
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"{field} must be an RFC3339 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field} must include a timezone")
    return parsed.astimezone(timezone.utc)


def _merge_overrides(target: Mapping[str, Any], overrides: Mapping[str, Any]) -> dict[str, Any]:
    source = deepcopy(dict(target))
    for section, section_overrides in overrides.items():
        if section not in source or not isinstance(source[section], Mapping):
            raise ValueError(f"topology_migration.from_overrides.{section} is unsupported")
        if not isinstance(section_overrides, Mapping) or not section_overrides:
            raise ValueError(
                f"topology_migration.from_overrides.{section} must be a non-empty object"
            )
        merged = dict(source[section])
        for field, value in section_overrides.items():
            if field not in merged:
                raise ValueError(
                    f"topology_migration.from_overrides.{section}.{field} is unsupported"
                )
            merged[field] = deepcopy(value)
        source[section] = merged
    return source


def _expected_fields(expectation: EventPipelineTopologyExpectation) -> tuple[Mapping[str, Any], ...]:
    return (
        {
            "subjects": tuple(sorted(expectation.stream.subjects)),
            "storage": expectation.stream.storage,
            "retention": expectation.stream.retention,
            "replicas": expectation.stream.replicas,
            "duplicate_window_seconds": expectation.stream.duplicate_window_seconds,
        },
        {
            "delivery_mode": expectation.business_consumer.delivery_mode,
            "deliver_policy": expectation.business_consumer.deliver_policy,
            "ack_policy": expectation.business_consumer.ack_policy,
            "filter_subject": expectation.business_consumer.filter_subject,
            "max_ack_pending": expectation.business_consumer.max_ack_pending,
            "max_deliver": expectation.business_consumer.max_deliver,
        },
        {
            "delivery_mode": expectation.canary_consumer.delivery_mode,
            "deliver_policy": expectation.canary_consumer.deliver_policy,
            "ack_policy": expectation.canary_consumer.ack_policy,
            "filter_subject": expectation.canary_consumer.filter_subject,
            "max_ack_pending": expectation.canary_consumer.max_ack_pending,
            "max_deliver": expectation.canary_consumer.max_deliver,
        },
    )


def _compare_resources(
    expectation: EventPipelineTopologyExpectation,
    resources: tuple[TopologyResourceSnapshot, ...],
) -> tuple[TopologyDrift, ...]:
    drift: list[TopologyDrift] = []
    for resource, expected in zip(resources, _expected_fields(expectation), strict=True):
        if not resource.exists:
            drift.append(
                TopologyDrift(
                    resource=resource.resource,
                    field="exists",
                    code=f"{resource.resource}_missing",
                    expected=True,
                    actual=False,
                )
            )
            continue
        for field, expected_value in expected.items():
            actual_value = resource.fields.get(field)
            equal = actual_value == expected_value
            if field == "duplicate_window_seconds" and actual_value is not None:
                equal = abs(float(actual_value) - float(expected_value)) <= 1e-6
            if not equal:
                drift.append(
                    TopologyDrift(
                        resource=resource.resource,
                        field=field,
                        code=f"{resource.resource}_{field}_mismatch",
                        expected=expected_value,
                        actual=actual_value,
                    )
                )
    return tuple(drift)


@dataclass(frozen=True)
class TopologyMigrationConfig:
    migration_id: str
    valid_until: datetime
    from_overrides: Mapping[str, Any]

    @classmethod
    def from_mapping(cls, value: Any) -> "TopologyMigrationConfig":
        if not isinstance(value, Mapping):
            raise ValueError("topology_migration must be an object")
        overrides = value.get("from_overrides")
        if not isinstance(overrides, Mapping) or not overrides:
            raise ValueError("topology_migration.from_overrides must be a non-empty object")
        return cls(
            migration_id=_required_name(value.get("id"), "topology_migration.id"),
            valid_until=_parse_timestamp(
                value.get("valid_until"), "topology_migration.valid_until"
            ),
            from_overrides=deepcopy(dict(overrides)),
        )

    def source_topology(
        self,
        target_topology: Mapping[str, Any],
        spec: Any,
    ) -> Mapping[str, Any]:
        target_config = DeploymentTopologyConfig.from_mapping(target_topology)
        target_config.validate(spec)
        source = _merge_overrides(target_topology, self.from_overrides)
        source_config = DeploymentTopologyConfig.from_mapping(source)
        source_config.validate(spec)
        if source == dict(target_topology):
            raise ValueError("topology_migration.from_overrides must change topology")
        return source

    def public_dict(self) -> dict[str, Any]:
        return {
            "id": self.migration_id,
            "valid_until": self.valid_until.isoformat(),
            "from_overrides": deepcopy(dict(self.from_overrides)),
        }


@dataclass(frozen=True)
class TopologyContractReport:
    observed_at: datetime
    status: ContractStatus
    code: str | None
    matched: MatchedTopology
    target: EventPipelineTopologyReport
    source_drift: tuple[TopologyDrift, ...]
    migration: TopologyMigrationConfig | None
    phase: MigrationPhase | None

    def to_dict(self) -> dict[str, Any]:
        migration = None
        if self.migration is not None:
            migration = {
                **self.migration.public_dict(),
                "phase": self.phase,
                "matched": self.matched,
                "source_drift_count": len(self.source_drift),
                "source_drift": [item.to_dict() for item in self.source_drift],
            }
        return {
            "observed_at": self.observed_at.isoformat(),
            "status": self.status,
            "code": self.code,
            "matched": self.matched,
            "collection": {"read_only": True, "atomic": False},
            "target": self.target.to_dict(),
            "migration": migration,
        }


class TopologyContractInspector:
    """Evaluate exact topology plus an optional time-bounded source-to-target rollout."""

    def __init__(self, server_url: str):
        self.inspector = NatsJetStreamTopologyInspector(server_url)

    def inspect(
        self,
        spec: Any,
        *,
        target_topology: Mapping[str, Any],
        migration_mapping: Mapping[str, Any] | None,
        observed_at: datetime | None = None,
    ) -> TopologyContractReport:
        observed_at = observed_at or datetime.now(timezone.utc)
        target_expectation = DeploymentTopologyConfig.from_mapping(
            target_topology
        ).expectation(spec)
        target_report = self.inspector.inspect(
            target_expectation,
            observed_at=observed_at,
        )
        if migration_mapping is None:
            return TopologyContractReport(
                observed_at=observed_at,
                status=target_report.status,
                code=None if target_report.status == "ok" else "topology_drift",
                matched="target" if target_report.status == "ok" else "neither",
                target=target_report,
                source_drift=(),
                migration=None,
                phase=None,
            )

        migration = TopologyMigrationConfig.from_mapping(migration_mapping)
        source_topology = migration.source_topology(target_topology, spec)
        source_expectation = DeploymentTopologyConfig.from_mapping(
            source_topology
        ).expectation(spec)
        source_drift = _compare_resources(source_expectation, target_report.resources)
        target_matches = target_report.status == "ok"
        source_matches = not source_drift
        phase: MigrationPhase = (
            "active" if observed_at <= migration.valid_until else "expired"
        )

        if phase == "active":
            if target_matches:
                status: ContractStatus = "ok"
                code = None
                matched: MatchedTopology = "target"
            elif source_matches:
                status = "ok"
                code = "topology_migration_source_accepted"
                matched = "source"
            else:
                status = "critical"
                code = "topology_migration_drift"
                matched = "neither"
        elif target_matches:
            status = "warning"
            code = "topology_migration_contract_expired"
            matched = "target"
        elif source_matches:
            status = "critical"
            code = "topology_migration_expired"
            matched = "source"
        else:
            status = "critical"
            code = "topology_drift"
            matched = "neither"

        return TopologyContractReport(
            observed_at=observed_at,
            status=status,
            code=code,
            matched=matched,
            target=target_report,
            source_drift=source_drift,
            migration=migration,
            phase=phase,
        )


def render_prometheus(report: TopologyContractReport, *, pipeline_id: str) -> str:
    def escape(value: str) -> str:
        return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")

    pipeline = escape(_required_name(pipeline_id, "pipeline_id"))
    status_value = {"ok": 0, "warning": 1, "critical": 2}[report.status]
    lines = [
        "# HELP spotwo_wms_event_pipeline_topology_contract_status Topology contract status: 0 ok, 1 warning, 2 critical.",
        "# TYPE spotwo_wms_event_pipeline_topology_contract_status gauge",
        f'spotwo_wms_event_pipeline_topology_contract_status{{pipeline="{pipeline}"}} {status_value}',
        "# HELP spotwo_wms_event_pipeline_topology_target_match Whether live topology exactly matches the steady-state target.",
        "# TYPE spotwo_wms_event_pipeline_topology_target_match gauge",
        f'spotwo_wms_event_pipeline_topology_target_match{{pipeline="{pipeline}"}} {1 if report.target.status == "ok" else 0}',
        "# HELP spotwo_wms_event_pipeline_topology_migration_active Whether a bounded migration contract is currently active.",
        "# TYPE spotwo_wms_event_pipeline_topology_migration_active gauge",
        f'spotwo_wms_event_pipeline_topology_migration_active{{pipeline="{pipeline}"}} {1 if report.phase == "active" else 0}',
        "# HELP spotwo_wms_event_pipeline_topology_match_state Current bounded topology match state.",
        "# TYPE spotwo_wms_event_pipeline_topology_match_state gauge",
        f'spotwo_wms_event_pipeline_topology_match_state{{pipeline="{pipeline}",state="{escape(report.matched)}"}} 1',
        "# HELP spotwo_wms_event_pipeline_topology_contract_snapshot_timestamp_seconds Unix timestamp of the contract snapshot.",
        "# TYPE spotwo_wms_event_pipeline_topology_contract_snapshot_timestamp_seconds gauge",
        f'spotwo_wms_event_pipeline_topology_contract_snapshot_timestamp_seconds{{pipeline="{pipeline}"}} {report.observed_at.timestamp():.6f}',
    ]
    return "\n".join(lines) + "\n"
