from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal, Mapping
from uuid import UUID

import psycopg
from psycopg.rows import dict_row

from event_pipeline_topology_provisioning_journal import ALL_STATES, TERMINAL_STATES

HealthStatus = Literal["ok", "warning", "critical"]
_STATUS_RANK: dict[HealthStatus, int] = {"ok": 0, "warning": 1, "critical": 2}


def _required_name(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise ValueError(f"{field} must be a normalized non-empty string")
    return value


def _aware(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must include a timezone")
    return value


def _nonnegative(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
        raise ValueError(f"{field} must be nonnegative")
    return float(value)


def _worst(*statuses: HealthStatus) -> HealthStatus:
    return max(statuses, key=lambda item: _STATUS_RANK[item])


@dataclass(frozen=True)
class ProvisioningExecutionObservation:
    run_id: UUID
    migration_id: str
    resource: str
    state: str
    state_code: str | None
    lease_expires_at: datetime | None
    attempt_count: int
    recovery_count: int
    started_at: datetime
    updated_at: datetime
    state_changed_at: datetime
    state_age_seconds: float
    lease_remaining_seconds: float | None
    lease_expired: bool

    @classmethod
    def from_row(
        cls,
        row: Mapping[str, Any],
        *,
        observed_at: datetime,
    ) -> "ProvisioningExecutionObservation":
        state_changed_at = _aware(row["state_changed_at"], "state_changed_at")
        lease_expires_at = row["lease_expires_at"]
        if lease_expires_at is not None:
            lease_expires_at = _aware(lease_expires_at, "lease_expires_at")
        state_age = max(0.0, (observed_at - state_changed_at).total_seconds())
        lease_remaining = (
            None
            if lease_expires_at is None
            else (lease_expires_at - observed_at).total_seconds()
        )
        return cls(
            run_id=row["run_id"],
            migration_id=_required_name(row["migration_id"], "migration_id"),
            resource=_required_name(row["resource"], "resource"),
            state=_required_name(row["state"], "state"),
            state_code=row["state_code"],
            lease_expires_at=lease_expires_at,
            attempt_count=int(row["attempt_count"]),
            recovery_count=int(row["recovery_count"]),
            started_at=_aware(row["started_at"], "started_at"),
            updated_at=_aware(row["updated_at"], "updated_at"),
            state_changed_at=state_changed_at,
            state_age_seconds=state_age,
            lease_remaining_seconds=lease_remaining,
            lease_expired=lease_remaining is not None and lease_remaining <= 0,
        )

    @property
    def terminal(self) -> bool:
        return self.state in TERMINAL_STATES

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": str(self.run_id),
            "migration_id": self.migration_id,
            "resource": self.resource,
            "state": self.state,
            "state_code": self.state_code,
            "terminal": self.terminal,
            "lease_expires_at": (
                self.lease_expires_at.isoformat() if self.lease_expires_at else None
            ),
            "lease_expired": self.lease_expired,
            "lease_remaining_seconds": self.lease_remaining_seconds,
            "attempt_count": self.attempt_count,
            "recovery_count": self.recovery_count,
            "started_at": self.started_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "state_changed_at": self.state_changed_at.isoformat(),
            "state_age_seconds": self.state_age_seconds,
        }


@dataclass(frozen=True)
class TopologyProvisioningHealthSnapshot:
    observed_at: datetime
    pipeline_id: str
    active: ProvisioningExecutionObservation | None
    latest_terminal: ProvisioningExecutionObservation | None
    state_counts: Mapping[str, int]

    def __post_init__(self) -> None:
        _aware(self.observed_at, "observed_at")
        _required_name(self.pipeline_id, "pipeline_id")
        unknown = set(self.state_counts) - set(ALL_STATES)
        if unknown:
            raise ValueError(f"unknown provisioning states in history: {sorted(unknown)}")
        if any(int(value) < 0 for value in self.state_counts.values()):
            raise ValueError("state counts must be nonnegative")
        if self.active is not None and self.active.terminal:
            raise ValueError("active provisioning observation cannot be terminal")
        if self.latest_terminal is not None and not self.latest_terminal.terminal:
            raise ValueError("latest terminal provisioning observation must be terminal")

    @property
    def total_runs(self) -> int:
        return sum(int(value) for value in self.state_counts.values())

    def to_dict(self) -> dict[str, Any]:
        return {
            "observed_at": self.observed_at.isoformat(),
            "pipeline": self.pipeline_id,
            "collection": {"read_only": True},
            "active": self.active.to_dict() if self.active else None,
            "latest_terminal": (
                self.latest_terminal.to_dict() if self.latest_terminal else None
            ),
            "history": {
                "total_runs": self.total_runs,
                "state_counts": {
                    state: int(self.state_counts.get(state, 0))
                    for state in sorted(ALL_STATES)
                },
            },
        }


class PostgresTopologyProvisioningHealthStore:
    """Read provisioning execution state without acquiring or renewing a lease."""

    def __init__(self, database_url: str):
        self.database_url = _required_name(database_url, "database_url")

    def _connect(self) -> psycopg.Connection:
        conn = psycopg.connect(self.database_url, row_factory=dict_row)
        conn.execute("SET statement_timeout = '8s'")
        conn.execute("SET lock_timeout = '5s'")
        return conn

    def snapshot(
        self,
        *,
        pipeline_id: str,
        observed_at: datetime | None = None,
    ) -> TopologyProvisioningHealthSnapshot:
        pipeline_id = _required_name(pipeline_id, "pipeline_id")
        if observed_at is not None:
            observed_at = _aware(observed_at, "observed_at")

        with self._connect() as conn:
            if observed_at is None:
                observed_at = conn.execute(
                    "SELECT clock_timestamp() AS observed_at"
                ).fetchone()["observed_at"]

            active_row = conn.execute(
                """
                SELECT *
                FROM kernel_lab.event_pipeline_topology_provisioning_runs
                WHERE pipeline_id = %s
                  AND state NOT IN ('completed', 'rolled_back', 'failed', 'manual_intervention')
                ORDER BY started_at DESC
                LIMIT 1
                """,
                (pipeline_id,),
            ).fetchone()
            terminal_row = conn.execute(
                """
                SELECT *
                FROM kernel_lab.event_pipeline_topology_provisioning_runs
                WHERE pipeline_id = %s
                  AND state IN ('completed', 'rolled_back', 'failed', 'manual_intervention')
                ORDER BY started_at DESC
                LIMIT 1
                """,
                (pipeline_id,),
            ).fetchone()
            count_rows = conn.execute(
                """
                SELECT state, count(*)::bigint AS count
                FROM kernel_lab.event_pipeline_topology_provisioning_runs
                WHERE pipeline_id = %s
                GROUP BY state
                """,
                (pipeline_id,),
            ).fetchall()

        return TopologyProvisioningHealthSnapshot(
            observed_at=observed_at,
            pipeline_id=pipeline_id,
            active=(
                None
                if active_row is None
                else ProvisioningExecutionObservation.from_row(
                    active_row, observed_at=observed_at
                )
            ),
            latest_terminal=(
                None
                if terminal_row is None
                else ProvisioningExecutionObservation.from_row(
                    terminal_row, observed_at=observed_at
                )
            ),
            state_counts={row["state"]: int(row["count"]) for row in count_rows},
        )


@dataclass(frozen=True)
class ProvisioningHealthAlert:
    code: str
    severity: Literal["warning", "critical"]

    def to_dict(self) -> dict[str, str]:
        return {"code": self.code, "severity": self.severity}


@dataclass(frozen=True)
class TopologyProvisioningHealthReport:
    snapshot: TopologyProvisioningHealthSnapshot
    status: HealthStatus
    current_status: HealthStatus
    current_code: str | None
    history_status: HealthStatus
    history_code: str | None
    alerts: tuple[ProvisioningHealthAlert, ...]

    @property
    def pipeline_id(self) -> str:
        return self.snapshot.pipeline_id

    @property
    def observed_at(self) -> datetime:
        return self.snapshot.observed_at

    def to_dict(self) -> dict[str, Any]:
        value = self.snapshot.to_dict()
        value.update(
            {
                "status": self.status,
                "current_execution": {
                    "status": self.current_status,
                    "code": self.current_code,
                },
                "latest_terminal_attention": {
                    "status": self.history_status,
                    "code": self.history_code,
                },
                "alerts": [alert.to_dict() for alert in self.alerts],
            }
        )
        return value


@dataclass(frozen=True)
class TopologyProvisioningHealthPolicy:
    warning_state_age_seconds: float = 60.0
    critical_state_age_seconds: float = 180.0

    def __post_init__(self) -> None:
        warning = _nonnegative(
            self.warning_state_age_seconds, "warning_state_age_seconds"
        )
        critical = _nonnegative(
            self.critical_state_age_seconds, "critical_state_age_seconds"
        )
        if critical < warning:
            raise ValueError("critical state age must not be below warning state age")

    def evaluate(
        self,
        snapshot: TopologyProvisioningHealthSnapshot,
    ) -> TopologyProvisioningHealthReport:
        current_status: HealthStatus = "ok"
        current_code: str | None = None
        active = snapshot.active
        if active is not None:
            if active.lease_expired:
                current_status = "critical"
                current_code = "topology_provisioning_lease_expired"
            elif active.state_age_seconds >= self.critical_state_age_seconds:
                current_status = "critical"
                current_code = "topology_provisioning_execution_stuck"
            elif active.state_age_seconds >= self.warning_state_age_seconds:
                current_status = "warning"
                current_code = "topology_provisioning_execution_slow"

        history_status: HealthStatus = "ok"
        history_code: str | None = None
        terminal = snapshot.latest_terminal
        if terminal is not None:
            if terminal.state == "rolled_back":
                history_status = "warning"
                history_code = "topology_provisioning_latest_rolled_back"
            elif terminal.state == "failed":
                history_status = "critical"
                history_code = "topology_provisioning_latest_failed"
            elif terminal.state == "manual_intervention":
                history_status = "critical"
                history_code = "topology_provisioning_manual_intervention"

        alerts: list[ProvisioningHealthAlert] = []
        if current_code is not None:
            alerts.append(ProvisioningHealthAlert(current_code, current_status))
        if history_code is not None:
            alerts.append(ProvisioningHealthAlert(history_code, history_status))

        return TopologyProvisioningHealthReport(
            snapshot=snapshot,
            status=_worst(current_status, history_status),
            current_status=current_status,
            current_code=current_code,
            history_status=history_status,
            history_code=history_code,
            alerts=tuple(alerts),
        )


def _escape_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def render_prometheus(report: TopologyProvisioningHealthReport) -> str:
    pipeline = _escape_label(report.pipeline_id)
    lines = [
        "# HELP spotwo_wms_event_pipeline_topology_provisioning_health Overall provisioning execution attention state.",
        "# TYPE spotwo_wms_event_pipeline_topology_provisioning_health gauge",
    ]
    for status in ("ok", "warning", "critical"):
        lines.append(
            f'spotwo_wms_event_pipeline_topology_provisioning_health{{pipeline="{pipeline}",status="{status}"}} {1 if report.status == status else 0}'
        )

    lines.extend(
        [
            "# HELP spotwo_wms_event_pipeline_topology_provisioning_current_health Current active execution health used by deployment readiness.",
            "# TYPE spotwo_wms_event_pipeline_topology_provisioning_current_health gauge",
        ]
    )
    for status in ("ok", "warning", "critical"):
        lines.append(
            f'spotwo_wms_event_pipeline_topology_provisioning_current_health{{pipeline="{pipeline}",status="{status}"}} {1 if report.current_status == status else 0}'
        )

    active = report.snapshot.active
    lines.extend(
        [
            "# HELP spotwo_wms_event_pipeline_topology_provisioning_active Active execution state; state and resource are bounded enums.",
            "# TYPE spotwo_wms_event_pipeline_topology_provisioning_active gauge",
        ]
    )
    if active is not None:
        lines.append(
            'spotwo_wms_event_pipeline_topology_provisioning_active'
            f'{{pipeline="{pipeline}",state="{_escape_label(active.state)}",resource="{_escape_label(active.resource)}"}} 1'
        )
        lines.extend(
            [
                "# HELP spotwo_wms_event_pipeline_topology_provisioning_state_age_seconds Seconds since the active execution entered its current durable state.",
                "# TYPE spotwo_wms_event_pipeline_topology_provisioning_state_age_seconds gauge",
                f'spotwo_wms_event_pipeline_topology_provisioning_state_age_seconds{{pipeline="{pipeline}"}} {active.state_age_seconds:.6f}',
                "# HELP spotwo_wms_event_pipeline_topology_provisioning_lease_remaining_seconds Seconds until the active single-writer lease expires.",
                "# TYPE spotwo_wms_event_pipeline_topology_provisioning_lease_remaining_seconds gauge",
                f'spotwo_wms_event_pipeline_topology_provisioning_lease_remaining_seconds{{pipeline="{pipeline}"}} {active.lease_remaining_seconds:.6f}',
                "# HELP spotwo_wms_event_pipeline_topology_provisioning_attempt_count Durable attempt count for the active execution.",
                "# TYPE spotwo_wms_event_pipeline_topology_provisioning_attempt_count gauge",
                f'spotwo_wms_event_pipeline_topology_provisioning_attempt_count{{pipeline="{pipeline}"}} {active.attempt_count}',
                "# HELP spotwo_wms_event_pipeline_topology_provisioning_recovery_count Durable recovery-owner handoff count for the active execution.",
                "# TYPE spotwo_wms_event_pipeline_topology_provisioning_recovery_count gauge",
                f'spotwo_wms_event_pipeline_topology_provisioning_recovery_count{{pipeline="{pipeline}"}} {active.recovery_count}',
            ]
        )

    lines.extend(
        [
            "# HELP spotwo_wms_event_pipeline_topology_provisioning_history_runs Stored execution count by bounded durable state.",
            "# TYPE spotwo_wms_event_pipeline_topology_provisioning_history_runs gauge",
        ]
    )
    for state in sorted(ALL_STATES):
        lines.append(
            'spotwo_wms_event_pipeline_topology_provisioning_history_runs'
            f'{{pipeline="{pipeline}",state="{state}"}} {int(report.snapshot.state_counts.get(state, 0))}'
        )

    terminal = report.snapshot.latest_terminal
    lines.extend(
        [
            "# HELP spotwo_wms_event_pipeline_topology_provisioning_latest_terminal Latest terminal execution state as a bounded one-hot gauge.",
            "# TYPE spotwo_wms_event_pipeline_topology_provisioning_latest_terminal gauge",
        ]
    )
    for state in sorted(TERMINAL_STATES):
        value = 1 if terminal is not None and terminal.state == state else 0
        lines.append(
            'spotwo_wms_event_pipeline_topology_provisioning_latest_terminal'
            f'{{pipeline="{pipeline}",state="{state}"}} {value}'
        )

    lines.extend(
        [
            "# HELP spotwo_wms_event_pipeline_topology_provisioning_alert Active bounded provisioning-health alerts.",
            "# TYPE spotwo_wms_event_pipeline_topology_provisioning_alert gauge",
        ]
    )
    for alert in report.alerts:
        lines.append(
            'spotwo_wms_event_pipeline_topology_provisioning_alert'
            f'{{pipeline="{pipeline}",code="{_escape_label(alert.code)}",severity="{alert.severity}"}} 1'
        )
    return "\n".join(lines) + "\n"
