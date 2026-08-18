from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

import psycopg

HealthStatus = Literal["ok", "warning", "critical"]
AlertSeverity = Literal["warning", "critical"]


def _stable_name(value: str, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} is required")
    if value != value.strip():
        raise ValueError(f"{field} cannot contain surrounding whitespace")
    return value


@dataclass(frozen=True)
class ProjectionGapTelemetrySnapshot:
    observed_at: datetime
    consumer_name: str | None
    gap_count: int
    pending_event_count: int
    max_pending_per_gap: int
    oldest_gap_age_seconds: float | None

    def __post_init__(self) -> None:
        if self.observed_at.tzinfo is None or self.observed_at.utcoffset() is None:
            raise ValueError("observed_at must include a timezone")
        if self.consumer_name is not None:
            _stable_name(self.consumer_name, "consumer_name")
        counts = (
            self.gap_count,
            self.pending_event_count,
            self.max_pending_per_gap,
        )
        if any(value < 0 for value in counts):
            raise ValueError("projection gap counts must be nonnegative")
        if self.oldest_gap_age_seconds is not None and self.oldest_gap_age_seconds < 0:
            raise ValueError("oldest gap age must be nonnegative")
        if self.gap_count == 0:
            if self.pending_event_count != 0 or self.max_pending_per_gap != 0:
                raise ValueError("empty gap set cannot contain pending events")
            if self.oldest_gap_age_seconds is not None:
                raise ValueError("empty gap set cannot have an oldest age")
        else:
            if self.pending_event_count < self.gap_count:
                raise ValueError("each projection gap requires a pending event")
            if not 1 <= self.max_pending_per_gap <= self.pending_event_count:
                raise ValueError("maximum pending count must fit the pending event set")
            if self.oldest_gap_age_seconds is None:
                raise ValueError("nonempty gap set requires an oldest age")

    def to_dict(self) -> dict:
        return {
            "observed_at": self.observed_at.isoformat(),
            "consumer_name": self.consumer_name,
            "gaps": {
                "aggregates": self.gap_count,
                "pending_events": self.pending_event_count,
                "max_pending_per_gap": self.max_pending_per_gap,
                "oldest_age_seconds": self.oldest_gap_age_seconds,
            },
        }


@dataclass(frozen=True)
class ProjectionGapAlert:
    code: str
    severity: AlertSeverity
    observed_value: int | float
    threshold: int | float

    def to_dict(self) -> dict:
        return {
            "code": self.code,
            "severity": self.severity,
            "observed_value": self.observed_value,
            "threshold": self.threshold,
        }


@dataclass(frozen=True)
class ProjectionGapHealthReport:
    status: HealthStatus
    snapshot: ProjectionGapTelemetrySnapshot
    alerts: tuple[ProjectionGapAlert, ...] = ()

    def to_dict(self) -> dict:
        return {
            "status": self.status,
            "alerts": [alert.to_dict() for alert in self.alerts],
            "telemetry": self.snapshot.to_dict(),
        }


@dataclass(frozen=True)
class ProjectionGapAlertPolicy:
    warning_gap_age_seconds: int = 300
    critical_gap_age_seconds: int = 1800
    warning_pending_event_count: int = 100
    critical_pending_event_count: int = 1000

    def __post_init__(self) -> None:
        if not 1 <= self.warning_gap_age_seconds <= self.critical_gap_age_seconds:
            raise ValueError("gap age thresholds must be positive and ordered")
        if not (
            1
            <= self.warning_pending_event_count
            <= self.critical_pending_event_count
        ):
            raise ValueError("pending event thresholds must be positive and ordered")

    def evaluate(
        self,
        snapshot: ProjectionGapTelemetrySnapshot,
    ) -> ProjectionGapHealthReport:
        alerts: list[ProjectionGapAlert] = []

        if snapshot.pending_event_count >= self.critical_pending_event_count:
            alerts.append(
                ProjectionGapAlert(
                    code="projection_pending_events_exceeded",
                    severity="critical",
                    observed_value=snapshot.pending_event_count,
                    threshold=self.critical_pending_event_count,
                )
            )
        elif snapshot.pending_event_count >= self.warning_pending_event_count:
            alerts.append(
                ProjectionGapAlert(
                    code="projection_pending_events_exceeded",
                    severity="warning",
                    observed_value=snapshot.pending_event_count,
                    threshold=self.warning_pending_event_count,
                )
            )

        gap_age = snapshot.oldest_gap_age_seconds
        if gap_age is not None and gap_age >= self.critical_gap_age_seconds:
            alerts.append(
                ProjectionGapAlert(
                    code="projection_gap_age_exceeded",
                    severity="critical",
                    observed_value=gap_age,
                    threshold=self.critical_gap_age_seconds,
                )
            )
        elif gap_age is not None and gap_age >= self.warning_gap_age_seconds:
            alerts.append(
                ProjectionGapAlert(
                    code="projection_gap_age_exceeded",
                    severity="warning",
                    observed_value=gap_age,
                    threshold=self.warning_gap_age_seconds,
                )
            )

        if any(alert.severity == "critical" for alert in alerts):
            status: HealthStatus = "critical"
        elif alerts:
            status = "warning"
        else:
            status = "ok"
        return ProjectionGapHealthReport(
            status=status,
            snapshot=snapshot,
            alerts=tuple(alerts),
        )


class PostgresProjectionGapTelemetryStore:
    def __init__(self, database_url: str):
        self.database_url = database_url

    def _connect(self) -> psycopg.Connection:
        conn = psycopg.connect(self.database_url)
        conn.execute("SET statement_timeout = '8s'")
        conn.execute("SET lock_timeout = '5s'")
        return conn

    def snapshot(
        self,
        *,
        observed_at: datetime | None = None,
        consumer_name: str | None = None,
    ) -> ProjectionGapTelemetrySnapshot:
        if observed_at is not None and (
            observed_at.tzinfo is None or observed_at.utcoffset() is None
        ):
            raise ValueError("observed_at must include a timezone")
        if consumer_name is not None:
            _stable_name(consumer_name, "consumer_name")

        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM kernel_lab.read_inventory_position_projection_gap_telemetry(
                  %s, %s
                )
                """,
                (observed_at, consumer_name),
            ).fetchone()

        if row is None:
            raise RuntimeError("projection gap telemetry query returned no snapshot")
        return ProjectionGapTelemetrySnapshot(
            observed_at=row[0],
            consumer_name=row[1],
            gap_count=row[2],
            pending_event_count=row[3],
            max_pending_per_gap=row[4],
            oldest_gap_age_seconds=(None if row[5] is None else float(row[5])),
        )


def render_prometheus(report: ProjectionGapHealthReport) -> str:
    snapshot = report.snapshot
    labels = _scope_labels(snapshot.consumer_name)
    lines = [
        "# HELP spotwo_wms_projection_gap_aggregates Aggregate cursors with unresolved version gaps.",
        "# TYPE spotwo_wms_projection_gap_aggregates gauge",
        f"spotwo_wms_projection_gap_aggregates{labels} {snapshot.gap_count}",
        "# HELP spotwo_wms_projection_pending_events Events buffered behind unresolved version gaps.",
        "# TYPE spotwo_wms_projection_pending_events gauge",
        f"spotwo_wms_projection_pending_events{labels} {snapshot.pending_event_count}",
        "# HELP spotwo_wms_projection_pending_events_max Largest pending tail behind one gap.",
        "# TYPE spotwo_wms_projection_pending_events_max gauge",
        f"spotwo_wms_projection_pending_events_max{labels} {snapshot.max_pending_per_gap}",
        "# HELP spotwo_wms_projection_gap_health_status Health status: 0 ok, 1 warning, 2 critical.",
        "# TYPE spotwo_wms_projection_gap_health_status gauge",
        f"spotwo_wms_projection_gap_health_status{labels} {_status_number(report.status)}",
        "# HELP spotwo_wms_projection_gap_snapshot_timestamp_seconds Snapshot observation time.",
        "# TYPE spotwo_wms_projection_gap_snapshot_timestamp_seconds gauge",
        (
            f"spotwo_wms_projection_gap_snapshot_timestamp_seconds{labels} "
            f"{_number(snapshot.observed_at.timestamp())}"
        ),
    ]

    if snapshot.oldest_gap_age_seconds is not None:
        lines.extend(
            [
                "# HELP spotwo_wms_projection_oldest_gap_age_seconds Age of the oldest unresolved gap.",
                "# TYPE spotwo_wms_projection_oldest_gap_age_seconds gauge",
                (
                    f"spotwo_wms_projection_oldest_gap_age_seconds{labels} "
                    f"{_number(snapshot.oldest_gap_age_seconds)}"
                ),
            ]
        )

    if report.alerts:
        lines.extend(
            [
                "# HELP spotwo_wms_projection_gap_alert Active projection gap alert.",
                "# TYPE spotwo_wms_projection_gap_alert gauge",
            ]
        )
        for alert in report.alerts:
            alert_labels = {
                "code": alert.code,
                "severity": alert.severity,
            }
            if snapshot.consumer_name is not None:
                alert_labels["consumer"] = snapshot.consumer_name
            rendered_labels = _labels(alert_labels)
            lines.append(f"spotwo_wms_projection_gap_alert{rendered_labels} 1")

    return "\n".join(lines) + "\n"


def _scope_labels(consumer_name: str | None) -> str:
    if consumer_name is None:
        return ""
    return _labels({"consumer": consumer_name})


def _labels(values: dict[str, str]) -> str:
    body = ",".join(
        f'{key}="{_escape_label(value)}"' for key, value in values.items()
    )
    return "{" + body + "}"


def _number(value: int | float) -> str:
    if isinstance(value, int):
        return str(value)
    return format(value, ".15g")


def _status_number(status: HealthStatus) -> int:
    return {"ok": 0, "warning": 1, "critical": 2}[status]


def _escape_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')
