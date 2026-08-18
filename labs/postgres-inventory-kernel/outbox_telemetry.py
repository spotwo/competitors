from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

import psycopg

HealthStatus = Literal["ok", "warning", "critical"]
AlertSeverity = Literal["warning", "critical"]


@dataclass(frozen=True)
class OutboxTelemetrySnapshot:
    observed_at: datetime
    backlog_count: int
    ready_count: int
    delayed_count: int
    leased_count: int
    quarantined_count: int
    attempts_0_count: int
    attempts_1_count: int
    attempts_2_to_4_count: int
    attempts_5_plus_count: int
    max_attempt_count: int | None
    oldest_backlog_age_seconds: float | None
    oldest_ready_age_seconds: float | None
    oldest_quarantined_age_seconds: float | None

    def __post_init__(self) -> None:
        if self.observed_at.tzinfo is None or self.observed_at.utcoffset() is None:
            raise ValueError("observed_at must include a timezone")
        counts = (
            self.backlog_count,
            self.ready_count,
            self.delayed_count,
            self.leased_count,
            self.quarantined_count,
            self.attempts_0_count,
            self.attempts_1_count,
            self.attempts_2_to_4_count,
            self.attempts_5_plus_count,
        )
        if any(value < 0 for value in counts):
            raise ValueError("telemetry counts must be nonnegative")
        if self.backlog_count != (
            self.ready_count
            + self.delayed_count
            + self.leased_count
            + self.quarantined_count
        ):
            raise ValueError("delivery state counts must equal backlog_count")
        if self.backlog_count != (
            self.attempts_0_count
            + self.attempts_1_count
            + self.attempts_2_to_4_count
            + self.attempts_5_plus_count
        ):
            raise ValueError("attempt bucket counts must equal backlog_count")
        ages = (
            self.oldest_backlog_age_seconds,
            self.oldest_ready_age_seconds,
            self.oldest_quarantined_age_seconds,
        )
        if any(value is not None and value < 0 for value in ages):
            raise ValueError("telemetry ages must be nonnegative")
        if self.backlog_count == 0 and self.max_attempt_count is not None:
            raise ValueError("empty backlog cannot have a maximum attempt count")
        if self.backlog_count > 0 and self.max_attempt_count is None:
            raise ValueError("nonempty backlog requires a maximum attempt count")
        if self.max_attempt_count is not None and self.max_attempt_count < 0:
            raise ValueError("maximum attempt count must be nonnegative")
        age_presence = (
            (self.backlog_count, self.oldest_backlog_age_seconds, "backlog"),
            (self.ready_count, self.oldest_ready_age_seconds, "ready"),
            (
                self.quarantined_count,
                self.oldest_quarantined_age_seconds,
                "quarantined",
            ),
        )
        for count, age, state in age_presence:
            if (count == 0) != (age is None):
                raise ValueError(f"{state} age presence must match its event count")

    def to_dict(self) -> dict:
        return {
            "observed_at": self.observed_at.isoformat(),
            "backlog": {
                "total": self.backlog_count,
                "ready": self.ready_count,
                "delayed": self.delayed_count,
                "leased": self.leased_count,
                "quarantined": self.quarantined_count,
            },
            "attempts": {
                "0": self.attempts_0_count,
                "1": self.attempts_1_count,
                "2_to_4": self.attempts_2_to_4_count,
                "5_plus": self.attempts_5_plus_count,
                "max": self.max_attempt_count,
            },
            "oldest_age_seconds": {
                "backlog": self.oldest_backlog_age_seconds,
                "ready": self.oldest_ready_age_seconds,
                "quarantined": self.oldest_quarantined_age_seconds,
            },
        }


@dataclass(frozen=True)
class OutboxAlert:
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
class OutboxHealthReport:
    status: HealthStatus
    snapshot: OutboxTelemetrySnapshot
    alerts: tuple[OutboxAlert, ...] = ()

    def to_dict(self) -> dict:
        return {
            "status": self.status,
            "alerts": [alert.to_dict() for alert in self.alerts],
            "telemetry": self.snapshot.to_dict(),
        }


@dataclass(frozen=True)
class OutboxAlertPolicy:
    warning_ready_age_seconds: int = 60
    critical_ready_age_seconds: int = 300
    warning_backlog_count: int = 1000
    critical_backlog_count: int = 10000

    def __post_init__(self) -> None:
        if not 1 <= self.warning_ready_age_seconds <= self.critical_ready_age_seconds:
            raise ValueError("ready age thresholds must be positive and ordered")
        if not 1 <= self.warning_backlog_count <= self.critical_backlog_count:
            raise ValueError("backlog count thresholds must be positive and ordered")

    def evaluate(self, snapshot: OutboxTelemetrySnapshot) -> OutboxHealthReport:
        alerts: list[OutboxAlert] = []

        if snapshot.quarantined_count > 0:
            alerts.append(
                OutboxAlert(
                    code="outbox_quarantined_events",
                    severity="critical",
                    observed_value=snapshot.quarantined_count,
                    threshold=0,
                )
            )

        if snapshot.backlog_count >= self.critical_backlog_count:
            alerts.append(
                OutboxAlert(
                    code="outbox_backlog_count_exceeded",
                    severity="critical",
                    observed_value=snapshot.backlog_count,
                    threshold=self.critical_backlog_count,
                )
            )
        elif snapshot.backlog_count >= self.warning_backlog_count:
            alerts.append(
                OutboxAlert(
                    code="outbox_backlog_count_exceeded",
                    severity="warning",
                    observed_value=snapshot.backlog_count,
                    threshold=self.warning_backlog_count,
                )
            )

        ready_age = snapshot.oldest_ready_age_seconds
        if ready_age is not None and ready_age >= self.critical_ready_age_seconds:
            alerts.append(
                OutboxAlert(
                    code="outbox_ready_age_exceeded",
                    severity="critical",
                    observed_value=ready_age,
                    threshold=self.critical_ready_age_seconds,
                )
            )
        elif ready_age is not None and ready_age >= self.warning_ready_age_seconds:
            alerts.append(
                OutboxAlert(
                    code="outbox_ready_age_exceeded",
                    severity="warning",
                    observed_value=ready_age,
                    threshold=self.warning_ready_age_seconds,
                )
            )

        if any(alert.severity == "critical" for alert in alerts):
            status: HealthStatus = "critical"
        elif alerts:
            status = "warning"
        else:
            status = "ok"
        return OutboxHealthReport(status=status, snapshot=snapshot, alerts=tuple(alerts))


class PostgresOutboxTelemetryStore:
    def __init__(self, database_url: str):
        self.database_url = database_url

    def _connect(self) -> psycopg.Connection:
        conn = psycopg.connect(self.database_url)
        conn.execute("SET statement_timeout = '8s'")
        conn.execute("SET lock_timeout = '5s'")
        return conn

    def snapshot(self, *, observed_at: datetime | None = None) -> OutboxTelemetrySnapshot:
        if observed_at is not None and (
            observed_at.tzinfo is None or observed_at.utcoffset() is None
        ):
            raise ValueError("observed_at must include a timezone")
        with self._connect() as conn:
            if observed_at is None:
                row = conn.execute(
                    "SELECT * FROM kernel_lab.read_domain_event_outbox_telemetry()"
                ).fetchone()
            else:
                row = conn.execute(
                    "SELECT * FROM kernel_lab.read_domain_event_outbox_telemetry(%s)",
                    (observed_at,),
                ).fetchone()

        if row is None:
            raise RuntimeError("outbox telemetry query returned no snapshot")

        return OutboxTelemetrySnapshot(
            observed_at=row[0],
            backlog_count=row[1],
            ready_count=row[2],
            delayed_count=row[3],
            leased_count=row[4],
            quarantined_count=row[5],
            attempts_0_count=row[6],
            attempts_1_count=row[7],
            attempts_2_to_4_count=row[8],
            attempts_5_plus_count=row[9],
            max_attempt_count=row[10],
            oldest_backlog_age_seconds=_float_or_none(row[11]),
            oldest_ready_age_seconds=_float_or_none(row[12]),
            oldest_quarantined_age_seconds=_float_or_none(row[13]),
        )


def render_prometheus(report: OutboxHealthReport) -> str:
    snapshot = report.snapshot
    lines = [
        "# HELP spotwo_wms_outbox_backlog_events Current unpublished Outbox events.",
        "# TYPE spotwo_wms_outbox_backlog_events gauge",
        f"spotwo_wms_outbox_backlog_events {snapshot.backlog_count}",
        "# HELP spotwo_wms_outbox_events Current unpublished Outbox events by exclusive state.",
        "# TYPE spotwo_wms_outbox_events gauge",
    ]
    states = {
        "ready": snapshot.ready_count,
        "delayed": snapshot.delayed_count,
        "leased": snapshot.leased_count,
        "quarantined": snapshot.quarantined_count,
    }
    lines.extend(
        f'spotwo_wms_outbox_events{{state="{state}"}} {value}'
        for state, value in states.items()
    )

    lines.extend(
        [
            "# HELP spotwo_wms_outbox_attempts Current unpublished events by attempt bucket.",
            "# TYPE spotwo_wms_outbox_attempts gauge",
        ]
    )
    buckets = {
        "0": snapshot.attempts_0_count,
        "1": snapshot.attempts_1_count,
        "2_to_4": snapshot.attempts_2_to_4_count,
        "5_plus": snapshot.attempts_5_plus_count,
    }
    lines.extend(
        f'spotwo_wms_outbox_attempts{{bucket="{bucket}"}} {value}'
        for bucket, value in buckets.items()
    )

    lines.extend(
        [
            "# HELP spotwo_wms_outbox_oldest_age_seconds Age of the oldest event by state.",
            "# TYPE spotwo_wms_outbox_oldest_age_seconds gauge",
        ]
    )
    ages = {
        "backlog": snapshot.oldest_backlog_age_seconds,
        "ready": snapshot.oldest_ready_age_seconds,
        "quarantined": snapshot.oldest_quarantined_age_seconds,
    }
    lines.extend(
        f'spotwo_wms_outbox_oldest_age_seconds{{state="{state}"}} {_number(age)}'
        for state, age in ages.items()
        if age is not None
    )

    lines.extend(
        [
            "# HELP spotwo_wms_outbox_health_status Health status: 0 ok, 1 warning, 2 critical.",
            "# TYPE spotwo_wms_outbox_health_status gauge",
            f"spotwo_wms_outbox_health_status {_status_number(report.status)}",
            "# HELP spotwo_wms_outbox_snapshot_timestamp_seconds Snapshot observation time.",
            "# TYPE spotwo_wms_outbox_snapshot_timestamp_seconds gauge",
            (
                "spotwo_wms_outbox_snapshot_timestamp_seconds "
                f"{_number(snapshot.observed_at.timestamp())}"
            ),
        ]
    )

    if snapshot.max_attempt_count is not None:
        lines.extend(
            [
                "# HELP spotwo_wms_outbox_attempts_max Highest current attempt count.",
                "# TYPE spotwo_wms_outbox_attempts_max gauge",
                f"spotwo_wms_outbox_attempts_max {snapshot.max_attempt_count}",
            ]
        )

    if report.alerts:
        lines.extend(
            [
                "# HELP spotwo_wms_outbox_alert Active Outbox alert by stable code and severity.",
                "# TYPE spotwo_wms_outbox_alert gauge",
            ]
        )
        lines.extend(
            (
                "spotwo_wms_outbox_alert"
                f'{{code="{_escape_label(alert.code)}",'
                f'severity="{alert.severity}"}} 1'
            )
            for alert in report.alerts
        )

    return "\n".join(lines) + "\n"


def _float_or_none(value) -> float | None:
    return None if value is None else float(value)


def _number(value: int | float) -> str:
    if isinstance(value, int):
        return str(value)
    return format(value, ".15g")


def _status_number(status: HealthStatus) -> int:
    return {"ok": 0, "warning": 1, "critical": 2}[status]


def _escape_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')
