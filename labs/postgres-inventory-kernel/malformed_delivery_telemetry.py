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


def _bounded_positive(value: int, field: str, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= maximum:
        raise ValueError(f"{field} must be between 1 and {maximum}")
    return value


def _float_or_none(value) -> float | None:
    return None if value is None else float(value)


@dataclass(frozen=True)
class MalformedDeliveryTelemetrySnapshot:
    observed_at: datetime
    consumer_name: str | None
    lookback_seconds: int
    retained_count: int
    recent_count: int
    recent_reobserved_count: int
    total_observation_count: int
    invalid_encoding_count: int
    invalid_json_count: int
    invalid_envelope_count: int
    invalid_headers_count: int
    other_failure_count: int
    max_observation_count: int | None
    oldest_age_seconds: float | None
    newest_age_seconds: float | None
    latest_observation_age_seconds: float | None

    def __post_init__(self) -> None:
        if self.observed_at.tzinfo is None or self.observed_at.utcoffset() is None:
            raise ValueError("observed_at must include a timezone")
        if self.consumer_name is not None:
            _stable_name(self.consumer_name, "consumer_name")
        _bounded_positive(self.lookback_seconds, "lookback_seconds", 604800)

        counts = (
            self.retained_count,
            self.recent_count,
            self.recent_reobserved_count,
            self.total_observation_count,
            self.invalid_encoding_count,
            self.invalid_json_count,
            self.invalid_envelope_count,
            self.invalid_headers_count,
            self.other_failure_count,
        )
        if any(isinstance(value, bool) or not isinstance(value, int) or value < 0 for value in counts):
            raise ValueError("malformed delivery telemetry counts must be nonnegative integers")
        if self.retained_count != (
            self.invalid_encoding_count
            + self.invalid_json_count
            + self.invalid_envelope_count
            + self.invalid_headers_count
            + self.other_failure_count
        ):
            raise ValueError("failure kind counts must equal retained_count")
        if self.recent_count > self.retained_count:
            raise ValueError("recent_count cannot exceed retained_count")
        if self.recent_reobserved_count > self.retained_count:
            raise ValueError("recent_reobserved_count cannot exceed retained_count")
        if self.total_observation_count < self.retained_count:
            raise ValueError("total observations cannot be lower than retained records")

        if self.retained_count == 0:
            if self.max_observation_count is not None:
                raise ValueError("empty poison inventory cannot have max observations")
        else:
            if self.max_observation_count is None or self.max_observation_count < 1:
                raise ValueError("nonempty poison inventory requires max observations")

        ages = (
            self.oldest_age_seconds,
            self.newest_age_seconds,
            self.latest_observation_age_seconds,
        )
        if any(value is not None and value < 0 for value in ages):
            raise ValueError("malformed delivery telemetry ages must be nonnegative")
        if self.retained_count == 0 and any(value is not None for value in ages):
            raise ValueError("empty poison inventory cannot have age samples")
        if self.retained_count > 0 and any(value is None for value in ages):
            raise ValueError("nonempty poison inventory requires all age samples")
        if self.retained_count > 0:
            assert self.oldest_age_seconds is not None
            assert self.newest_age_seconds is not None
            assert self.latest_observation_age_seconds is not None
            if not (
                self.latest_observation_age_seconds
                <= self.newest_age_seconds
                <= self.oldest_age_seconds
            ):
                raise ValueError("poison age samples are not chronologically consistent")

    def to_dict(self) -> dict:
        return {
            "observed_at": self.observed_at.isoformat(),
            "consumer_name": self.consumer_name,
            "lookback_seconds": self.lookback_seconds,
            "records": {
                "retained": self.retained_count,
                "recent": self.recent_count,
                "recent_reobserved": self.recent_reobserved_count,
            },
            "observations": {
                "total": self.total_observation_count,
                "max_per_record": self.max_observation_count,
            },
            "failure_kinds": {
                "invalid_encoding": self.invalid_encoding_count,
                "invalid_json": self.invalid_json_count,
                "invalid_envelope": self.invalid_envelope_count,
                "invalid_headers": self.invalid_headers_count,
                "other": self.other_failure_count,
            },
            "age_seconds": {
                "oldest": self.oldest_age_seconds,
                "newest": self.newest_age_seconds,
                "latest_observation": self.latest_observation_age_seconds,
            },
        }


@dataclass(frozen=True)
class MalformedDeliveryAlert:
    code: str
    severity: AlertSeverity
    observed_value: int
    threshold: int

    def to_dict(self) -> dict:
        return {
            "code": self.code,
            "severity": self.severity,
            "observed_value": self.observed_value,
            "threshold": self.threshold,
        }


@dataclass(frozen=True)
class MalformedDeliveryHealthReport:
    status: HealthStatus
    snapshot: MalformedDeliveryTelemetrySnapshot
    alerts: tuple[MalformedDeliveryAlert, ...] = ()

    def to_dict(self) -> dict:
        return {
            "status": self.status,
            "alerts": [alert.to_dict() for alert in self.alerts],
            "telemetry": self.snapshot.to_dict(),
        }


@dataclass(frozen=True)
class MalformedDeliveryAlertPolicy:
    warning_recent_count: int = 1
    critical_recent_count: int = 10
    warning_reobserved_count: int = 1
    critical_reobserved_count: int = 5

    def __post_init__(self) -> None:
        if not 1 <= self.warning_recent_count <= self.critical_recent_count:
            raise ValueError("recent poison thresholds must be positive and ordered")
        if not 1 <= self.warning_reobserved_count <= self.critical_reobserved_count:
            raise ValueError("reobserved poison thresholds must be positive and ordered")

    def evaluate(
        self,
        snapshot: MalformedDeliveryTelemetrySnapshot,
    ) -> MalformedDeliveryHealthReport:
        alerts: list[MalformedDeliveryAlert] = []

        if snapshot.recent_count >= self.critical_recent_count:
            alerts.append(
                MalformedDeliveryAlert(
                    code="malformed_delivery_recent_records",
                    severity="critical",
                    observed_value=snapshot.recent_count,
                    threshold=self.critical_recent_count,
                )
            )
        elif snapshot.recent_count >= self.warning_recent_count:
            alerts.append(
                MalformedDeliveryAlert(
                    code="malformed_delivery_recent_records",
                    severity="warning",
                    observed_value=snapshot.recent_count,
                    threshold=self.warning_recent_count,
                )
            )

        if snapshot.recent_reobserved_count >= self.critical_reobserved_count:
            alerts.append(
                MalformedDeliveryAlert(
                    code="malformed_delivery_recent_reobserved_records",
                    severity="critical",
                    observed_value=snapshot.recent_reobserved_count,
                    threshold=self.critical_reobserved_count,
                )
            )
        elif snapshot.recent_reobserved_count >= self.warning_reobserved_count:
            alerts.append(
                MalformedDeliveryAlert(
                    code="malformed_delivery_recent_reobserved_records",
                    severity="warning",
                    observed_value=snapshot.recent_reobserved_count,
                    threshold=self.warning_reobserved_count,
                )
            )

        if any(alert.severity == "critical" for alert in alerts):
            status: HealthStatus = "critical"
        elif alerts:
            status = "warning"
        else:
            status = "ok"
        return MalformedDeliveryHealthReport(
            status=status,
            snapshot=snapshot,
            alerts=tuple(alerts),
        )


class PostgresMalformedDeliveryTelemetryStore:
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
        lookback_seconds: int = 300,
    ) -> MalformedDeliveryTelemetrySnapshot:
        if observed_at is not None and (
            observed_at.tzinfo is None or observed_at.utcoffset() is None
        ):
            raise ValueError("observed_at must include a timezone")
        if consumer_name is not None:
            _stable_name(consumer_name, "consumer_name")
        _bounded_positive(lookback_seconds, "lookback_seconds", 604800)

        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM kernel_lab.read_nats_jetstream_consumer_poison_telemetry(
                  %s, %s, %s
                )
                """,
                (observed_at, consumer_name, lookback_seconds),
            ).fetchone()

        if row is None:
            raise RuntimeError("malformed delivery telemetry query returned no snapshot")
        return MalformedDeliveryTelemetrySnapshot(
            observed_at=row[0],
            consumer_name=row[1],
            lookback_seconds=row[2],
            retained_count=row[3],
            recent_count=row[4],
            recent_reobserved_count=row[5],
            total_observation_count=row[6],
            invalid_encoding_count=row[7],
            invalid_json_count=row[8],
            invalid_envelope_count=row[9],
            invalid_headers_count=row[10],
            other_failure_count=row[11],
            max_observation_count=row[12],
            oldest_age_seconds=_float_or_none(row[13]),
            newest_age_seconds=_float_or_none(row[14]),
            latest_observation_age_seconds=_float_or_none(row[15]),
        )


def render_prometheus(report: MalformedDeliveryHealthReport) -> str:
    snapshot = report.snapshot
    scope = _scope_labels(snapshot.consumer_name)
    lines = [
        "# HELP spotwo_wms_malformed_delivery_retained_records Retained malformed JetStream delivery records.",
        "# TYPE spotwo_wms_malformed_delivery_retained_records gauge",
        f"spotwo_wms_malformed_delivery_retained_records{scope} {snapshot.retained_count}",
        "# HELP spotwo_wms_malformed_delivery_recent_records Records first quarantined within the configured lookback.",
        "# TYPE spotwo_wms_malformed_delivery_recent_records gauge",
        f"spotwo_wms_malformed_delivery_recent_records{scope} {snapshot.recent_count}",
        "# HELP spotwo_wms_malformed_delivery_recent_reobserved_records Reobserved poison records with a latest observation inside the lookback.",
        "# TYPE spotwo_wms_malformed_delivery_recent_reobserved_records gauge",
        f"spotwo_wms_malformed_delivery_recent_reobserved_records{scope} {snapshot.recent_reobserved_count}",
        "# HELP spotwo_wms_malformed_delivery_observations Retained all-time observation total across poison records.",
        "# TYPE spotwo_wms_malformed_delivery_observations gauge",
        f"spotwo_wms_malformed_delivery_observations{scope} {snapshot.total_observation_count}",
        "# HELP spotwo_wms_malformed_delivery_failure_records Retained poison records by bounded failure kind.",
        "# TYPE spotwo_wms_malformed_delivery_failure_records gauge",
    ]

    failure_kinds = {
        "invalid_encoding": snapshot.invalid_encoding_count,
        "invalid_json": snapshot.invalid_json_count,
        "invalid_envelope": snapshot.invalid_envelope_count,
        "invalid_headers": snapshot.invalid_headers_count,
        "other": snapshot.other_failure_count,
    }
    for kind, value in failure_kinds.items():
        labels = _metric_labels(snapshot.consumer_name, kind=kind)
        lines.append(f"spotwo_wms_malformed_delivery_failure_records{labels} {value}")

    lines.extend(
        [
            "# HELP spotwo_wms_malformed_delivery_age_seconds Age of retained poison evidence.",
            "# TYPE spotwo_wms_malformed_delivery_age_seconds gauge",
        ]
    )
    ages = {
        "oldest": snapshot.oldest_age_seconds,
        "newest": snapshot.newest_age_seconds,
        "latest_observation": snapshot.latest_observation_age_seconds,
    }
    for kind, value in ages.items():
        if value is not None:
            labels = _metric_labels(snapshot.consumer_name, kind=kind)
            lines.append(f"spotwo_wms_malformed_delivery_age_seconds{labels} {_number(value)}")

    lines.extend(
        [
            "# HELP spotwo_wms_malformed_delivery_lookback_seconds Configured recent-activity lookback window.",
            "# TYPE spotwo_wms_malformed_delivery_lookback_seconds gauge",
            f"spotwo_wms_malformed_delivery_lookback_seconds{scope} {snapshot.lookback_seconds}",
            "# HELP spotwo_wms_malformed_delivery_health_status Health status: 0 ok, 1 warning, 2 critical.",
            "# TYPE spotwo_wms_malformed_delivery_health_status gauge",
            f"spotwo_wms_malformed_delivery_health_status{scope} {_status_number(report.status)}",
            "# HELP spotwo_wms_malformed_delivery_snapshot_timestamp_seconds Snapshot observation time.",
            "# TYPE spotwo_wms_malformed_delivery_snapshot_timestamp_seconds gauge",
            f"spotwo_wms_malformed_delivery_snapshot_timestamp_seconds{scope} {_number(snapshot.observed_at.timestamp())}",
        ]
    )

    if snapshot.max_observation_count is not None:
        lines.extend(
            [
                "# HELP spotwo_wms_malformed_delivery_observations_max Highest observation count on one retained poison record.",
                "# TYPE spotwo_wms_malformed_delivery_observations_max gauge",
                f"spotwo_wms_malformed_delivery_observations_max{scope} {snapshot.max_observation_count}",
            ]
        )

    if report.alerts:
        lines.extend(
            [
                "# HELP spotwo_wms_malformed_delivery_alert Active malformed-delivery alert.",
                "# TYPE spotwo_wms_malformed_delivery_alert gauge",
            ]
        )
        for alert in report.alerts:
            labels = _metric_labels(
                snapshot.consumer_name,
                code=alert.code,
                severity=alert.severity,
            )
            lines.append(f"spotwo_wms_malformed_delivery_alert{labels} 1")

    return "\n".join(lines) + "\n"


def _scope_labels(consumer_name: str | None) -> str:
    return "" if consumer_name is None else _labels({"consumer": consumer_name})


def _metric_labels(consumer_name: str | None, **values: str) -> str:
    labels = dict(values)
    if consumer_name is not None:
        labels["consumer"] = consumer_name
    return _labels(labels)


def _labels(values: dict[str, str]) -> str:
    if not values:
        return ""
    rendered = ",".join(
        f'{key}="{_escape_label(value)}"' for key, value in values.items()
    )
    return "{" + rendered + "}"


def _escape_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _number(value: float) -> str:
    return format(value, ".15g")


def _status_number(status: HealthStatus) -> int:
    return {"ok": 0, "warning": 1, "critical": 2}[status]
