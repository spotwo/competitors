from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, Mapping

import psycopg

from event_pipeline_canary import EventPipelineCanaryHealthReport

SliHealthStatus = Literal["ok", "warning", "critical"]
SliAlertSeverity = Literal["warning", "critical"]

DEFAULT_WINDOWS = (300, 3600, 86400)
WINDOW_LABELS = {300: "5m", 3600: "1h", 86400: "24h"}
TERMINAL_FAILURE_CODES = frozenset(
    {
        "canary_consumer_configuration_invalid",
        "canary_stream_identity_mismatch",
        "canary_durable_identity_mismatch",
        "canary_consumer_identity_mismatch",
        "canary_publish_timeout",
        "canary_delivery_timeout",
        "canary_projection_timeout",
        "canary_ack_timeout",
        "canary_incomplete_timeout",
        "canary_ack_observer_unavailable",
        "canary_incomplete",
    }
)
TERMINAL_FAILURE_PRIORITY = (
    "canary_publish_timeout",
    "canary_delivery_timeout",
    "canary_projection_timeout",
    "canary_ack_timeout",
    "canary_incomplete_timeout",
    "canary_consumer_configuration_invalid",
    "canary_stream_identity_mismatch",
    "canary_durable_identity_mismatch",
    "canary_consumer_identity_mismatch",
    "canary_ack_observer_unavailable",
)
LATENCY_STAGES = (
    "outbox_publish_confirm",
    "broker_to_inbox",
    "inbox_to_projection",
    "end_to_end_projection",
)
QUANTILES = ("p50", "p95", "p99")


def _required_name(value: str, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} is required")
    if value != value.strip():
        raise ValueError(f"{field} cannot contain surrounding whitespace")
    return value


def _ratio(value: float, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field} must be numeric")
    value = float(value)
    if not 0 < value < 1:
        raise ValueError(f"{field} must be between zero and one")
    return value


def _positive(value: float, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
        raise ValueError(f"{field} must be positive")
    return float(value)


def _optional_nonnegative(value: float | None) -> float | None:
    if value is None or value < 0:
        return None
    return value


def _terminal_code(report: EventPipelineCanaryHealthReport) -> str | None:
    if report.snapshot.complete:
        return None
    observed_codes = {alert.code for alert in report.alerts}
    for code in TERMINAL_FAILURE_PRIORITY:
        if code in observed_codes:
            return code
    return "canary_incomplete"


def _clock_skew_detected(report: EventPipelineCanaryHealthReport) -> bool:
    return any(alert.code == "canary_clock_skew_detected" for alert in report.alerts)


@dataclass(frozen=True)
class EventPipelineCanaryOutcomeReceipt:
    created: bool
    finalized_at: datetime

    def to_dict(self) -> dict[str, Any]:
        return {
            "created": self.created,
            "finalized_at": self.finalized_at.isoformat(),
        }


@dataclass(frozen=True)
class EventPipelineCanarySliWindow:
    observed_at: datetime
    window_seconds: int
    total_runs: int
    successful_runs: int
    failed_runs: int
    availability_success_ratio: float | None
    availability_burn_rate: float | None
    latency_good_runs: int
    latency_success_ratio: float | None
    latency_burn_rate: float | None
    last_run_at: datetime | None
    last_success_at: datetime | None
    consecutive_failures: int
    failure_codes: Mapping[str, int]
    latency_quantiles: Mapping[str, Mapping[str, float | None]]

    @property
    def window_label(self) -> str:
        return WINDOW_LABELS.get(self.window_seconds, f"{self.window_seconds}s")

    @property
    def last_run_age_seconds(self) -> float | None:
        if self.last_run_at is None:
            return None
        return max(0.0, (self.observed_at - self.last_run_at).total_seconds())

    def to_dict(self) -> dict[str, Any]:
        return {
            "window": self.window_label,
            "window_seconds": self.window_seconds,
            "total_runs": self.total_runs,
            "successful_runs": self.successful_runs,
            "failed_runs": self.failed_runs,
            "availability": {
                "success_ratio": self.availability_success_ratio,
                "burn_rate": self.availability_burn_rate,
            },
            "latency": {
                "good_runs": self.latency_good_runs,
                "success_ratio": self.latency_success_ratio,
                "burn_rate": self.latency_burn_rate,
                "quantiles_seconds": self.latency_quantiles,
            },
            "last_run_at": self.last_run_at.isoformat() if self.last_run_at else None,
            "last_success_at": (
                self.last_success_at.isoformat() if self.last_success_at else None
            ),
            "last_run_age_seconds": self.last_run_age_seconds,
            "consecutive_failures": self.consecutive_failures,
            "failure_codes": dict(self.failure_codes),
        }


@dataclass(frozen=True)
class EventPipelineCanarySliSnapshot:
    stream_name: str
    durable_name: str
    consumer_name: str
    availability_target: float
    latency_target_ratio: float
    latency_target_seconds: float
    windows: tuple[EventPipelineCanarySliWindow, ...]

    def __post_init__(self) -> None:
        _required_name(self.stream_name, "stream_name")
        _required_name(self.durable_name, "durable_name")
        _required_name(self.consumer_name, "consumer_name")
        _ratio(self.availability_target, "availability_target")
        _ratio(self.latency_target_ratio, "latency_target_ratio")
        _positive(self.latency_target_seconds, "latency_target_seconds")
        if tuple(window.window_seconds for window in self.windows) != DEFAULT_WINDOWS:
            raise ValueError("canary SLI snapshot requires 5m, 1h, and 24h windows")

    def window(self, seconds: int) -> EventPipelineCanarySliWindow:
        for window in self.windows:
            if window.window_seconds == seconds:
                return window
        raise KeyError(seconds)

    def to_dict(self) -> dict[str, Any]:
        return {
            "scope": {
                "stream": self.stream_name,
                "durable": self.durable_name,
                "consumer": self.consumer_name,
            },
            "objectives": {
                "availability_target": self.availability_target,
                "latency_target_ratio": self.latency_target_ratio,
                "latency_target_seconds": self.latency_target_seconds,
            },
            "windows": [window.to_dict() for window in self.windows],
        }


@dataclass(frozen=True)
class EventPipelineCanarySliAlert:
    code: str
    severity: SliAlertSeverity
    observed_value: int | float | str
    threshold: int | float | str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "severity": self.severity,
            "observed_value": self.observed_value,
            "threshold": self.threshold,
        }


@dataclass(frozen=True)
class EventPipelineCanarySliReport:
    status: SliHealthStatus
    snapshot: EventPipelineCanarySliSnapshot
    alerts: tuple[EventPipelineCanarySliAlert, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "alerts": [alert.to_dict() for alert in self.alerts],
            "sli": self.snapshot.to_dict(),
        }


class PostgresEventPipelineCanarySliStore:
    """Durable canary outcome history and read-only SLI window snapshots."""

    def __init__(self, database_url: str):
        self.database_url = _required_name(database_url, "database_url")

    def _connect(self) -> psycopg.Connection:
        conn = psycopg.connect(self.database_url)
        conn.execute("SET statement_timeout = '8s'")
        conn.execute("SET lock_timeout = '5s'")
        return conn

    def record(
        self,
        report: EventPipelineCanaryHealthReport,
    ) -> EventPipelineCanaryOutcomeReceipt:
        snapshot = report.snapshot
        database = snapshot.database
        terminal_code = _terminal_code(report)
        outcome = "succeeded" if snapshot.complete else "failed"
        if terminal_code is not None and terminal_code not in TERMINAL_FAILURE_CODES:
            raise ValueError("canary terminal code is not bounded")

        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT created, finalized_at
                FROM kernel_lab.record_event_pipeline_canary_outcome(
                  %s, %s, %s, %s, %s, %s, %s,
                  %s, %s, %s, %s, %s, %s, %s
                )
                """,
                (
                    database.canary_id,
                    snapshot.expected_stream_name,
                    snapshot.expected_durable_name,
                    snapshot.expected_consumer_name,
                    outcome,
                    report.status,
                    terminal_code,
                    report.timed_out,
                    snapshot.ack_confirmed,
                    _clock_skew_detected(report),
                    _optional_nonnegative(database.outbox_publish_confirm_seconds),
                    database.broker_to_inbox_seconds,
                    _optional_nonnegative(database.inbox_to_projection_seconds),
                    _optional_nonnegative(database.end_to_end_projection_seconds),
                ),
            ).fetchone()

        if row is None:
            raise RuntimeError("canary outcome persistence returned no result")
        return EventPipelineCanaryOutcomeReceipt(created=row[0], finalized_at=row[1])

    def read_window(
        self,
        *,
        stream_name: str,
        durable_name: str,
        consumer_name: str,
        window_seconds: int,
        availability_target: float,
        latency_target_ratio: float,
        latency_target_seconds: float,
    ) -> EventPipelineCanarySliWindow:
        _required_name(stream_name, "stream_name")
        _required_name(durable_name, "durable_name")
        _required_name(consumer_name, "consumer_name")
        _ratio(availability_target, "availability_target")
        _ratio(latency_target_ratio, "latency_target_ratio")
        _positive(latency_target_seconds, "latency_target_seconds")
        if window_seconds not in DEFAULT_WINDOWS:
            raise ValueError("window_seconds must be one of 300, 3600, or 86400")

        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM kernel_lab.read_event_pipeline_canary_sli(
                  %s, %s, %s, %s, %s, %s, %s
                )
                """,
                (
                    stream_name,
                    durable_name,
                    consumer_name,
                    window_seconds,
                    availability_target,
                    latency_target_ratio,
                    latency_target_seconds,
                ),
            ).fetchone()

        if row is None:
            raise RuntimeError("canary SLI snapshot returned no result")
        failure_codes = {
            str(code): int(count) for code, count in dict(row[13] or {}).items()
        }
        raw_quantiles = dict(row[14] or {})
        latency_quantiles: dict[str, dict[str, float | None]] = {}
        for stage in LATENCY_STAGES:
            raw_stage = dict(raw_quantiles.get(stage) or {})
            latency_quantiles[stage] = {
                quantile: (
                    float(raw_stage[quantile])
                    if raw_stage.get(quantile) is not None
                    else None
                )
                for quantile in QUANTILES
            }

        return EventPipelineCanarySliWindow(
            observed_at=row[0],
            window_seconds=row[1],
            total_runs=row[2],
            successful_runs=row[3],
            failed_runs=row[4],
            availability_success_ratio=row[5],
            availability_burn_rate=row[6],
            latency_good_runs=row[7],
            latency_success_ratio=row[8],
            latency_burn_rate=row[9],
            last_run_at=row[10],
            last_success_at=row[11],
            consecutive_failures=row[12],
            failure_codes=failure_codes,
            latency_quantiles=latency_quantiles,
        )

    def snapshot(
        self,
        *,
        stream_name: str,
        durable_name: str,
        consumer_name: str,
        availability_target: float = 0.999,
        latency_target_ratio: float = 0.99,
        latency_target_seconds: float = 5.0,
    ) -> EventPipelineCanarySliSnapshot:
        windows = tuple(
            self.read_window(
                stream_name=stream_name,
                durable_name=durable_name,
                consumer_name=consumer_name,
                window_seconds=window_seconds,
                availability_target=availability_target,
                latency_target_ratio=latency_target_ratio,
                latency_target_seconds=latency_target_seconds,
            )
            for window_seconds in DEFAULT_WINDOWS
        )
        return EventPipelineCanarySliSnapshot(
            stream_name=stream_name,
            durable_name=durable_name,
            consumer_name=consumer_name,
            availability_target=availability_target,
            latency_target_ratio=latency_target_ratio,
            latency_target_seconds=latency_target_seconds,
            windows=windows,
        )


@dataclass(frozen=True)
class EventPipelineCanarySliPolicy:
    stale_after_seconds: float = 180.0
    warning_consecutive_failures: int = 2
    critical_consecutive_failures: int = 3
    warning_slow_burn_rate: float = 3.0
    critical_fast_burn_rate: float = 14.4

    def __post_init__(self) -> None:
        _positive(self.stale_after_seconds, "stale_after_seconds")
        if self.warning_consecutive_failures < 1:
            raise ValueError("warning_consecutive_failures must be positive")
        if self.critical_consecutive_failures < self.warning_consecutive_failures:
            raise ValueError("critical consecutive failures must not be lower than warning")
        if not 0 < self.warning_slow_burn_rate <= self.critical_fast_burn_rate:
            raise ValueError("burn-rate thresholds must be positive and ordered")

    def evaluate(
        self,
        snapshot: EventPipelineCanarySliSnapshot,
    ) -> EventPipelineCanarySliReport:
        alerts: list[EventPipelineCanarySliAlert] = []
        short = snapshot.window(300)
        hour = snapshot.window(3600)
        day = snapshot.window(86400)

        last_run_age = short.last_run_age_seconds
        if last_run_age is None:
            alerts.append(
                EventPipelineCanarySliAlert(
                    "canary_sli_no_observations",
                    "critical",
                    "missing",
                    self.stale_after_seconds,
                )
            )
        elif last_run_age >= self.stale_after_seconds:
            alerts.append(
                EventPipelineCanarySliAlert(
                    "canary_sli_stale",
                    "critical",
                    last_run_age,
                    self.stale_after_seconds,
                )
            )

        streak = short.consecutive_failures
        if streak >= self.critical_consecutive_failures:
            alerts.append(
                EventPipelineCanarySliAlert(
                    "canary_sli_consecutive_failures",
                    "critical",
                    streak,
                    self.critical_consecutive_failures,
                )
            )
        elif streak >= self.warning_consecutive_failures:
            alerts.append(
                EventPipelineCanarySliAlert(
                    "canary_sli_consecutive_failures",
                    "warning",
                    streak,
                    self.warning_consecutive_failures,
                )
            )

        self._evaluate_burn_pair(
            alerts,
            code="canary_sli_availability_fast_burn",
            severity="critical",
            short_rate=short.availability_burn_rate,
            long_rate=hour.availability_burn_rate,
            threshold=self.critical_fast_burn_rate,
        )
        self._evaluate_burn_pair(
            alerts,
            code="canary_sli_availability_slow_burn",
            severity="warning",
            short_rate=hour.availability_burn_rate,
            long_rate=day.availability_burn_rate,
            threshold=self.warning_slow_burn_rate,
        )
        self._evaluate_burn_pair(
            alerts,
            code="canary_sli_latency_fast_burn",
            severity="critical",
            short_rate=short.latency_burn_rate,
            long_rate=hour.latency_burn_rate,
            threshold=self.critical_fast_burn_rate,
        )
        self._evaluate_burn_pair(
            alerts,
            code="canary_sli_latency_slow_burn",
            severity="warning",
            short_rate=hour.latency_burn_rate,
            long_rate=day.latency_burn_rate,
            threshold=self.warning_slow_burn_rate,
        )

        if any(alert.severity == "critical" for alert in alerts):
            status: SliHealthStatus = "critical"
        elif alerts:
            status = "warning"
        else:
            status = "ok"
        return EventPipelineCanarySliReport(
            status=status,
            snapshot=snapshot,
            alerts=tuple(alerts),
        )

    @staticmethod
    def _evaluate_burn_pair(
        alerts: list[EventPipelineCanarySliAlert],
        *,
        code: str,
        severity: SliAlertSeverity,
        short_rate: float | None,
        long_rate: float | None,
        threshold: float,
    ) -> None:
        if short_rate is None or long_rate is None:
            return
        if short_rate >= threshold and long_rate >= threshold:
            alerts.append(
                EventPipelineCanarySliAlert(
                    code,
                    severity,
                    max(short_rate, long_rate),
                    threshold,
                )
            )


def render_prometheus(report: EventPipelineCanarySliReport) -> str:
    snapshot = report.snapshot
    base_scope = {
        "stream": snapshot.stream_name,
        "durable": snapshot.durable_name,
        "consumer": snapshot.consumer_name,
    }
    lines = [
        "# HELP spotwo_wms_event_pipeline_canary_sli_health_status Canary SLI health: 0 ok, 1 warning, 2 critical.",
        "# TYPE spotwo_wms_event_pipeline_canary_sli_health_status gauge",
        f"spotwo_wms_event_pipeline_canary_sli_health_status{_labels(base_scope)} {_status_number(report.status)}",
        "# HELP spotwo_wms_event_pipeline_canary_sli_runs Canary outcomes observed in the window.",
        "# TYPE spotwo_wms_event_pipeline_canary_sli_runs gauge",
        "# HELP spotwo_wms_event_pipeline_canary_sli_availability_success_ratio Availability success ratio for the window.",
        "# TYPE spotwo_wms_event_pipeline_canary_sli_availability_success_ratio gauge",
        "# HELP spotwo_wms_event_pipeline_canary_sli_availability_burn_rate Availability error-budget burn rate for the window.",
        "# TYPE spotwo_wms_event_pipeline_canary_sli_availability_burn_rate gauge",
        "# HELP spotwo_wms_event_pipeline_canary_sli_latency_success_ratio Fraction of successful canaries meeting the end-to-end latency target.",
        "# TYPE spotwo_wms_event_pipeline_canary_sli_latency_success_ratio gauge",
        "# HELP spotwo_wms_event_pipeline_canary_sli_latency_burn_rate Latency error-budget burn rate for the window.",
        "# TYPE spotwo_wms_event_pipeline_canary_sli_latency_burn_rate gauge",
        "# HELP spotwo_wms_event_pipeline_canary_sli_latency_seconds Canary latency quantiles by bounded stage and quantile.",
        "# TYPE spotwo_wms_event_pipeline_canary_sli_latency_seconds gauge",
    ]

    for window in snapshot.windows:
        window_scope = dict(base_scope, window=window.window_label)
        for outcome, value in (
            ("total", window.total_runs),
            ("succeeded", window.successful_runs),
            ("failed", window.failed_runs),
        ):
            lines.append(
                "spotwo_wms_event_pipeline_canary_sli_runs"
                f"{_labels(dict(window_scope, outcome=outcome))} {value}"
            )
        if window.availability_success_ratio is not None:
            lines.append(
                "spotwo_wms_event_pipeline_canary_sli_availability_success_ratio"
                f"{_labels(window_scope)} {_number(window.availability_success_ratio)}"
            )
        if window.availability_burn_rate is not None:
            lines.append(
                "spotwo_wms_event_pipeline_canary_sli_availability_burn_rate"
                f"{_labels(window_scope)} {_number(window.availability_burn_rate)}"
            )
        if window.latency_success_ratio is not None:
            lines.append(
                "spotwo_wms_event_pipeline_canary_sli_latency_success_ratio"
                f"{_labels(window_scope)} {_number(window.latency_success_ratio)}"
            )
        if window.latency_burn_rate is not None:
            lines.append(
                "spotwo_wms_event_pipeline_canary_sli_latency_burn_rate"
                f"{_labels(window_scope)} {_number(window.latency_burn_rate)}"
            )
        for stage in LATENCY_STAGES:
            stage_values = window.latency_quantiles.get(stage, {})
            for quantile in QUANTILES:
                value = stage_values.get(quantile)
                if value is None:
                    continue
                labels = dict(
                    window_scope,
                    stage=stage,
                    quantile=quantile,
                )
                lines.append(
                    "spotwo_wms_event_pipeline_canary_sli_latency_seconds"
                    f"{_labels(labels)} {_number(value)}"
                )

        for code, count in sorted(window.failure_codes.items()):
            bounded_code = code if code in TERMINAL_FAILURE_CODES else "other"
            labels = dict(window_scope, code=bounded_code)
            lines.append(
                "spotwo_wms_event_pipeline_canary_sli_failures"
                f"{_labels(labels)} {count}"
            )

    short = snapshot.window(300)
    lines.extend(
        [
            "# HELP spotwo_wms_event_pipeline_canary_sli_consecutive_failures Consecutive failed canary outcomes for the deployment scope.",
            "# TYPE spotwo_wms_event_pipeline_canary_sli_consecutive_failures gauge",
            f"spotwo_wms_event_pipeline_canary_sli_consecutive_failures{_labels(base_scope)} {short.consecutive_failures}",
            "# HELP spotwo_wms_event_pipeline_canary_sli_last_run_age_seconds Age of the latest recorded canary outcome.",
            "# TYPE spotwo_wms_event_pipeline_canary_sli_last_run_age_seconds gauge",
        ]
    )
    if short.last_run_age_seconds is not None:
        lines.append(
            "spotwo_wms_event_pipeline_canary_sli_last_run_age_seconds"
            f"{_labels(base_scope)} {_number(short.last_run_age_seconds)}"
        )

    if report.alerts:
        lines.extend(
            [
                "# HELP spotwo_wms_event_pipeline_canary_sli_alert Active bounded canary SLI alert.",
                "# TYPE spotwo_wms_event_pipeline_canary_sli_alert gauge",
            ]
        )
        for alert in report.alerts:
            labels = dict(base_scope, code=alert.code, severity=alert.severity)
            lines.append(
                f"spotwo_wms_event_pipeline_canary_sli_alert{_labels(labels)} 1"
            )

    return "\n".join(lines) + "\n"


def _labels(values: Mapping[str, str]) -> str:
    body = ",".join(
        f'{key}="{_escape_label(value)}"' for key, value in values.items()
    )
    return "{" + body + "}"


def _escape_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _number(value: int | float) -> str:
    if isinstance(value, int):
        return str(value)
    return format(value, ".15g")


def _status_number(status: SliHealthStatus) -> int:
    return {"ok": 0, "warning": 1, "critical": 2}[status]
