from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from math import floor
from typing import Any, Literal, Mapping, Sequence

HealthStatus = Literal["ok", "warning", "critical"]
AlertSeverity = Literal["warning", "critical"]
ExecutionOutcome = Literal["running", "succeeded", "failed"]
ProbeStatus = Literal["ok", "warning", "critical"]

KNOWN_FAILURE_CODES = frozenset(
    {
        "scheduler_launch_failed",
        "postgres_probe_store_unavailable",
        "probe_process_failed",
        "probe_timeout",
        "probe_output_invalid",
    }
)
_STATUS_RANK: dict[HealthStatus, int] = {"ok": 0, "warning": 1, "critical": 2}


def _required_name(value: str, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} is required")
    if value != value.strip():
        raise ValueError(f"{field} cannot contain surrounding whitespace")
    return value


def _aware(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must include a timezone")
    return value


def _positive(value: float, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
        raise ValueError(f"{field} must be positive")
    return float(value)


def _nonnegative(value: float, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
        raise ValueError(f"{field} must be nonnegative")
    return float(value)


def _parse_time(value: Any, field: str) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{field} must be an ISO-8601 timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field} must be an ISO-8601 timestamp") from exc
    return _aware(parsed, field)


def _required_time(value: Any, field: str) -> datetime:
    parsed = _parse_time(value, field)
    if parsed is None:
        raise ValueError(f"{field} is required")
    return parsed


@dataclass(frozen=True)
class ExternalProbeExecutionReceipt:
    execution_id: str
    scheduled_at: datetime
    outcome: ExecutionOutcome
    started_at: datetime | None = None
    finished_at: datetime | None = None
    probe_status: ProbeStatus | None = None
    failure_code: str | None = None
    exit_code: int | None = None

    def __post_init__(self) -> None:
        _required_name(self.execution_id, "execution_id")
        _aware(self.scheduled_at, "scheduled_at")
        if self.outcome not in ("running", "succeeded", "failed"):
            raise ValueError("outcome must be running, succeeded, or failed")
        if self.started_at is not None:
            _aware(self.started_at, "started_at")
            if self.started_at < self.scheduled_at:
                raise ValueError("started_at cannot precede scheduled_at")
        if self.finished_at is not None:
            _aware(self.finished_at, "finished_at")
            reference = self.started_at or self.scheduled_at
            if self.finished_at < reference:
                raise ValueError("finished_at cannot precede execution start")
        if self.exit_code is not None and (
            isinstance(self.exit_code, bool) or not isinstance(self.exit_code, int)
        ):
            raise ValueError("exit_code must be an integer")

        if self.outcome == "running":
            if self.started_at is None or self.finished_at is not None:
                raise ValueError("running execution requires started_at and no finished_at")
            if self.probe_status is not None or self.failure_code is not None:
                raise ValueError("running execution cannot have terminal result fields")
            if self.exit_code is not None:
                raise ValueError("running execution cannot have exit_code")
        elif self.outcome == "succeeded":
            if self.started_at is None or self.finished_at is None:
                raise ValueError("succeeded execution requires start and finish timestamps")
            if self.probe_status not in _STATUS_RANK:
                raise ValueError("succeeded execution requires a probe_status")
            if self.failure_code is not None:
                raise ValueError("succeeded execution cannot have failure_code")
            if self.exit_code not in (None, 0):
                raise ValueError("succeeded execution exit_code must be zero when present")
        else:
            if self.finished_at is None:
                raise ValueError("failed execution requires finished_at")
            if self.probe_status is not None:
                raise ValueError("failed execution cannot have probe_status")
            if self.failure_code not in KNOWN_FAILURE_CODES:
                raise ValueError("failed execution requires a bounded failure_code")

    @property
    def scheduler_lag_seconds(self) -> float | None:
        if self.started_at is None:
            return None
        return max(0.0, (self.started_at - self.scheduled_at).total_seconds())

    def age_seconds(self, observed_at: datetime) -> float | None:
        _aware(observed_at, "observed_at")
        if self.outcome != "running" or self.started_at is None:
            return None
        return max(0.0, (observed_at - self.started_at).total_seconds())

    def to_dict(self) -> dict[str, Any]:
        return {
            "execution_id": self.execution_id,
            "scheduled_at": self.scheduled_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "outcome": self.outcome,
            "probe_status": self.probe_status,
            "failure_code": self.failure_code,
            "exit_code": self.exit_code,
            "scheduler_lag_seconds": self.scheduler_lag_seconds,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ExternalProbeExecutionReceipt":
        if not isinstance(value, Mapping):
            raise ValueError("receipt must be an object")
        return cls(
            execution_id=value.get("execution_id"),
            scheduled_at=_required_time(value.get("scheduled_at"), "scheduled_at"),
            started_at=_parse_time(value.get("started_at"), "started_at"),
            finished_at=_parse_time(value.get("finished_at"), "finished_at"),
            outcome=value.get("outcome"),
            probe_status=value.get("probe_status"),
            failure_code=value.get("failure_code"),
            exit_code=value.get("exit_code"),
        )


@dataclass(frozen=True)
class ExternalProbeWatchdogSnapshot:
    observed_at: datetime
    stream_name: str
    durable_name: str
    consumer_name: str
    expected_execution_at: datetime
    cadence_seconds: float
    receipts: tuple[ExternalProbeExecutionReceipt, ...]

    def __post_init__(self) -> None:
        _aware(self.observed_at, "observed_at")
        _aware(self.expected_execution_at, "expected_execution_at")
        _required_name(self.stream_name, "stream_name")
        _required_name(self.durable_name, "durable_name")
        _required_name(self.consumer_name, "consumer_name")
        _positive(self.cadence_seconds, "cadence_seconds")
        if self.expected_execution_at > self.observed_at:
            raise ValueError("expected_execution_at cannot be in the future")
        execution_ids = [receipt.execution_id for receipt in self.receipts]
        if len(set(execution_ids)) != len(execution_ids):
            raise ValueError("execution_id must be unique")
        schedule_keys = [receipt.scheduled_at for receipt in self.receipts]
        if len(set(schedule_keys)) != len(schedule_keys):
            raise ValueError("only one receipt is allowed per scheduled execution")
        if any(receipt.scheduled_at > self.expected_execution_at for receipt in self.receipts):
            raise ValueError("receipt cannot belong to a future schedule slot")
        for receipt in self.receipts:
            if receipt.started_at is not None and receipt.started_at > self.observed_at:
                raise ValueError("receipt started_at cannot be in the future")
            if receipt.finished_at is not None and receipt.finished_at > self.observed_at:
                raise ValueError("receipt finished_at cannot be in the future")

    @property
    def current_receipt(self) -> ExternalProbeExecutionReceipt | None:
        for receipt in self.receipts:
            if receipt.scheduled_at == self.expected_execution_at:
                return receipt
        return None

    @property
    def latest_receipt(self) -> ExternalProbeExecutionReceipt | None:
        if not self.receipts:
            return None
        return max(self.receipts, key=lambda receipt: receipt.scheduled_at)

    @property
    def latest_completed_receipt(self) -> ExternalProbeExecutionReceipt | None:
        terminal = [receipt for receipt in self.receipts if receipt.finished_at is not None]
        if not terminal:
            return None
        return max(
            terminal,
            key=lambda receipt: (receipt.finished_at or receipt.scheduled_at),
        )

    @property
    def missed_executions_since_latest_receipt(self) -> int:
        if self.current_receipt is not None:
            return 0
        latest = self.latest_receipt
        if latest is None:
            return 1
        delta = (self.expected_execution_at - latest.scheduled_at).total_seconds()
        return max(1, floor(delta / self.cadence_seconds))

    @property
    def latest_completion_age_seconds(self) -> float | None:
        latest = self.latest_completed_receipt
        if latest is None or latest.finished_at is None:
            return None
        return max(0.0, (self.observed_at - latest.finished_at).total_seconds())

    def to_dict(self) -> dict[str, Any]:
        current = self.current_receipt
        latest = self.latest_receipt
        completed = self.latest_completed_receipt
        return {
            "observed_at": self.observed_at.isoformat(),
            "scope": {
                "stream": self.stream_name,
                "durable": self.durable_name,
                "consumer": self.consumer_name,
            },
            "schedule": {
                "expected_execution_at": self.expected_execution_at.isoformat(),
                "cadence_seconds": self.cadence_seconds,
                "current_receipt_present": current is not None,
                "missed_executions_since_latest_receipt": (
                    self.missed_executions_since_latest_receipt
                ),
            },
            "current": None if current is None else current.to_dict(),
            "latest": None if latest is None else latest.to_dict(),
            "latest_completed": None if completed is None else completed.to_dict(),
            "latest_completion_age_seconds": self.latest_completion_age_seconds,
            "receipt_count": len(self.receipts),
        }

    @classmethod
    def from_dict(
        cls,
        value: Mapping[str, Any],
        *,
        observed_at: datetime | None = None,
    ) -> "ExternalProbeWatchdogSnapshot":
        if not isinstance(value, Mapping):
            raise ValueError("watchdog state must be an object")
        scope = value.get("scope")
        if not isinstance(scope, Mapping):
            raise ValueError("scope must be an object")
        resolved_observed_at = observed_at or _parse_time(
            value.get("observed_at"), "observed_at"
        )
        if resolved_observed_at is None:
            resolved_observed_at = datetime.now(timezone.utc)
        receipts_value = value.get("receipts", [])
        if not isinstance(receipts_value, Sequence) or isinstance(
            receipts_value, (str, bytes, bytearray)
        ):
            raise ValueError("receipts must be an array")
        receipts = tuple(
            sorted(
                (ExternalProbeExecutionReceipt.from_dict(item) for item in receipts_value),
                key=lambda receipt: receipt.scheduled_at,
            )
        )
        return cls(
            observed_at=resolved_observed_at,
            stream_name=scope.get("stream"),
            durable_name=scope.get("durable"),
            consumer_name=scope.get("consumer"),
            expected_execution_at=_required_time(
                value.get("expected_execution_at"), "expected_execution_at"
            ),
            cadence_seconds=value.get("cadence_seconds"),
            receipts=receipts,
        )


@dataclass(frozen=True)
class ExternalProbeWatchdogAlert:
    code: str
    severity: AlertSeverity
    observed_value: int | float | str
    threshold: int | float | str | None = None

    def __post_init__(self) -> None:
        _required_name(self.code, "code")
        if self.severity not in ("warning", "critical"):
            raise ValueError("severity must be warning or critical")

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "severity": self.severity,
            "observed_value": self.observed_value,
            "threshold": self.threshold,
        }


@dataclass(frozen=True)
class ExternalProbeWatchdogReport:
    status: HealthStatus
    snapshot: ExternalProbeWatchdogSnapshot
    alerts: tuple[ExternalProbeWatchdogAlert, ...] = ()

    def __post_init__(self) -> None:
        if self.status not in _STATUS_RANK:
            raise ValueError("invalid watchdog health status")
        expected = "ok"
        if any(alert.severity == "critical" for alert in self.alerts):
            expected = "critical"
        elif self.alerts:
            expected = "warning"
        if self.status != expected:
            raise ValueError("watchdog status must match active alerts")

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "alerts": [alert.to_dict() for alert in self.alerts],
            "watchdog": self.snapshot.to_dict(),
        }


@dataclass(frozen=True)
class ExternalProbeWatchdogPolicy:
    start_grace_seconds: float = 30.0
    warning_missing_executions: int = 1
    critical_missing_executions: int = 2
    warning_scheduler_lag_seconds: float = 15.0
    critical_scheduler_lag_seconds: float = 60.0
    execution_timeout_seconds: float = 45.0

    def __post_init__(self) -> None:
        _nonnegative(self.start_grace_seconds, "start_grace_seconds")
        if self.warning_missing_executions < 1:
            raise ValueError("warning_missing_executions must be positive")
        if self.critical_missing_executions < self.warning_missing_executions:
            raise ValueError(
                "critical_missing_executions must not be below warning threshold"
            )
        _nonnegative(self.warning_scheduler_lag_seconds, "warning_scheduler_lag_seconds")
        if self.critical_scheduler_lag_seconds < self.warning_scheduler_lag_seconds:
            raise ValueError(
                "critical scheduler lag must not be below warning scheduler lag"
            )
        _positive(self.execution_timeout_seconds, "execution_timeout_seconds")

    def evaluate(
        self, snapshot: ExternalProbeWatchdogSnapshot
    ) -> ExternalProbeWatchdogReport:
        alerts: list[ExternalProbeWatchdogAlert] = []
        current = snapshot.current_receipt
        overdue_seconds = max(
            0.0,
            (snapshot.observed_at - snapshot.expected_execution_at).total_seconds(),
        )

        if current is None:
            if overdue_seconds >= self.start_grace_seconds:
                missed = snapshot.missed_executions_since_latest_receipt
                if not snapshot.receipts:
                    alerts.append(
                        ExternalProbeWatchdogAlert(
                            "probe_execution_never_observed",
                            "critical",
                            "missing",
                            self.start_grace_seconds,
                        )
                    )
                elif missed >= self.critical_missing_executions:
                    alerts.append(
                        ExternalProbeWatchdogAlert(
                            "probe_execution_missing",
                            "critical",
                            missed,
                            self.critical_missing_executions,
                        )
                    )
                elif missed >= self.warning_missing_executions:
                    alerts.append(
                        ExternalProbeWatchdogAlert(
                            "probe_execution_missing",
                            "warning",
                            missed,
                            self.warning_missing_executions,
                        )
                    )
        else:
            lag = current.scheduler_lag_seconds
            if lag is not None:
                if lag >= self.critical_scheduler_lag_seconds:
                    alerts.append(
                        ExternalProbeWatchdogAlert(
                            "probe_scheduler_lag",
                            "critical",
                            lag,
                            self.critical_scheduler_lag_seconds,
                        )
                    )
                elif lag >= self.warning_scheduler_lag_seconds:
                    alerts.append(
                        ExternalProbeWatchdogAlert(
                            "probe_scheduler_lag",
                            "warning",
                            lag,
                            self.warning_scheduler_lag_seconds,
                        )
                    )

            if current.outcome == "running":
                running_age = current.age_seconds(snapshot.observed_at)
                assert running_age is not None
                if running_age >= self.execution_timeout_seconds:
                    alerts.append(
                        ExternalProbeWatchdogAlert(
                            "probe_execution_stuck",
                            "critical",
                            running_age,
                            self.execution_timeout_seconds,
                        )
                    )
            elif current.outcome == "failed":
                alerts.append(
                    ExternalProbeWatchdogAlert(
                        "probe_execution_failed",
                        "critical",
                        current.failure_code or "unknown",
                        None,
                    )
                )

        if any(alert.severity == "critical" for alert in alerts):
            status: HealthStatus = "critical"
        elif alerts:
            status = "warning"
        else:
            status = "ok"
        return ExternalProbeWatchdogReport(
            status=status, snapshot=snapshot, alerts=tuple(alerts)
        )


def render_prometheus(report: ExternalProbeWatchdogReport) -> str:
    snapshot = report.snapshot
    scope = {
        "stream": snapshot.stream_name,
        "durable": snapshot.durable_name,
        "consumer": snapshot.consumer_name,
    }
    current = snapshot.current_receipt
    lines = [
        "# HELP spotwo_wms_event_pipeline_probe_watchdog_health_status External probe execution watchdog health: 0 ok, 1 warning, 2 critical.",
        "# TYPE spotwo_wms_event_pipeline_probe_watchdog_health_status gauge",
        f"spotwo_wms_event_pipeline_probe_watchdog_health_status{_labels(scope)} {_status_number(report.status)}",
        "# HELP spotwo_wms_event_pipeline_probe_expected_timestamp_seconds Latest execution timestamp expected by the external scheduler.",
        "# TYPE spotwo_wms_event_pipeline_probe_expected_timestamp_seconds gauge",
        f"spotwo_wms_event_pipeline_probe_expected_timestamp_seconds{_labels(scope)} {_number(snapshot.expected_execution_at.timestamp())}",
        "# HELP spotwo_wms_event_pipeline_probe_current_receipt_present Whether the expected schedule slot has an external receipt.",
        "# TYPE spotwo_wms_event_pipeline_probe_current_receipt_present gauge",
        f"spotwo_wms_event_pipeline_probe_current_receipt_present{_labels(scope)} {int(current is not None)}",
        "# HELP spotwo_wms_event_pipeline_probe_missed_executions Observed missing schedule slots since the latest retained receipt.",
        "# TYPE spotwo_wms_event_pipeline_probe_missed_executions gauge",
        f"spotwo_wms_event_pipeline_probe_missed_executions{_labels(scope)} {snapshot.missed_executions_since_latest_receipt}",
        "# HELP spotwo_wms_event_pipeline_probe_receipts Retained external execution receipts in the supplied watchdog state.",
        "# TYPE spotwo_wms_event_pipeline_probe_receipts gauge",
        f"spotwo_wms_event_pipeline_probe_receipts{_labels(scope)} {len(snapshot.receipts)}",
    ]

    if snapshot.latest_completion_age_seconds is not None:
        lines.extend(
            [
                "# HELP spotwo_wms_event_pipeline_probe_latest_completion_age_seconds Age of the latest terminal external probe execution.",
                "# TYPE spotwo_wms_event_pipeline_probe_latest_completion_age_seconds gauge",
                f"spotwo_wms_event_pipeline_probe_latest_completion_age_seconds{_labels(scope)} {_number(snapshot.latest_completion_age_seconds)}",
            ]
        )

    if current is not None:
        lines.extend(
            [
                "# HELP spotwo_wms_event_pipeline_probe_current_outcome Current execution outcome using a bounded outcome label.",
                "# TYPE spotwo_wms_event_pipeline_probe_current_outcome gauge",
                f"spotwo_wms_event_pipeline_probe_current_outcome{_labels(dict(scope, outcome=current.outcome))} 1",
            ]
        )
        if current.scheduler_lag_seconds is not None:
            lines.extend(
                [
                    "# HELP spotwo_wms_event_pipeline_probe_scheduler_lag_seconds Delay between the expected schedule time and actual probe start.",
                    "# TYPE spotwo_wms_event_pipeline_probe_scheduler_lag_seconds gauge",
                    f"spotwo_wms_event_pipeline_probe_scheduler_lag_seconds{_labels(scope)} {_number(current.scheduler_lag_seconds)}",
                ]
            )
        running_age = current.age_seconds(snapshot.observed_at)
        if running_age is not None:
            lines.extend(
                [
                    "# HELP spotwo_wms_event_pipeline_probe_execution_age_seconds Age of the currently running external probe execution.",
                    "# TYPE spotwo_wms_event_pipeline_probe_execution_age_seconds gauge",
                    f"spotwo_wms_event_pipeline_probe_execution_age_seconds{_labels(scope)} {_number(running_age)}",
                ]
            )
        if current.outcome == "succeeded" and current.probe_status is not None:
            lines.extend(
                [
                    "# HELP spotwo_wms_event_pipeline_probe_result_status Health returned by the completed canary without changing watchdog health.",
                    "# TYPE spotwo_wms_event_pipeline_probe_result_status gauge",
                    f"spotwo_wms_event_pipeline_probe_result_status{_labels(dict(scope, status=current.probe_status))} 1",
                ]
            )
        if current.outcome == "failed" and current.failure_code is not None:
            lines.extend(
                [
                    "# HELP spotwo_wms_event_pipeline_probe_execution_failure Current bounded external execution failure.",
                    "# TYPE spotwo_wms_event_pipeline_probe_execution_failure gauge",
                    f"spotwo_wms_event_pipeline_probe_execution_failure{_labels(dict(scope, code=current.failure_code))} 1",
                ]
            )

    if report.alerts:
        lines.extend(
            [
                "# HELP spotwo_wms_event_pipeline_probe_watchdog_alert Active bounded external probe watchdog alert.",
                "# TYPE spotwo_wms_event_pipeline_probe_watchdog_alert gauge",
            ]
        )
        for alert in report.alerts:
            lines.append(
                "spotwo_wms_event_pipeline_probe_watchdog_alert"
                f"{_labels(dict(scope, code=alert.code, severity=alert.severity))} 1"
            )
    return "\n".join(lines) + "\n"


def _labels(values: Mapping[str, str]) -> str:
    return "{" + ",".join(
        f'{key}="{_escape_label(value)}"' for key, value in values.items()
    ) + "}"


def _escape_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _number(value: int | float) -> str:
    return str(value) if isinstance(value, int) else format(value, ".15g")


def _status_number(status: HealthStatus) -> int:
    return _STATUS_RANK[status]
