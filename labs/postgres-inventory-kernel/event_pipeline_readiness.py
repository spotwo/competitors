from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal, Mapping, Sequence

import yaml

SignalStatus = Literal["ok", "warning", "critical"]
ReadinessStatus = Literal["ready", "degraded", "not_ready"]

SIGNAL_ORDER = (
    "configuration",
    "pipeline_health",
    "watchdog",
    "canary_sli",
)
_STATUS_RANK: dict[SignalStatus, int] = {"ok": 0, "warning": 1, "critical": 2}


def _mapping(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field} must be an object")
    return value


def _required_name(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} is required")
    if value != value.strip():
        raise ValueError(f"{field} cannot contain surrounding whitespace")
    return value


def _positive(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
        raise ValueError(f"{field} must be positive")
    return float(value)


def _nonnegative(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
        raise ValueError(f"{field} must be nonnegative")
    return float(value)


def _positive_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{field} must be a positive integer")
    return value


def _ratio(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field} must be numeric")
    value = float(value)
    if not 0 < value < 1:
        raise ValueError(f"{field} must be between zero and one")
    return value


def _aware(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must include a timezone")
    return value


@dataclass(frozen=True)
class RuntimeConfig:
    database_url_env: str
    nats_url_env: str
    watchdog_state_path_env: str

    def __post_init__(self) -> None:
        for field, value in (
            ("database_url_env", self.database_url_env),
            ("nats_url_env", self.nats_url_env),
            ("watchdog_state_path_env", self.watchdog_state_path_env),
        ):
            name = _required_name(value, field)
            if not name.replace("_", "").isalnum() or not name[0].isalpha() or name.upper() != name:
                raise ValueError(f"{field} must be an uppercase environment variable name")
        if len({self.database_url_env, self.nats_url_env, self.watchdog_state_path_env}) != 3:
            raise ValueError("runtime environment variable names must be distinct")

    @classmethod
    def from_mapping(cls, value: Any) -> "RuntimeConfig":
        item = _mapping(value, "runtime")
        return cls(
            database_url_env=item.get("database_url_env"),
            nats_url_env=item.get("nats_url_env"),
            watchdog_state_path_env=item.get("watchdog_state_path_env"),
        )


@dataclass(frozen=True)
class TransportConfig:
    stream: str
    subject_prefix: str

    def __post_init__(self) -> None:
        _required_name(self.stream, "transport.stream")
        prefix = _required_name(self.subject_prefix, "transport.subject_prefix")
        if prefix.startswith(".") or prefix.endswith(".") or ".." in prefix:
            raise ValueError("transport.subject_prefix must be a normalized NATS subject prefix")

    @classmethod
    def from_mapping(cls, value: Any) -> "TransportConfig":
        item = _mapping(value, "transport")
        return cls(stream=item.get("stream"), subject_prefix=item.get("subject_prefix"))


@dataclass(frozen=True)
class ConsumerConfig:
    durable: str
    inbox_consumer_name: str
    malformed_lookback_seconds: int
    projection_gap_monitor: Literal["inventory_position", "none"] = "inventory_position"
    handler_module: str | None = None
    handler_class: str | None = None

    def __post_init__(self) -> None:
        _required_name(self.durable, "consumer.durable")
        _required_name(self.inbox_consumer_name, "consumer.inbox_consumer_name")
        _positive_int(self.malformed_lookback_seconds, "consumer.malformed_lookback_seconds")
        if self.projection_gap_monitor not in ("inventory_position", "none"):
            raise ValueError("consumer.projection_gap_monitor must be inventory_position or none")
        if (self.handler_module is None) != (self.handler_class is None):
            raise ValueError("consumer.handler_module and handler_class must be supplied together")
        if self.handler_module is not None:
            _required_name(self.handler_module, "consumer.handler_module")
            _required_name(self.handler_class, "consumer.handler_class")

    @classmethod
    def from_mapping(cls, value: Any) -> "ConsumerConfig":
        item = _mapping(value, "consumer")
        return cls(
            durable=item.get("durable"),
            inbox_consumer_name=item.get("inbox_consumer_name"),
            malformed_lookback_seconds=item.get("malformed_lookback_seconds"),
            projection_gap_monitor=item.get("projection_gap_monitor", "inventory_position"),
            handler_module=item.get("handler_module"),
            handler_class=item.get("handler_class"),
        )


@dataclass(frozen=True)
class CanaryConfig:
    durable: str
    consumer_name: str
    cadence_seconds: float
    timeout_seconds: float

    def __post_init__(self) -> None:
        _required_name(self.durable, "canary.durable")
        _required_name(self.consumer_name, "canary.consumer_name")
        _positive(self.cadence_seconds, "canary.cadence_seconds")
        timeout = _positive(self.timeout_seconds, "canary.timeout_seconds")
        if timeout > 300:
            raise ValueError("canary.timeout_seconds must not exceed 300")

    @classmethod
    def from_mapping(cls, value: Any) -> "CanaryConfig":
        item = _mapping(value, "canary")
        return cls(
            durable=item.get("durable"),
            consumer_name=item.get("consumer_name"),
            cadence_seconds=item.get("cadence_seconds"),
            timeout_seconds=item.get("timeout_seconds"),
        )


@dataclass(frozen=True)
class SloConfig:
    availability_target: float
    latency_target_ratio: float
    latency_target_seconds: float
    stale_after_seconds: float
    warning_consecutive_failures: int
    critical_consecutive_failures: int
    warning_slow_burn_rate: float
    critical_fast_burn_rate: float

    def __post_init__(self) -> None:
        _ratio(self.availability_target, "slo.availability_target")
        _ratio(self.latency_target_ratio, "slo.latency_target_ratio")
        _positive(self.latency_target_seconds, "slo.latency_target_seconds")
        _positive(self.stale_after_seconds, "slo.stale_after_seconds")
        warning_failures = _positive_int(
            self.warning_consecutive_failures, "slo.warning_consecutive_failures"
        )
        critical_failures = _positive_int(
            self.critical_consecutive_failures, "slo.critical_consecutive_failures"
        )
        if critical_failures < warning_failures:
            raise ValueError("critical consecutive failures must not be below warning threshold")
        _positive(self.warning_slow_burn_rate, "slo.warning_slow_burn_rate")
        _positive(self.critical_fast_burn_rate, "slo.critical_fast_burn_rate")

    @classmethod
    def from_mapping(cls, value: Any) -> "SloConfig":
        item = _mapping(value, "slo")
        return cls(
            availability_target=item.get("availability_target"),
            latency_target_ratio=item.get("latency_target_ratio"),
            latency_target_seconds=item.get("latency_target_seconds"),
            stale_after_seconds=item.get("stale_after_seconds"),
            warning_consecutive_failures=item.get("warning_consecutive_failures"),
            critical_consecutive_failures=item.get("critical_consecutive_failures"),
            warning_slow_burn_rate=item.get("warning_slow_burn_rate"),
            critical_fast_burn_rate=item.get("critical_fast_burn_rate"),
        )


@dataclass(frozen=True)
class WatchdogConfig:
    start_grace_seconds: float
    warning_missing_executions: int
    critical_missing_executions: int
    warning_scheduler_lag_seconds: float
    critical_scheduler_lag_seconds: float
    execution_timeout_seconds: float

    def __post_init__(self) -> None:
        _nonnegative(self.start_grace_seconds, "watchdog.start_grace_seconds")
        warning_missing = _positive_int(
            self.warning_missing_executions, "watchdog.warning_missing_executions"
        )
        critical_missing = _positive_int(
            self.critical_missing_executions, "watchdog.critical_missing_executions"
        )
        if critical_missing < warning_missing:
            raise ValueError("critical missing executions must not be below warning threshold")
        warning_lag = _nonnegative(
            self.warning_scheduler_lag_seconds, "watchdog.warning_scheduler_lag_seconds"
        )
        critical_lag = _nonnegative(
            self.critical_scheduler_lag_seconds, "watchdog.critical_scheduler_lag_seconds"
        )
        if critical_lag < warning_lag:
            raise ValueError("critical scheduler lag must not be below warning threshold")
        _positive(self.execution_timeout_seconds, "watchdog.execution_timeout_seconds")

    @classmethod
    def from_mapping(cls, value: Any) -> "WatchdogConfig":
        item = _mapping(value, "watchdog")
        return cls(
            start_grace_seconds=item.get("start_grace_seconds"),
            warning_missing_executions=item.get("warning_missing_executions"),
            critical_missing_executions=item.get("critical_missing_executions"),
            warning_scheduler_lag_seconds=item.get("warning_scheduler_lag_seconds"),
            critical_scheduler_lag_seconds=item.get("critical_scheduler_lag_seconds"),
            execution_timeout_seconds=item.get("execution_timeout_seconds"),
        )


@dataclass(frozen=True)
class EventPipelineDeploymentSpec:
    pipeline_id: str
    enabled: bool
    runtime: RuntimeConfig
    transport: TransportConfig
    consumer: ConsumerConfig
    canary: CanaryConfig
    slo: SloConfig
    watchdog: WatchdogConfig

    def __post_init__(self) -> None:
        pipeline_id = _required_name(self.pipeline_id, "id")
        if any(character not in "abcdefghijklmnopqrstuvwxyz0123456789-" for character in pipeline_id):
            raise ValueError("id must use lowercase letters, numbers, and hyphens")
        if not isinstance(self.enabled, bool):
            raise ValueError("enabled must be boolean")
        if self.consumer.durable == self.canary.durable:
            raise ValueError("canary durable must be separate from the business durable")
        if self.consumer.inbox_consumer_name == self.canary.consumer_name:
            raise ValueError("canary consumer identity must be separate from the business consumer")
        if self.slo.stale_after_seconds < self.canary.cadence_seconds:
            raise ValueError("slo.stale_after_seconds must cover at least one canary cadence")
        if self.watchdog.start_grace_seconds > self.canary.cadence_seconds:
            raise ValueError("watchdog start grace must not exceed canary cadence")
        if self.watchdog.execution_timeout_seconds <= self.canary.timeout_seconds:
            raise ValueError("watchdog execution timeout must exceed canary timeout")

    @classmethod
    def from_mapping(cls, value: Any) -> "EventPipelineDeploymentSpec":
        item = _mapping(value, "pipeline")
        return cls(
            pipeline_id=item.get("id"),
            enabled=item.get("enabled"),
            runtime=RuntimeConfig.from_mapping(item.get("runtime")),
            transport=TransportConfig.from_mapping(item.get("transport")),
            consumer=ConsumerConfig.from_mapping(item.get("consumer")),
            canary=CanaryConfig.from_mapping(item.get("canary")),
            slo=SloConfig.from_mapping(item.get("slo")),
            watchdog=WatchdogConfig.from_mapping(item.get("watchdog")),
        )

    def public_config(self) -> dict[str, Any]:
        return {
            "id": self.pipeline_id,
            "enabled": self.enabled,
            "runtime": {
                "database_url_env": self.runtime.database_url_env,
                "nats_url_env": self.runtime.nats_url_env,
                "watchdog_state_path_env": self.runtime.watchdog_state_path_env,
            },
            "transport": {
                "stream": self.transport.stream,
                "subject_prefix": self.transport.subject_prefix,
            },
            "consumer": {
                "durable": self.consumer.durable,
                "inbox_consumer_name": self.consumer.inbox_consumer_name,
                "malformed_lookback_seconds": self.consumer.malformed_lookback_seconds,
                "projection_gap_monitor": self.consumer.projection_gap_monitor,
                "handler_module": self.consumer.handler_module,
                "handler_class": self.consumer.handler_class,
            },
            "canary": {
                "durable": self.canary.durable,
                "consumer_name": self.canary.consumer_name,
                "cadence_seconds": self.canary.cadence_seconds,
                "timeout_seconds": self.canary.timeout_seconds,
            },
            "slo": self.slo.__dict__.copy(),
            "watchdog": self.watchdog.__dict__.copy(),
        }


@dataclass(frozen=True)
class EventPipelineDeploymentRegistry:
    version: int
    pipelines: tuple[EventPipelineDeploymentSpec, ...]

    def __post_init__(self) -> None:
        if self.version != 1:
            raise ValueError("deployment registry version must be 1")
        if not self.pipelines:
            raise ValueError("deployment registry requires at least one pipeline")
        ids = [pipeline.pipeline_id for pipeline in self.pipelines]
        if len(ids) != len(set(ids)):
            raise ValueError("deployment pipeline ids must be unique")

    @classmethod
    def from_mapping(cls, value: Any) -> "EventPipelineDeploymentRegistry":
        root = _mapping(value, "registry")
        raw_pipelines = root.get("pipelines")
        if not isinstance(raw_pipelines, Sequence) or isinstance(
            raw_pipelines, (str, bytes, bytearray)
        ):
            raise ValueError("pipelines must be an array")
        return cls(
            version=root.get("version"),
            pipelines=tuple(EventPipelineDeploymentSpec.from_mapping(item) for item in raw_pipelines),
        )

    def get(self, pipeline_id: str) -> EventPipelineDeploymentSpec:
        pipeline_id = _required_name(pipeline_id, "pipeline_id")
        for pipeline in self.pipelines:
            if pipeline.pipeline_id == pipeline_id:
                return pipeline
        raise KeyError(pipeline_id)


def load_registry(path: str | Path) -> EventPipelineDeploymentRegistry:
    source = Path(path)
    with source.open("r", encoding="utf-8") as handle:
        value = yaml.safe_load(handle)
    return EventPipelineDeploymentRegistry.from_mapping(value)


@dataclass(frozen=True)
class ReadinessSignal:
    name: str
    status: SignalStatus
    available: bool
    code: str | None
    details: Mapping[str, Any]

    def __post_init__(self) -> None:
        _required_name(self.name, "signal.name")
        if self.status not in _STATUS_RANK:
            raise ValueError("invalid readiness signal status")
        if not isinstance(self.available, bool):
            raise ValueError("signal.available must be boolean")
        if self.status == "ok" and self.code is not None:
            raise ValueError("healthy readiness signal cannot have a code")
        if self.status != "ok" and self.code is None:
            raise ValueError("degraded readiness signal requires a bounded code")
        if not self.available and self.status != "critical":
            raise ValueError("unavailable readiness signal must be critical")

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "available": self.available,
            "code": self.code,
            "details": dict(self.details),
        }


@dataclass(frozen=True)
class EventPipelineReadinessReport:
    observed_at: datetime
    pipeline_id: str
    status: ReadinessStatus
    signals: tuple[ReadinessSignal, ...]
    config: Mapping[str, Any]

    def __post_init__(self) -> None:
        _aware(self.observed_at, "observed_at")
        _required_name(self.pipeline_id, "pipeline_id")
        if self.status not in ("ready", "degraded", "not_ready"):
            raise ValueError("invalid readiness status")
        names = [signal.name for signal in self.signals]
        if len(names) != len(set(names)):
            raise ValueError("readiness signal names must be unique")
        expected = readiness_status(self.signals)
        if self.status != expected:
            raise ValueError("readiness status must match signal severity")

    @property
    def root_cause_candidate(self) -> ReadinessSignal | None:
        if self.status == "ready":
            return None
        worst = max(_STATUS_RANK[signal.status] for signal in self.signals)
        for name in SIGNAL_ORDER:
            for signal in self.signals:
                if signal.name == name and _STATUS_RANK[signal.status] == worst:
                    return signal
        return next(signal for signal in self.signals if _STATUS_RANK[signal.status] == worst)

    def to_dict(self) -> dict[str, Any]:
        root = self.root_cause_candidate
        return {
            "observed_at": self.observed_at.isoformat(),
            "pipeline": self.pipeline_id,
            "status": self.status,
            "collection": {"read_only": True, "atomic": False},
            "root_cause_candidate": (
                None
                if root is None
                else {"signal": root.name, "code": root.code, "status": root.status}
            ),
            "config": dict(self.config),
            "signals": {signal.name: signal.to_dict() for signal in self.signals},
        }


def readiness_status(signals: Sequence[ReadinessSignal]) -> ReadinessStatus:
    if any(signal.status == "critical" for signal in signals):
        return "not_ready"
    if any(signal.status == "warning" for signal in signals):
        return "degraded"
    return "ready"


def _signal_from_report(
    name: str,
    report: Any,
    *,
    warning_code: str,
    critical_code: str,
) -> ReadinessSignal:
    source_status = getattr(report, "status", None)
    if source_status not in _STATUS_RANK:
        return ReadinessSignal(
            name=name,
            status="critical",
            available=False,
            code=f"{name}_status_invalid",
            details={"error_class": type(report).__name__},
        )
    return ReadinessSignal(
        name=name,
        status=source_status,
        available=True,
        code=(
            None
            if source_status == "ok"
            else warning_code if source_status == "warning" else critical_code
        ),
        details=report.to_dict(),
    )


def _unavailable(name: str, code: str, error: Exception | None = None) -> ReadinessSignal:
    details: dict[str, Any] = {}
    if error is not None:
        details["error_class"] = type(error).__name__
    return ReadinessSignal(
        name=name,
        status="critical",
        available=False,
        code=code,
        details=details,
    )


class EventPipelineReadinessCollector:
    """Compose deployment config, current health, external watchdog, and canary SLI."""

    def collect(
        self,
        spec: EventPipelineDeploymentSpec,
        *,
        database_url: str | None,
        nats_url: str | None,
        watchdog_state: Mapping[str, Any] | None,
        observed_at: datetime | None = None,
    ) -> EventPipelineReadinessReport:
        observed_at = observed_at or datetime.now(timezone.utc)
        _aware(observed_at, "observed_at")

        config_signal = ReadinessSignal(
            name="configuration",
            status="ok" if spec.enabled else "critical",
            available=True,
            code=None if spec.enabled else "deployment_disabled",
            details={"enabled": spec.enabled},
        )
        if not spec.enabled:
            signals = (config_signal,)
            return EventPipelineReadinessReport(
                observed_at=observed_at,
                pipeline_id=spec.pipeline_id,
                status=readiness_status(signals),
                signals=signals,
                config=spec.public_config(),
            )

        pipeline_signal = self._pipeline_health_signal(
            spec,
            database_url=database_url,
            nats_url=nats_url,
            observed_at=observed_at,
        )
        watchdog_signal = self._watchdog_signal(
            spec,
            watchdog_state=watchdog_state,
            observed_at=observed_at,
        )
        sli_signal = self._canary_sli_signal(
            spec,
            database_url=database_url,
        )
        signals = (config_signal, pipeline_signal, watchdog_signal, sli_signal)
        return EventPipelineReadinessReport(
            observed_at=observed_at,
            pipeline_id=spec.pipeline_id,
            status=readiness_status(signals),
            signals=signals,
            config=spec.public_config(),
        )

    def _pipeline_health_signal(
        self,
        spec: EventPipelineDeploymentSpec,
        *,
        database_url: str | None,
        nats_url: str | None,
        observed_at: datetime,
    ) -> ReadinessSignal:
        if not database_url or not nats_url:
            return _unavailable("pipeline_health", "pipeline_runtime_endpoint_missing")
        try:
            from event_pipeline_telemetry import EventPipelineHealthCollector

            report = EventPipelineHealthCollector(database_url, nats_url).collect(
                stream_name=spec.transport.stream,
                durable_name=spec.consumer.durable,
                consumer_name=spec.consumer.inbox_consumer_name,
                malformed_lookback_seconds=spec.consumer.malformed_lookback_seconds,
                projection_gap_monitor=spec.consumer.projection_gap_monitor,
                observed_at=observed_at,
            )
        except Exception as exc:
            return _unavailable("pipeline_health", "pipeline_health_unavailable", exc)
        return _signal_from_report(
            "pipeline_health",
            report,
            warning_code="pipeline_health_degraded",
            critical_code="pipeline_health_critical",
        )

    def _canary_sli_signal(
        self,
        spec: EventPipelineDeploymentSpec,
        *,
        database_url: str | None,
    ) -> ReadinessSignal:
        if not database_url:
            return _unavailable("canary_sli", "canary_sli_database_url_missing")
        try:
            from event_pipeline_canary_sli import (
                EventPipelineCanarySliPolicy,
                PostgresEventPipelineCanarySliStore,
            )

            snapshot = PostgresEventPipelineCanarySliStore(database_url).snapshot(
                stream_name=spec.transport.stream,
                durable_name=spec.canary.durable,
                consumer_name=spec.canary.consumer_name,
                availability_target=spec.slo.availability_target,
                latency_target_ratio=spec.slo.latency_target_ratio,
                latency_target_seconds=spec.slo.latency_target_seconds,
            )
            report = EventPipelineCanarySliPolicy(
                stale_after_seconds=spec.slo.stale_after_seconds,
                warning_consecutive_failures=spec.slo.warning_consecutive_failures,
                critical_consecutive_failures=spec.slo.critical_consecutive_failures,
                warning_slow_burn_rate=spec.slo.warning_slow_burn_rate,
                critical_fast_burn_rate=spec.slo.critical_fast_burn_rate,
            ).evaluate(snapshot)
        except Exception as exc:
            return _unavailable("canary_sli", "canary_sli_unavailable", exc)
        return _signal_from_report(
            "canary_sli",
            report,
            warning_code="canary_sli_degraded",
            critical_code="canary_sli_critical",
        )

    def _watchdog_signal(
        self,
        spec: EventPipelineDeploymentSpec,
        *,
        watchdog_state: Mapping[str, Any] | None,
        observed_at: datetime,
    ) -> ReadinessSignal:
        if watchdog_state is None:
            return _unavailable("watchdog", "watchdog_state_unavailable")
        try:
            from event_pipeline_probe_watchdog import (
                ExternalProbeWatchdogPolicy,
                ExternalProbeWatchdogSnapshot,
            )

            snapshot = ExternalProbeWatchdogSnapshot.from_dict(
                watchdog_state,
                observed_at=observed_at,
            )
            expected_scope = (
                spec.transport.stream,
                spec.canary.durable,
                spec.canary.consumer_name,
            )
            actual_scope = (
                snapshot.stream_name,
                snapshot.durable_name,
                snapshot.consumer_name,
            )
            if actual_scope != expected_scope:
                return ReadinessSignal(
                    name="watchdog",
                    status="critical",
                    available=True,
                    code="watchdog_scope_mismatch",
                    details={"watchdog": snapshot.to_dict()},
                )
            if abs(snapshot.cadence_seconds - spec.canary.cadence_seconds) > 1e-9:
                return ReadinessSignal(
                    name="watchdog",
                    status="critical",
                    available=True,
                    code="watchdog_cadence_mismatch",
                    details={"watchdog": snapshot.to_dict()},
                )
            report = ExternalProbeWatchdogPolicy(
                start_grace_seconds=spec.watchdog.start_grace_seconds,
                warning_missing_executions=spec.watchdog.warning_missing_executions,
                critical_missing_executions=spec.watchdog.critical_missing_executions,
                warning_scheduler_lag_seconds=spec.watchdog.warning_scheduler_lag_seconds,
                critical_scheduler_lag_seconds=spec.watchdog.critical_scheduler_lag_seconds,
                execution_timeout_seconds=spec.watchdog.execution_timeout_seconds,
            ).evaluate(snapshot)
        except Exception as exc:
            return _unavailable("watchdog", "watchdog_state_invalid", exc)
        return _signal_from_report(
            "watchdog",
            report,
            warning_code="watchdog_degraded",
            critical_code="watchdog_critical",
        )


def _escape_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def render_prometheus(report: EventPipelineReadinessReport) -> str:
    pipeline = _escape_label(report.pipeline_id)
    lines = [
        "# HELP spotwo_wms_event_pipeline_readiness Overall deployment readiness state.",
        "# TYPE spotwo_wms_event_pipeline_readiness gauge",
    ]
    for status in ("ready", "degraded", "not_ready"):
        value = 1 if report.status == status else 0
        lines.append(
            f'spotwo_wms_event_pipeline_readiness{{pipeline="{pipeline}",status="{status}"}} {value}'
        )

    lines.extend(
        [
            "# HELP spotwo_wms_event_pipeline_readiness_signal Readiness signal health by bounded component and status.",
            "# TYPE spotwo_wms_event_pipeline_readiness_signal gauge",
        ]
    )
    for signal in report.signals:
        name = _escape_label(signal.name)
        for status in ("ok", "warning", "critical"):
            value = 1 if signal.status == status else 0
            lines.append(
                f'spotwo_wms_event_pipeline_readiness_signal{{pipeline="{pipeline}",signal="{name}",status="{status}"}} {value}'
            )
        lines.append(
            f'spotwo_wms_event_pipeline_readiness_signal_available{{pipeline="{pipeline}",signal="{name}"}} {1 if signal.available else 0}'
        )
        if signal.code is not None:
            code = _escape_label(signal.code)
            severity = "critical" if signal.status == "critical" else "warning"
            lines.append(
                f'spotwo_wms_event_pipeline_readiness_alert{{pipeline="{pipeline}",signal="{name}",code="{code}",severity="{severity}"}} 1'
            )

    lines.extend(
        [
            "# HELP spotwo_wms_event_pipeline_readiness_snapshot_timestamp_seconds Unix timestamp of the readiness snapshot.",
            "# TYPE spotwo_wms_event_pipeline_readiness_snapshot_timestamp_seconds gauge",
            f'spotwo_wms_event_pipeline_readiness_snapshot_timestamp_seconds{{pipeline="{pipeline}"}} {report.observed_at.timestamp():.6f}',
        ]
    )
    return "\n".join(lines) + "\n"
