from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Literal

from consumer_failure_telemetry import (
    ConsumerFailureAlertPolicy,
    PostgresConsumerFailureTelemetryStore,
)
from malformed_delivery_telemetry import (
    MalformedDeliveryAlertPolicy,
    PostgresMalformedDeliveryTelemetryStore,
)
from nats_consumer_telemetry import (
    NatsConsumerAlertPolicy,
    NatsJetStreamConsumerTelemetryStore,
)
from nats_stream_telemetry import (
    NatsJetStreamStreamTelemetryStore,
    NatsStreamAlertPolicy,
)
from outbox_telemetry import OutboxAlertPolicy, PostgresOutboxTelemetryStore
from projection_gap_telemetry import (
    PostgresProjectionGapTelemetryStore,
    ProjectionGapAlertPolicy,
)

HealthStatus = Literal["ok", "warning", "critical"]
AlertSeverity = Literal["warning", "critical"]

COMPONENT_ORDER = (
    "outbox",
    "nats_stream",
    "nats_consumer",
    "malformed_delivery",
    "consumer_failure",
    "projection_gap",
)

STRICT_COMPONENT_ORDER = COMPONENT_ORDER[:-1]

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


@dataclass(frozen=True)
class EventPipelineAlert:
    component: str
    code: str
    severity: AlertSeverity
    observed_value: int | float | str | None = None
    threshold: int | float | str | None = None
    synthetic: bool = False

    def __post_init__(self) -> None:
        _required_name(self.component, "component")
        _required_name(self.code, "code")
        if self.severity not in ("warning", "critical"):
            raise ValueError("severity must be warning or critical")
        if not isinstance(self.synthetic, bool):
            raise ValueError("synthetic must be boolean")

    def to_dict(self) -> dict:
        return {
            "component": self.component,
            "code": self.code,
            "severity": self.severity,
            "observed_value": self.observed_value,
            "threshold": self.threshold,
            "synthetic": self.synthetic,
        }


@dataclass(frozen=True)
class EventPipelineComponentHealth:
    component: str
    status: HealthStatus
    available: bool
    alerts: tuple[EventPipelineAlert, ...]
    telemetry: dict | None
    error_code: str | None = None

    def __post_init__(self) -> None:
        _required_name(self.component, "component")
        if self.status not in _STATUS_RANK:
            raise ValueError("invalid component health status")
        if not isinstance(self.available, bool):
            raise ValueError("available must be boolean")
        if any(alert.component != self.component for alert in self.alerts):
            raise ValueError("component alert identity mismatch")
        if self.available:
            if self.telemetry is None:
                raise ValueError("available component requires telemetry")
            if self.error_code is not None:
                raise ValueError("available component cannot expose an error code")
        else:
            if self.status != "critical":
                raise ValueError("unavailable component must be critical")
            if self.telemetry is not None:
                raise ValueError("unavailable component cannot expose telemetry")
            if self.error_code is None:
                raise ValueError("unavailable component requires an error code")
            if not self.alerts:
                raise ValueError("unavailable component requires a synthetic alert")

    def to_dict(self) -> dict:
        return {
            "status": self.status,
            "available": self.available,
            "error_code": self.error_code,
            "alerts": [alert.to_dict() for alert in self.alerts],
            "telemetry": self.telemetry,
        }


@dataclass(frozen=True)
class EventPipelineHealthReport:
    observed_at: datetime
    stream_name: str
    durable_name: str
    consumer_name: str
    status: HealthStatus
    components: tuple[EventPipelineComponentHealth, ...]
    root_cause_candidate: EventPipelineAlert | None

    def __post_init__(self) -> None:
        _aware(self.observed_at, "observed_at")
        _required_name(self.stream_name, "stream_name")
        _required_name(self.durable_name, "durable_name")
        _required_name(self.consumer_name, "consumer_name")
        if self.status not in _STATUS_RANK:
            raise ValueError("invalid pipeline health status")
        names = tuple(component.component for component in self.components)
        if names not in (COMPONENT_ORDER, STRICT_COMPONENT_ORDER):
            raise ValueError("pipeline components must use a supported topology order")
        expected = _status_from_components(self.components)
        if self.status != expected:
            raise ValueError("pipeline status must equal the worst component status")
        if self.status == "ok" and self.root_cause_candidate is not None:
            raise ValueError("healthy pipeline cannot have a root cause candidate")
        if self.status != "ok" and self.root_cause_candidate is None:
            raise ValueError("degraded pipeline requires a root cause candidate")

    @property
    def alerts(self) -> tuple[EventPipelineAlert, ...]:
        return tuple(alert for component in self.components for alert in component.alerts)

    @property
    def degraded_components(self) -> tuple[str, ...]:
        return tuple(
            component.component for component in self.components if component.status != "ok"
        )

    def to_dict(self) -> dict:
        return {
            "observed_at": self.observed_at.isoformat(),
            "status": self.status,
            "scope": {
                "stream": self.stream_name,
                "durable": self.durable_name,
                "consumer_name": self.consumer_name,
            },
            "collection": {
                "read_only": True,
                "atomic": False,
            },
            "root_cause_candidate": (
                None
                if self.root_cause_candidate is None
                else self.root_cause_candidate.to_dict()
            ),
            "degraded_components": list(self.degraded_components),
            "alerts": [alert.to_dict() for alert in self.alerts],
            "components": {
                component.component: component.to_dict() for component in self.components
            },
        }


class EventPipelineHealthCollector:
    """Compose existing read-only component health reports into one pipeline view."""

    def __init__(
        self,
        database_url: str,
        nats_server_url: str,
        *,
        outbox_policy: OutboxAlertPolicy | None = None,
        stream_policy: NatsStreamAlertPolicy | None = None,
        consumer_policy: NatsConsumerAlertPolicy | None = None,
        malformed_policy: MalformedDeliveryAlertPolicy | None = None,
        consumer_failure_policy: ConsumerFailureAlertPolicy | None = None,
        projection_gap_policy: ProjectionGapAlertPolicy | None = None,
    ):
        if not isinstance(database_url, str) or not database_url.strip():
            raise ValueError("database_url is required")
        if not isinstance(nats_server_url, str) or not nats_server_url.strip():
            raise ValueError("nats_server_url is required")

        self.outbox_store = PostgresOutboxTelemetryStore(database_url.strip())
        self.stream_store = NatsJetStreamStreamTelemetryStore(nats_server_url.strip())
        self.consumer_store = NatsJetStreamConsumerTelemetryStore(nats_server_url.strip())
        self.malformed_store = PostgresMalformedDeliveryTelemetryStore(database_url.strip())
        self.consumer_failure_store = PostgresConsumerFailureTelemetryStore(
            database_url.strip()
        )
        self.projection_gap_store = PostgresProjectionGapTelemetryStore(database_url.strip())

        self.outbox_policy = outbox_policy or OutboxAlertPolicy()
        self.stream_policy = stream_policy or NatsStreamAlertPolicy()
        self.consumer_policy = consumer_policy or NatsConsumerAlertPolicy()
        self.malformed_policy = malformed_policy or MalformedDeliveryAlertPolicy()
        self.consumer_failure_policy = (
            consumer_failure_policy or ConsumerFailureAlertPolicy()
        )
        self.projection_gap_policy = projection_gap_policy or ProjectionGapAlertPolicy()

    def collect(
        self,
        *,
        stream_name: str,
        durable_name: str,
        consumer_name: str,
        malformed_lookback_seconds: int = 300,
        projection_gap_monitor: Literal["inventory_position", "none"] = "inventory_position",
        observed_at: datetime | None = None,
    ) -> EventPipelineHealthReport:
        stream_name = _required_name(stream_name, "stream_name")
        durable_name = _required_name(durable_name, "durable_name")
        consumer_name = _required_name(consumer_name, "consumer_name")
        if projection_gap_monitor not in ("inventory_position", "none"):
            raise ValueError("projection_gap_monitor must be inventory_position or none")
        if observed_at is None:
            observed_at = datetime.now(timezone.utc)
        else:
            _aware(observed_at, "observed_at")

        components = (
            self._collect_component(
                "outbox",
                lambda: self.outbox_policy.evaluate(
                    self.outbox_store.snapshot(observed_at=observed_at)
                ),
            ),
            self._collect_component(
                "nats_stream",
                lambda: self.stream_policy.evaluate(
                    self.stream_store.snapshot(
                        stream_name=stream_name,
                        observed_at=observed_at,
                    )
                ),
            ),
            self._collect_component(
                "nats_consumer",
                lambda: self.consumer_policy.evaluate(
                    self.consumer_store.snapshot(
                        stream_name=stream_name,
                        durable_name=durable_name,
                        observed_at=observed_at,
                    )
                ),
            ),
            self._collect_component(
                "malformed_delivery",
                lambda: self.malformed_policy.evaluate(
                    self.malformed_store.snapshot(
                        observed_at=observed_at,
                        consumer_name=consumer_name,
                        lookback_seconds=malformed_lookback_seconds,
                    )
                ),
            ),
            self._collect_component(
                "consumer_failure",
                lambda: self.consumer_failure_policy.evaluate(
                    self.consumer_failure_store.snapshot(
                        observed_at=observed_at,
                        consumer_name=consumer_name,
                    )
                ),
            ),
        )
        if projection_gap_monitor == "inventory_position":
            components += (
                self._collect_component(
                    "projection_gap",
                    lambda: self.projection_gap_policy.evaluate(
                        self.projection_gap_store.snapshot(
                            observed_at=observed_at,
                            consumer_name=consumer_name,
                        )
                    ),
                ),
            )

        status = _status_from_components(components)
        return EventPipelineHealthReport(
            observed_at=observed_at,
            stream_name=stream_name,
            durable_name=durable_name,
            consumer_name=consumer_name,
            status=status,
            components=components,
            root_cause_candidate=_root_cause_candidate(components, status),
        )

    @staticmethod
    def _collect_component(
        component: str,
        read_report: Callable[[], object],
    ) -> EventPipelineComponentHealth:
        try:
            report = read_report()
        except Exception:
            code = f"{component}_telemetry_unavailable"
            alert = EventPipelineAlert(
                component=component,
                code=code,
                severity="critical",
                observed_value="unavailable",
                synthetic=True,
            )
            return EventPipelineComponentHealth(
                component=component,
                status="critical",
                available=False,
                alerts=(alert,),
                telemetry=None,
                error_code=code,
            )

        status = getattr(report, "status", None)
        if status not in _STATUS_RANK:
            raise ValueError(f"{component} report returned an invalid health status")
        snapshot = getattr(report, "snapshot", None)
        if snapshot is None or not hasattr(snapshot, "to_dict"):
            raise ValueError(f"{component} report is missing telemetry")

        alerts = tuple(
            EventPipelineAlert(
                component=component,
                code=alert.code,
                severity=alert.severity,
                observed_value=getattr(alert, "observed_value", None),
                threshold=getattr(alert, "threshold", None),
            )
            for alert in getattr(report, "alerts", ())
        )
        return EventPipelineComponentHealth(
            component=component,
            status=status,
            available=True,
            alerts=alerts,
            telemetry=snapshot.to_dict(),
        )


def _status_from_components(
    components: tuple[EventPipelineComponentHealth, ...],
) -> HealthStatus:
    if any(component.status == "critical" for component in components):
        return "critical"
    if any(component.status == "warning" for component in components):
        return "warning"
    return "ok"


def _root_cause_candidate(
    components: tuple[EventPipelineComponentHealth, ...],
    status: HealthStatus,
) -> EventPipelineAlert | None:
    if status == "ok":
        return None

    target_severity: AlertSeverity = "critical" if status == "critical" else "warning"
    for component in components:
        for alert in component.alerts:
            if alert.severity == target_severity:
                return alert

    # Component reports are expected to explain every degraded status with an alert.
    # Keep a deterministic fallback rather than fabricating a new causal claim.
    for component in components:
        if component.alerts:
            return component.alerts[0]
    return None


def render_prometheus(report: EventPipelineHealthReport) -> str:
    scope = _scope_labels(report)
    lines = [
        "# HELP spotwo_wms_event_pipeline_health_status Event pipeline health: 0 ok, 1 warning, 2 critical.",
        "# TYPE spotwo_wms_event_pipeline_health_status gauge",
        f"spotwo_wms_event_pipeline_health_status{scope} {_status_number(report.status)}",
        "# HELP spotwo_wms_event_pipeline_snapshot_timestamp_seconds Logical observation time shared by component reads.",
        "# TYPE spotwo_wms_event_pipeline_snapshot_timestamp_seconds gauge",
        (
            f"spotwo_wms_event_pipeline_snapshot_timestamp_seconds{scope} "
            f"{_number(report.observed_at.timestamp())}"
        ),
        "# HELP spotwo_wms_event_pipeline_degraded_components Number of warning or critical pipeline components.",
        "# TYPE spotwo_wms_event_pipeline_degraded_components gauge",
        f"spotwo_wms_event_pipeline_degraded_components{scope} {len(report.degraded_components)}",
        "# HELP spotwo_wms_event_pipeline_component_health_status Component health: 0 ok, 1 warning, 2 critical.",
        "# TYPE spotwo_wms_event_pipeline_component_health_status gauge",
    ]

    for component in report.components:
        labels = _component_labels(report, component.component)
        lines.append(
            f"spotwo_wms_event_pipeline_component_health_status{labels} "
            f"{_status_number(component.status)}"
        )

    lines.extend(
        [
            "# HELP spotwo_wms_event_pipeline_component_available Whether component telemetry was readable.",
            "# TYPE spotwo_wms_event_pipeline_component_available gauge",
        ]
    )
    for component in report.components:
        labels = _component_labels(report, component.component)
        lines.append(
            f"spotwo_wms_event_pipeline_component_available{labels} "
            f"{1 if component.available else 0}"
        )

    if report.alerts:
        lines.extend(
            [
                "# HELP spotwo_wms_event_pipeline_alert Active pipeline alert by bounded component, code, and severity.",
                "# TYPE spotwo_wms_event_pipeline_alert gauge",
            ]
        )
        for alert in report.alerts:
            labels = _labels(
                {
                    **_scope_values(report),
                    "component": alert.component,
                    "code": alert.code,
                    "severity": alert.severity,
                }
            )
            lines.append(f"spotwo_wms_event_pipeline_alert{labels} 1")

    if report.root_cause_candidate is not None:
        root = report.root_cause_candidate
        labels = _labels(
            {
                **_scope_values(report),
                "component": root.component,
                "code": root.code,
                "severity": root.severity,
            }
        )
        lines.extend(
            [
                "# HELP spotwo_wms_event_pipeline_root_cause_candidate Deterministic highest-severity upstream-first cause candidate.",
                "# TYPE spotwo_wms_event_pipeline_root_cause_candidate gauge",
                f"spotwo_wms_event_pipeline_root_cause_candidate{labels} 1",
            ]
        )

    return "\n".join(lines) + "\n"


def _scope_values(report: EventPipelineHealthReport) -> dict[str, str]:
    return {
        "stream": report.stream_name,
        "durable": report.durable_name,
        "consumer": report.consumer_name,
    }


def _scope_labels(report: EventPipelineHealthReport) -> str:
    return _labels(_scope_values(report))


def _component_labels(report: EventPipelineHealthReport, component: str) -> str:
    return _labels({**_scope_values(report), "component": component})


def _labels(values: dict[str, str]) -> str:
    rendered = ",".join(
        f'{key}="{_escape_label(value)}"' for key, value in values.items()
    )
    return "{" + rendered + "}"


def _escape_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _number(value: float) -> str:
    return format(value, ".15g")


def _status_number(status: HealthStatus) -> int:
    return _STATUS_RANK[status]
