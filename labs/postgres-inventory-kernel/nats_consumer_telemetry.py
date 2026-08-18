from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal

from nats.aio.client import Client as NatsClient
from nats.js.api import AckPolicy

HealthStatus = Literal["ok", "warning", "critical"]
AlertSeverity = Literal["warning", "critical"]

DEFAULT_WARNING_PENDING_MESSAGES = 100
DEFAULT_CRITICAL_PENDING_MESSAGES = 1000
DEFAULT_WARNING_REDELIVERED_MESSAGES = 1
DEFAULT_CRITICAL_REDELIVERED_MESSAGES = 10
DEFAULT_WARNING_STALLED_SECONDS = 300
DEFAULT_CRITICAL_STALLED_SECONDS = 1800
DEFAULT_WARNING_ACK_CAPACITY_RATIO = 0.80
DEFAULT_CRITICAL_ACK_CAPACITY_RATIO = 1.00


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


def _counter(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field} must be a nonnegative integer")
    return value


def _optional_datetime(value: Any, field: str) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, datetime):
        raise ValueError(f"{field} must be a datetime when present")
    return _aware(value, field)


def _age_seconds(observed_at: datetime, value: datetime | None) -> float | None:
    if value is None:
        return None
    # Broker and monitor wall clocks can differ slightly. A tiny negative age is
    # not operationally useful, so expose it as zero rather than false lag.
    return max(0.0, (observed_at - value).total_seconds())


def _finite_ack_capacity(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError("max_ack_pending must be an integer when present")
    return value if value > 0 else None


def _optional_positive_float(value: Any, field: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
        raise ValueError(f"{field} must be positive when present")
    return float(value)


@dataclass(frozen=True)
class NatsConsumerTelemetrySnapshot:
    observed_at: datetime
    stream_name: str
    durable_name: str
    created_at: datetime
    pending_messages: int
    ack_pending_messages: int
    redelivered_messages: int
    waiting_pull_requests: int
    delivered_consumer_sequence: int
    delivered_stream_sequence: int
    ack_floor_consumer_sequence: int
    ack_floor_stream_sequence: int
    last_delivery_at: datetime | None
    last_ack_at: datetime | None
    paused: bool
    configuration_valid: bool
    max_ack_pending: int | None
    max_deliver: int | None
    ack_wait_seconds: float | None

    def __post_init__(self) -> None:
        _aware(self.observed_at, "observed_at")
        _required_name(self.stream_name, "stream_name")
        _required_name(self.durable_name, "durable_name")
        _aware(self.created_at, "created_at")
        _optional_datetime(self.last_delivery_at, "last_delivery_at")
        _optional_datetime(self.last_ack_at, "last_ack_at")

        for field, value in (
            ("pending_messages", self.pending_messages),
            ("ack_pending_messages", self.ack_pending_messages),
            ("redelivered_messages", self.redelivered_messages),
            ("waiting_pull_requests", self.waiting_pull_requests),
            ("delivered_consumer_sequence", self.delivered_consumer_sequence),
            ("delivered_stream_sequence", self.delivered_stream_sequence),
            ("ack_floor_consumer_sequence", self.ack_floor_consumer_sequence),
            ("ack_floor_stream_sequence", self.ack_floor_stream_sequence),
        ):
            _counter(value, field)

        if self.ack_floor_consumer_sequence > self.delivered_consumer_sequence:
            raise ValueError("consumer ack floor cannot exceed delivered sequence")
        if self.ack_floor_stream_sequence > self.delivered_stream_sequence:
            raise ValueError("stream ack floor cannot exceed delivered sequence")
        if not isinstance(self.paused, bool):
            raise ValueError("paused must be boolean")
        if not isinstance(self.configuration_valid, bool):
            raise ValueError("configuration_valid must be boolean")
        if self.max_ack_pending is not None and self.max_ack_pending <= 0:
            raise ValueError("max_ack_pending must be positive when present")
        if self.max_deliver is not None and (
            isinstance(self.max_deliver, bool) or not isinstance(self.max_deliver, int)
        ):
            raise ValueError("max_deliver must be an integer when present")
        if self.ack_wait_seconds is not None and self.ack_wait_seconds <= 0:
            raise ValueError("ack_wait_seconds must be positive when present")

    @property
    def created_age_seconds(self) -> float:
        return _age_seconds(self.observed_at, self.created_at) or 0.0

    @property
    def delivery_activity_age_seconds(self) -> float | None:
        return _age_seconds(self.observed_at, self.last_delivery_at)

    @property
    def ack_activity_age_seconds(self) -> float | None:
        return _age_seconds(self.observed_at, self.last_ack_at)

    @property
    def delivery_backlog_stall_age_seconds(self) -> float | None:
        if self.pending_messages == 0:
            return None
        reference = self.last_delivery_at or self.created_at
        return _age_seconds(self.observed_at, reference)

    @property
    def ack_backlog_stall_age_seconds(self) -> float | None:
        if self.ack_pending_messages == 0:
            return None
        # Before the first ACK, the first delivery is the earliest trustworthy
        # point at which the current ACK backlog could have started.
        reference = self.last_ack_at or self.last_delivery_at or self.created_at
        return _age_seconds(self.observed_at, reference)

    @property
    def ack_pending_ratio(self) -> float | None:
        if self.max_ack_pending is None:
            return None
        return self.ack_pending_messages / self.max_ack_pending

    @property
    def total_backlog_messages(self) -> int:
        return self.pending_messages + self.ack_pending_messages

    def to_dict(self) -> dict:
        return {
            "observed_at": self.observed_at.isoformat(),
            "stream": self.stream_name,
            "durable": self.durable_name,
            "created_at": self.created_at.isoformat(),
            "configuration": {
                "valid": self.configuration_valid,
                "paused": self.paused,
                "max_ack_pending": self.max_ack_pending,
                "max_deliver": self.max_deliver,
                "ack_wait_seconds": self.ack_wait_seconds,
            },
            "backlog": {
                "pending": self.pending_messages,
                "ack_pending": self.ack_pending_messages,
                "total": self.total_backlog_messages,
                "redelivered": self.redelivered_messages,
                "waiting_pull_requests": self.waiting_pull_requests,
                "ack_pending_ratio": self.ack_pending_ratio,
            },
            "sequence": {
                "delivered": {
                    "consumer": self.delivered_consumer_sequence,
                    "stream": self.delivered_stream_sequence,
                },
                "ack_floor": {
                    "consumer": self.ack_floor_consumer_sequence,
                    "stream": self.ack_floor_stream_sequence,
                },
            },
            "activity": {
                "last_delivery_at": (
                    self.last_delivery_at.isoformat()
                    if self.last_delivery_at is not None
                    else None
                ),
                "last_ack_at": (
                    self.last_ack_at.isoformat() if self.last_ack_at is not None else None
                ),
                "age_seconds": {
                    "consumer_created": self.created_age_seconds,
                    "last_delivery": self.delivery_activity_age_seconds,
                    "last_ack": self.ack_activity_age_seconds,
                    "delivery_backlog_stall": self.delivery_backlog_stall_age_seconds,
                    "ack_backlog_stall": self.ack_backlog_stall_age_seconds,
                },
            },
        }


@dataclass(frozen=True)
class NatsConsumerAlert:
    code: str
    severity: AlertSeverity
    observed_value: int | float | str
    threshold: int | float | str | None = None

    def to_dict(self) -> dict:
        return {
            "code": self.code,
            "severity": self.severity,
            "observed_value": self.observed_value,
            "threshold": self.threshold,
        }


@dataclass(frozen=True)
class NatsConsumerHealthReport:
    status: HealthStatus
    snapshot: NatsConsumerTelemetrySnapshot
    alerts: tuple[NatsConsumerAlert, ...] = ()

    def to_dict(self) -> dict:
        return {
            "status": self.status,
            "alerts": [alert.to_dict() for alert in self.alerts],
            "telemetry": self.snapshot.to_dict(),
        }


@dataclass(frozen=True)
class NatsConsumerAlertPolicy:
    warning_pending_messages: int = DEFAULT_WARNING_PENDING_MESSAGES
    critical_pending_messages: int = DEFAULT_CRITICAL_PENDING_MESSAGES
    warning_redelivered_messages: int = DEFAULT_WARNING_REDELIVERED_MESSAGES
    critical_redelivered_messages: int = DEFAULT_CRITICAL_REDELIVERED_MESSAGES
    warning_stalled_seconds: int = DEFAULT_WARNING_STALLED_SECONDS
    critical_stalled_seconds: int = DEFAULT_CRITICAL_STALLED_SECONDS
    warning_ack_capacity_ratio: float = DEFAULT_WARNING_ACK_CAPACITY_RATIO
    critical_ack_capacity_ratio: float = DEFAULT_CRITICAL_ACK_CAPACITY_RATIO

    def __post_init__(self) -> None:
        if not 1 <= self.warning_pending_messages <= self.critical_pending_messages:
            raise ValueError("pending message thresholds must be positive and ordered")
        if not (
            1
            <= self.warning_redelivered_messages
            <= self.critical_redelivered_messages
        ):
            raise ValueError("redelivery thresholds must be positive and ordered")
        if not 1 <= self.warning_stalled_seconds <= self.critical_stalled_seconds:
            raise ValueError("stalled activity thresholds must be positive and ordered")
        if not (
            0
            < self.warning_ack_capacity_ratio
            <= self.critical_ack_capacity_ratio
            <= 1
        ):
            raise ValueError("ACK capacity ratios must be within (0, 1] and ordered")

    def evaluate(
        self,
        snapshot: NatsConsumerTelemetrySnapshot,
    ) -> NatsConsumerHealthReport:
        alerts: list[NatsConsumerAlert] = []

        if not snapshot.configuration_valid:
            alerts.append(
                NatsConsumerAlert(
                    code="nats_consumer_configuration_invalid",
                    severity="critical",
                    observed_value="invalid",
                )
            )

        if snapshot.pending_messages >= self.critical_pending_messages:
            alerts.append(
                NatsConsumerAlert(
                    code="nats_consumer_pending_backlog",
                    severity="critical",
                    observed_value=snapshot.pending_messages,
                    threshold=self.critical_pending_messages,
                )
            )
        elif snapshot.pending_messages >= self.warning_pending_messages:
            alerts.append(
                NatsConsumerAlert(
                    code="nats_consumer_pending_backlog",
                    severity="warning",
                    observed_value=snapshot.pending_messages,
                    threshold=self.warning_pending_messages,
                )
            )

        if snapshot.redelivered_messages >= self.critical_redelivered_messages:
            alerts.append(
                NatsConsumerAlert(
                    code="nats_consumer_redeliveries",
                    severity="critical",
                    observed_value=snapshot.redelivered_messages,
                    threshold=self.critical_redelivered_messages,
                )
            )
        elif snapshot.redelivered_messages >= self.warning_redelivered_messages:
            alerts.append(
                NatsConsumerAlert(
                    code="nats_consumer_redeliveries",
                    severity="warning",
                    observed_value=snapshot.redelivered_messages,
                    threshold=self.warning_redelivered_messages,
                )
            )

        ratio = snapshot.ack_pending_ratio
        if ratio is not None:
            if ratio >= self.critical_ack_capacity_ratio:
                alerts.append(
                    NatsConsumerAlert(
                        code="nats_consumer_ack_capacity",
                        severity="critical",
                        observed_value=ratio,
                        threshold=self.critical_ack_capacity_ratio,
                    )
                )
            elif ratio >= self.warning_ack_capacity_ratio:
                alerts.append(
                    NatsConsumerAlert(
                        code="nats_consumer_ack_capacity",
                        severity="warning",
                        observed_value=ratio,
                        threshold=self.warning_ack_capacity_ratio,
                    )
                )

        delivery_stall = snapshot.delivery_backlog_stall_age_seconds
        if delivery_stall is not None:
            if delivery_stall >= self.critical_stalled_seconds:
                alerts.append(
                    NatsConsumerAlert(
                        code="nats_consumer_delivery_stalled",
                        severity="critical",
                        observed_value=delivery_stall,
                        threshold=self.critical_stalled_seconds,
                    )
                )
            elif delivery_stall >= self.warning_stalled_seconds:
                alerts.append(
                    NatsConsumerAlert(
                        code="nats_consumer_delivery_stalled",
                        severity="warning",
                        observed_value=delivery_stall,
                        threshold=self.warning_stalled_seconds,
                    )
                )

        ack_stall = snapshot.ack_backlog_stall_age_seconds
        if ack_stall is not None:
            if ack_stall >= self.critical_stalled_seconds:
                alerts.append(
                    NatsConsumerAlert(
                        code="nats_consumer_ack_stalled",
                        severity="critical",
                        observed_value=ack_stall,
                        threshold=self.critical_stalled_seconds,
                    )
                )
            elif ack_stall >= self.warning_stalled_seconds:
                alerts.append(
                    NatsConsumerAlert(
                        code="nats_consumer_ack_stalled",
                        severity="warning",
                        observed_value=ack_stall,
                        threshold=self.warning_stalled_seconds,
                    )
                )

        if snapshot.paused and snapshot.total_backlog_messages > 0:
            alerts.append(
                NatsConsumerAlert(
                    code="nats_consumer_paused_with_backlog",
                    severity="warning",
                    observed_value=snapshot.total_backlog_messages,
                )
            )

        if any(alert.severity == "critical" for alert in alerts):
            status: HealthStatus = "critical"
        elif alerts:
            status = "warning"
        else:
            status = "ok"

        return NatsConsumerHealthReport(
            status=status,
            snapshot=snapshot,
            alerts=tuple(alerts),
        )


class NatsJetStreamConsumerTelemetryStore:
    """Read-only operational snapshot for one pre-provisioned durable consumer."""

    def __init__(
        self,
        server_url: str,
        *,
        client_name: str = "spotwo-wms-nats-consumer-telemetry",
        connect_timeout_seconds: float = 2.0,
    ):
        if not isinstance(server_url, str) or not server_url.strip():
            raise ValueError("server_url is required")
        if connect_timeout_seconds <= 0:
            raise ValueError("connect_timeout_seconds must be positive")
        self.server_url = server_url.strip()
        self.client_name = _required_name(client_name, "client_name")
        self.connect_timeout_seconds = float(connect_timeout_seconds)

    def snapshot(
        self,
        *,
        stream_name: str,
        durable_name: str,
        observed_at: datetime | None = None,
    ) -> NatsConsumerTelemetrySnapshot:
        stream_name = _required_name(stream_name, "stream_name")
        durable_name = _required_name(durable_name, "durable_name")
        if observed_at is not None:
            _aware(observed_at, "observed_at")

        with asyncio.Runner() as runner:
            info = runner.run(self._read_consumer_info(stream_name, durable_name))

        return snapshot_from_consumer_info(
            info,
            stream_name=stream_name,
            durable_name=durable_name,
            observed_at=observed_at or datetime.now(timezone.utc),
        )

    async def _read_consumer_info(self, stream_name: str, durable_name: str):
        client = NatsClient()
        try:
            await asyncio.wait_for(
                client.connect(
                    servers=[self.server_url],
                    name=self.client_name,
                    error_cb=self._capture_client_error,
                    allow_reconnect=False,
                    connect_timeout=self.connect_timeout_seconds,
                    reconnect_time_wait=0,
                    max_reconnect_attempts=1,
                ),
                timeout=self.connect_timeout_seconds,
            )
            return await client.jetstream().consumer_info(stream_name, durable_name)
        finally:
            if not client.is_closed:
                await client.close()

    @staticmethod
    async def _capture_client_error(_error: Exception) -> None:
        # The caller receives the final connection/API error synchronously.
        return None


def snapshot_from_consumer_info(
    info: Any,
    *,
    stream_name: str,
    durable_name: str,
    observed_at: datetime,
) -> NatsConsumerTelemetrySnapshot:
    _aware(observed_at, "observed_at")
    stream_name = _required_name(stream_name, "stream_name")
    durable_name = _required_name(durable_name, "durable_name")

    if getattr(info, "stream_name", None) != stream_name:
        raise ValueError("JetStream consumer info belongs to another stream")
    if getattr(info, "name", None) != durable_name:
        raise ValueError("JetStream consumer info belongs to another consumer")

    config = getattr(info, "config", None)
    if config is None:
        raise ValueError("JetStream consumer info is missing config")

    configured_durable = getattr(config, "durable_name", None)
    configuration_valid = (
        configured_durable == durable_name
        and getattr(config, "ack_policy", None) == AckPolicy.EXPLICIT
        and getattr(config, "deliver_subject", None) is None
    )

    delivered = getattr(info, "delivered", None)
    ack_floor = getattr(info, "ack_floor", None)
    if delivered is None or ack_floor is None:
        raise ValueError("JetStream consumer info is missing sequence state")

    created_at = getattr(info, "created", None)
    if not isinstance(created_at, datetime):
        raise ValueError("JetStream consumer info is missing creation time")

    max_deliver = getattr(config, "max_deliver", None)
    if max_deliver is not None and (
        isinstance(max_deliver, bool) or not isinstance(max_deliver, int)
    ):
        raise ValueError("max_deliver must be an integer when present")

    return NatsConsumerTelemetrySnapshot(
        observed_at=observed_at,
        stream_name=stream_name,
        durable_name=durable_name,
        created_at=_aware(created_at, "consumer created time"),
        pending_messages=_counter(getattr(info, "num_pending", None), "num_pending"),
        ack_pending_messages=_counter(
            getattr(info, "num_ack_pending", None), "num_ack_pending"
        ),
        redelivered_messages=_counter(
            getattr(info, "num_redelivered", None), "num_redelivered"
        ),
        waiting_pull_requests=_counter(
            getattr(info, "num_waiting", None), "num_waiting"
        ),
        delivered_consumer_sequence=_counter(
            getattr(delivered, "consumer_seq", None), "delivered consumer sequence"
        ),
        delivered_stream_sequence=_counter(
            getattr(delivered, "stream_seq", None), "delivered stream sequence"
        ),
        ack_floor_consumer_sequence=_counter(
            getattr(ack_floor, "consumer_seq", None), "ack floor consumer sequence"
        ),
        ack_floor_stream_sequence=_counter(
            getattr(ack_floor, "stream_seq", None), "ack floor stream sequence"
        ),
        last_delivery_at=_optional_datetime(
            getattr(delivered, "last_active", None), "last delivery time"
        ),
        last_ack_at=_optional_datetime(
            getattr(ack_floor, "last_active", None), "last ACK time"
        ),
        paused=bool(getattr(info, "paused", False) or False),
        configuration_valid=configuration_valid,
        max_ack_pending=_finite_ack_capacity(getattr(config, "max_ack_pending", None)),
        max_deliver=max_deliver,
        ack_wait_seconds=_optional_positive_float(
            getattr(config, "ack_wait", None), "ack_wait"
        ),
    )


def render_prometheus(report: NatsConsumerHealthReport) -> str:
    snapshot = report.snapshot
    scope = _labels({"stream": snapshot.stream_name, "durable": snapshot.durable_name})
    lines = [
        "# HELP spotwo_wms_nats_consumer_pending_messages Messages waiting for first delivery to the durable consumer.",
        "# TYPE spotwo_wms_nats_consumer_pending_messages gauge",
        f"spotwo_wms_nats_consumer_pending_messages{scope} {snapshot.pending_messages}",
        "# HELP spotwo_wms_nats_consumer_ack_pending_messages Delivered messages still awaiting acknowledgement.",
        "# TYPE spotwo_wms_nats_consumer_ack_pending_messages gauge",
        f"spotwo_wms_nats_consumer_ack_pending_messages{scope} {snapshot.ack_pending_messages}",
        "# HELP spotwo_wms_nats_consumer_redelivered_messages JetStream consumer redelivery count from ConsumerInfo.",
        "# TYPE spotwo_wms_nats_consumer_redelivered_messages gauge",
        f"spotwo_wms_nats_consumer_redelivered_messages{scope} {snapshot.redelivered_messages}",
        "# HELP spotwo_wms_nats_consumer_waiting_pull_requests Pull requests currently waiting at the durable consumer.",
        "# TYPE spotwo_wms_nats_consumer_waiting_pull_requests gauge",
        f"spotwo_wms_nats_consumer_waiting_pull_requests{scope} {snapshot.waiting_pull_requests}",
        "# HELP spotwo_wms_nats_consumer_sequence JetStream durable consumer sequence positions.",
        "# TYPE spotwo_wms_nats_consumer_sequence gauge",
    ]

    sequences = {
        "delivered_consumer": snapshot.delivered_consumer_sequence,
        "delivered_stream": snapshot.delivered_stream_sequence,
        "ack_floor_consumer": snapshot.ack_floor_consumer_sequence,
        "ack_floor_stream": snapshot.ack_floor_stream_sequence,
    }
    for kind, value in sequences.items():
        labels = _metric_labels(snapshot, kind=kind)
        lines.append(f"spotwo_wms_nats_consumer_sequence{labels} {value}")

    lines.extend(
        [
            "# HELP spotwo_wms_nats_consumer_activity_age_seconds Consumer activity and backlog stall ages.",
            "# TYPE spotwo_wms_nats_consumer_activity_age_seconds gauge",
        ]
    )
    ages = {
        "consumer_created": snapshot.created_age_seconds,
        "last_delivery": snapshot.delivery_activity_age_seconds,
        "last_ack": snapshot.ack_activity_age_seconds,
        "delivery_backlog_stall": snapshot.delivery_backlog_stall_age_seconds,
        "ack_backlog_stall": snapshot.ack_backlog_stall_age_seconds,
    }
    for kind, value in ages.items():
        if value is not None:
            labels = _metric_labels(snapshot, kind=kind)
            lines.append(
                f"spotwo_wms_nats_consumer_activity_age_seconds{labels} {_number(value)}"
            )

    lines.extend(
        [
            "# HELP spotwo_wms_nats_consumer_paused Whether JetStream reports the consumer paused.",
            "# TYPE spotwo_wms_nats_consumer_paused gauge",
            f"spotwo_wms_nats_consumer_paused{scope} {1 if snapshot.paused else 0}",
            "# HELP spotwo_wms_nats_consumer_configuration_valid Whether the durable consumer matches Spotwo explicit-ACK pull-consumer requirements.",
            "# TYPE spotwo_wms_nats_consumer_configuration_valid gauge",
            f"spotwo_wms_nats_consumer_configuration_valid{scope} {1 if snapshot.configuration_valid else 0}",
            "# HELP spotwo_wms_nats_consumer_health_status Health status: 0 ok, 1 warning, 2 critical.",
            "# TYPE spotwo_wms_nats_consumer_health_status gauge",
            f"spotwo_wms_nats_consumer_health_status{scope} {_status_number(report.status)}",
            "# HELP spotwo_wms_nats_consumer_snapshot_timestamp_seconds Snapshot observation time.",
            "# TYPE spotwo_wms_nats_consumer_snapshot_timestamp_seconds gauge",
            f"spotwo_wms_nats_consumer_snapshot_timestamp_seconds{scope} {_number(snapshot.observed_at.timestamp())}",
        ]
    )

    if snapshot.max_ack_pending is not None:
        lines.extend(
            [
                "# HELP spotwo_wms_nats_consumer_max_ack_pending Configured finite maximum unacknowledged messages.",
                "# TYPE spotwo_wms_nats_consumer_max_ack_pending gauge",
                f"spotwo_wms_nats_consumer_max_ack_pending{scope} {snapshot.max_ack_pending}",
                "# HELP spotwo_wms_nats_consumer_ack_pending_ratio Fraction of configured finite ACK capacity currently occupied.",
                "# TYPE spotwo_wms_nats_consumer_ack_pending_ratio gauge",
                f"spotwo_wms_nats_consumer_ack_pending_ratio{scope} {_number(snapshot.ack_pending_ratio or 0.0)}",
            ]
        )

    if report.alerts:
        lines.extend(
            [
                "# HELP spotwo_wms_nats_consumer_alert Active durable-consumer health alert.",
                "# TYPE spotwo_wms_nats_consumer_alert gauge",
            ]
        )
        for alert in report.alerts:
            labels = _metric_labels(
                snapshot,
                code=alert.code,
                severity=alert.severity,
            )
            lines.append(f"spotwo_wms_nats_consumer_alert{labels} 1")

    return "\n".join(lines) + "\n"


def _metric_labels(snapshot: NatsConsumerTelemetrySnapshot, **values: str) -> str:
    labels = {
        **values,
        "stream": snapshot.stream_name,
        "durable": snapshot.durable_name,
    }
    return _labels(labels)


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
    return {"ok": 0, "warning": 1, "critical": 2}[status]
