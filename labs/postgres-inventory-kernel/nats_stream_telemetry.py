from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal

from nats.aio.client import Client as NatsClient

HealthStatus = Literal["ok", "warning", "critical"]
AlertSeverity = Literal["warning", "critical"]

DEFAULT_WARNING_CAPACITY_RATIO = 0.80
DEFAULT_CRITICAL_CAPACITY_RATIO = 0.95
DEFAULT_WARNING_REPLICA_LAG_MESSAGES = 1000
DEFAULT_CRITICAL_REPLICA_LAG_MESSAGES = 10000


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


def _optional_datetime(value: Any, field: str) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, datetime):
        raise ValueError(f"{field} must be a datetime when present")
    return _aware(value, field)


def _counter(value: Any, field: str) -> int:
    if value is None:
        return 0
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field} must be a nonnegative integer")
    return value


def _finite_limit(value: Any, field: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field} must be an integer when present")
    return value if value > 0 else None


def _positive_float_or_none(value: Any, field: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field} must be numeric when present")
    return float(value) if value > 0 else None


def _enum_value(value: Any) -> str | None:
    if value is None:
        return None
    raw = getattr(value, "value", value)
    return str(raw)


def _lost_message_count(value: Any) -> int:
    if value is None:
        return 0
    messages = value.get("msgs") if isinstance(value, dict) else getattr(value, "msgs", None)
    if messages is None:
        return 0
    return len(messages)


def _age_seconds(observed_at: datetime, value: datetime | None) -> float | None:
    if value is None:
        return None
    return max(0.0, (observed_at - value).total_seconds())


@dataclass(frozen=True)
class NatsStreamTelemetrySnapshot:
    observed_at: datetime
    stream_name: str
    messages: int
    bytes: int
    first_sequence: int
    last_sequence: int
    first_message_at: datetime | None
    last_message_at: datetime | None
    consumer_count: int
    deleted_messages: int
    lost_messages: int
    max_messages: int | None
    max_bytes: int | None
    max_age_seconds: float | None
    configured_replicas: int
    storage: str | None
    retention: str | None
    discard: str | None
    configuration_valid: bool
    clustered: bool
    cluster_leader_present: bool | None
    follower_count: int
    offline_followers: int
    not_current_followers: int
    max_replica_lag_messages: int

    def __post_init__(self) -> None:
        _aware(self.observed_at, "observed_at")
        _required_name(self.stream_name, "stream_name")
        _optional_datetime(self.first_message_at, "first_message_at")
        _optional_datetime(self.last_message_at, "last_message_at")
        for field, value in (
            ("messages", self.messages),
            ("bytes", self.bytes),
            ("first_sequence", self.first_sequence),
            ("last_sequence", self.last_sequence),
            ("consumer_count", self.consumer_count),
            ("deleted_messages", self.deleted_messages),
            ("lost_messages", self.lost_messages),
            ("follower_count", self.follower_count),
            ("offline_followers", self.offline_followers),
            ("not_current_followers", self.not_current_followers),
            ("max_replica_lag_messages", self.max_replica_lag_messages),
        ):
            _counter(value, field)
        if self.messages == 0 and self.first_message_at is not None:
            raise ValueError("empty stream cannot have first_message_at")
        if self.messages == 0 and self.last_message_at is not None:
            raise ValueError("empty stream cannot have last_message_at")
        if self.messages > 0 and self.last_sequence < self.first_sequence:
            raise ValueError("last_sequence cannot precede first_sequence")
        if self.max_messages is not None and self.max_messages <= 0:
            raise ValueError("max_messages must be positive when present")
        if self.max_bytes is not None and self.max_bytes <= 0:
            raise ValueError("max_bytes must be positive when present")
        if self.max_age_seconds is not None and self.max_age_seconds <= 0:
            raise ValueError("max_age_seconds must be positive when present")
        if isinstance(self.configured_replicas, bool) or self.configured_replicas < 1:
            raise ValueError("configured_replicas must be positive")
        if self.offline_followers > self.follower_count:
            raise ValueError("offline_followers cannot exceed follower_count")
        if self.not_current_followers > self.follower_count:
            raise ValueError("not_current_followers cannot exceed follower_count")
        if self.clustered and self.cluster_leader_present is None:
            raise ValueError("clustered stream requires leader state")
        if not self.clustered and self.cluster_leader_present is not None:
            raise ValueError("standalone stream cannot expose cluster leader state")

    @property
    def message_capacity_ratio(self) -> float | None:
        return None if self.max_messages is None else self.messages / self.max_messages

    @property
    def byte_capacity_ratio(self) -> float | None:
        return None if self.max_bytes is None else self.bytes / self.max_bytes

    @property
    def oldest_message_age_seconds(self) -> float | None:
        return _age_seconds(self.observed_at, self.first_message_at)

    @property
    def newest_message_age_seconds(self) -> float | None:
        return _age_seconds(self.observed_at, self.last_message_at)

    def to_dict(self) -> dict:
        return {
            "observed_at": self.observed_at.isoformat(),
            "stream": self.stream_name,
            "state": {
                "messages": self.messages,
                "bytes": self.bytes,
                "first_sequence": self.first_sequence,
                "last_sequence": self.last_sequence,
                "consumer_count": self.consumer_count,
                "deleted_messages": self.deleted_messages,
                "lost_messages": self.lost_messages,
                "first_message_at": self.first_message_at.isoformat() if self.first_message_at else None,
                "last_message_at": self.last_message_at.isoformat() if self.last_message_at else None,
                "oldest_message_age_seconds": self.oldest_message_age_seconds,
                "newest_message_age_seconds": self.newest_message_age_seconds,
            },
            "limits": {
                "max_messages": self.max_messages,
                "max_bytes": self.max_bytes,
                "max_age_seconds": self.max_age_seconds,
                "message_capacity_ratio": self.message_capacity_ratio,
                "byte_capacity_ratio": self.byte_capacity_ratio,
            },
            "configuration": {
                "valid": self.configuration_valid,
                "storage": self.storage,
                "retention": self.retention,
                "discard": self.discard,
                "replicas": self.configured_replicas,
            },
            "cluster": {
                "clustered": self.clustered,
                "leader_present": self.cluster_leader_present,
                "followers": self.follower_count,
                "offline_followers": self.offline_followers,
                "not_current_followers": self.not_current_followers,
                "max_replica_lag_messages": self.max_replica_lag_messages,
            },
        }


@dataclass(frozen=True)
class NatsStreamAlert:
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
class NatsStreamHealthReport:
    status: HealthStatus
    snapshot: NatsStreamTelemetrySnapshot
    alerts: tuple[NatsStreamAlert, ...] = ()

    def to_dict(self) -> dict:
        return {
            "status": self.status,
            "alerts": [alert.to_dict() for alert in self.alerts],
            "telemetry": self.snapshot.to_dict(),
        }


@dataclass(frozen=True)
class NatsStreamAlertPolicy:
    warning_capacity_ratio: float = DEFAULT_WARNING_CAPACITY_RATIO
    critical_capacity_ratio: float = DEFAULT_CRITICAL_CAPACITY_RATIO
    warning_replica_lag_messages: int = DEFAULT_WARNING_REPLICA_LAG_MESSAGES
    critical_replica_lag_messages: int = DEFAULT_CRITICAL_REPLICA_LAG_MESSAGES

    def __post_init__(self) -> None:
        if not 0 < self.warning_capacity_ratio <= self.critical_capacity_ratio <= 1:
            raise ValueError("capacity ratios must be within (0, 1] and ordered")
        if not 1 <= self.warning_replica_lag_messages <= self.critical_replica_lag_messages:
            raise ValueError("replica lag thresholds must be positive and ordered")

    def evaluate(self, snapshot: NatsStreamTelemetrySnapshot) -> NatsStreamHealthReport:
        alerts: list[NatsStreamAlert] = []

        if not snapshot.configuration_valid:
            alerts.append(NatsStreamAlert("nats_stream_configuration_invalid", "critical", "invalid"))
        if snapshot.lost_messages > 0:
            alerts.append(NatsStreamAlert("nats_stream_lost_messages", "critical", snapshot.lost_messages, 0))
        if snapshot.clustered and not snapshot.cluster_leader_present:
            alerts.append(NatsStreamAlert("nats_stream_leader_missing", "critical", "missing"))
        if snapshot.offline_followers > 0:
            alerts.append(NatsStreamAlert("nats_stream_offline_followers", "critical", snapshot.offline_followers, 0))
        elif snapshot.not_current_followers > 0:
            alerts.append(NatsStreamAlert("nats_stream_followers_not_current", "warning", snapshot.not_current_followers, 0))

        lag = snapshot.max_replica_lag_messages
        if lag >= self.critical_replica_lag_messages:
            alerts.append(NatsStreamAlert("nats_stream_replica_lag", "critical", lag, self.critical_replica_lag_messages))
        elif lag >= self.warning_replica_lag_messages:
            alerts.append(NatsStreamAlert("nats_stream_replica_lag", "warning", lag, self.warning_replica_lag_messages))

        for code, ratio in (
            ("nats_stream_message_capacity", snapshot.message_capacity_ratio),
            ("nats_stream_byte_capacity", snapshot.byte_capacity_ratio),
        ):
            if ratio is None:
                continue
            if ratio >= self.critical_capacity_ratio:
                alerts.append(NatsStreamAlert(code, "critical", ratio, self.critical_capacity_ratio))
            elif ratio >= self.warning_capacity_ratio:
                alerts.append(NatsStreamAlert(code, "warning", ratio, self.warning_capacity_ratio))

        if any(alert.severity == "critical" for alert in alerts):
            status: HealthStatus = "critical"
        elif alerts:
            status = "warning"
        else:
            status = "ok"
        return NatsStreamHealthReport(status=status, snapshot=snapshot, alerts=tuple(alerts))


class NatsJetStreamStreamTelemetryStore:
    """Read-only operational snapshot for one existing JetStream stream."""

    def __init__(self, server_url: str, *, client_name: str = "spotwo-wms-nats-stream-telemetry", connect_timeout_seconds: float = 2.0):
        if not isinstance(server_url, str) or not server_url.strip():
            raise ValueError("server_url is required")
        if connect_timeout_seconds <= 0:
            raise ValueError("connect_timeout_seconds must be positive")
        self.server_url = server_url.strip()
        self.client_name = _required_name(client_name, "client_name")
        self.connect_timeout_seconds = float(connect_timeout_seconds)

    def snapshot(self, *, stream_name: str, observed_at: datetime | None = None) -> NatsStreamTelemetrySnapshot:
        stream_name = _required_name(stream_name, "stream_name")
        if observed_at is not None:
            _aware(observed_at, "observed_at")
        with asyncio.Runner() as runner:
            info = runner.run(self._read_stream_info(stream_name))
        return snapshot_from_stream_info(info, stream_name=stream_name, observed_at=observed_at or datetime.now(timezone.utc))

    async def _read_stream_info(self, stream_name: str):
        client = NatsClient()
        try:
            await asyncio.wait_for(
                client.connect(
                    servers=[self.server_url],
                    name=self.client_name,
                    allow_reconnect=False,
                    connect_timeout=self.connect_timeout_seconds,
                    max_reconnect_attempts=1,
                ),
                timeout=self.connect_timeout_seconds,
            )
            return await client.jetstream().stream_info(stream_name)
        finally:
            if not client.is_closed:
                await client.close()


def snapshot_from_stream_info(info: Any, *, stream_name: str, observed_at: datetime) -> NatsStreamTelemetrySnapshot:
    _aware(observed_at, "observed_at")
    config = info.config
    state = info.state
    cluster = getattr(info, "cluster", None)
    followers = list(getattr(cluster, "replicas", None) or ()) if cluster is not None else []
    config_name = getattr(config, "name", None)
    configured_replicas = getattr(config, "num_replicas", None)
    if not isinstance(configured_replicas, int) or configured_replicas < 1:
        configured_replicas = 1

    return NatsStreamTelemetrySnapshot(
        observed_at=observed_at,
        stream_name=stream_name,
        messages=_counter(getattr(state, "messages", 0), "messages"),
        bytes=_counter(getattr(state, "bytes", 0), "bytes"),
        first_sequence=_counter(getattr(state, "first_seq", 0), "first_sequence"),
        last_sequence=_counter(getattr(state, "last_seq", 0), "last_sequence"),
        first_message_at=_optional_datetime(getattr(state, "first_ts", None), "first_message_at"),
        last_message_at=_optional_datetime(getattr(state, "last_ts", None), "last_message_at"),
        consumer_count=_counter(getattr(state, "consumer_count", 0), "consumer_count"),
        deleted_messages=_counter(getattr(state, "num_deleted", 0), "deleted_messages"),
        lost_messages=_lost_message_count(getattr(state, "lost", None)),
        max_messages=_finite_limit(getattr(config, "max_msgs", None), "max_messages"),
        max_bytes=_finite_limit(getattr(config, "max_bytes", None), "max_bytes"),
        max_age_seconds=_positive_float_or_none(getattr(config, "max_age", None), "max_age_seconds"),
        configured_replicas=configured_replicas,
        storage=_enum_value(getattr(config, "storage", None)),
        retention=_enum_value(getattr(config, "retention", None)),
        discard=_enum_value(getattr(config, "discard", None)),
        configuration_valid=(config_name == stream_name),
        clustered=cluster is not None,
        cluster_leader_present=(None if cluster is None else bool(getattr(cluster, "leader", None))),
        follower_count=len(followers),
        offline_followers=sum(1 for peer in followers if bool(getattr(peer, "offline", False))),
        not_current_followers=sum(1 for peer in followers if getattr(peer, "current", True) is False),
        max_replica_lag_messages=max((_counter(getattr(peer, "lag", 0), "replica_lag") for peer in followers), default=0),
    )


def render_prometheus(report: NatsStreamHealthReport) -> str:
    s = report.snapshot
    labels = _labels({"stream": s.stream_name})
    lines = [
        "# HELP spotwo_wms_nats_stream_messages Messages retained in the JetStream stream.",
        "# TYPE spotwo_wms_nats_stream_messages gauge",
        f"spotwo_wms_nats_stream_messages{labels} {s.messages}",
        "# HELP spotwo_wms_nats_stream_bytes Bytes retained in the JetStream stream.",
        "# TYPE spotwo_wms_nats_stream_bytes gauge",
        f"spotwo_wms_nats_stream_bytes{labels} {s.bytes}",
        "# HELP spotwo_wms_nats_stream_consumers Consumers attached to the stream.",
        "# TYPE spotwo_wms_nats_stream_consumers gauge",
        f"spotwo_wms_nats_stream_consumers{labels} {s.consumer_count}",
        "# HELP spotwo_wms_nats_stream_deleted_messages Deleted sequence entries reported by JetStream.",
        "# TYPE spotwo_wms_nats_stream_deleted_messages gauge",
        f"spotwo_wms_nats_stream_deleted_messages{labels} {s.deleted_messages}",
        "# HELP spotwo_wms_nats_stream_lost_messages Lost messages reported by JetStream storage state.",
        "# TYPE spotwo_wms_nats_stream_lost_messages gauge",
        f"spotwo_wms_nats_stream_lost_messages{labels} {s.lost_messages}",
        "# HELP spotwo_wms_nats_stream_replica_lag_messages Maximum follower lag reported by JetStream.",
        "# TYPE spotwo_wms_nats_stream_replica_lag_messages gauge",
        f"spotwo_wms_nats_stream_replica_lag_messages{labels} {s.max_replica_lag_messages}",
        "# HELP spotwo_wms_nats_stream_offline_followers Offline JetStream follower replicas.",
        "# TYPE spotwo_wms_nats_stream_offline_followers gauge",
        f"spotwo_wms_nats_stream_offline_followers{labels} {s.offline_followers}",
        "# HELP spotwo_wms_nats_stream_not_current_followers Followers not current with the leader.",
        "# TYPE spotwo_wms_nats_stream_not_current_followers gauge",
        f"spotwo_wms_nats_stream_not_current_followers{labels} {s.not_current_followers}",
        "# HELP spotwo_wms_nats_stream_health_status Health status: 0 ok, 1 warning, 2 critical.",
        "# TYPE spotwo_wms_nats_stream_health_status gauge",
        f"spotwo_wms_nats_stream_health_status{labels} {_status_number(report.status)}",
        "# HELP spotwo_wms_nats_stream_snapshot_timestamp_seconds Snapshot observation time.",
        "# TYPE spotwo_wms_nats_stream_snapshot_timestamp_seconds gauge",
        f"spotwo_wms_nats_stream_snapshot_timestamp_seconds{labels} {_number(s.observed_at.timestamp())}",
    ]
    for kind, value in (("messages", s.message_capacity_ratio), ("bytes", s.byte_capacity_ratio)):
        if value is not None:
            capacity_labels = _labels({"stream": s.stream_name, "kind": kind})
            lines.extend([
                "# HELP spotwo_wms_nats_stream_capacity_ratio Fraction of configured finite stream capacity in use.",
                "# TYPE spotwo_wms_nats_stream_capacity_ratio gauge",
                f"spotwo_wms_nats_stream_capacity_ratio{capacity_labels} {_number(value)}",
            ])
    for kind, value in (("oldest", s.oldest_message_age_seconds), ("newest", s.newest_message_age_seconds)):
        if value is not None:
            age_labels = _labels({"stream": s.stream_name, "kind": kind})
            lines.extend([
                "# HELP spotwo_wms_nats_stream_message_age_seconds Age of oldest/newest retained stream messages.",
                "# TYPE spotwo_wms_nats_stream_message_age_seconds gauge",
                f"spotwo_wms_nats_stream_message_age_seconds{age_labels} {_number(value)}",
            ])
    if report.alerts:
        lines.extend(["# HELP spotwo_wms_nats_stream_alert Active stream-health alert.", "# TYPE spotwo_wms_nats_stream_alert gauge"])
        for alert in report.alerts:
            alert_labels = _labels({"stream": s.stream_name, "code": alert.code, "severity": alert.severity})
            lines.append(f"spotwo_wms_nats_stream_alert{alert_labels} 1")
    return "\n".join(lines) + "\n"


def _labels(values: dict[str, str]) -> str:
    rendered = ",".join(f'{key}="{_escape_label(value)}"' for key, value in values.items())
    return "{" + rendered + "}"


def _escape_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _number(value: float) -> str:
    return format(value, ".15g")


def _status_number(status: HealthStatus) -> int:
    return {"ok": 0, "warning": 1, "critical": 2}[status]
