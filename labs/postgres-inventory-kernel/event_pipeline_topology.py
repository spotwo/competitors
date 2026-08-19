from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Literal, Mapping

import nats
from nats.aio.client import Client as NatsClient
from nats.js.errors import NotFoundError

TopologyStatus = Literal["ok", "critical"]
ResourceKind = Literal["stream", "business_consumer", "canary_consumer"]

_RESOURCE_ORDER: tuple[ResourceKind, ...] = (
    "stream",
    "business_consumer",
    "canary_consumer",
)


def _required_name(value: str, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} is required")
    if value != value.strip():
        raise ValueError(f"{field} cannot contain surrounding whitespace")
    return value


def _positive_int(value: int, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{field} must be a positive integer")
    return value


def _positive(value: float, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
        raise ValueError(f"{field} must be positive")
    return float(value)


def _enum_value(value: Any) -> str | None:
    if value is None:
        return None
    raw = getattr(value, "value", value)
    return str(raw).lower()


def _seconds(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, timedelta):
        return value.total_seconds()
    return float(value)


def _json_value(value: Any) -> Any:
    if isinstance(value, tuple):
        return list(value)
    return value


@dataclass(frozen=True)
class StreamTopologyExpectation:
    subjects: tuple[str, ...]
    storage: str
    retention: str
    replicas: int
    duplicate_window_seconds: float

    def __post_init__(self) -> None:
        if not self.subjects:
            raise ValueError("stream topology requires at least one subject")
        normalized = tuple(_required_name(subject, "stream.subjects") for subject in self.subjects)
        if len(normalized) != len(set(normalized)):
            raise ValueError("stream topology subjects must be unique")
        if self.storage not in ("file", "memory"):
            raise ValueError("stream storage must be file or memory")
        if self.retention not in ("limits", "interest", "workqueue"):
            raise ValueError("stream retention must be limits, interest, or workqueue")
        _positive_int(self.replicas, "stream.replicas")
        _positive(self.duplicate_window_seconds, "stream.duplicate_window_seconds")


@dataclass(frozen=True)
class ConsumerTopologyExpectation:
    durable_name: str
    delivery_mode: str
    deliver_policy: str
    ack_policy: str
    filter_subject: str
    max_ack_pending: int
    max_deliver: int

    def __post_init__(self) -> None:
        _required_name(self.durable_name, "consumer.durable_name")
        if self.delivery_mode not in ("pull", "push"):
            raise ValueError("consumer delivery_mode must be pull or push")
        if self.deliver_policy not in (
            "all",
            "last",
            "new",
            "by_start_sequence",
            "by_start_time",
            "last_per_subject",
        ):
            raise ValueError("consumer deliver_policy is unsupported")
        if self.ack_policy not in ("explicit", "all", "none"):
            raise ValueError("consumer ack_policy is unsupported")
        _required_name(self.filter_subject, "consumer.filter_subject")
        _positive_int(self.max_ack_pending, "consumer.max_ack_pending")
        _positive_int(self.max_deliver, "consumer.max_deliver")


@dataclass(frozen=True)
class EventPipelineTopologyExpectation:
    stream_name: str
    stream: StreamTopologyExpectation
    business_consumer: ConsumerTopologyExpectation
    canary_consumer: ConsumerTopologyExpectation

    def __post_init__(self) -> None:
        _required_name(self.stream_name, "stream_name")
        if self.business_consumer.durable_name == self.canary_consumer.durable_name:
            raise ValueError("business and canary durable names must be distinct")


@dataclass(frozen=True)
class TopologyResourceSnapshot:
    resource: ResourceKind
    name: str
    exists: bool
    fields: Mapping[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "resource": self.resource,
            "name": self.name,
            "exists": self.exists,
            "fields": {key: _json_value(value) for key, value in self.fields.items()},
        }


@dataclass(frozen=True)
class TopologyDrift:
    resource: ResourceKind
    field: str
    code: str
    expected: Any
    actual: Any

    def to_dict(self) -> dict[str, Any]:
        return {
            "resource": self.resource,
            "field": self.field,
            "code": self.code,
            "expected": _json_value(self.expected),
            "actual": _json_value(self.actual),
        }


@dataclass(frozen=True)
class EventPipelineTopologyReport:
    observed_at: datetime
    stream_name: str
    status: TopologyStatus
    resources: tuple[TopologyResourceSnapshot, ...]
    drift: tuple[TopologyDrift, ...]

    def __post_init__(self) -> None:
        if self.observed_at.tzinfo is None or self.observed_at.utcoffset() is None:
            raise ValueError("observed_at must include a timezone")
        _required_name(self.stream_name, "stream_name")
        expected_status: TopologyStatus = "critical" if self.drift else "ok"
        if self.status != expected_status:
            raise ValueError("topology status must match drift evidence")
        if tuple(resource.resource for resource in self.resources) != _RESOURCE_ORDER:
            raise ValueError("topology resources must use canonical resource order")

    def to_dict(self) -> dict[str, Any]:
        return {
            "observed_at": self.observed_at.isoformat(),
            "stream": self.stream_name,
            "status": self.status,
            "collection": {"read_only": True, "atomic": False},
            "drift_count": len(self.drift),
            "resources": {resource.resource: resource.to_dict() for resource in self.resources},
            "drift": [item.to_dict() for item in self.drift],
        }


def _stream_snapshot(info: Any, stream_name: str) -> TopologyResourceSnapshot:
    if info is None:
        return TopologyResourceSnapshot("stream", stream_name, False, {})
    config = info.config
    fields = {
        "subjects": tuple(sorted(config.subjects or ())),
        "storage": _enum_value(config.storage),
        "retention": _enum_value(config.retention),
        "replicas": int(config.num_replicas),
        "duplicate_window_seconds": _seconds(config.duplicate_window),
    }
    return TopologyResourceSnapshot("stream", stream_name, True, fields)


def _consumer_snapshot(
    resource: Literal["business_consumer", "canary_consumer"],
    info: Any,
    durable_name: str,
) -> TopologyResourceSnapshot:
    if info is None:
        return TopologyResourceSnapshot(resource, durable_name, False, {})
    config = info.config
    fields = {
        "delivery_mode": "pull" if config.deliver_subject is None else "push",
        "deliver_policy": _enum_value(config.deliver_policy),
        "ack_policy": _enum_value(config.ack_policy),
        "filter_subject": config.filter_subject,
        "max_ack_pending": int(config.max_ack_pending),
        "max_deliver": int(config.max_deliver),
    }
    return TopologyResourceSnapshot(resource, durable_name, True, fields)


def _compare_resource(
    snapshot: TopologyResourceSnapshot,
    expected: Mapping[str, Any],
) -> list[TopologyDrift]:
    if not snapshot.exists:
        return [
            TopologyDrift(
                resource=snapshot.resource,
                field="exists",
                code=f"{snapshot.resource}_missing",
                expected=True,
                actual=False,
            )
        ]

    drift: list[TopologyDrift] = []
    for field, expected_value in expected.items():
        actual_value = snapshot.fields.get(field)
        equal = actual_value == expected_value
        if field == "duplicate_window_seconds" and actual_value is not None:
            equal = abs(float(actual_value) - float(expected_value)) <= 1e-6
        if equal:
            continue
        drift.append(
            TopologyDrift(
                resource=snapshot.resource,
                field=field,
                code=f"{snapshot.resource}_{field}_mismatch",
                expected=expected_value,
                actual=actual_value,
            )
        )
    return drift


def evaluate_topology(
    expectation: EventPipelineTopologyExpectation,
    *,
    stream_info: Any,
    business_consumer_info: Any,
    canary_consumer_info: Any,
    observed_at: datetime | None = None,
) -> EventPipelineTopologyReport:
    observed_at = observed_at or datetime.now(timezone.utc)
    resources = (
        _stream_snapshot(stream_info, expectation.stream_name),
        _consumer_snapshot(
            "business_consumer",
            business_consumer_info,
            expectation.business_consumer.durable_name,
        ),
        _consumer_snapshot(
            "canary_consumer",
            canary_consumer_info,
            expectation.canary_consumer.durable_name,
        ),
    )
    expected_stream = {
        "subjects": tuple(sorted(expectation.stream.subjects)),
        "storage": expectation.stream.storage,
        "retention": expectation.stream.retention,
        "replicas": expectation.stream.replicas,
        "duplicate_window_seconds": expectation.stream.duplicate_window_seconds,
    }
    expected_business = {
        "delivery_mode": expectation.business_consumer.delivery_mode,
        "deliver_policy": expectation.business_consumer.deliver_policy,
        "ack_policy": expectation.business_consumer.ack_policy,
        "filter_subject": expectation.business_consumer.filter_subject,
        "max_ack_pending": expectation.business_consumer.max_ack_pending,
        "max_deliver": expectation.business_consumer.max_deliver,
    }
    expected_canary = {
        "delivery_mode": expectation.canary_consumer.delivery_mode,
        "deliver_policy": expectation.canary_consumer.deliver_policy,
        "ack_policy": expectation.canary_consumer.ack_policy,
        "filter_subject": expectation.canary_consumer.filter_subject,
        "max_ack_pending": expectation.canary_consumer.max_ack_pending,
        "max_deliver": expectation.canary_consumer.max_deliver,
    }
    drift = tuple(
        _compare_resource(resources[0], expected_stream)
        + _compare_resource(resources[1], expected_business)
        + _compare_resource(resources[2], expected_canary)
    )
    return EventPipelineTopologyReport(
        observed_at=observed_at,
        stream_name=expectation.stream_name,
        status="critical" if drift else "ok",
        resources=resources,
        drift=drift,
    )


class NatsJetStreamTopologyInspector:
    """Read only JetStream stream and consumer configuration inspection."""

    def __init__(
        self,
        server_url: str,
        *,
        client_name: str = "spotwo-wms-topology-inspector",
        connect_timeout_seconds: float = 2.0,
    ):
        self.server_url = _required_name(server_url, "server_url")
        self.client_name = _required_name(client_name, "client_name")
        self.connect_timeout_seconds = _positive(
            connect_timeout_seconds, "connect_timeout_seconds"
        )

    def inspect(
        self,
        expectation: EventPipelineTopologyExpectation,
        *,
        observed_at: datetime | None = None,
    ) -> EventPipelineTopologyReport:
        runner = asyncio.Runner()
        try:
            stream_info, business_info, canary_info = runner.run(
                self._read(expectation)
            )
        finally:
            runner.close()
        return evaluate_topology(
            expectation,
            stream_info=stream_info,
            business_consumer_info=business_info,
            canary_consumer_info=canary_info,
            observed_at=observed_at,
        )

    async def _read(
        self,
        expectation: EventPipelineTopologyExpectation,
    ) -> tuple[Any, Any, Any]:
        client: NatsClient | None = None
        try:
            client = await asyncio.wait_for(
                nats.connect(
                    servers=[self.server_url],
                    name=self.client_name,
                    allow_reconnect=False,
                    connect_timeout=self.connect_timeout_seconds,
                    reconnect_time_wait=0,
                    max_reconnect_attempts=1,
                ),
                timeout=self.connect_timeout_seconds,
            )
            jetstream = client.jetstream()
            try:
                stream_info = await jetstream.stream_info(expectation.stream_name)
            except NotFoundError:
                return None, None, None

            async def consumer_info(durable_name: str) -> Any:
                try:
                    return await jetstream.consumer_info(
                        expectation.stream_name,
                        durable_name,
                    )
                except NotFoundError:
                    return None

            business_info = await consumer_info(
                expectation.business_consumer.durable_name
            )
            canary_info = await consumer_info(expectation.canary_consumer.durable_name)
            return stream_info, business_info, canary_info
        finally:
            if client is not None and not client.is_closed:
                await client.close()


def _escape_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def render_prometheus(report: EventPipelineTopologyReport, *, pipeline_id: str) -> str:
    pipeline = _escape_label(_required_name(pipeline_id, "pipeline_id"))
    lines = [
        "# HELP spotwo_wms_event_pipeline_topology_match Whether deployed JetStream topology exactly matches the registry contract.",
        "# TYPE spotwo_wms_event_pipeline_topology_match gauge",
        f'spotwo_wms_event_pipeline_topology_match{{pipeline="{pipeline}"}} {1 if report.status == "ok" else 0}',
        "# HELP spotwo_wms_event_pipeline_topology_resource_exists Whether a required bounded JetStream resource exists.",
        "# TYPE spotwo_wms_event_pipeline_topology_resource_exists gauge",
    ]
    for resource in report.resources:
        resource_name = _escape_label(resource.resource)
        lines.append(
            f'spotwo_wms_event_pipeline_topology_resource_exists{{pipeline="{pipeline}",resource="{resource_name}"}} {1 if resource.exists else 0}'
        )

    lines.extend(
        [
            "# HELP spotwo_wms_event_pipeline_topology_drift Active bounded topology drift by resource and field.",
            "# TYPE spotwo_wms_event_pipeline_topology_drift gauge",
        ]
    )
    for item in report.drift:
        resource = _escape_label(item.resource)
        field = _escape_label(item.field)
        code = _escape_label(item.code)
        lines.append(
            f'spotwo_wms_event_pipeline_topology_drift{{pipeline="{pipeline}",resource="{resource}",field="{field}",code="{code}"}} 1'
        )
    lines.extend(
        [
            "# HELP spotwo_wms_event_pipeline_topology_snapshot_timestamp_seconds Unix timestamp of the topology snapshot.",
            "# TYPE spotwo_wms_event_pipeline_topology_snapshot_timestamp_seconds gauge",
            f'spotwo_wms_event_pipeline_topology_snapshot_timestamp_seconds{{pipeline="{pipeline}"}} {report.observed_at.timestamp():.6f}',
        ]
    )
    return "\n".join(lines) + "\n"
