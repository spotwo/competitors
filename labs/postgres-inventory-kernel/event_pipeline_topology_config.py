from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from event_pipeline_topology import (
    ConsumerTopologyExpectation,
    EventPipelineTopologyExpectation,
    StreamTopologyExpectation,
)


def _mapping(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field} must be an object")
    return value


def _sequence_of_names(value: Any, field: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise ValueError(f"{field} must be a non-empty array")
    result: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item.strip() or item != item.strip():
            raise ValueError(f"{field} entries must be normalized strings")
        result.append(item)
    return tuple(result)


@dataclass(frozen=True)
class DeploymentTopologyConfig:
    stream: Mapping[str, Any]
    business_consumer: Mapping[str, Any]
    canary_consumer: Mapping[str, Any]

    @classmethod
    def from_mapping(cls, value: Any) -> "DeploymentTopologyConfig":
        item = _mapping(value, "topology")
        return cls(
            stream=_mapping(item.get("stream"), "topology.stream"),
            business_consumer=_mapping(
                item.get("business_consumer"), "topology.business_consumer"
            ),
            canary_consumer=_mapping(
                item.get("canary_consumer"), "topology.canary_consumer"
            ),
        )

    def expectation(self, spec: Any) -> EventPipelineTopologyExpectation:
        stream = StreamTopologyExpectation(
            subjects=_sequence_of_names(
                self.stream.get("subjects"), "topology.stream.subjects"
            ),
            storage=self.stream.get("storage"),
            retention=self.stream.get("retention"),
            replicas=self.stream.get("replicas"),
            duplicate_window_seconds=self.stream.get("duplicate_window_seconds"),
        )
        business = ConsumerTopologyExpectation(
            durable_name=spec.consumer.durable,
            delivery_mode=self.business_consumer.get("delivery_mode"),
            deliver_policy=self.business_consumer.get("deliver_policy"),
            ack_policy=self.business_consumer.get("ack_policy"),
            filter_subject=self.business_consumer.get("filter_subject"),
            max_ack_pending=self.business_consumer.get("max_ack_pending"),
            max_deliver=self.business_consumer.get("max_deliver"),
        )
        canary = ConsumerTopologyExpectation(
            durable_name=spec.canary.durable,
            delivery_mode=self.canary_consumer.get("delivery_mode"),
            deliver_policy=self.canary_consumer.get("deliver_policy"),
            ack_policy=self.canary_consumer.get("ack_policy"),
            filter_subject=self.canary_consumer.get("filter_subject"),
            max_ack_pending=self.canary_consumer.get("max_ack_pending"),
            max_deliver=self.canary_consumer.get("max_deliver"),
        )
        expectation = EventPipelineTopologyExpectation(
            stream_name=spec.transport.stream,
            stream=stream,
            business_consumer=business,
            canary_consumer=canary,
        )
        self._validate_cross_fields(spec, expectation)
        return expectation

    @staticmethod
    def _validate_cross_fields(
        spec: Any,
        expectation: EventPipelineTopologyExpectation,
    ) -> None:
        broad_subject = f"{spec.transport.subject_prefix}.>"
        canary_subject = f"{spec.transport.subject_prefix}.health_check.ping"
        if broad_subject not in expectation.stream.subjects:
            raise ValueError(
                "topology.stream.subjects must include the publisher subject prefix wildcard"
            )
        if expectation.business_consumer.filter_subject != broad_subject:
            raise ValueError(
                "topology.business_consumer.filter_subject must match the publisher subject wildcard"
            )
        if expectation.canary_consumer.filter_subject != canary_subject:
            raise ValueError(
                "topology.canary_consumer.filter_subject must match the exact synthetic canary subject"
            )
        for name, consumer in (
            ("business_consumer", expectation.business_consumer),
            ("canary_consumer", expectation.canary_consumer),
        ):
            if consumer.delivery_mode != "pull":
                raise ValueError(f"topology.{name} must remain a pull consumer")
            if consumer.ack_policy != "explicit":
                raise ValueError(f"topology.{name} must use explicit acknowledgement")


def pipeline_topology_mapping(
    registry_data: Mapping[str, Any],
    pipeline_id: str,
) -> Mapping[str, Any]:
    pipelines = registry_data.get("pipelines")
    if not isinstance(pipelines, list):
        raise ValueError("pipelines must be an array")
    for item in pipelines:
        if isinstance(item, Mapping) and item.get("id") == pipeline_id:
            return _mapping(item.get("topology"), f"pipeline {pipeline_id} topology")
    raise KeyError(pipeline_id)
