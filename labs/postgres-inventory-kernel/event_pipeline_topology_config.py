from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


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


def _positive_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{field} must be a positive integer")
    return value


def _positive_number(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
        raise ValueError(f"{field} must be positive")
    return float(value)


def _subject_matches(filter_subject: str, subject: str) -> bool:
    filter_tokens = filter_subject.split(".")
    subject_tokens = subject.split(".")
    subject_index = 0
    for filter_index, token in enumerate(filter_tokens):
        if token == ">":
            return filter_index == len(filter_tokens) - 1 and subject_index < len(subject_tokens)
        if subject_index >= len(subject_tokens):
            return False
        if token != "*" and token != subject_tokens[subject_index]:
            return False
        subject_index += 1
    return subject_index == len(subject_tokens)


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

    def validate(self, spec: Any) -> None:
        subjects = _sequence_of_names(
            self.stream.get("subjects"), "topology.stream.subjects"
        )
        storage = self.stream.get("storage")
        retention = self.stream.get("retention")
        if storage not in ("file", "memory"):
            raise ValueError("topology.stream.storage must be file or memory")
        if retention not in ("limits", "interest", "workqueue"):
            raise ValueError(
                "topology.stream.retention must be limits, interest, or workqueue"
            )
        _positive_int(self.stream.get("replicas"), "topology.stream.replicas")
        _positive_number(
            self.stream.get("duplicate_window_seconds"),
            "topology.stream.duplicate_window_seconds",
        )

        broad_subject = f"{spec.transport.subject_prefix}.>"
        canary_subject = f"{spec.transport.subject_prefix}.health_check.ping"
        if broad_subject not in subjects:
            raise ValueError(
                "topology.stream.subjects must include the publisher subject prefix wildcard"
            )

        for name, consumer in (
            ("business_consumer", self.business_consumer),
            ("canary_consumer", self.canary_consumer),
        ):
            if consumer.get("delivery_mode") != "pull":
                raise ValueError(f"topology.{name} must remain a pull consumer")
            if consumer.get("ack_policy") != "explicit":
                raise ValueError(f"topology.{name} must use explicit acknowledgement")
            if consumer.get("deliver_policy") not in (
                "all",
                "last",
                "new",
                "by_start_sequence",
                "by_start_time",
                "last_per_subject",
            ):
                raise ValueError(f"topology.{name}.deliver_policy is unsupported")
            _positive_int(
                consumer.get("max_ack_pending"),
                f"topology.{name}.max_ack_pending",
            )
            _positive_int(consumer.get("max_deliver"), f"topology.{name}.max_deliver")

        business_filter = self.business_consumer.get("filter_subject")
        if not isinstance(business_filter, str) or not business_filter.startswith(
            f"{spec.transport.subject_prefix}."
        ):
            raise ValueError(
                "topology.business_consumer.filter_subject must stay within the publisher subject prefix"
            )
        if _subject_matches(business_filter, canary_subject):
            raise ValueError(
                "topology.business_consumer.filter_subject must exclude the synthetic canary subject"
            )

        canary_filter = self.canary_consumer.get("filter_subject")
        if canary_filter != canary_subject:
            raise ValueError(
                "topology.canary_consumer.filter_subject must match the exact synthetic canary subject"
            )

    def expectation(self, spec: Any):
        self.validate(spec)
        from event_pipeline_topology import (
            ConsumerTopologyExpectation,
            EventPipelineTopologyExpectation,
            StreamTopologyExpectation,
        )

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
        return EventPipelineTopologyExpectation(
            stream_name=spec.transport.stream,
            stream=stream,
            business_consumer=business,
            canary_consumer=canary,
        )


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
