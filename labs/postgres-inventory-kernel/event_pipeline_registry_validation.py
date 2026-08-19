from __future__ import annotations

from event_pipeline_readiness import EventPipelineDeploymentRegistry


def validate_global_consumer_identities(
    registry: EventPipelineDeploymentRegistry,
) -> None:
    """Reject delivery or Inbox identities reused by separate pipelines."""
    durable_names: list[str] = []
    inbox_names: list[str] = []
    for pipeline in registry.pipelines:
        durable_names.extend((pipeline.consumer.durable, pipeline.canary.durable))
        inbox_names.extend(
            (pipeline.consumer.inbox_consumer_name, pipeline.canary.consumer_name)
        )

    if len(durable_names) != len(set(durable_names)):
        raise ValueError("JetStream durable identities must be unique across pipelines")
    if len(inbox_names) != len(set(inbox_names)):
        raise ValueError("Inbox consumer identities must be unique across pipelines")
