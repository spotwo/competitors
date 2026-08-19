from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping

from event_pipeline_readiness import (
    EventPipelineReadinessCollector,
    EventPipelineReadinessReport,
    ReadinessSignal,
    readiness_status,
)
from event_pipeline_topology import NatsJetStreamTopologyInspector
from event_pipeline_topology_config import DeploymentTopologyConfig


class TopologyAwareEventPipelineReadinessCollector:
    """Add exact JetStream deployment topology to the existing readiness gate."""

    def __init__(self):
        self.base = EventPipelineReadinessCollector()

    def collect(
        self,
        spec: Any,
        *,
        topology: Mapping[str, Any],
        database_url: str | None,
        nats_url: str | None,
        watchdog_state: Mapping[str, Any] | None,
        observed_at: datetime | None = None,
    ) -> EventPipelineReadinessReport:
        observed_at = observed_at or datetime.now(timezone.utc)
        base_report = self.base.collect(
            spec,
            database_url=database_url,
            nats_url=nats_url,
            watchdog_state=watchdog_state,
            observed_at=observed_at,
        )
        if not spec.enabled:
            return base_report

        topology_signal = self._topology_signal(
            spec,
            topology=topology,
            nats_url=nats_url,
            observed_at=observed_at,
        )
        signals = (
            base_report.signals[0],
            topology_signal,
            *base_report.signals[1:],
        )
        public_config = dict(base_report.config)
        public_config["topology"] = dict(topology)
        return EventPipelineReadinessReport(
            observed_at=observed_at,
            pipeline_id=spec.pipeline_id,
            status=readiness_status(signals),
            signals=signals,
            config=public_config,
        )

    @staticmethod
    def _topology_signal(
        spec: Any,
        *,
        topology: Mapping[str, Any],
        nats_url: str | None,
        observed_at: datetime,
    ) -> ReadinessSignal:
        if not nats_url:
            return ReadinessSignal(
                name="topology",
                status="critical",
                available=False,
                code="topology_nats_url_missing",
                details={},
            )
        try:
            expectation = DeploymentTopologyConfig.from_mapping(topology).expectation(spec)
            report = NatsJetStreamTopologyInspector(nats_url).inspect(
                expectation,
                observed_at=observed_at,
            )
        except Exception as exc:
            return ReadinessSignal(
                name="topology",
                status="critical",
                available=False,
                code="topology_unavailable",
                details={"error_class": type(exc).__name__},
            )
        return ReadinessSignal(
            name="topology",
            status=report.status,
            available=True,
            code=None if report.status == "ok" else "topology_drift",
            details=report.to_dict(),
        )
