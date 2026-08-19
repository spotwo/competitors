from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping

from event_pipeline_readiness import (
    EventPipelineReadinessCollector,
    EventPipelineReadinessReport,
    ReadinessSignal,
    readiness_status,
)
from event_pipeline_topology_migration import TopologyContractInspector


class TopologyAwareEventPipelineReadinessCollector:
    """Add exact or time-bounded JetStream topology contract to readiness."""

    def __init__(self):
        self.base = EventPipelineReadinessCollector()

    def collect(
        self,
        spec: Any,
        *,
        topology: Mapping[str, Any],
        topology_migration: Mapping[str, Any] | None = None,
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
            topology_migration=topology_migration,
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
        if topology_migration is not None:
            public_config["topology_migration"] = dict(topology_migration)
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
        topology_migration: Mapping[str, Any] | None,
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
            report = TopologyContractInspector(nats_url).inspect(
                spec,
                target_topology=topology,
                migration_mapping=topology_migration,
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
            code=None if report.status == "ok" else report.code,
            details=report.to_dict(),
        )
