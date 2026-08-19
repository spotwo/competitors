from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, Mapping

from event_pipeline_canary import EventPipelineCanaryHealthReport

FaultScenario = Literal[
    "publisher_stopped",
    "nats_unavailable",
    "consumer_stopped",
    "consumer_misconfigured",
    "inbox_handler_failure",
    "ack_confirmation_failure",
    "postgres_unavailable",
]
FaultVisibility = Literal["canary_sli", "control_plane"]


@dataclass(frozen=True)
class EventPipelineFaultExpectation:
    scenario: FaultScenario
    visibility: FaultVisibility
    expected_terminal_code: str | None
    expected_secondary_code: str | None = None

    def __post_init__(self) -> None:
        if self.visibility == "canary_sli" and self.expected_terminal_code is None:
            raise ValueError("canary-visible fault requires a terminal code")
        if self.visibility == "control_plane" and self.expected_terminal_code is not None:
            raise ValueError("control-plane fault cannot invent a canary terminal code")


FAULT_EXPECTATIONS: Mapping[FaultScenario, EventPipelineFaultExpectation] = {
    "publisher_stopped": EventPipelineFaultExpectation(
        scenario="publisher_stopped",
        visibility="canary_sli",
        expected_terminal_code="canary_publish_timeout",
    ),
    "nats_unavailable": EventPipelineFaultExpectation(
        scenario="nats_unavailable",
        visibility="canary_sli",
        expected_terminal_code="canary_publish_timeout",
        expected_secondary_code="outbox_publish_failed",
    ),
    "consumer_stopped": EventPipelineFaultExpectation(
        scenario="consumer_stopped",
        visibility="canary_sli",
        expected_terminal_code="canary_delivery_timeout",
    ),
    "consumer_misconfigured": EventPipelineFaultExpectation(
        scenario="consumer_misconfigured",
        visibility="canary_sli",
        expected_terminal_code="canary_consumer_configuration_invalid",
    ),
    "inbox_handler_failure": EventPipelineFaultExpectation(
        scenario="inbox_handler_failure",
        visibility="canary_sli",
        expected_terminal_code="canary_delivery_timeout",
        expected_secondary_code="handler_failure",
    ),
    "ack_confirmation_failure": EventPipelineFaultExpectation(
        scenario="ack_confirmation_failure",
        visibility="canary_sli",
        expected_terminal_code="canary_ack_timeout",
    ),
    "postgres_unavailable": EventPipelineFaultExpectation(
        scenario="postgres_unavailable",
        visibility="control_plane",
        expected_terminal_code=None,
        expected_secondary_code="postgres_probe_store_unavailable",
    ),
}


@dataclass(frozen=True)
class EventPipelineFaultDrillResult:
    scenario: FaultScenario
    visibility: FaultVisibility
    expected_terminal_code: str | None
    observed_terminal_code: str | None
    secondary_code: str | None
    fault_detected: bool
    recovery_canary_healthy: bool
    historical_failure_retained: bool

    @property
    def passed(self) -> bool:
        return bool(
            self.fault_detected
            and self.recovery_canary_healthy
            and (
                self.historical_failure_retained
                if self.visibility == "canary_sli"
                else True
            )
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "scenario": self.scenario,
            "visibility": self.visibility,
            "expected_terminal_code": self.expected_terminal_code,
            "observed_terminal_code": self.observed_terminal_code,
            "secondary_code": self.secondary_code,
            "fault_detected": self.fault_detected,
            "recovery_canary_healthy": self.recovery_canary_healthy,
            "historical_failure_retained": self.historical_failure_retained,
            "passed": self.passed,
        }


def terminal_code(report: EventPipelineCanaryHealthReport) -> str | None:
    """Return the bounded terminal alert code emitted by a failed canary run."""
    if report.snapshot.complete:
        return None
    timeout_codes = (
        "canary_publish_timeout",
        "canary_delivery_timeout",
        "canary_projection_timeout",
        "canary_ack_timeout",
        "canary_incomplete_timeout",
    )
    identity_codes = (
        "canary_consumer_configuration_invalid",
        "canary_stream_identity_mismatch",
        "canary_durable_identity_mismatch",
        "canary_consumer_identity_mismatch",
        "canary_ack_observer_unavailable",
    )
    observed = {alert.code for alert in report.alerts}
    for code in timeout_codes + identity_codes:
        if code in observed:
            return code
    return "canary_incomplete"


def verify_canary_fault(
    scenario: FaultScenario,
    report: EventPipelineCanaryHealthReport,
    *,
    secondary_code: str | None = None,
) -> EventPipelineFaultDrillResult:
    expectation = FAULT_EXPECTATIONS[scenario]
    if expectation.visibility != "canary_sli":
        raise ValueError("control-plane fault must be verified without inventing a canary result")
    observed = terminal_code(report)
    secondary_matches = (
        expectation.expected_secondary_code is None
        or secondary_code == expectation.expected_secondary_code
    )
    detected = bool(
        not report.snapshot.complete
        and report.status == "critical"
        and observed == expectation.expected_terminal_code
        and secondary_matches
    )
    return EventPipelineFaultDrillResult(
        scenario=scenario,
        visibility=expectation.visibility,
        expected_terminal_code=expectation.expected_terminal_code,
        observed_terminal_code=observed,
        secondary_code=secondary_code,
        fault_detected=detected,
        recovery_canary_healthy=False,
        historical_failure_retained=False,
    )


def verify_control_plane_fault(
    scenario: FaultScenario,
    *,
    secondary_code: str,
) -> EventPipelineFaultDrillResult:
    expectation = FAULT_EXPECTATIONS[scenario]
    if expectation.visibility != "control_plane":
        raise ValueError("canary-visible fault requires a canary health report")
    return EventPipelineFaultDrillResult(
        scenario=scenario,
        visibility=expectation.visibility,
        expected_terminal_code=None,
        observed_terminal_code=None,
        secondary_code=secondary_code,
        fault_detected=secondary_code == expectation.expected_secondary_code,
        recovery_canary_healthy=False,
        historical_failure_retained=False,
    )


def with_recovery(
    result: EventPipelineFaultDrillResult,
    *,
    recovery_report: EventPipelineCanaryHealthReport,
    historical_failure_retained: bool,
) -> EventPipelineFaultDrillResult:
    recovery_healthy = bool(
        recovery_report.snapshot.complete
        and recovery_report.status == "ok"
        and not recovery_report.alerts
    )
    return EventPipelineFaultDrillResult(
        scenario=result.scenario,
        visibility=result.visibility,
        expected_terminal_code=result.expected_terminal_code,
        observed_terminal_code=result.observed_terminal_code,
        secondary_code=result.secondary_code,
        fault_detected=result.fault_detected,
        recovery_canary_healthy=recovery_healthy,
        historical_failure_retained=historical_failure_retained,
    )
