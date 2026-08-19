from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

import psycopg
import pytest

import conftest as lab
from event_pipeline_canary import (
    CanaryAckState,
    EventPipelineCanaryAlert,
    EventPipelineCanaryDbSnapshot,
    EventPipelineCanaryHealthReport,
    EventPipelineCanaryRunSnapshot,
)
from event_pipeline_canary_sli import (
    EventPipelineCanarySliPolicy,
    EventPipelineCanarySliSnapshot,
    EventPipelineCanarySliWindow,
    PostgresEventPipelineCanarySliStore,
    render_prometheus,
)

STREAM = "WMS_EVENTS"
DURABLE = "EVENT_PIPELINE_CANARY"
CONSUMER = "event_pipeline_canary"
FILTER = "spotwo.wms.events.health_check.ping"


def _start_tracked(store: PostgresEventPipelineCanarySliStore):
    return store.start_tracked(
        tenant_id=lab.TENANT,
        stream_name=STREAM,
        durable_name=DURABLE,
        consumer_name=CONSUMER,
        timeout_seconds=30,
    )


def _success_report(*, canary_id, event_id, started_at):
    published_at = started_at + timedelta(seconds=1)
    broker_at = started_at + timedelta(seconds=1.2)
    inbox_at = started_at + timedelta(seconds=1.5)
    projected_at = started_at + timedelta(seconds=2)
    observed_at = started_at + timedelta(seconds=2.1)
    database = EventPipelineCanaryDbSnapshot(
        canary_id=canary_id,
        tenant_id=lab.TENANT,
        event_id=event_id,
        started_at=started_at,
        published_at=published_at,
        projected_at=projected_at,
        projected_consumer_name=CONSUMER,
        receipt_consumer_name=CONSUMER,
        inbox_first_seen_at=inbox_at,
        transport_message_id=f"{STREAM}:10",
        stream_name=STREAM,
        durable_name=DURABLE,
        stream_sequence=10,
        consumer_sequence=5,
        delivery_count=1,
        broker_timestamp=broker_at,
    )
    snapshot = EventPipelineCanaryRunSnapshot(
        observed_at=observed_at,
        expected_stream_name=STREAM,
        expected_durable_name=DURABLE,
        expected_consumer_name=CONSUMER,
        expected_filter_subject=FILTER,
        database=database,
        ack=CanaryAckState(
            observed_at=observed_at,
            available=True,
            configuration_valid=True,
            ack_floor_stream_sequence=10,
            filter_subject=FILTER,
            last_ack_at=observed_at,
        ),
    )
    return EventPipelineCanaryHealthReport(status="ok", snapshot=snapshot, timed_out=False)


def _timeout_report(*, canary_id, event_id, started_at):
    observed_at = started_at + timedelta(seconds=30)
    database = EventPipelineCanaryDbSnapshot(
        canary_id=canary_id,
        tenant_id=lab.TENANT,
        event_id=event_id,
        started_at=started_at,
        published_at=started_at + timedelta(seconds=1),
        projected_at=None,
        projected_consumer_name=None,
        receipt_consumer_name=None,
        inbox_first_seen_at=None,
        transport_message_id=None,
        stream_name=None,
        durable_name=None,
        stream_sequence=None,
        consumer_sequence=None,
        delivery_count=None,
        broker_timestamp=None,
    )
    snapshot = EventPipelineCanaryRunSnapshot(
        observed_at=observed_at,
        expected_stream_name=STREAM,
        expected_durable_name=DURABLE,
        expected_consumer_name=CONSUMER,
        expected_filter_subject=FILTER,
        database=database,
        ack=CanaryAckState(
            observed_at=observed_at,
            available=True,
            configuration_valid=True,
            ack_floor_stream_sequence=0,
            filter_subject=FILTER,
            last_ack_at=None,
        ),
    )
    return EventPipelineCanaryHealthReport(
        status="critical",
        snapshot=snapshot,
        timed_out=True,
        alerts=(
            EventPipelineCanaryAlert(
                code="canary_delivery_timeout",
                severity="critical",
                observed_value=30,
            ),
        ),
    )


def test_tracked_start_is_atomic_with_scope_and_deadline():
    store = PostgresEventPipelineCanarySliStore(lab.DATABASE_URL)
    identity = _start_tracked(store)

    with lab.connect() as conn:
        run = conn.execute(
            "SELECT tenant_id, event_id FROM kernel_lab.event_pipeline_canary_runs WHERE canary_id = %s",
            (identity.canary_id,),
        ).fetchone()
        outcome = conn.execute(
            """
            SELECT tenant_id, stream_name, durable_name, consumer_name,
                   outcome, deadline_at > started_at
            FROM kernel_lab.event_pipeline_canary_outcomes
            WHERE canary_id = %s
            """,
            (identity.canary_id,),
        ).fetchone()

    assert run == (lab.TENANT, identity.event_id)
    assert outcome == (lab.TENANT, STREAM, DURABLE, CONSUMER, "pending", True)


def test_outcome_recording_is_idempotent_and_fails_closed_on_evidence_collision():
    store = PostgresEventPipelineCanarySliStore(lab.DATABASE_URL)
    identity = _start_tracked(store)
    report = _success_report(
        canary_id=identity.canary_id,
        event_id=identity.event_id,
        started_at=identity.recorded_at,
    )

    first = store.record(report)
    second = store.record(report)
    assert first.created is True
    assert second.created is False
    assert second.finalized_at == first.finalized_at

    conflicting = _timeout_report(
        canary_id=identity.canary_id,
        event_id=identity.event_id,
        started_at=identity.recorded_at,
    )
    with pytest.raises(psycopg.errors.UniqueViolation, match="different evidence"):
        store.record(conflicting)


def test_timeout_outcome_records_bounded_failure_code():
    store = PostgresEventPipelineCanarySliStore(lab.DATABASE_URL)
    identity = _start_tracked(store)
    report = _timeout_report(
        canary_id=identity.canary_id,
        event_id=identity.event_id,
        started_at=identity.recorded_at,
    )
    receipt = store.record(report)
    assert receipt.created is True

    with lab.connect() as conn:
        row = conn.execute(
            """
            SELECT outcome, terminal_code, timed_out, ack_confirmed
            FROM kernel_lab.event_pipeline_canary_outcomes
            WHERE canary_id = %s
            """,
            (identity.canary_id,),
        ).fetchone()
    assert row == ("failed", "canary_delivery_timeout", True, False)


def _record_direct_outcome(
    *,
    succeeded: bool,
    end_to_end_seconds: float | None,
    terminal_code: str | None = None,
):
    store = PostgresEventPipelineCanarySliStore(lab.DATABASE_URL)
    identity = _start_tracked(store)
    with lab.connect() as conn:
        conn.execute(
            """
            SELECT * FROM kernel_lab.record_event_pipeline_canary_outcome(
              %s, %s, %s, %s, %s, %s,
              %s, %s, %s, %s, %s
            )
            """,
            (
                identity.canary_id,
                "succeeded" if succeeded else "failed",
                "ok" if succeeded else "critical",
                terminal_code,
                False if succeeded else True,
                succeeded,
                False,
                1.0 if succeeded else None,
                0.2 if succeeded else None,
                0.3 if succeeded else None,
                end_to_end_seconds,
            ),
        )
        conn.commit()
    return identity


def test_sli_windows_compute_ratios_burn_rates_failure_codes_and_quantiles():
    for latency in range(1, 9):
        _record_direct_outcome(succeeded=True, end_to_end_seconds=float(latency))
    _record_direct_outcome(
        succeeded=False,
        end_to_end_seconds=None,
        terminal_code="canary_delivery_timeout",
    )
    _record_direct_outcome(
        succeeded=False,
        end_to_end_seconds=None,
        terminal_code="canary_ack_timeout",
    )

    snapshot = PostgresEventPipelineCanarySliStore(lab.DATABASE_URL).snapshot(
        stream_name=STREAM,
        durable_name=DURABLE,
        consumer_name=CONSUMER,
        availability_target=0.999,
        latency_target_ratio=0.99,
        latency_target_seconds=5.0,
    )
    five_minutes = snapshot.window(300)

    assert five_minutes.total_runs == 10
    assert five_minutes.successful_runs == 8
    assert five_minutes.failed_runs == 2
    assert five_minutes.availability_success_ratio == pytest.approx(0.8)
    assert five_minutes.availability_burn_rate == pytest.approx(200.0)
    assert five_minutes.latency_good_runs == 5
    assert five_minutes.latency_success_ratio == pytest.approx(0.625)
    assert five_minutes.latency_burn_rate == pytest.approx(37.5)
    assert five_minutes.failure_codes == {
        "canary_ack_timeout": 1,
        "canary_delivery_timeout": 1,
    }
    quantiles = five_minutes.latency_quantiles["end_to_end_projection"]
    assert quantiles["p50"] == pytest.approx(4.5)
    assert quantiles["p95"] == pytest.approx(7.65)
    assert quantiles["p99"] == pytest.approx(7.93)


def test_expired_pending_probe_counts_as_abandoned_failure():
    store = PostgresEventPipelineCanarySliStore(lab.DATABASE_URL)
    identity = _start_tracked(store)
    with lab.connect() as conn:
        conn.execute(
            """
            UPDATE kernel_lab.event_pipeline_canary_outcomes
            SET started_at = clock_timestamp() - interval '2 minutes',
                deadline_at = clock_timestamp() - interval '1 minute'
            WHERE canary_id = %s
            """,
            (identity.canary_id,),
        )
        conn.commit()

    window = store.snapshot(
        stream_name=STREAM,
        durable_name=DURABLE,
        consumer_name=CONSUMER,
    ).window(300)
    assert window.total_runs == 1
    assert window.failed_runs == 1
    assert window.consecutive_failures == 1
    assert window.failure_codes == {"canary_probe_abandoned": 1}


def _window(
    seconds: int,
    *,
    observed_at: datetime,
    last_run_at: datetime | None,
    availability_burn: float | None,
    latency_burn: float | None,
    consecutive_failures: int,
) -> EventPipelineCanarySliWindow:
    return EventPipelineCanarySliWindow(
        observed_at=observed_at,
        window_seconds=seconds,
        total_runs=10,
        successful_runs=9,
        failed_runs=1,
        availability_success_ratio=0.9,
        availability_burn_rate=availability_burn,
        latency_good_runs=9,
        latency_success_ratio=1.0,
        latency_burn_rate=latency_burn,
        last_run_at=last_run_at,
        last_success_at=last_run_at,
        consecutive_failures=consecutive_failures,
        failure_codes={"canary_ack_timeout": 1},
        latency_quantiles={
            stage: {"p50": 1.0, "p95": 2.0, "p99": 3.0}
            for stage in (
                "outbox_publish_confirm",
                "broker_to_inbox",
                "inbox_to_projection",
                "end_to_end_projection",
            )
        },
    )


def test_multiwindow_policy_detects_fast_burn_and_stale_series():
    observed = datetime.now(timezone.utc)
    snapshot = EventPipelineCanarySliSnapshot(
        stream_name=STREAM,
        durable_name=DURABLE,
        consumer_name=CONSUMER,
        availability_target=0.999,
        latency_target_ratio=0.99,
        latency_target_seconds=5.0,
        windows=(
            _window(300, observed_at=observed, last_run_at=observed - timedelta(seconds=10), availability_burn=20.0, latency_burn=0.0, consecutive_failures=1),
            _window(3600, observed_at=observed, last_run_at=observed - timedelta(seconds=10), availability_burn=15.0, latency_burn=0.0, consecutive_failures=1),
            _window(86400, observed_at=observed, last_run_at=observed - timedelta(seconds=10), availability_burn=2.0, latency_burn=0.0, consecutive_failures=1),
        ),
    )
    report = EventPipelineCanarySliPolicy().evaluate(snapshot)
    assert report.status == "critical"
    assert [alert.code for alert in report.alerts] == ["canary_sli_availability_fast_burn"]

    stale_snapshot = EventPipelineCanarySliSnapshot(
        stream_name=STREAM,
        durable_name=DURABLE,
        consumer_name=CONSUMER,
        availability_target=0.999,
        latency_target_ratio=0.99,
        latency_target_seconds=5.0,
        windows=tuple(
            _window(seconds, observed_at=observed, last_run_at=observed - timedelta(seconds=181), availability_burn=0.0, latency_burn=0.0, consecutive_failures=0)
            for seconds in (300, 3600, 86400)
        ),
    )
    stale_report = EventPipelineCanarySliPolicy().evaluate(stale_snapshot)
    assert stale_report.status == "critical"
    assert stale_report.alerts[0].code == "canary_sli_stale"


def test_prometheus_uses_only_bounded_scope_window_stage_quantile_and_codes():
    observed = datetime.now(timezone.utc)
    snapshot = EventPipelineCanarySliSnapshot(
        stream_name=STREAM,
        durable_name=DURABLE,
        consumer_name=CONSUMER,
        availability_target=0.999,
        latency_target_ratio=0.99,
        latency_target_seconds=5.0,
        windows=tuple(
            _window(seconds, observed_at=observed, last_run_at=observed, availability_burn=0.0, latency_burn=0.0, consecutive_failures=0)
            for seconds in (300, 3600, 86400)
        ),
    )
    prometheus = render_prometheus(EventPipelineCanarySliPolicy().evaluate(snapshot))

    assert 'window="5m"' in prometheus
    assert 'stage="end_to_end_projection"' in prometheus
    assert 'quantile="p95"' in prometheus
    assert 'code="canary_ack_timeout"' in prometheus
    for forbidden in (
        "canary_id",
        "event_id",
        "transport_message_id",
        "stream_sequence",
        str(uuid4()),
    ):
        assert forbidden not in prometheus
