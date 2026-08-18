from __future__ import annotations

from datetime import datetime, timedelta, timezone

import psycopg
import pytest

import conftest as lab
from outbox_telemetry import (
    OutboxAlertPolicy,
    OutboxTelemetrySnapshot,
    PostgresOutboxTelemetryStore,
    render_prometheus,
)


def enqueue_event(conn: psycopg.Connection, *, key: str, ordinal: int):
    return conn.execute(
        """
        SELECT kernel_lab.enqueue_domain_event(
          %s,
          %s,
          'inventory.telemetry.test',
          %s,
          'InventoryPosition',
          %s,
          %s,
          jsonb_build_object('ordinal', %s)
        )
        """,
        (
            lab.TENANT,
            key,
            f"inventory-position/telemetry-{ordinal}",
            f"telemetry-{ordinal}",
            ordinal,
            ordinal,
        ),
    ).fetchone()[0]


def test_postgres_snapshot_classifies_backlog_and_attempt_buckets():
    observed_at = datetime(2026, 8, 18, 18, 0, tzinfo=timezone.utc)

    with lab.connect() as conn:
        ready_id = enqueue_event(conn, key="telemetry:ready", ordinal=1)
        delayed_id = enqueue_event(conn, key="telemetry:delayed", ordinal=2)
        leased_id = enqueue_event(conn, key="telemetry:leased", ordinal=3)
        quarantined_id = enqueue_event(conn, key="telemetry:quarantined", ordinal=4)
        published_id = enqueue_event(conn, key="telemetry:published", ordinal=5)

        conn.execute(
            """
            UPDATE kernel_lab.domain_event_outbox
            SET recorded_at = %s - interval '120 seconds',
                available_at = %s - interval '1 second',
                attempt_count = 0
            WHERE event_id = %s
            """,
            (observed_at, observed_at, ready_id),
        )
        conn.execute(
            """
            UPDATE kernel_lab.domain_event_outbox
            SET recorded_at = %s - interval '90 seconds',
                available_at = %s + interval '30 seconds',
                attempt_count = 1,
                last_error = 'broker unavailable',
                last_failed_at = %s - interval '1 second'
            WHERE event_id = %s
            """,
            (observed_at, observed_at, observed_at, delayed_id),
        )
        conn.execute(
            """
            UPDATE kernel_lab.domain_event_outbox
            SET recorded_at = %s - interval '60 seconds',
                available_at = %s - interval '10 seconds',
                claimed_by = 'publisher-a',
                claim_token = %s,
                claimed_until = %s + interval '30 seconds',
                attempt_count = 2
            WHERE event_id = %s
            """,
            (observed_at, observed_at, lab.new_id(), observed_at, leased_id),
        )
        conn.execute(
            """
            UPDATE kernel_lab.domain_event_outbox
            SET recorded_at = %s - interval '300 seconds',
                available_at = %s - interval '100 seconds',
                attempt_count = 5,
                last_error = 'schema rejected',
                last_failed_at = %s - interval '40 seconds',
                quarantined_at = %s - interval '40 seconds',
                quarantine_reason = 'publication failed on attempt 5 of 5'
            WHERE event_id = %s
            """,
            (observed_at, observed_at, observed_at, observed_at, quarantined_id),
        )
        conn.execute(
            """
            UPDATE kernel_lab.domain_event_outbox
            SET recorded_at = %s - interval '500 seconds',
                published_at = %s - interval '450 seconds'
            WHERE event_id = %s
            """,
            (observed_at, observed_at, published_id),
        )
        conn.commit()

    store = PostgresOutboxTelemetryStore(lab.DATABASE_URL)
    snapshot = store.snapshot(observed_at=observed_at)

    assert snapshot.observed_at == observed_at
    assert (
        snapshot.backlog_count,
        snapshot.ready_count,
        snapshot.delayed_count,
        snapshot.leased_count,
        snapshot.quarantined_count,
    ) == (4, 1, 1, 1, 1)
    assert (
        snapshot.attempts_0_count,
        snapshot.attempts_1_count,
        snapshot.attempts_2_to_4_count,
        snapshot.attempts_5_plus_count,
        snapshot.max_attempt_count,
    ) == (1, 1, 1, 1, 5)
    assert snapshot.oldest_backlog_age_seconds == 300
    assert snapshot.oldest_ready_age_seconds == 120
    assert snapshot.oldest_quarantined_age_seconds == 40

    after_deadlines = store.snapshot(observed_at=observed_at + timedelta(seconds=31))
    assert (
        after_deadlines.ready_count,
        after_deadlines.delayed_count,
        after_deadlines.leased_count,
        after_deadlines.quarantined_count,
    ) == (3, 0, 0, 1)


def test_empty_snapshot_is_healthy_and_omits_age_samples():
    observed_at = datetime(2026, 8, 18, 18, 0, tzinfo=timezone.utc)
    snapshot = PostgresOutboxTelemetryStore(lab.DATABASE_URL).snapshot(
        observed_at=observed_at
    )
    report = OutboxAlertPolicy().evaluate(snapshot)

    assert snapshot.backlog_count == 0
    assert snapshot.max_attempt_count is None
    assert snapshot.oldest_backlog_age_seconds is None
    assert report.status == "ok"
    assert report.alerts == ()

    prometheus = render_prometheus(report)
    assert "spotwo_wms_outbox_backlog_events 0" in prometheus
    assert "spotwo_wms_outbox_oldest_age_seconds{state=" not in prometheus
    assert "spotwo_wms_outbox_health_status 0" in prometheus


def test_alert_policy_reports_quarantine_backlog_and_ready_age():
    snapshot = OutboxTelemetrySnapshot(
        observed_at=datetime(2026, 8, 18, 18, 0, tzinfo=timezone.utc),
        backlog_count=1200,
        ready_count=1197,
        delayed_count=1,
        leased_count=0,
        quarantined_count=2,
        attempts_0_count=1000,
        attempts_1_count=100,
        attempts_2_to_4_count=98,
        attempts_5_plus_count=2,
        max_attempt_count=6,
        oldest_backlog_age_seconds=400,
        oldest_ready_age_seconds=120,
        oldest_quarantined_age_seconds=30,
    )

    report = OutboxAlertPolicy().evaluate(snapshot)

    assert report.status == "critical"
    assert [(alert.code, alert.severity) for alert in report.alerts] == [
        ("outbox_quarantined_events", "critical"),
        ("outbox_backlog_count_exceeded", "warning"),
        ("outbox_ready_age_exceeded", "warning"),
    ]
    assert report.to_dict()["telemetry"]["backlog"] == {
        "total": 1200,
        "ready": 1197,
        "delayed": 1,
        "leased": 0,
        "quarantined": 2,
    }

    prometheus = render_prometheus(report)
    assert 'spotwo_wms_outbox_events{state="quarantined"} 2' in prometheus
    assert 'spotwo_wms_outbox_attempts{bucket="5_plus"} 2' in prometheus
    assert 'spotwo_wms_outbox_oldest_age_seconds{state="ready"} 120' in prometheus
    assert "spotwo_wms_outbox_health_status 2" in prometheus
    assert (
        'spotwo_wms_outbox_alert{code="outbox_quarantined_events",'
        'severity="critical"} 1'
    ) in prometheus
    assert "event_id" not in prometheus
    assert "schema rejected" not in prometheus


def test_snapshot_rejects_non_partitioned_counts():
    with pytest.raises(ValueError, match="delivery state counts"):
        OutboxTelemetrySnapshot(
            observed_at=datetime(2026, 8, 18, 18, 0, tzinfo=timezone.utc),
            backlog_count=2,
            ready_count=1,
            delayed_count=0,
            leased_count=0,
            quarantined_count=0,
            attempts_0_count=2,
            attempts_1_count=0,
            attempts_2_to_4_count=0,
            attempts_5_plus_count=0,
            max_attempt_count=0,
            oldest_backlog_age_seconds=1,
            oldest_ready_age_seconds=1,
            oldest_quarantined_age_seconds=None,
        )

    with pytest.raises(ValueError, match="attempt bucket counts"):
        OutboxTelemetrySnapshot(
            observed_at=datetime(2026, 8, 18, 18, 0, tzinfo=timezone.utc),
            backlog_count=2,
            ready_count=2,
            delayed_count=0,
            leased_count=0,
            quarantined_count=0,
            attempts_0_count=1,
            attempts_1_count=0,
            attempts_2_to_4_count=0,
            attempts_5_plus_count=0,
            max_attempt_count=0,
            oldest_backlog_age_seconds=1,
            oldest_ready_age_seconds=1,
            oldest_quarantined_age_seconds=None,
        )


def test_alert_policy_promotes_threshold_breaches_to_critical():
    snapshot = OutboxTelemetrySnapshot(
        observed_at=datetime(2026, 8, 18, 18, 0, tzinfo=timezone.utc),
        backlog_count=10000,
        ready_count=10000,
        delayed_count=0,
        leased_count=0,
        quarantined_count=0,
        attempts_0_count=10000,
        attempts_1_count=0,
        attempts_2_to_4_count=0,
        attempts_5_plus_count=0,
        max_attempt_count=0,
        oldest_backlog_age_seconds=300,
        oldest_ready_age_seconds=300,
        oldest_quarantined_age_seconds=None,
    )

    report = OutboxAlertPolicy().evaluate(snapshot)

    assert report.status == "critical"
    assert [(alert.code, alert.severity) for alert in report.alerts] == [
        ("outbox_backlog_count_exceeded", "critical"),
        ("outbox_ready_age_exceeded", "critical"),
    ]
