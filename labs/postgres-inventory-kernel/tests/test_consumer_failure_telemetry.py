from __future__ import annotations

from datetime import datetime, timedelta, timezone

import psycopg
import pytest
from psycopg.types.json import Jsonb

import conftest as lab
from consumer_failure_telemetry import (
    ConsumerFailureAlertPolicy,
    ConsumerFailureTelemetrySnapshot,
    PostgresConsumerFailureTelemetryStore,
    render_prometheus,
)

CONSUMER_NAME = "inventory-position-quantity-projector"


def insert_failure(
    conn: psycopg.Connection,
    *,
    observed_at: datetime,
    consumer_name: str = CONSUMER_NAME,
    status: str,
    retryable: bool,
    attempt_count: int,
    first_failed_seconds_ago: int,
    available_offset_seconds: int = -1,
    lease_offset_seconds: int | None = None,
    quarantined_seconds_ago: int | None = None,
    resolved_seconds_ago: int | None = None,
    last_error: str = "sensitive handler detail",
):
    event_id = lab.new_id()
    claimed = lease_offset_seconds is not None
    quarantined = status == "quarantined"
    resolved = status == "resolved"
    conn.execute(
        """
        INSERT INTO kernel_lab.domain_event_consumer_failures (
          consumer_name,
          event_id,
          envelope,
          delivery_metadata,
          status,
          failure_code,
          retryable,
          attempt_count,
          available_at,
          claimed_by,
          claim_token,
          claimed_until,
          last_error,
          first_failed_at,
          last_failed_at,
          quarantined_at,
          quarantine_reason,
          resolved_at,
          resolution
        ) VALUES (
          %s, %s, %s, %s, %s, %s, %s, %s,
          %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """,
        (
            consumer_name,
            event_id,
            Jsonb({"event_id": str(event_id)}),
            Jsonb({"transport": "test", "worker": "sensitive-worker"}),
            status,
            "projection_version_conflict" if not retryable else "handler_failure",
            retryable,
            attempt_count,
            observed_at + timedelta(seconds=available_offset_seconds),
            "retry-worker" if claimed else None,
            lab.new_id() if claimed else None,
            (
                observed_at + timedelta(seconds=lease_offset_seconds)
                if lease_offset_seconds is not None
                else None
            ),
            last_error,
            observed_at - timedelta(seconds=first_failed_seconds_ago),
            observed_at - timedelta(seconds=1),
            (
                observed_at - timedelta(seconds=quarantined_seconds_ago)
                if quarantined_seconds_ago is not None
                else None
            ),
            "terminal:test" if quarantined else None,
            (
                observed_at - timedelta(seconds=resolved_seconds_ago)
                if resolved_seconds_ago is not None
                else None
            ),
            "applied" if resolved else None,
        ),
    )
    return event_id


def test_postgres_snapshot_classifies_unresolved_failures_and_attempts():
    observed_at = datetime(2026, 8, 18, 23, 30, tzinfo=timezone.utc)

    with lab.connect() as conn:
        insert_failure(
            conn,
            observed_at=observed_at,
            status="deferred",
            retryable=True,
            attempt_count=0,
            first_failed_seconds_ago=120,
        )
        insert_failure(
            conn,
            observed_at=observed_at,
            status="deferred",
            retryable=True,
            attempt_count=1,
            first_failed_seconds_ago=90,
            available_offset_seconds=30,
        )
        insert_failure(
            conn,
            observed_at=observed_at,
            status="deferred",
            retryable=True,
            attempt_count=3,
            first_failed_seconds_ago=60,
            lease_offset_seconds=30,
        )
        insert_failure(
            conn,
            observed_at=observed_at,
            status="quarantined",
            retryable=False,
            attempt_count=1,
            first_failed_seconds_ago=300,
            quarantined_seconds_ago=40,
        )
        insert_failure(
            conn,
            observed_at=observed_at,
            status="quarantined",
            retryable=True,
            attempt_count=5,
            first_failed_seconds_ago=200,
            quarantined_seconds_ago=20,
        )
        insert_failure(
            conn,
            observed_at=observed_at,
            status="resolved",
            retryable=True,
            attempt_count=2,
            first_failed_seconds_ago=500,
            resolved_seconds_ago=450,
        )
        insert_failure(
            conn,
            observed_at=observed_at,
            consumer_name="another-projector",
            status="quarantined",
            retryable=False,
            attempt_count=1,
            first_failed_seconds_ago=600,
            quarantined_seconds_ago=500,
        )
        conn.commit()
        before_rows = conn.execute(
            """
            SELECT consumer_name, event_id, status, attempt_count,
                   available_at, claimed_until, quarantined_at, resolved_at
            FROM kernel_lab.domain_event_consumer_failures
            ORDER BY consumer_name, event_id
            """
        ).fetchall()

    store = PostgresConsumerFailureTelemetryStore(lab.DATABASE_URL)
    snapshot = store.snapshot(
        observed_at=observed_at,
        consumer_name=CONSUMER_NAME,
    )

    assert snapshot.observed_at == observed_at
    assert snapshot.consumer_name == CONSUMER_NAME
    assert (
        snapshot.backlog_count,
        snapshot.ready_count,
        snapshot.delayed_count,
        snapshot.leased_count,
        snapshot.quarantined_count,
    ) == (5, 1, 1, 1, 2)
    assert (
        snapshot.terminal_quarantined_count,
        snapshot.exhausted_quarantined_count,
    ) == (1, 1)
    assert (
        snapshot.attempts_0_count,
        snapshot.attempts_1_count,
        snapshot.attempts_2_to_4_count,
        snapshot.attempts_5_plus_count,
        snapshot.max_attempt_count,
    ) == (1, 2, 1, 1, 5)
    assert snapshot.oldest_backlog_age_seconds == 300
    assert snapshot.oldest_ready_age_seconds == 120
    assert snapshot.oldest_quarantined_age_seconds == 40

    after_deadlines = store.snapshot(
        observed_at=observed_at + timedelta(seconds=31),
        consumer_name=CONSUMER_NAME,
    )
    assert (
        after_deadlines.ready_count,
        after_deadlines.delayed_count,
        after_deadlines.leased_count,
        after_deadlines.quarantined_count,
    ) == (3, 0, 0, 2)

    global_snapshot = store.snapshot(observed_at=observed_at)
    assert global_snapshot.consumer_name is None
    assert global_snapshot.backlog_count == 6

    with lab.connect() as conn:
        after_rows = conn.execute(
            """
            SELECT consumer_name, event_id, status, attempt_count,
                   available_at, claimed_until, quarantined_at, resolved_at
            FROM kernel_lab.domain_event_consumer_failures
            ORDER BY consumer_name, event_id
            """
        ).fetchall()
    assert after_rows == before_rows


def test_empty_snapshot_is_healthy_and_omits_age_samples():
    observed_at = datetime(2026, 8, 18, 23, 30, tzinfo=timezone.utc)
    snapshot = PostgresConsumerFailureTelemetryStore(lab.DATABASE_URL).snapshot(
        observed_at=observed_at,
        consumer_name=CONSUMER_NAME,
    )
    report = ConsumerFailureAlertPolicy().evaluate(snapshot)

    assert snapshot.backlog_count == 0
    assert snapshot.max_attempt_count is None
    assert snapshot.oldest_backlog_age_seconds is None
    assert report.status == "ok"
    assert report.alerts == ()

    prometheus = render_prometheus(report)
    assert (
        "spotwo_wms_consumer_failure_backlog_events"
        '{consumer="inventory-position-quantity-projector"} 0'
        in prometheus
    )
    assert "spotwo_wms_consumer_failure_oldest_age_seconds{state=" not in prometheus
    assert (
        "spotwo_wms_consumer_failure_health_status"
        '{consumer="inventory-position-quantity-projector"} 0'
        in prometheus
    )


def test_alert_policy_and_prometheus_use_only_bounded_labels():
    snapshot = ConsumerFailureTelemetrySnapshot(
        observed_at=datetime(2026, 8, 18, 23, 30, tzinfo=timezone.utc),
        consumer_name=CONSUMER_NAME,
        backlog_count=120,
        ready_count=117,
        delayed_count=1,
        leased_count=0,
        quarantined_count=2,
        terminal_quarantined_count=1,
        exhausted_quarantined_count=1,
        attempts_0_count=10,
        attempts_1_count=100,
        attempts_2_to_4_count=8,
        attempts_5_plus_count=2,
        max_attempt_count=6,
        oldest_backlog_age_seconds=700,
        oldest_ready_age_seconds=400,
        oldest_quarantined_age_seconds=30,
    )

    report = ConsumerFailureAlertPolicy().evaluate(snapshot)

    assert report.status == "critical"
    assert [(alert.code, alert.severity) for alert in report.alerts] == [
        ("consumer_failure_quarantined_events", "critical"),
        ("consumer_failure_backlog_count_exceeded", "warning"),
        ("consumer_failure_ready_age_exceeded", "warning"),
    ]
    assert report.to_dict()["telemetry"]["quarantine"] == {
        "terminal": 1,
        "attempt_limit": 1,
    }

    prometheus = render_prometheus(report)
    assert (
        "spotwo_wms_consumer_failure_events"
        '{state="quarantined",consumer="inventory-position-quantity-projector"} 2'
        in prometheus
    )
    assert (
        "spotwo_wms_consumer_failure_quarantined_events"
        '{kind="terminal",consumer="inventory-position-quantity-projector"} 1'
        in prometheus
    )
    assert (
        "spotwo_wms_consumer_failure_attempts"
        '{bucket="5_plus",consumer="inventory-position-quantity-projector"} 2'
        in prometheus
    )
    assert "event_id" not in prometheus
    assert "position_id" not in prometheus
    assert "projection_version_conflict" not in prometheus
    assert "sensitive handler detail" not in prometheus
    assert "sensitive-worker" not in prometheus


def test_snapshot_rejects_inconsistent_partitions():
    common = {
        "observed_at": datetime(2026, 8, 18, 23, 30, tzinfo=timezone.utc),
        "consumer_name": None,
        "backlog_count": 2,
        "ready_count": 1,
        "delayed_count": 0,
        "leased_count": 0,
        "quarantined_count": 1,
        "terminal_quarantined_count": 1,
        "exhausted_quarantined_count": 0,
        "attempts_0_count": 0,
        "attempts_1_count": 2,
        "attempts_2_to_4_count": 0,
        "attempts_5_plus_count": 0,
        "max_attempt_count": 1,
        "oldest_backlog_age_seconds": 2,
        "oldest_ready_age_seconds": 1,
        "oldest_quarantined_age_seconds": 2,
    }

    with pytest.raises(ValueError, match="failure state counts"):
        ConsumerFailureTelemetrySnapshot(**(common | {"ready_count": 0}))
    with pytest.raises(ValueError, match="quarantine kind counts"):
        ConsumerFailureTelemetrySnapshot(
            **(common | {"terminal_quarantined_count": 0})
        )
    with pytest.raises(ValueError, match="attempt bucket counts"):
        ConsumerFailureTelemetrySnapshot(**(common | {"attempts_1_count": 1}))


def test_alert_policy_promotes_threshold_breaches_to_critical():
    snapshot = ConsumerFailureTelemetrySnapshot(
        observed_at=datetime(2026, 8, 18, 23, 30, tzinfo=timezone.utc),
        consumer_name=None,
        backlog_count=1000,
        ready_count=1000,
        delayed_count=0,
        leased_count=0,
        quarantined_count=0,
        terminal_quarantined_count=0,
        exhausted_quarantined_count=0,
        attempts_0_count=0,
        attempts_1_count=1000,
        attempts_2_to_4_count=0,
        attempts_5_plus_count=0,
        max_attempt_count=1,
        oldest_backlog_age_seconds=1800,
        oldest_ready_age_seconds=1800,
        oldest_quarantined_age_seconds=None,
    )

    report = ConsumerFailureAlertPolicy().evaluate(snapshot)

    assert report.status == "critical"
    assert [(alert.code, alert.severity) for alert in report.alerts] == [
        ("consumer_failure_backlog_count_exceeded", "critical"),
        ("consumer_failure_ready_age_exceeded", "critical"),
    ]
