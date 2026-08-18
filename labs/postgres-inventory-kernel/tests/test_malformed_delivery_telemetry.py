from __future__ import annotations

from datetime import datetime, timedelta, timezone

import psycopg
import pytest
from psycopg.types.json import Jsonb

import conftest as lab
from malformed_delivery_telemetry import (
    MalformedDeliveryAlertPolicy,
    MalformedDeliveryTelemetrySnapshot,
    PostgresMalformedDeliveryTelemetryStore,
    render_prometheus,
)

CONSUMER_NAME = "inventory-position-quantity-projector"


def insert_poison(
    conn: psycopg.Connection,
    *,
    observed_at: datetime,
    failure_code: str,
    first_seen_seconds_ago: int,
    last_seen_seconds_ago: int | None = None,
    observation_count: int = 1,
    consumer_name: str = CONSUMER_NAME,
):
    sequence = conn.execute(
        "SELECT COALESCE(max(stream_sequence), 0) + 1 FROM kernel_lab.nats_jetstream_consumer_poison_deliveries"
    ).fetchone()[0]
    first_seen = observed_at - timedelta(seconds=first_seen_seconds_ago)
    last_seen = observed_at - timedelta(
        seconds=(
            first_seen_seconds_ago
            if last_seen_seconds_ago is None
            else last_seen_seconds_ago
        )
    )
    conn.execute(
        """
        INSERT INTO kernel_lab.nats_jetstream_consumer_poison_deliveries (
          consumer_name,
          stream,
          durable_consumer,
          stream_sequence,
          subject,
          failure_code,
          payload_sha256,
          payload_size,
          payload_preview,
          payload_truncated,
          headers,
          first_delivery_metadata,
          last_delivery_metadata,
          last_error,
          observation_count,
          first_seen_at,
          last_seen_at,
          quarantined_at
        ) VALUES (
          %s, 'WMS_EVENTS', 'POSITION_PROJECTOR', %s,
          'spotwo.wms.events.inventory.position.changed', %s,
          %s, 3, %s, false, %s, %s, %s,
          'sensitive malformed detail', %s, %s, %s, %s
        )
        """,
        (
            consumer_name,
            sequence,
            failure_code,
            f"{sequence:064x}",
            b"bad",
            Jsonb({"X-Sensitive": "do-not-export"}),
            Jsonb({"transport": "nats-jetstream", "stream_sequence": sequence}),
            Jsonb({"transport": "nats-jetstream", "stream_sequence": sequence}),
            observation_count,
            first_seen,
            last_seen,
            first_seen,
        ),
    )
    return sequence


def test_postgres_snapshot_reports_inventory_recent_activity_and_bounded_kinds():
    observed_at = datetime(2026, 8, 19, 0, 30, tzinfo=timezone.utc)

    with lab.connect() as conn:
        insert_poison(
            conn,
            observed_at=observed_at,
            failure_code="invalid_message_encoding",
            first_seen_seconds_ago=30,
            last_seen_seconds_ago=20,
        )
        insert_poison(
            conn,
            observed_at=observed_at,
            failure_code="invalid_message_json",
            first_seen_seconds_ago=600,
            last_seen_seconds_ago=10,
            observation_count=3,
        )
        insert_poison(
            conn,
            observed_at=observed_at,
            failure_code="invalid_event_envelope",
            first_seen_seconds_ago=100,
        )
        insert_poison(
            conn,
            observed_at=observed_at,
            failure_code="invalid_transport_headers",
            first_seen_seconds_ago=1000,
        )
        insert_poison(
            conn,
            observed_at=observed_at,
            failure_code="future_bounded_failure",
            first_seen_seconds_ago=50,
        )
        insert_poison(
            conn,
            observed_at=observed_at,
            consumer_name="another-projector",
            failure_code="invalid_message_json",
            first_seen_seconds_ago=40,
        )
        conn.commit()
        before_rows = conn.execute(
            """
            SELECT consumer_name, stream_sequence, failure_code, observation_count,
                   first_seen_at, last_seen_at
            FROM kernel_lab.nats_jetstream_consumer_poison_deliveries
            ORDER BY consumer_name, stream_sequence
            """
        ).fetchall()

    store = PostgresMalformedDeliveryTelemetryStore(lab.DATABASE_URL)
    snapshot = store.snapshot(
        observed_at=observed_at,
        consumer_name=CONSUMER_NAME,
        lookback_seconds=300,
    )

    assert snapshot.observed_at == observed_at
    assert snapshot.consumer_name == CONSUMER_NAME
    assert snapshot.lookback_seconds == 300
    assert (
        snapshot.retained_count,
        snapshot.recent_count,
        snapshot.recent_reobserved_count,
        snapshot.total_observation_count,
    ) == (5, 3, 1, 7)
    assert (
        snapshot.invalid_encoding_count,
        snapshot.invalid_json_count,
        snapshot.invalid_envelope_count,
        snapshot.invalid_headers_count,
        snapshot.other_failure_count,
    ) == (1, 1, 1, 1, 1)
    assert snapshot.max_observation_count == 3
    assert snapshot.oldest_age_seconds == 1000
    assert snapshot.newest_age_seconds == 30
    assert snapshot.latest_observation_age_seconds == 10

    global_snapshot = store.snapshot(
        observed_at=observed_at,
        lookback_seconds=300,
    )
    assert global_snapshot.consumer_name is None
    assert global_snapshot.retained_count == 6
    assert global_snapshot.recent_count == 4

    with lab.connect() as conn:
        after_rows = conn.execute(
            """
            SELECT consumer_name, stream_sequence, failure_code, observation_count,
                   first_seen_at, last_seen_at
            FROM kernel_lab.nats_jetstream_consumer_poison_deliveries
            ORDER BY consumer_name, stream_sequence
            """
        ).fetchall()
    assert after_rows == before_rows


def test_empty_snapshot_is_healthy_and_retained_history_does_not_alarm_by_itself():
    observed_at = datetime(2026, 8, 19, 0, 30, tzinfo=timezone.utc)
    store = PostgresMalformedDeliveryTelemetryStore(lab.DATABASE_URL)

    empty = store.snapshot(
        observed_at=observed_at,
        consumer_name=CONSUMER_NAME,
    )
    empty_report = MalformedDeliveryAlertPolicy().evaluate(empty)
    assert empty.retained_count == 0
    assert empty.max_observation_count is None
    assert empty.oldest_age_seconds is None
    assert empty_report.status == "ok"

    with lab.connect() as conn:
        insert_poison(
            conn,
            observed_at=observed_at,
            failure_code="invalid_message_json",
            first_seen_seconds_ago=3600,
        )
        conn.commit()

    historical = store.snapshot(
        observed_at=observed_at,
        consumer_name=CONSUMER_NAME,
        lookback_seconds=300,
    )
    report = MalformedDeliveryAlertPolicy().evaluate(historical)

    assert historical.retained_count == 1
    assert historical.recent_count == 0
    assert historical.recent_reobserved_count == 0
    assert report.status == "ok"
    assert report.alerts == ()

    prometheus = render_prometheus(report)
    assert (
        "spotwo_wms_malformed_delivery_retained_records"
        '{consumer="inventory-position-quantity-projector"} 1'
        in prometheus
    )
    assert "stream_sequence" not in prometheus
    assert "sensitive malformed detail" not in prometheus


def test_alert_policy_and_prometheus_export_only_bounded_dimensions():
    snapshot = MalformedDeliveryTelemetrySnapshot(
        observed_at=datetime(2026, 8, 19, 0, 30, tzinfo=timezone.utc),
        consumer_name=CONSUMER_NAME,
        lookback_seconds=300,
        retained_count=20,
        recent_count=10,
        recent_reobserved_count=1,
        total_observation_count=25,
        invalid_encoding_count=4,
        invalid_json_count=5,
        invalid_envelope_count=4,
        invalid_headers_count=6,
        other_failure_count=1,
        max_observation_count=4,
        oldest_age_seconds=7200,
        newest_age_seconds=5,
        latest_observation_age_seconds=2,
    )

    report = MalformedDeliveryAlertPolicy().evaluate(snapshot)

    assert report.status == "critical"
    assert [(alert.code, alert.severity) for alert in report.alerts] == [
        ("malformed_delivery_recent_records", "critical"),
        ("malformed_delivery_recent_reobserved_records", "warning"),
    ]
    assert report.to_dict()["telemetry"]["failure_kinds"]["invalid_json"] == 5

    prometheus = render_prometheus(report)
    assert (
        "spotwo_wms_malformed_delivery_failure_records"
        '{kind="invalid_json",consumer="inventory-position-quantity-projector"} 5'
        in prometheus
    )
    assert (
        "spotwo_wms_malformed_delivery_alert"
        '{code="malformed_delivery_recent_records",severity="critical",consumer="inventory-position-quantity-projector"} 1'
        in prometheus
    )
    for forbidden in (
        "event_id",
        "stream_sequence",
        "payload_sha256",
        "subject=",
        "invalid_message_json",
        "sensitive malformed detail",
    ):
        assert forbidden not in prometheus


def test_snapshot_rejects_inconsistent_counts_and_ages():
    common = {
        "observed_at": datetime(2026, 8, 19, 0, 30, tzinfo=timezone.utc),
        "consumer_name": None,
        "lookback_seconds": 300,
        "retained_count": 2,
        "recent_count": 1,
        "recent_reobserved_count": 0,
        "total_observation_count": 2,
        "invalid_encoding_count": 1,
        "invalid_json_count": 1,
        "invalid_envelope_count": 0,
        "invalid_headers_count": 0,
        "other_failure_count": 0,
        "max_observation_count": 1,
        "oldest_age_seconds": 100,
        "newest_age_seconds": 10,
        "latest_observation_age_seconds": 5,
    }

    with pytest.raises(ValueError, match="failure kind counts"):
        MalformedDeliveryTelemetrySnapshot(
            **(common | {"invalid_json_count": 0})
        )
    with pytest.raises(ValueError, match="total observations"):
        MalformedDeliveryTelemetrySnapshot(
            **(common | {"total_observation_count": 1})
        )
    with pytest.raises(ValueError, match="chronologically consistent"):
        MalformedDeliveryTelemetrySnapshot(
            **(common | {"latest_observation_age_seconds": 20})
        )


def test_alert_policy_promotes_reobservation_threshold_to_critical():
    snapshot = MalformedDeliveryTelemetrySnapshot(
        observed_at=datetime(2026, 8, 19, 0, 30, tzinfo=timezone.utc),
        consumer_name=None,
        lookback_seconds=300,
        retained_count=6,
        recent_count=0,
        recent_reobserved_count=5,
        total_observation_count=15,
        invalid_encoding_count=0,
        invalid_json_count=6,
        invalid_envelope_count=0,
        invalid_headers_count=0,
        other_failure_count=0,
        max_observation_count=3,
        oldest_age_seconds=1000,
        newest_age_seconds=500,
        latest_observation_age_seconds=1,
    )

    report = MalformedDeliveryAlertPolicy().evaluate(snapshot)

    assert report.status == "critical"
    assert [(alert.code, alert.severity) for alert in report.alerts] == [
        ("malformed_delivery_recent_reobserved_records", "critical"),
    ]
