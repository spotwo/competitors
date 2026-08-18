from __future__ import annotations

import hashlib
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from threading import Barrier
from typing import Any

import psycopg
import pytest

import conftest as lab
from consumer_runtime import ConsumedEvent, InboxDeliveryMetadata, PostgresInboxStore
from position_projection import InventoryPositionQuantityProjector
from position_projection_snapshot import (
    InventoryPositionQuantitySnapshot,
    PostgresPositionProjectionBootstrapStore,
    load_snapshot_artifact,
)
from projection_gap_telemetry import (
    PostgresProjectionGapTelemetryStore,
    ProjectionGapAlertPolicy,
    ProjectionGapTelemetrySnapshot,
    render_prometheus,
)

CONSUMER_NAME = "inventory-position-quantity-projector"
RECORDED_AT = datetime(2026, 8, 18, 20, 0, tzinfo=timezone.utc)
CHECKSUM = "sha256:" + ("a" * 64)


def position_event(
    *,
    position_id,
    version: int,
    physical_delta: int | float = 0,
    reserved_delta: int | float = 0,
    allocated_delta: int | float = 0,
) -> ConsumedEvent:
    event_id = lab.new_id()
    transaction_id = lab.new_id()
    recorded_at = RECORDED_AT + timedelta(seconds=version)
    return ConsumedEvent.from_envelope(
        {
            "event_id": str(event_id),
            "type": "inventory.position.changed",
            "source": "spotwo.wms.inventory-kernel",
            "subject": f"inventory-position/{position_id}",
            "occurred_at": recorded_at.isoformat(),
            "recorded_at": recorded_at.isoformat(),
            "aggregate_type": "InventoryPosition",
            "aggregate_id": str(position_id),
            "aggregate_version": version,
            "schema_version": 1,
            "data": {
                "transaction_id": str(transaction_id),
                "transaction_type": "projection-operations-test",
                "position_id": str(position_id),
                "physical_delta": physical_delta,
                "reserved_delta": reserved_delta,
                "allocated_delta": allocated_delta,
            },
        }
    )


def process_event(event: ConsumedEvent) -> bool:
    return PostgresInboxStore(lab.DATABASE_URL).process_once(
        consumer_name=CONSUMER_NAME,
        event=event,
        delivery_metadata=InboxDeliveryMetadata(
            transport="test",
            transport_message_id=f"test:{event.event_id}",
            subject="spotwo.wms.events.inventory.position.changed",
            delivery_count=1,
        ).to_dict(),
        handler=InventoryPositionQuantityProjector(consumer_name=CONSUMER_NAME),
    )


def snapshot(
    *,
    position_id,
    version: int = 40,
    physical_qty: str = "10",
    reserved_qty: str = "4",
    allocated_qty: str = "2",
) -> InventoryPositionQuantitySnapshot:
    return InventoryPositionQuantitySnapshot.from_document(
        {
            "position_id": str(position_id),
            "aggregate_version": version,
            "physical_qty": physical_qty,
            "reserved_qty": reserved_qty,
            "allocated_qty": allocated_qty,
            "source": "authoritative-inventory-export",
            "reference": f"position-snapshot-v{version}",
            "recorded_at": RECORDED_AT.isoformat(),
        },
        artifact_checksum=CHECKSUM,
    )


def bootstrap(position_id, *, bootstrap_id=None, reason="initial projection seed"):
    bootstrap_id = bootstrap_id or lab.new_id()
    result = PostgresPositionProjectionBootstrapStore(lab.DATABASE_URL).bootstrap(
        bootstrap_id=bootstrap_id,
        consumer_name=CONSUMER_NAME,
        snapshot=snapshot(position_id=position_id),
        operator="operator@example.com",
        reason=reason,
    )
    return bootstrap_id, result


def projection_row(position_id) -> tuple[Any, ...]:
    with lab.connect() as conn:
        return conn.execute(
            """
            SELECT aggregate_version, physical_qty, reserved_qty, allocated_qty,
                   last_event_id, cursor_source, bootstrap_version,
                   bootstrap_physical_qty, bootstrap_reserved_qty,
                   bootstrap_allocated_qty, bootstrap_source,
                   bootstrap_reference, bootstrap_checksum,
                   bootstrapped_by, bootstrap_reason
            FROM kernel_lab.inventory_position_quantity_projection
            WHERE consumer_name = %s
              AND position_id = %s
            """,
            (CONSUMER_NAME, position_id),
        ).fetchone()


def test_verified_snapshot_is_idempotent_and_next_event_advances_cursor():
    position_id = lab.new_id()
    bootstrap_id, first = bootstrap(position_id)
    _, duplicate = bootstrap(position_id, bootstrap_id=bootstrap_id)

    assert first.outcome == "bootstrapped"
    assert duplicate.outcome == "duplicate"
    assert first.bootstrap_version == duplicate.bootstrap_version == 40
    assert projection_row(position_id) == (
        40,
        Decimal("10"),
        Decimal("4"),
        Decimal("2"),
        None,
        "snapshot",
        40,
        Decimal("10"),
        Decimal("4"),
        Decimal("2"),
        "authoritative-inventory-export",
        "position-snapshot-v40",
        CHECKSUM,
        "operator@example.com",
        "initial projection seed",
    )

    included_event = position_event(
        position_id=position_id,
        version=40,
        physical_delta=999,
    )
    next_event = position_event(
        position_id=position_id,
        version=41,
        physical_delta=5,
    )
    assert process_event(included_event) is True
    assert process_event(next_event) is True

    row = projection_row(position_id)
    assert row[:7] == (
        41,
        Decimal("15"),
        Decimal("4"),
        Decimal("2"),
        next_event.event_id,
        "event",
        40,
    )
    with lab.connect() as conn:
        metadata = conn.execute(
            """
            SELECT metadata -> 'projection'
            FROM kernel_lab.domain_event_inbox
            WHERE consumer_name = %s AND event_id = %s
            """,
            (CONSUMER_NAME, included_event.event_id),
        ).fetchone()[0]
    assert metadata["status"] == "stale"
    assert metadata["projection_cursor_source"] == "snapshot"

    _, retry_after_progress = bootstrap(position_id, bootstrap_id=bootstrap_id)
    assert retry_after_progress.outcome == "duplicate"
    assert retry_after_progress.projection_version == 41
    assert projection_row(position_id)[:4] == row[:4]


def test_snapshot_cursor_buffers_and_drains_post_snapshot_gap():
    position_id = lab.new_id()
    bootstrap(position_id)
    version_42 = position_event(
        position_id=position_id,
        version=42,
        allocated_delta=2,
    )
    version_41 = position_event(
        position_id=position_id,
        version=41,
        reserved_delta=1,
    )

    assert process_event(version_42) is True
    assert projection_row(position_id)[:6] == (
        40,
        Decimal("10"),
        Decimal("4"),
        Decimal("2"),
        None,
        "snapshot",
    )
    assert process_event(version_41) is True
    assert projection_row(position_id)[:6] == (
        42,
        Decimal("10"),
        Decimal("5"),
        Decimal("4"),
        version_42.event_id,
        "event",
    )
    with lab.connect() as conn:
        assert conn.execute(
            "SELECT count(*) FROM kernel_lab.inventory_position_projection_pending"
        ).fetchone()[0] == 0


def test_snapshot_rejects_live_cursor_and_payload_collision():
    position_id = lab.new_id()
    buffered = position_event(position_id=position_id, version=3, physical_delta=1)
    assert process_event(buffered) is True

    with pytest.raises(psycopg.errors.UniqueViolation, match="already initialized"):
        bootstrap(position_id)

    clean_position_id = lab.new_id()
    bootstrap_id, _ = bootstrap(clean_position_id)
    with pytest.raises(psycopg.errors.UniqueViolation, match="already initialized"):
        bootstrap(
            clean_position_id,
            bootstrap_id=bootstrap_id,
            reason="changed payload under the same bootstrap identity",
        )
    with pytest.raises(psycopg.errors.UniqueViolation, match="already initialized"):
        bootstrap(clean_position_id)


def test_snapshot_and_first_event_race_never_overwrite_each_other():
    position_id = lab.new_id()
    event = position_event(position_id=position_id, version=41, physical_delta=1)
    barrier = Barrier(2)

    def run_bootstrap():
        barrier.wait(timeout=3)
        try:
            return ("bootstrapped", bootstrap(position_id)[1])
        except psycopg.errors.UniqueViolation:
            return ("conflict", None)

    def run_event():
        barrier.wait(timeout=3)
        return process_event(event)

    with ThreadPoolExecutor(max_workers=2) as pool:
        bootstrap_future = pool.submit(run_bootstrap)
        event_future = pool.submit(run_event)
        bootstrap_outcome, _ = bootstrap_future.result()
        assert event_future.result() is True

    row = projection_row(position_id)
    with lab.connect() as conn:
        pending_versions = conn.execute(
            """
            SELECT aggregate_version
            FROM kernel_lab.inventory_position_projection_pending
            WHERE consumer_name = %s AND position_id = %s
            ORDER BY aggregate_version
            """,
            (CONSUMER_NAME, position_id),
        ).fetchall()

    if bootstrap_outcome == "bootstrapped":
        assert row[:6] == (
            41,
            Decimal("11"),
            Decimal("4"),
            Decimal("2"),
            event.event_id,
            "event",
        )
        assert pending_versions == []
    else:
        assert row[:6] == (
            0,
            Decimal("0"),
            Decimal("0"),
            Decimal("0"),
            None,
            "empty",
        )
        assert pending_versions == [(41,)]


def test_snapshot_artifact_is_strict_and_bound_to_exact_file_bytes(tmp_path):
    position_id = lab.new_id()
    document = {
        "position_id": str(position_id),
        "aggregate_version": 40,
        "physical_qty": "10.000000",
        "reserved_qty": "4",
        "allocated_qty": "2",
        "source": "authoritative-inventory-export",
        "reference": "snapshot-40",
        "recorded_at": RECORDED_AT.isoformat(),
    }
    path = tmp_path / "position-snapshot.json"
    artifact = json.dumps(document, sort_keys=True).encode()
    path.write_bytes(artifact)

    loaded = load_snapshot_artifact(path)
    assert loaded.artifact_checksum == f"sha256:{hashlib.sha256(artifact).hexdigest()}"
    assert loaded.physical_qty == Decimal("10.000000")

    document["unexpected"] = True
    path.write_text(json.dumps(document))
    with pytest.raises(ValueError, match="unknown fields"):
        load_snapshot_artifact(path)

    document.pop("unexpected")
    document["physical_qty"] = 10.5
    path.write_text(json.dumps(document))
    with pytest.raises(ValueError, match="exact decimal string or integer"):
        load_snapshot_artifact(path)


def test_projection_gap_telemetry_aggregates_and_alerts_without_event_labels():
    observed_at = datetime(2026, 8, 18, 21, 0, tzinfo=timezone.utc)
    first_position = lab.new_id()
    second_position = lab.new_id()
    first_v3 = position_event(position_id=first_position, version=3)
    first_v4 = position_event(position_id=first_position, version=4)
    second_v2 = position_event(position_id=second_position, version=2)
    for event in (first_v3, first_v4, second_v2):
        assert process_event(event) is True

    with lab.connect() as conn:
        conn.execute(
            """
            UPDATE kernel_lab.inventory_position_projection_pending
            SET buffered_at = CASE event_id
              WHEN %s THEN %s - interval '120 seconds'
              WHEN %s THEN %s - interval '60 seconds'
              ELSE %s - interval '30 seconds'
            END
            """,
            (
                first_v3.event_id,
                observed_at,
                first_v4.event_id,
                observed_at,
                observed_at,
            ),
        )
        conn.commit()

    snapshot_result = PostgresProjectionGapTelemetryStore(lab.DATABASE_URL).snapshot(
        observed_at=observed_at,
        consumer_name=CONSUMER_NAME,
    )
    assert snapshot_result == ProjectionGapTelemetrySnapshot(
        observed_at=observed_at,
        consumer_name=CONSUMER_NAME,
        gap_count=2,
        pending_event_count=3,
        max_pending_per_gap=2,
        oldest_gap_age_seconds=120,
    )

    warning_report = ProjectionGapAlertPolicy(
        warning_gap_age_seconds=60,
        critical_gap_age_seconds=300,
        warning_pending_event_count=10,
        critical_pending_event_count=20,
    ).evaluate(snapshot_result)
    assert warning_report.status == "warning"
    assert [(alert.code, alert.severity) for alert in warning_report.alerts] == [
        ("projection_gap_age_exceeded", "warning")
    ]

    critical_report = ProjectionGapAlertPolicy(
        warning_gap_age_seconds=300,
        critical_gap_age_seconds=600,
        warning_pending_event_count=2,
        critical_pending_event_count=3,
    ).evaluate(snapshot_result)
    assert critical_report.status == "critical"
    assert [(alert.code, alert.severity) for alert in critical_report.alerts] == [
        ("projection_pending_events_exceeded", "critical")
    ]

    prometheus = render_prometheus(critical_report)
    assert (
        'spotwo_wms_projection_gap_aggregates'
        f'{{consumer="{CONSUMER_NAME}"}} 2'
    ) in prometheus
    assert (
        'spotwo_wms_projection_pending_events'
        f'{{consumer="{CONSUMER_NAME}"}} 3'
    ) in prometheus
    assert "spotwo_wms_projection_oldest_gap_age_seconds" in prometheus
    assert str(first_position) not in prometheus
    assert str(first_v3.event_id) not in prometheus


def test_empty_projection_gap_snapshot_is_healthy_and_omits_age_metric():
    observed_at = datetime(2026, 8, 18, 21, 0, tzinfo=timezone.utc)
    snapshot_result = PostgresProjectionGapTelemetryStore(lab.DATABASE_URL).snapshot(
        observed_at=observed_at
    )
    report = ProjectionGapAlertPolicy().evaluate(snapshot_result)

    assert snapshot_result.gap_count == 0
    assert snapshot_result.pending_event_count == 0
    assert snapshot_result.oldest_gap_age_seconds is None
    assert report.status == "ok"
    assert report.alerts == ()
    prometheus = render_prometheus(report)
    assert "spotwo_wms_projection_gap_aggregates 0" in prometheus
    assert "spotwo_wms_projection_oldest_gap_age_seconds" not in prometheus


def test_projection_gap_snapshot_rejects_inconsistent_counts():
    with pytest.raises(ValueError, match="each projection gap"):
        ProjectionGapTelemetrySnapshot(
            observed_at=RECORDED_AT,
            consumer_name=None,
            gap_count=2,
            pending_event_count=1,
            max_pending_per_gap=1,
            oldest_gap_age_seconds=1,
        )
