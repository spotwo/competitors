from __future__ import annotations

import json
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

import psycopg

import conftest as lab
from event_pipeline_topology_provisioning_health import (
    PostgresTopologyProvisioningHealthStore,
    TopologyProvisioningHealthPolicy,
    render_prometheus,
)

ROOT = Path(__file__).resolve().parents[3]
BASE = datetime(2026, 8, 19, 10, 0, tzinfo=timezone.utc)
RUN_ID = UUID("00000000-0000-0000-0000-00000000a001")


def insert_run(
    *,
    state: str,
    state_changed_at: datetime,
    lease_expires_at: datetime | None = None,
    state_code: str | None = None,
    migration_id: str = "ack-window-10-to-20",
    attempt_count: int = 1,
    recovery_count: int = 0,
) -> None:
    terminal = state in {"completed", "rolled_back", "failed", "manual_intervention"}
    if terminal:
        lease_owner = None
        lease_expires_at = None
    else:
        lease_owner = "health-test-owner"
        lease_expires_at = lease_expires_at or BASE + timedelta(seconds=120)

    with psycopg.connect(lab.DATABASE_URL) as conn:
        conn.execute(
            """
            INSERT INTO kernel_lab.event_pipeline_topology_provisioning_runs (
              run_id,
              pipeline_id,
              migration_id,
              resource,
              state,
              state_code,
              source_snapshot,
              changes,
              intent_fingerprint,
              lease_owner,
              lease_expires_at,
              attempt_count,
              recovery_count,
              started_at,
              updated_at,
              state_changed_at
            ) VALUES (
              %s,
              'inventory-position-projection',
              %s,
              'business_consumer',
              %s,
              %s,
              '{"resource":"business_consumer","name":"POSITION_PROJECTOR","exists":true,"fields":{}}'::jsonb,
              '[{"resource":"business_consumer","field":"max_ack_pending","current":10,"target":20}]'::jsonb,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s
            )
            """,
            (
                RUN_ID,
                migration_id,
                state,
                state_code,
                "a" * 64,
                lease_owner,
                lease_expires_at,
                attempt_count,
                recovery_count,
                BASE - timedelta(minutes=5),
                state_changed_at,
                state_changed_at,
            ),
        )


def snapshot(observed_at: datetime = BASE):
    return PostgresTopologyProvisioningHealthStore(lab.DATABASE_URL).snapshot(
        pipeline_id="inventory-position-projection",
        observed_at=observed_at,
    )


def test_empty_history_is_healthy():
    report = TopologyProvisioningHealthPolicy().evaluate(snapshot())

    assert report.status == "ok"
    assert report.current_status == "ok"
    assert report.history_status == "ok"
    assert report.snapshot.active is None
    assert report.snapshot.latest_terminal is None
    assert report.snapshot.total_runs == 0


def test_young_active_execution_is_healthy():
    insert_run(
        state="applying",
        state_changed_at=BASE - timedelta(seconds=20),
        lease_expires_at=BASE + timedelta(seconds=90),
    )

    report = TopologyProvisioningHealthPolicy().evaluate(snapshot())

    assert report.status == "ok"
    assert report.snapshot.active is not None
    assert report.snapshot.active.state == "applying"
    assert report.snapshot.active.state_age_seconds == 20
    assert report.snapshot.active.lease_remaining_seconds == 90


def test_slow_active_execution_is_warning():
    insert_run(
        state="target_verified",
        state_changed_at=BASE - timedelta(seconds=75),
        lease_expires_at=BASE + timedelta(seconds=30),
    )

    report = TopologyProvisioningHealthPolicy(
        warning_state_age_seconds=60,
        critical_state_age_seconds=180,
    ).evaluate(snapshot())

    assert report.status == "warning"
    assert report.current_code == "topology_provisioning_execution_slow"


def test_stuck_active_execution_is_critical_before_lease_expiry():
    insert_run(
        state="canary_verified",
        state_changed_at=BASE - timedelta(seconds=190),
        lease_expires_at=BASE + timedelta(seconds=30),
    )

    report = TopologyProvisioningHealthPolicy(
        warning_state_age_seconds=60,
        critical_state_age_seconds=180,
    ).evaluate(snapshot())

    assert report.status == "critical"
    assert report.current_code == "topology_provisioning_execution_stuck"


def test_expired_lease_is_critical_even_when_state_is_fresh():
    insert_run(
        state="prepared",
        state_changed_at=BASE - timedelta(seconds=5),
        lease_expires_at=BASE - timedelta(seconds=1),
    )

    report = TopologyProvisioningHealthPolicy().evaluate(snapshot())

    assert report.status == "critical"
    assert report.current_code == "topology_provisioning_lease_expired"
    assert report.snapshot.active is not None
    assert report.snapshot.active.lease_expired is True


def test_latest_rollback_is_visible_without_becoming_current_failure():
    insert_run(
        state="rolled_back",
        state_changed_at=BASE - timedelta(seconds=10),
        state_code="topology_postapply_canary_not_ok",
        recovery_count=1,
        attempt_count=2,
    )

    report = TopologyProvisioningHealthPolicy().evaluate(snapshot())

    assert report.status == "warning"
    assert report.current_status == "ok"
    assert report.history_status == "warning"
    assert report.history_code == "topology_provisioning_latest_rolled_back"
    assert report.snapshot.latest_terminal is not None
    assert report.snapshot.latest_terminal.recovery_count == 1


def test_latest_failed_and_manual_intervention_are_critical_history():
    insert_run(
        state="failed",
        state_changed_at=BASE - timedelta(seconds=10),
        state_code="topology_rollback_failed",
    )
    failed = TopologyProvisioningHealthPolicy().evaluate(snapshot())
    assert failed.history_status == "critical"
    assert failed.history_code == "topology_provisioning_latest_failed"

    with psycopg.connect(lab.DATABASE_URL) as conn:
        conn.execute(
            "TRUNCATE kernel_lab.event_pipeline_topology_provisioning_runs"
        )
    insert_run(
        state="manual_intervention",
        state_changed_at=BASE - timedelta(seconds=10),
        state_code="topology_recovery_manual_intervention_required",
    )
    manual = TopologyProvisioningHealthPolicy().evaluate(snapshot())
    assert manual.history_status == "critical"
    assert manual.history_code == "topology_provisioning_manual_intervention"


def test_state_changed_at_moves_only_when_durable_state_changes():
    insert_run(
        state="prepared",
        state_changed_at=BASE - timedelta(seconds=30),
        lease_expires_at=BASE + timedelta(seconds=90),
    )
    with psycopg.connect(lab.DATABASE_URL) as conn:
        before = conn.execute(
            "SELECT state_changed_at FROM kernel_lab.event_pipeline_topology_provisioning_runs WHERE run_id = %s",
            (RUN_ID,),
        ).fetchone()[0]
        conn.execute(
            "UPDATE kernel_lab.event_pipeline_topology_provisioning_runs SET updated_at = clock_timestamp() WHERE run_id = %s",
            (RUN_ID,),
        )
        unchanged = conn.execute(
            "SELECT state_changed_at FROM kernel_lab.event_pipeline_topology_provisioning_runs WHERE run_id = %s",
            (RUN_ID,),
        ).fetchone()[0]
        conn.execute(
            "UPDATE kernel_lab.event_pipeline_topology_provisioning_runs SET state = 'applying' WHERE run_id = %s",
            (RUN_ID,),
        )
        changed = conn.execute(
            "SELECT state_changed_at FROM kernel_lab.event_pipeline_topology_provisioning_runs WHERE run_id = %s",
            (RUN_ID,),
        ).fetchone()[0]

    assert unchanged == before
    assert changed > before


def test_prometheus_keeps_execution_identity_out_of_labels():
    insert_run(
        state="applying",
        state_changed_at=BASE - timedelta(seconds=75),
        lease_expires_at=BASE + timedelta(seconds=30),
        migration_id="secret-looking-migration-id",
        recovery_count=3,
        attempt_count=4,
    )
    report = TopologyProvisioningHealthPolicy().evaluate(snapshot())
    rendered = render_prometheus(report)

    assert 'pipeline="inventory-position-projection"' in rendered
    assert 'state="applying"' in rendered
    assert 'resource="business_consumer"' in rendered
    assert "secret-looking-migration-id" not in rendered
    assert str(RUN_ID) not in rendered
    assert "health-test-owner" not in rendered


def test_cli_reads_health_without_acquiring_or_renewing_lease():
    now = datetime.now(timezone.utc)
    insert_run(
        state="applying",
        state_changed_at=now - timedelta(seconds=5),
        lease_expires_at=now + timedelta(seconds=300),
    )
    with psycopg.connect(lab.DATABASE_URL) as conn:
        before = conn.execute(
            "SELECT lease_owner, lease_expires_at, attempt_count, recovery_count FROM kernel_lab.event_pipeline_topology_provisioning_runs WHERE run_id = %s",
            (RUN_ID,),
        ).fetchone()

    result = subprocess.run(
        [
            str(ROOT / "bin" / "inspect-kernel-event-pipeline-topology-provisioning"),
            "--pipeline",
            "inventory-position-projection",
            "--database-url",
            lab.DATABASE_URL,
            "--check",
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    with psycopg.connect(lab.DATABASE_URL) as conn:
        after = conn.execute(
            "SELECT lease_owner, lease_expires_at, attempt_count, recovery_count FROM kernel_lab.event_pipeline_topology_provisioning_runs WHERE run_id = %s",
            (RUN_ID,),
        ).fetchone()
    assert after == before
