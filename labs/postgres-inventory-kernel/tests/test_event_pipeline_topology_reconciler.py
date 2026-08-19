from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg
import yaml

import conftest as lab
from event_pipeline_readiness import load_registry
from event_pipeline_topology import TopologyResourceSnapshot
from event_pipeline_topology_config import pipeline_topology_mapping
from event_pipeline_topology_preflight import TopologyPlanChange
from event_pipeline_topology_provisioning_journal import PostgresTopologyProvisioningJournal
from event_pipeline_topology_reconciler import UnattendedTopologyReconciliationController

ROOT = Path(__file__).resolve().parents[3]
REGISTRY = ROOT / "operations" / "event-pipelines" / "registry.yml"
BASE = datetime(2026, 8, 19, 12, 0, tzinfo=timezone.utc)


def configured_pipeline():
    registry = load_registry(REGISTRY)
    with REGISTRY.open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)
    spec = registry.get("inventory-position-projection")
    topology = pipeline_topology_mapping(raw, spec.pipeline_id)
    return spec, topology


def migration(migration_id: str = "business-ack-window-10-to-20") -> dict:
    return {
        "id": migration_id,
        "valid_until": "2099-01-01T00:00:00Z",
        "from_overrides": {"business_consumer": {"max_ack_pending": 10}},
    }


def source_snapshot() -> TopologyResourceSnapshot:
    return TopologyResourceSnapshot(
        resource="business_consumer",
        name="POSITION_PROJECTOR",
        exists=True,
        fields={
            "delivery_mode": "pull",
            "deliver_policy": "all",
            "ack_policy": "explicit",
            "filter_subject": "spotwo.wms.events.inventory.position.changed",
            "max_ack_pending": 10,
            "max_deliver": 3,
        },
    )


def change() -> TopologyPlanChange:
    return TopologyPlanChange(
        resource="business_consumer",
        field="max_ack_pending",
        current=10,
        target=20,
    )


def start_run(*, owner: str, lease_seconds: float = 120.0):
    journal = PostgresTopologyProvisioningJournal(
        lab.DATABASE_URL,
        owner_id=owner,
        lease_seconds=lease_seconds,
    )
    return journal.start(
        pipeline_id="inventory-position-projection",
        migration_id=migration()["id"],
        resource="business_consumer",
        source_snapshot=source_snapshot(),
        changes=(change(),),
    )


@dataclass
class FakeProvisioning:
    status: str = "succeeded"
    code: str = "topology_target_recovered_and_verified"

    def to_dict(self):
        return {"status": self.status, "code": self.code}


def controller(*, owner: str, recovery_executor=None):
    return UnattendedTopologyReconciliationController(
        database_url=lab.DATABASE_URL,
        nats_url="nats://not-used",
        owner_id=owner,
        lease_seconds=120,
        recovery_executor=recovery_executor,
    )


def no_verify():
    raise AssertionError("verification must not run in this coordination test")


def test_no_active_execution_is_idle_and_does_not_create_a_run():
    spec, topology = configured_pipeline()
    report = controller(owner="reconciler-a").reconcile_once(
        spec,
        target_topology=topology,
        migration_mapping=migration(),
        readiness_verifier=no_verify,
        canary_verifier=no_verify,
        observed_at=BASE,
    )

    assert report.status == "idle"
    assert report.code == "topology_reconciler_no_active_execution"
    with psycopg.connect(lab.DATABASE_URL) as conn:
        assert conn.execute(
            "SELECT count(*) FROM kernel_lab.event_pipeline_topology_provisioning_runs"
        ).fetchone()[0] == 0


def test_live_lease_is_deferred_without_stealing_ownership():
    spec, topology = configured_pipeline()
    run = start_run(owner="active-owner")

    report = controller(owner="reconciler-b").reconcile_once(
        spec,
        target_topology=topology,
        migration_mapping=migration(),
        readiness_verifier=no_verify,
        canary_verifier=no_verify,
        observed_at=datetime.now(timezone.utc),
    )

    assert report.status == "deferred"
    assert report.code == "topology_reconciler_live_lease"
    stored = PostgresTopologyProvisioningJournal(
        lab.DATABASE_URL, owner_id="observer"
    ).get(run.run_id)
    assert stored is not None
    assert stored.lease_owner == "active-owner"
    assert stored.recovery_count == 0


def test_expired_execution_requires_the_reviewed_migration_contract_before_claim():
    spec, topology = configured_pipeline()
    run = start_run(owner="dead-owner")
    with psycopg.connect(lab.DATABASE_URL) as conn:
        conn.execute(
            "UPDATE kernel_lab.event_pipeline_topology_provisioning_runs SET lease_expires_at = clock_timestamp() - interval '1 second' WHERE run_id = %s",
            (run.run_id,),
        )

    report = controller(owner="reconciler-c").reconcile_once(
        spec,
        target_topology=topology,
        migration_mapping=None,
        readiness_verifier=no_verify,
        canary_verifier=no_verify,
        observed_at=datetime.now(timezone.utc),
    )

    assert report.status == "blocked"
    assert report.code == "topology_reconciler_migration_contract_missing"
    stored = PostgresTopologyProvisioningJournal(
        lab.DATABASE_URL, owner_id="observer"
    ).get(run.run_id)
    assert stored is not None
    assert stored.lease_owner == "dead-owner"
    assert stored.recovery_count == 0


def test_expired_execution_rejects_a_different_reviewed_migration_id():
    spec, topology = configured_pipeline()
    run = start_run(owner="dead-owner")
    with psycopg.connect(lab.DATABASE_URL) as conn:
        conn.execute(
            "UPDATE kernel_lab.event_pipeline_topology_provisioning_runs SET lease_expires_at = clock_timestamp() - interval '1 second' WHERE run_id = %s",
            (run.run_id,),
        )

    report = controller(owner="reconciler-d").reconcile_once(
        spec,
        target_topology=topology,
        migration_mapping=migration("different-reviewed-migration"),
        readiness_verifier=no_verify,
        canary_verifier=no_verify,
        observed_at=datetime.now(timezone.utc),
    )

    assert report.status == "blocked"
    assert report.code == "topology_reconciler_migration_contract_mismatch"


def test_expired_execution_is_claimed_once_and_handed_to_recovery_only_executor():
    spec, topology = configured_pipeline()
    run = start_run(owner="dead-owner")
    with psycopg.connect(lab.DATABASE_URL) as conn:
        conn.execute(
            "UPDATE kernel_lab.event_pipeline_topology_provisioning_runs SET lease_expires_at = clock_timestamp() - interval '1 second' WHERE run_id = %s",
            (run.run_id,),
        )

    calls = []

    def recover(**kwargs):
        calls.append(kwargs)
        assert kwargs["run"].run_id == run.run_id
        assert kwargs["run"].recovered is True
        return FakeProvisioning()

    report = controller(owner="reconciler-e", recovery_executor=recover).reconcile_once(
        spec,
        target_topology=topology,
        migration_mapping=migration(),
        readiness_verifier=no_verify,
        canary_verifier=no_verify,
        observed_at=datetime.now(timezone.utc),
    )

    assert report.status == "reconciled"
    assert report.code == "topology_target_recovered_and_verified"
    assert len(calls) == 1
    stored = PostgresTopologyProvisioningJournal(
        lab.DATABASE_URL, owner_id="observer"
    ).get(run.run_id)
    assert stored is not None
    assert stored.lease_owner == "reconciler-e"
    assert stored.recovery_count == 1
    assert stored.attempt_count == 2


def test_cli_once_is_idle_on_an_empty_journal_and_does_not_require_live_nats():
    result = subprocess.run(
        [
            str(ROOT / "bin" / "run-kernel-event-pipeline-topology-reconciler"),
            "--pipeline",
            "inventory-position-projection",
            "--database-url",
            lab.DATABASE_URL,
            "--nats-url",
            "nats://127.0.0.1:1",
            "--tenant-id",
            str(lab.TENANT),
            "--once",
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0
    payload = json.loads(result.stdout)
    assert payload["status"] == "idle"
    assert payload["recovery_only"] is True
    assert payload["starts_new_migrations"] is False
