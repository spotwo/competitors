from __future__ import annotations

import json
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import psycopg
import yaml

import conftest as lab
from event_pipeline_readiness import load_registry
from event_pipeline_topology_config import pipeline_topology_mapping
from event_pipeline_topology_finalization import (
    TopologyMigrationFinalizationInspector,
    render_prometheus,
)

ROOT = Path(__file__).resolve().parents[3]
REGISTRY = ROOT / "operations" / "event-pipelines" / "registry.yml"
BASE = datetime(2026, 8, 19, 13, 0, tzinfo=timezone.utc)
RUN_ID = UUID("00000000-0000-0000-0000-00000000f001")
INTERVENTION_ID = UUID("00000000-0000-0000-0000-00000000f101")
MIGRATION_ID = "business-ack-window-10-to-20"


def configured_pipeline():
    registry = load_registry(REGISTRY)
    with REGISTRY.open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)
    spec = registry.get("inventory-position-projection")
    return spec, pipeline_topology_mapping(raw, spec.pipeline_id)


def migration(migration_id: str = MIGRATION_ID) -> dict:
    return {
        "id": migration_id,
        "valid_until": "2099-01-01T00:00:00Z",
        "from_overrides": {"business_consumer": {"max_ack_pending": 10}},
    }


def insert_run(*, state: str, migration_id: str = MIGRATION_ID) -> None:
    terminal = state in {"completed", "rolled_back", "failed", "manual_intervention"}
    with psycopg.connect(lab.DATABASE_URL) as conn:
        conn.execute(
            """
            INSERT INTO kernel_lab.event_pipeline_topology_provisioning_runs (
              run_id, pipeline_id, migration_id, resource, state, state_code,
              source_snapshot, changes, intent_fingerprint,
              lease_owner, lease_expires_at, started_at, updated_at, state_changed_at
            ) VALUES (
              %s, 'inventory-position-projection', %s, 'business_consumer', %s, NULL,
              '{"resource":"business_consumer","name":"POSITION_PROJECTOR","exists":true,"fields":{"max_ack_pending":10}}'::jsonb,
              '[{"resource":"business_consumer","field":"max_ack_pending","current":10,"target":20}]'::jsonb,
              %s, %s, %s, %s, %s, %s
            )
            """,
            (
                RUN_ID,
                migration_id,
                state,
                "f" * 64,
                None if terminal else "finalization-test-owner",
                None if terminal else BASE + timedelta(minutes=5),
                BASE - timedelta(minutes=2),
                BASE - timedelta(seconds=10),
                BASE - timedelta(seconds=10),
            ),
        )


def insert_intervention(state: str) -> None:
    with psycopg.connect(lab.DATABASE_URL) as conn:
        conn.execute(
            """
            INSERT INTO kernel_lab.event_pipeline_topology_interventions (
              intervention_id, run_id, pipeline_id, migration_id, resource,
              state, evidence, evidence_fingerprint, proposed_action,
              proposed_by, proposed_at, confirmed_by, confirmed_at, resolution_code
            ) VALUES (
              %s, %s, 'inventory-position-projection', %s, 'business_consumer',
              %s, '{}'::jsonb, %s, 'accept_target',
              'operator-a', %s, 'operator-b', %s, 'test-resolution'
            )
            """,
            (
                INTERVENTION_ID,
                RUN_ID,
                MIGRATION_ID,
                state,
                "e" * 64,
                BASE - timedelta(minutes=1),
                BASE - timedelta(seconds=30),
            ),
        )


class FakeContractInspector:
    def __init__(self, *, matched: str = "target", target_status: str = "ok"):
        self.matched = matched
        self.target_status = target_status

    def inspect(self, *_args, **_kwargs):
        return SimpleNamespace(
            matched=self.matched,
            target=SimpleNamespace(status=self.target_status),
        )


class FakeReadinessCollector:
    def __init__(self, status: str = "ready"):
        self.status = status

    def collect(self, *_args, **_kwargs):
        return SimpleNamespace(status=self.status)


def inspector(*, matched: str = "target", target_status: str = "ok", readiness: str = "ready"):
    return TopologyMigrationFinalizationInspector(
        database_url=lab.DATABASE_URL,
        nats_url="nats://not-used",
        contract_inspector=FakeContractInspector(
            matched=matched, target_status=target_status
        ),
        readiness_collector=FakeReadinessCollector(readiness),
    )


def inspect_current(*, migration_mapping=... , **kwargs):
    spec, topology = configured_pipeline()
    if migration_mapping is ...:
        migration_mapping = migration()
    return inspector(**kwargs).inspect(
        spec,
        target_topology=topology,
        migration_mapping=migration_mapping,
        watchdog_state={},
        observed_at=BASE,
    )


def test_absent_migration_is_already_finalized_without_runtime_dependencies():
    spec, topology = configured_pipeline()
    report = TopologyMigrationFinalizationInspector(
        database_url=None,
        nats_url=None,
    ).inspect(
        spec,
        target_topology=topology,
        migration_mapping=None,
        watchdog_state=None,
        observed_at=BASE,
    )

    assert report.status == "finalized"
    assert report.code == "topology_migration_not_present"
    assert report.safe_to_remove_topology_migration is True


def test_runtime_inputs_fail_closed_when_a_migration_exists():
    spec, topology = configured_pipeline()
    report = TopologyMigrationFinalizationInspector(
        database_url=None,
        nats_url=None,
    ).inspect(
        spec,
        target_topology=topology,
        migration_mapping=migration(),
        watchdog_state=None,
        observed_at=BASE,
    )

    assert report.status == "blocked"
    assert report.code == "topology_migration_finalization_runtime_missing"


def test_active_provisioning_execution_blocks_finalization():
    insert_run(state="applying")
    report = inspect_current()

    assert report.status == "blocked"
    assert report.code == "topology_migration_provisioning_active"
    assert report.provisioning_state == "applying"


def test_missing_or_different_migration_history_blocks_finalization():
    missing = inspect_current()
    assert missing.code == "topology_migration_finalization_history_missing"

    insert_run(state="completed", migration_id="different-migration")
    mismatch = inspect_current()
    assert mismatch.code == "topology_migration_finalization_history_mismatch"


def test_rolled_back_or_failed_history_cannot_be_finalized_from_live_state_alone():
    insert_run(state="rolled_back")
    report = inspect_current()

    assert report.code == "topology_migration_finalization_history_not_successful"
    assert report.safe_to_remove_topology_migration is False


def test_completed_run_exact_target_and_ready_pipeline_is_safe_to_finalize():
    insert_run(state="completed")
    report = inspect_current()

    assert report.status == "ready"
    assert report.code == "topology_migration_finalization_ready"
    assert report.safe_to_remove_topology_migration is True
    assert report.lineage == "completed"
    assert report.live_match == "target"
    assert report.readiness_status == "ready"
    assert report.to_dict()["registry_change"]["operation"] == "remove_topology_migration"


def test_source_still_live_keeps_migration_compatibility_contract():
    insert_run(state="completed")
    report = inspect_current(matched="source", target_status="critical")

    assert report.status == "blocked"
    assert report.code == "topology_migration_source_still_live"
    assert report.live_match == "source"


def test_unknown_live_topology_blocks_finalization():
    insert_run(state="completed")
    report = inspect_current(matched="neither", target_status="critical")

    assert report.code == "topology_migration_live_topology_not_target"


def test_manual_intervention_requires_resolved_target_lineage():
    insert_run(state="manual_intervention")
    missing = inspect_current()
    assert missing.code == "topology_migration_manual_intervention_resolution_missing"

    insert_intervention("resolved_source")
    source = inspect_current()
    assert source.code == "topology_migration_manual_intervention_not_target"


def test_resolved_target_manual_intervention_can_finalize_after_fresh_gates():
    insert_run(state="manual_intervention")
    insert_intervention("resolved_target")
    report = inspect_current()

    assert report.status == "ready"
    assert report.lineage == "resolved_target"
    assert report.safe_to_remove_topology_migration is True


def test_non_ready_deployment_blocks_contract_removal_even_at_exact_target():
    insert_run(state="completed")
    report = inspect_current(readiness="degraded")

    assert report.status == "blocked"
    assert report.code == "topology_migration_finalization_readiness_not_ready"
    assert report.readiness_status == "degraded"


def test_prometheus_keeps_migration_and_execution_identity_out_of_labels():
    insert_run(state="completed")
    report = inspect_current()
    rendered = render_prometheus(report)

    assert 'pipeline="inventory-position-projection"' in rendered
    assert "topology_migration_removal_safe" in rendered
    assert MIGRATION_ID not in rendered
    assert str(RUN_ID) not in rendered
    assert "operator-a" not in rendered


def test_cli_current_registry_without_migration_is_finalized_and_needs_no_runtime():
    result = subprocess.run(
        [
            str(ROOT / "bin" / "inspect-kernel-event-pipeline-topology-finalization"),
            "--pipeline",
            "inventory-position-projection",
            "--check",
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
        env={},
    )

    assert result.returncode == 0
    payload = json.loads(result.stdout)
    assert payload["status"] == "finalized"
    assert payload["code"] == "topology_migration_not_present"
    assert payload["safe_to_remove_topology_migration"] is True
