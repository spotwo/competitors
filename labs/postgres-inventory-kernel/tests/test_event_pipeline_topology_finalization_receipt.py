from __future__ import annotations

import json
import os
import subprocess
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid4

import psycopg
import pytest
import yaml

import conftest as lab
from event_pipeline_readiness import load_registry
from event_pipeline_topology import TopologyResourceSnapshot
from event_pipeline_topology_config import DeploymentTopologyConfig, pipeline_topology_mapping
from event_pipeline_topology_finalization_receipt import (
    PostgresTopologyFinalizationReceiptStore,
    TopologyFinalizationReceiptController,
    canonical_fingerprint,
    live_contract_payload,
    render_prometheus,
    target_contract_payload,
)

ROOT = Path(__file__).resolve().parents[3]
REGISTRY = ROOT / "operations" / "event-pipelines" / "registry.yml"
BASE = datetime(2026, 8, 19, 13, 30, tzinfo=timezone.utc)
RUN_ID = UUID("00000000-0000-0000-0000-00000000aa01")
INTERVENTION_ID = UUID("00000000-0000-0000-0000-00000000aa02")
MIGRATION_ID = "business-ack-window-5-to-10"


def cli_env() -> dict[str, str]:
    env = dict(os.environ)
    env.pop("KERNEL_LAB_DATABASE_URL", None)
    env.pop("KERNEL_LAB_NATS_URL", None)
    env.pop("KERNEL_LAB_CANARY_WATCHDOG_STATE_PATH", None)
    return env


def configured_pipeline():
    registry = load_registry(REGISTRY)
    with REGISTRY.open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)
    spec = registry.get("inventory-position-projection")
    return spec, pipeline_topology_mapping(raw, spec.pipeline_id)


def migration() -> dict:
    return {
        "id": MIGRATION_ID,
        "valid_until": "2099-01-01T00:00:00Z",
        "from_overrides": {"business_consumer": {"max_ack_pending": 5}},
    }


def insert_run(*, state: str = "completed", migration_id: str = MIGRATION_ID) -> None:
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
              '{"resource":"business_consumer","name":"POSITION_PROJECTOR","exists":true,"fields":{"max_ack_pending":5}}'::jsonb,
              '[{"resource":"business_consumer","field":"max_ack_pending","current":5,"target":10}]'::jsonb,
              %s, %s, %s, %s, %s, %s
            )
            """,
            (
                RUN_ID,
                migration_id,
                state,
                "f" * 64,
                None if terminal else "receipt-test-owner",
                None if terminal else BASE + timedelta(minutes=5),
                BASE - timedelta(minutes=5),
                BASE - timedelta(seconds=20),
                BASE - timedelta(seconds=20),
            ),
        )


def insert_resolved_target_intervention() -> None:
    with psycopg.connect(lab.DATABASE_URL) as conn:
        conn.execute(
            """
            INSERT INTO kernel_lab.event_pipeline_topology_interventions (
              intervention_id, run_id, pipeline_id, migration_id, resource,
              state, evidence, evidence_fingerprint, proposed_action,
              proposed_by, proposed_at, confirmed_by, confirmed_at, resolution_code
            ) VALUES (
              %s, %s, 'inventory-position-projection', %s, 'business_consumer',
              'resolved_target', '{}'::jsonb, %s, 'accept_target',
              'operator-a', %s, 'operator-b', %s, 'target-accepted'
            )
            """,
            (
                INTERVENTION_ID,
                RUN_ID,
                MIGRATION_ID,
                "e" * 64,
                BASE - timedelta(minutes=2),
                BASE - timedelta(minutes=1),
            ),
        )


def exact_resources(spec, topology, *, business_max_ack_pending: int | None = None):
    expectation = DeploymentTopologyConfig.from_mapping(topology).expectation(spec)
    business_max_ack_pending = (
        expectation.business_consumer.max_ack_pending
        if business_max_ack_pending is None
        else business_max_ack_pending
    )
    return (
        TopologyResourceSnapshot(
            "stream",
            expectation.stream_name,
            True,
            {
                "subjects": tuple(sorted(expectation.stream.subjects)),
                "storage": expectation.stream.storage,
                "retention": expectation.stream.retention,
                "replicas": expectation.stream.replicas,
                "duplicate_window_seconds": float(
                    expectation.stream.duplicate_window_seconds
                ),
            },
        ),
        TopologyResourceSnapshot(
            "business_consumer",
            expectation.business_consumer.durable_name,
            True,
            {
                "delivery_mode": expectation.business_consumer.delivery_mode,
                "deliver_policy": expectation.business_consumer.deliver_policy,
                "ack_policy": expectation.business_consumer.ack_policy,
                "filter_subject": expectation.business_consumer.filter_subject,
                "max_ack_pending": business_max_ack_pending,
                "max_deliver": expectation.business_consumer.max_deliver,
            },
        ),
        TopologyResourceSnapshot(
            "canary_consumer",
            expectation.canary_consumer.durable_name,
            True,
            {
                "delivery_mode": expectation.canary_consumer.delivery_mode,
                "deliver_policy": expectation.canary_consumer.deliver_policy,
                "ack_policy": expectation.canary_consumer.ack_policy,
                "filter_subject": expectation.canary_consumer.filter_subject,
                "max_ack_pending": expectation.canary_consumer.max_ack_pending,
                "max_deliver": expectation.canary_consumer.max_deliver,
            },
        ),
    )


class FakeFinalizationInspector:
    def __init__(
        self,
        *,
        safe: bool = True,
        code: str = "topology_migration_finalization_ready",
        lineage: str = "completed",
    ):
        self.safe = safe
        self.code = code
        self.lineage = lineage

    def inspect(self, *_args, **_kwargs):
        return SimpleNamespace(
            safe_to_remove_topology_migration=self.safe,
            code=self.code,
            lineage=self.lineage,
        )


class FakeContractInspector:
    def __init__(
        self,
        spec,
        topology,
        *,
        matched: str = "target",
        target_status: str = "ok",
        business_max_ack_pending: int | None = None,
    ):
        self.spec = spec
        self.topology = topology
        self.matched = matched
        self.target_status = target_status
        self.business_max_ack_pending = business_max_ack_pending

    def inspect(self, *_args, **_kwargs):
        return SimpleNamespace(
            matched=self.matched,
            target=SimpleNamespace(
                status=self.target_status,
                resources=exact_resources(
                    self.spec,
                    self.topology,
                    business_max_ack_pending=self.business_max_ack_pending,
                ),
            ),
        )


class FakeReadinessCollector:
    def __init__(self, status: str = "ready"):
        self.status = status

    def collect(self, *_args, **_kwargs):
        return SimpleNamespace(status=self.status)


def controller(
    spec,
    topology,
    *,
    finalization_safe: bool = True,
    lineage: str = "completed",
    matched: str = "target",
    target_status: str = "ok",
    business_max_ack_pending: int | None = None,
    readiness: str = "ready",
):
    return TopologyFinalizationReceiptController(
        database_url=lab.DATABASE_URL,
        nats_url="nats://not-used",
        finalization_inspector=FakeFinalizationInspector(
            safe=finalization_safe,
            code=(
                "topology_migration_finalization_ready"
                if finalization_safe
                else "topology_migration_finalization_readiness_not_ready"
            ),
            lineage=lineage,
        ),
        contract_inspector=FakeContractInspector(
            spec,
            topology,
            matched=matched,
            target_status=target_status,
            business_max_ack_pending=business_max_ack_pending,
        ),
        readiness_collector=FakeReadinessCollector(readiness),
    )


def issue_receipt(spec, topology, **kwargs):
    insert_run(state="completed")
    return controller(spec, topology, **kwargs).issue(
        spec,
        target_topology=topology,
        migration_mapping=migration(),
        watchdog_state={},
        observed_at=BASE,
    )


def test_exact_target_and_live_contract_have_the_same_canonical_fingerprint():
    spec, topology = configured_pipeline()
    contract = FakeContractInspector(spec, topology).inspect()

    target = target_contract_payload(spec, topology)
    live = live_contract_payload(spec, contract)

    assert canonical_fingerprint(target) == canonical_fingerprint(live)
    assert target["stream"]["duplicate_window_seconds"] == 120.0


def test_issue_requires_a_migration_contract():
    spec, topology = configured_pipeline()
    report = controller(spec, topology).issue(
        spec,
        target_topology=topology,
        migration_mapping=None,
        watchdog_state=None,
        observed_at=BASE,
    )

    assert report.status == "blocked"
    assert report.code == "topology_finalization_receipt_migration_missing"


def test_issue_requires_the_existing_finalization_gate_to_be_safe():
    spec, topology = configured_pipeline()
    insert_run(state="completed")
    report = controller(spec, topology, finalization_safe=False).issue(
        spec,
        target_topology=topology,
        migration_mapping=migration(),
        watchdog_state={},
        observed_at=BASE,
    )

    assert report.status == "blocked"
    assert report.code == "topology_migration_finalization_readiness_not_ready"


def test_completed_rollout_issues_immutable_receipt_bound_to_target_and_live_fingerprint():
    spec, topology = configured_pipeline()
    report = issue_receipt(spec, topology)

    assert report.status == "issued"
    assert report.code == "topology_finalization_receipt_issued"
    assert report.receipt is not None
    assert report.receipt.run_id == RUN_ID
    assert report.receipt.lineage == "completed"
    assert report.receipt.target_fingerprint == report.receipt.live_fingerprint
    assert report.receipt.migration_fingerprint == canonical_fingerprint(migration())
    assert report.receipt.to_dict()["immutable"] is True


def test_receipt_issue_is_idempotent_for_the_same_reviewed_evidence():
    spec, topology = configured_pipeline()
    first = issue_receipt(spec, topology)
    second = controller(spec, topology).issue(
        spec,
        target_topology=topology,
        migration_mapping=migration(),
        watchdog_state={},
        observed_at=BASE,
    )

    assert second.status == "issued"
    assert second.code == "topology_finalization_receipt_already_issued"
    assert second.receipt.receipt_id == first.receipt.receipt_id


def test_same_migration_id_cannot_be_rebound_to_a_different_target():
    spec, topology = configured_pipeline()
    first = issue_receipt(spec, topology)
    changed = deepcopy(topology)
    changed["business_consumer"]["max_deliver"] = 4
    second = controller(spec, changed).issue(
        spec,
        target_topology=changed,
        migration_mapping=migration(),
        watchdog_state={},
        observed_at=BASE,
    )

    assert first.receipt is not None
    assert second.status == "blocked"
    assert second.code == "topology_finalization_receipt_conflict"
    assert second.receipt.receipt_id == first.receipt.receipt_id


def test_receipt_rows_are_database_immutable():
    spec, topology = configured_pipeline()
    receipt = issue_receipt(spec, topology).receipt

    with psycopg.connect(lab.DATABASE_URL) as conn:
        with pytest.raises(psycopg.errors.RaiseException):
            conn.execute(
                """
                UPDATE kernel_lab.event_pipeline_topology_finalization_receipts
                SET finalization_code = 'tampered'
                WHERE receipt_id = %s
                """,
                (receipt.receipt_id,),
            )


def test_post_merge_verification_refuses_to_close_while_compatibility_stanza_is_present():
    spec, topology = configured_pipeline()
    receipt = issue_receipt(spec, topology).receipt
    report = controller(spec, topology).verify_post_merge(
        spec,
        receipt_id=receipt.receipt_id,
        target_topology=topology,
        migration_mapping=migration(),
        watchdog_state={},
        observed_at=BASE,
    )

    assert report.status == "blocked"
    assert report.code == "topology_finalization_cleanup_not_merged"
    assert report.verification.status == "blocked"


def test_post_merge_verification_binds_cleanup_to_the_same_registry_target():
    spec, topology = configured_pipeline()
    receipt = issue_receipt(spec, topology).receipt
    changed = deepcopy(topology)
    changed["business_consumer"]["max_deliver"] = 4
    report = controller(spec, changed).verify_post_merge(
        spec,
        receipt_id=receipt.receipt_id,
        target_topology=changed,
        migration_mapping=None,
        watchdog_state={},
        observed_at=BASE,
    )

    assert report.status == "blocked"
    assert report.code == "topology_finalization_registry_target_changed"


def test_post_merge_verification_rejects_live_configuration_change():
    spec, topology = configured_pipeline()
    receipt = issue_receipt(spec, topology).receipt
    report = controller(
        spec,
        topology,
        business_max_ack_pending=9,
    ).verify_post_merge(
        spec,
        receipt_id=receipt.receipt_id,
        target_topology=topology,
        migration_mapping=None,
        watchdog_state={},
        observed_at=BASE,
    )

    assert report.status == "blocked"
    assert report.code == "topology_finalization_post_merge_live_fingerprint_changed"


def test_post_merge_verification_requires_fresh_ready_state():
    spec, topology = configured_pipeline()
    receipt = issue_receipt(spec, topology).receipt
    report = controller(spec, topology, readiness="degraded").verify_post_merge(
        spec,
        receipt_id=receipt.receipt_id,
        target_topology=topology,
        migration_mapping=None,
        watchdog_state={},
        observed_at=BASE,
    )

    assert report.status == "blocked"
    assert report.code == "topology_finalization_post_merge_readiness_not_ready"
    assert report.verification.readiness_status == "degraded"


def test_exact_post_merge_state_closes_the_migration_lifecycle():
    spec, topology = configured_pipeline()
    receipt = issue_receipt(spec, topology).receipt
    report = controller(spec, topology).verify_post_merge(
        spec,
        receipt_id=receipt.receipt_id,
        target_topology=topology,
        migration_mapping=None,
        watchdog_state={},
        observed_at=BASE,
    )

    assert report.status == "closed"
    assert report.code == "topology_migration_closed"
    assert report.migration_closed is True if hasattr(report, "migration_closed") else report.closed
    assert report.verification.status == "closed"
    assert report.verification.registry_target_fingerprint == receipt.target_fingerprint
    assert report.verification.live_fingerprint == receipt.live_fingerprint
    assert report.verification.readiness_status == "ready"


def test_closed_post_merge_verification_is_idempotent():
    spec, topology = configured_pipeline()
    receipt = issue_receipt(spec, topology).receipt
    first = controller(spec, topology).verify_post_merge(
        spec,
        receipt_id=receipt.receipt_id,
        target_topology=topology,
        migration_mapping=None,
        watchdog_state={},
        observed_at=BASE,
    )
    second = controller(spec, topology, readiness="not_ready").verify_post_merge(
        spec,
        receipt_id=receipt.receipt_id,
        target_topology=topology,
        migration_mapping=None,
        watchdog_state={},
        observed_at=BASE + timedelta(minutes=1),
    )

    assert second.status == "closed"
    assert second.code == "topology_migration_already_closed"
    assert second.verification.verification_id == first.verification.verification_id


def test_resolved_target_manual_intervention_lineage_can_close():
    spec, topology = configured_pipeline()
    insert_run(state="manual_intervention")
    insert_resolved_target_intervention()
    issued = controller(spec, topology, lineage="resolved_target").issue(
        spec,
        target_topology=topology,
        migration_mapping=migration(),
        watchdog_state={},
        observed_at=BASE,
    )

    assert issued.status == "issued"
    assert issued.receipt.lineage == "resolved_target"
    closed = controller(spec, topology).verify_post_merge(
        spec,
        receipt_id=issued.receipt.receipt_id,
        target_topology=topology,
        migration_mapping=None,
        watchdog_state={},
        observed_at=BASE,
    )
    assert closed.status == "closed"


def test_prometheus_keeps_receipt_run_and_migration_identity_out_of_labels():
    spec, topology = configured_pipeline()
    receipt = issue_receipt(spec, topology).receipt
    report = controller(spec, topology).verify_post_merge(
        spec,
        receipt_id=receipt.receipt_id,
        target_topology=topology,
        migration_mapping=None,
        watchdog_state={},
        observed_at=BASE,
    )
    rendered = render_prometheus(report)

    assert 'pipeline="inventory-position-projection"' in rendered
    assert "topology_migration_closed" in rendered
    assert str(receipt.receipt_id) not in rendered
    assert str(receipt.run_id) not in rendered
    assert MIGRATION_ID not in rendered


def test_verification_rows_are_append_only():
    spec, topology = configured_pipeline()
    receipt = issue_receipt(spec, topology).receipt
    report = controller(spec, topology).verify_post_merge(
        spec,
        receipt_id=receipt.receipt_id,
        target_topology=topology,
        migration_mapping=None,
        watchdog_state={},
        observed_at=BASE,
    )

    with psycopg.connect(lab.DATABASE_URL) as conn:
        with pytest.raises(psycopg.errors.RaiseException):
            conn.execute(
                """
                DELETE FROM kernel_lab.event_pipeline_topology_finalization_verifications
                WHERE verification_id = %s
                """,
                (report.verification.verification_id,),
            )


def test_current_steady_state_cli_refuses_to_issue_a_receipt_without_a_migration():
    result = subprocess.run(
        [
            str(ROOT / "bin" / "record-kernel-event-pipeline-topology-finalization"),
            "--pipeline",
            "inventory-position-projection",
            "--check",
            "issue-receipt",
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
        env=cli_env(),
    )

    assert result.returncode == 2
    payload = json.loads(result.stdout)
    assert payload["status"] == "blocked"
    assert payload["code"] == "topology_finalization_receipt_migration_missing"


def test_post_merge_cli_fails_closed_for_unknown_receipt_before_touching_nats():
    result = subprocess.run(
        [
            str(ROOT / "bin" / "record-kernel-event-pipeline-topology-finalization"),
            "--pipeline",
            "inventory-position-projection",
            "--database-url",
            lab.DATABASE_URL,
            "--check",
            "verify-post-merge",
            "--receipt",
            str(uuid4()),
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
        env=cli_env(),
    )

    assert result.returncode == 2
    payload = json.loads(result.stdout)
    assert payload["code"] == "topology_finalization_receipt_missing"