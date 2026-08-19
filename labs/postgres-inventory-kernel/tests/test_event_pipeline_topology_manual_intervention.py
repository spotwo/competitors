from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

import yaml

import conftest as lab
from event_pipeline_readiness import load_registry
from event_pipeline_topology import TopologyResourceSnapshot
from event_pipeline_topology_config import pipeline_topology_mapping
from event_pipeline_topology_manual_intervention import (
    ExplicitTopologyManualInterventionController,
    PostgresTopologyManualInterventionStore,
    TopologyManualInterventionError,
    inspect_manual_intervention_health,
    render_prometheus,
)
from event_pipeline_topology_preflight import TopologyPlanChange
from event_pipeline_topology_provisioning_journal import PostgresTopologyProvisioningJournal

ROOT = Path(__file__).resolve().parents[3]
REGISTRY = ROOT / "operations" / "event-pipelines" / "registry.yml"


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


def make_manual_run():
    journal = PostgresTopologyProvisioningJournal(
        lab.DATABASE_URL, owner_id="failed-provisioner", lease_seconds=120
    )
    run = journal.start(
        pipeline_id="inventory-position-projection",
        migration_id=migration()["id"],
        resource="business_consumer",
        source_snapshot=source_snapshot(),
        changes=(
            TopologyPlanChange(
                resource="business_consumer",
                field="max_ack_pending",
                current=10,
                target=20,
            ),
        ),
    )
    return journal.transition(
        run.run_id,
        "manual_intervention",
        code="topology_recovery_manual_intervention_required",
    )


def live_snapshot(max_ack_pending: int = 15) -> TopologyResourceSnapshot:
    value = source_snapshot().to_dict()
    fields = dict(value["fields"])
    fields["max_ack_pending"] = max_ack_pending
    return TopologyResourceSnapshot(
        resource=value["resource"],
        name=value["name"],
        exists=True,
        fields=fields,
    )


class FakeInspector:
    def __init__(self, *, matched="neither", max_ack_pending=15):
        self.matched = matched
        self.max_ack_pending = max_ack_pending
        self.calls = 0

    def inspect(self, *_args, **_kwargs):
        self.calls += 1
        target_status = "ok" if self.matched == "target" else "critical"
        return SimpleNamespace(
            matched=self.matched,
            target=SimpleNamespace(
                status=target_status,
                resources=(live_snapshot(self.max_ack_pending),),
            ),
        )


class FakeMutator:
    def __init__(self, inspector: FakeInspector):
        self.inspector = inspector
        self.calls = 0

    def rollback_to_snapshot(self, **_kwargs):
        self.calls += 1
        self.inspector.matched = "source"
        self.inspector.max_ack_pending = 10


@dataclass
class Status:
    status: str


def controller(inspector: FakeInspector, mutator=None):
    return ExplicitTopologyManualInterventionController(
        database_url=lab.DATABASE_URL,
        nats_url="nats://not-used",
        contract_inspector=inspector,
        mutator=mutator or FakeMutator(inspector),
    )


def detect(controller_value, spec, topology):
    return controller_value.detect(
        spec,
        target_topology=topology,
        migration_mapping=migration(),
    ).intervention


def approve(controller_value, intervention, action):
    controller_value.propose(
        intervention.intervention_id,
        action=action,
        operator="operator-a",
    )
    return controller_value.confirm(
        intervention.intervention_id,
        operator="operator-b",
    ).intervention


def test_detect_is_idle_without_a_manual_intervention_run():
    spec, topology = configured_pipeline()
    report = controller(FakeInspector()).detect(
        spec,
        target_topology=topology,
        migration_mapping=migration(),
    )
    assert report.status == "idle"
    assert report.code == "topology_manual_intervention_not_required"


def test_detect_requires_matching_reviewed_migration_contract():
    make_manual_run()
    spec, topology = configured_pipeline()
    value = controller(FakeInspector())
    try:
        value.detect(spec, target_topology=topology, migration_mapping=None)
    except TopologyManualInterventionError as exc:
        assert exc.code == "topology_manual_intervention_migration_contract_missing"
    else:
        raise AssertionError("missing migration contract must fail closed")


def test_detection_is_idempotent_while_intervention_is_active():
    make_manual_run()
    spec, topology = configured_pipeline()
    value = controller(FakeInspector())
    first = detect(value, spec, topology)
    second = detect(value, spec, topology)
    assert first is not None and second is not None
    assert first.intervention_id == second.intervention_id


def test_proposal_requires_a_distinct_second_confirmer():
    make_manual_run()
    spec, topology = configured_pipeline()
    value = controller(FakeInspector())
    intervention = detect(value, spec, topology)
    assert intervention is not None
    value.propose(intervention.intervention_id, action="abort", operator="operator-a")
    try:
        value.confirm(intervention.intervention_id, operator="operator-a")
    except TopologyManualInterventionError as exc:
        assert exc.code == "topology_manual_intervention_second_operator_required"
    else:
        raise AssertionError("same operator must not confirm their own proposal")


def test_stale_evidence_fails_before_any_topology_mutation():
    make_manual_run()
    spec, topology = configured_pipeline()
    inspector = FakeInspector(max_ack_pending=15)
    mutator = FakeMutator(inspector)
    value = controller(inspector, mutator)
    intervention = detect(value, spec, topology)
    assert intervention is not None
    approve(value, intervention, "restore_source")
    inspector.max_ack_pending = 16

    report = value.execute(
        intervention.intervention_id,
        spec,
        target_topology=topology,
        migration_mapping=migration(),
        canary_verifier=lambda: Status("ok"),
        readiness_verifier=lambda: Status("ready"),
    )

    assert report.status == "failed"
    assert report.code == "topology_manual_intervention_evidence_stale"
    assert mutator.calls == 0


def test_restore_source_mutates_only_after_confirmation_and_verifies_source():
    make_manual_run()
    spec, topology = configured_pipeline()
    inspector = FakeInspector(max_ack_pending=15)
    mutator = FakeMutator(inspector)
    value = controller(inspector, mutator)
    intervention = detect(value, spec, topology)
    assert intervention is not None
    approve(value, intervention, "restore_source")

    report = value.execute(
        intervention.intervention_id,
        spec,
        target_topology=topology,
        migration_mapping=migration(),
        canary_verifier=lambda: Status("ok"),
        readiness_verifier=lambda: Status("ready"),
    )

    assert report.status == "resolved"
    assert report.code == "topology_manual_intervention_source_restored"
    assert report.mutates_topology is True
    assert report.intervention is not None
    assert report.intervention.state == "resolved_source"
    assert mutator.calls == 1


def test_accept_target_never_calls_mutator_and_requires_exact_target():
    make_manual_run()
    spec, topology = configured_pipeline()
    inspector = FakeInspector(matched="target", max_ack_pending=20)
    mutator = FakeMutator(inspector)
    value = controller(inspector, mutator)
    intervention = detect(value, spec, topology)
    assert intervention is not None
    approve(value, intervention, "accept_target")

    report = value.execute(
        intervention.intervention_id,
        spec,
        target_topology=topology,
        migration_mapping=migration(),
        canary_verifier=lambda: Status("ok"),
        readiness_verifier=lambda: Status("ready"),
    )

    assert report.status == "resolved"
    assert report.intervention is not None
    assert report.intervention.state == "resolved_target"
    assert report.mutates_topology is False
    assert mutator.calls == 0


def test_abort_is_confirmed_and_does_not_touch_topology():
    make_manual_run()
    spec, topology = configured_pipeline()
    inspector = FakeInspector()
    mutator = FakeMutator(inspector)
    value = controller(inspector, mutator)
    intervention = detect(value, spec, topology)
    assert intervention is not None
    approve(value, intervention, "abort")

    report = value.execute(
        intervention.intervention_id,
        spec,
        target_topology=topology,
        migration_mapping=migration(),
        canary_verifier=lambda: (_ for _ in ()).throw(AssertionError("must not run")),
        readiness_verifier=lambda: (_ for _ in ()).throw(AssertionError("must not run")),
    )

    assert report.status == "aborted"
    assert report.intervention is not None
    assert report.intervention.state == "aborted"
    assert mutator.calls == 0


def test_failed_attempt_can_be_detected_again_but_resolved_attempt_closes_run():
    run = make_manual_run()
    spec, topology = configured_pipeline()
    inspector = FakeInspector(max_ack_pending=15)
    value = controller(inspector)
    intervention = detect(value, spec, topology)
    assert intervention is not None
    approve(value, intervention, "accept_target")
    failed = value.execute(
        intervention.intervention_id,
        spec,
        target_topology=topology,
        migration_mapping=migration(),
        canary_verifier=lambda: Status("ok"),
        readiness_verifier=lambda: Status("ready"),
    )
    assert failed.status == "failed"

    retry = detect(value, spec, topology)
    assert retry is not None
    assert retry.intervention_id != intervention.intervention_id
    assert retry.run_id == run.run_id


def test_intervention_health_and_prometheus_do_not_expose_operator_or_ids():
    make_manual_run()
    spec, topology = configured_pipeline()
    value = controller(FakeInspector())
    intervention = detect(value, spec, topology)
    assert intervention is not None
    value.propose(intervention.intervention_id, action="abort", operator="alice@example")

    health = inspect_manual_intervention_health(
        lab.DATABASE_URL, pipeline_id=spec.pipeline_id
    )
    metrics = render_prometheus(health)

    assert health.status == "critical"
    assert "alice@example" not in metrics
    assert str(intervention.intervention_id) not in metrics
    assert str(intervention.run_id) not in metrics


def test_inspector_cli_reports_clear_for_empty_history():
    result = subprocess.run(
        [
            str(ROOT / "bin" / "inspect-kernel-event-pipeline-topology-interventions"),
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
    assert payload["status"] == "clear"


def test_store_keeps_only_one_active_intervention_per_manual_run():
    run = make_manual_run()
    store = PostgresTopologyManualInterventionStore(lab.DATABASE_URL)
    first = store.create_detected(
        run=run,
        evidence={"snapshot": {"x": 1}},
        evidence_fingerprint="0" * 64,
    )
    second = store.create_detected(
        run=run,
        evidence={"snapshot": {"x": 2}},
        evidence_fingerprint="1" * 64,
    )
    assert first.intervention_id == second.intervention_id
