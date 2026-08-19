from __future__ import annotations

import sys
from copy import deepcopy
from pathlib import Path
from uuid import UUID

import yaml

ROOT = Path(__file__).resolve().parents[3]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from event_pipeline_readiness import load_registry
from event_pipeline_topology_config import pipeline_topology_mapping
from event_pipeline_topology_finalization_receipt import target_contract_payload as runtime_target_payload
from topology_finalization_pr_guard import (
    REGISTRY_PATH,
    build_cleanup_reference,
    canonical_fingerprint,
    deterministic_reference_path,
    evaluate_cleanup_pr,
    pipelines_by_id,
    target_contract_payload,
    validate_reference_structure,
)

REGISTRY = ROOT / "operations" / "event-pipelines" / "registry.yml"
PIPELINE_ID = "inventory-position-projection"
MIGRATION_ID = "business-ack-window-5-to-10"
RECEIPT_ID = "00000000-0000-0000-0000-00000000bb01"
RUN_ID = "00000000-0000-0000-0000-00000000bb02"


def raw_registry() -> dict:
    with REGISTRY.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def migration() -> dict:
    return {
        "id": MIGRATION_ID,
        "valid_until": "2099-01-01T00:00:00Z",
        "from_overrides": {"business_consumer": {"max_ack_pending": 5}},
    }


def cleanup_pair() -> tuple[dict, dict]:
    head = raw_registry()
    base = deepcopy(head)
    pipelines_by_id(base)[PIPELINE_ID]["topology_migration"] = migration()
    return base, head


def reference_for(base: dict, head: dict) -> dict:
    head_pipeline = pipelines_by_id(head)[PIPELINE_ID]
    receipt = {
        "receipt_id": RECEIPT_ID,
        "pipeline": PIPELINE_ID,
        "migration_id": MIGRATION_ID,
        "run_id": RUN_ID,
        "lineage": "completed",
        "target_fingerprint": canonical_fingerprint(target_contract_payload(head_pipeline)),
        "live_fingerprint": canonical_fingerprint(target_contract_payload(head_pipeline)),
        "migration_fingerprint": canonical_fingerprint(
            pipelines_by_id(base)[PIPELINE_ID]["topology_migration"]
        ),
        "readiness_status": "ready",
        "finalization_code": "topology_migration_finalization_ready",
        "created_at": "2026-08-19T12:30:00+00:00",
    }
    return build_cleanup_reference(receipt)


def evaluate(base: dict, head: dict, reference: dict | None = None, *, files=None, body=None):
    path = deterministic_reference_path(PIPELINE_ID, MIGRATION_ID)
    references = {} if reference is None else {path: reference}
    return evaluate_cleanup_pr(
        base_registry=base,
        head_registry=head,
        changed_files=set(files or {REGISTRY_PATH, path}),
        references=references,
        pr_body=body if body is not None else f"Topology-Finalization-Receipt: {RECEIPT_ID}",
    )


def test_guard_target_fingerprint_matches_runtime_receipt_contract():
    raw = raw_registry()
    pipeline = pipelines_by_id(raw)[PIPELINE_ID]
    registry = load_registry(REGISTRY)
    spec = registry.get(PIPELINE_ID)
    topology = pipeline_topology_mapping(raw, PIPELINE_ID)

    assert target_contract_payload(pipeline) == runtime_target_payload(spec, topology)
    assert canonical_fingerprint(target_contract_payload(pipeline)) == canonical_fingerprint(
        runtime_target_payload(spec, topology)
    )


def test_valid_cleanup_pr_is_bound_to_receipt_and_passes():
    base, head = cleanup_pair()
    reference = reference_for(base, head)

    report = evaluate(base, head, reference)

    assert report.status == "ok"
    assert report.code == "topology_finalization_cleanup_guard_passed"
    assert report.cleanups[0].migration_id == MIGRATION_ID


def test_non_cleanup_pr_is_not_applicable():
    head = raw_registry()
    base = deepcopy(head)

    report = evaluate_cleanup_pr(
        base_registry=base,
        head_registry=head,
        changed_files={"decisions/domain/example.md"},
        references={},
        pr_body=None,
    )

    assert report.status == "not_applicable"


def test_cleanup_cannot_mix_target_topology_change():
    base, head = cleanup_pair()
    changed_head = deepcopy(head)
    pipelines_by_id(changed_head)[PIPELINE_ID]["topology"]["business_consumer"][
        "max_deliver"
    ] = 4
    reference = reference_for(base, changed_head)

    report = evaluate(base, changed_head, reference)

    assert report.status == "blocked"
    assert any("only remove topology_migration" in error for error in report.errors)


def test_cleanup_cannot_mix_unrelated_file_change():
    base, head = cleanup_pair()
    reference = reference_for(base, head)
    path = deterministic_reference_path(PIPELINE_ID, MIGRATION_ID)

    report = evaluate(
        base,
        head,
        reference,
        files={REGISTRY_PATH, path, "README.md"},
    )

    assert report.status == "blocked"
    assert any("unrelated files" in error for error in report.errors)


def test_cleanup_requires_deterministic_reference_file():
    base, head = cleanup_pair()

    report = evaluate(base, head, None)

    assert report.status == "blocked"
    assert any("reference file is missing" in error for error in report.errors)


def test_cleanup_requires_visible_matching_pr_body_receipt_trailer():
    base, head = cleanup_pair()
    reference = reference_for(base, head)

    report = evaluate(base, head, reference, body="No receipt trailer here")

    assert report.status == "blocked"
    assert any("PR body receipt trailers" in error for error in report.errors)


def test_cleanup_rejects_target_fingerprint_drift():
    base, head = cleanup_pair()
    reference = reference_for(base, head)
    reference["receipt"]["target_fingerprint"] = "a" * 64
    reference["receipt_fingerprint"] = canonical_fingerprint(reference["receipt"])

    report = evaluate(base, head, reference)

    assert report.status == "blocked"
    assert any("target_fingerprint" in error for error in report.errors)


def test_cleanup_rejects_live_fingerprint_drift():
    base, head = cleanup_pair()
    reference = reference_for(base, head)
    reference["receipt"]["live_fingerprint"] = "b" * 64
    reference["receipt_fingerprint"] = canonical_fingerprint(reference["receipt"])

    report = evaluate(base, head, reference)

    assert report.status == "blocked"
    assert any("live_fingerprint" in error for error in report.errors)


def test_cleanup_rejects_removed_migration_fingerprint_mismatch():
    base, head = cleanup_pair()
    reference = reference_for(base, head)
    reference["receipt"]["migration_fingerprint"] = "c" * 64
    reference["receipt_fingerprint"] = canonical_fingerprint(reference["receipt"])

    report = evaluate(base, head, reference)

    assert report.status == "blocked"
    assert any("migration_fingerprint" in error for error in report.errors)


def test_reference_payload_is_tamper_evident():
    base, head = cleanup_pair()
    reference = reference_for(base, head)
    reference["receipt"]["run_id"] = str(UUID("00000000-0000-0000-0000-00000000bb03"))

    report = evaluate(base, head, reference)

    assert report.status == "blocked"
    assert any("receipt_fingerprint" in error for error in report.errors)


def test_reference_builder_uses_canonical_uuid_and_deterministic_path():
    base, head = cleanup_pair()
    reference = reference_for(base, head)

    validate_reference_structure(reference)
    assert reference["receipt"]["receipt_id"] == RECEIPT_ID
    assert deterministic_reference_path(PIPELINE_ID, MIGRATION_ID) == (
        "operations/event-pipelines/finalizations/"
        "inventory-position-projection/business-ack-window-5-to-10.yml"
    )
