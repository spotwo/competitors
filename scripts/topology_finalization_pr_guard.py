from __future__ import annotations

import hashlib
import json
import re
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from pathlib import PurePosixPath
from typing import Any, Mapping
from uuid import UUID

REGISTRY_PATH = "operations/event-pipelines/registry.yml"
FINALIZATIONS_ROOT = PurePosixPath("operations/event-pipelines/finalizations")
RECEIPT_TRAILER = re.compile(
    r"(?mi)^Topology-Finalization-Receipt:\s*([0-9a-fA-F-]{36})\s*$"
)
_HEX_64 = re.compile(r"^[0-9a-f]{64}$")


class GuardError(ValueError):
    pass


def _mapping(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise GuardError(f"{field} must be an object")
    return value


def _normalized_name(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise GuardError(f"{field} must be a normalized non-empty string")
    return value


def _positive_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise GuardError(f"{field} must be a positive integer")
    return value


def _positive_number(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
        raise GuardError(f"{field} must be positive")
    return float(value)


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_jsonable(item) for item in value]
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def canonical_fingerprint(value: Any) -> str:
    encoded = json.dumps(
        _jsonable(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def pipelines_by_id(registry: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    pipelines = registry.get("pipelines")
    if not isinstance(pipelines, list):
        raise GuardError("registry pipelines must be an array")
    result: dict[str, Mapping[str, Any]] = {}
    for index, item in enumerate(pipelines):
        pipeline = _mapping(item, f"pipelines[{index}]")
        pipeline_id = _normalized_name(pipeline.get("id"), f"pipelines[{index}].id")
        if pipeline_id in result:
            raise GuardError(f"duplicate pipeline id: {pipeline_id}")
        result[pipeline_id] = pipeline
    return result


def _topology_consumer_payload(
    *,
    identity: Mapping[str, Any],
    topology: Mapping[str, Any],
    identity_field: str,
) -> dict[str, Any]:
    return {
        "name": _normalized_name(identity.get(identity_field), f"{identity_field}"),
        "delivery_mode": _normalized_name(topology.get("delivery_mode"), "delivery_mode"),
        "deliver_policy": _normalized_name(topology.get("deliver_policy"), "deliver_policy"),
        "ack_policy": _normalized_name(topology.get("ack_policy"), "ack_policy"),
        "filter_subject": _normalized_name(topology.get("filter_subject"), "filter_subject"),
        "max_ack_pending": _positive_int(topology.get("max_ack_pending"), "max_ack_pending"),
        "max_deliver": _positive_int(topology.get("max_deliver"), "max_deliver"),
    }


def target_contract_payload(pipeline: Mapping[str, Any]) -> dict[str, Any]:
    pipeline_id = _normalized_name(pipeline.get("id"), "pipeline.id")
    transport = _mapping(pipeline.get("transport"), "pipeline.transport")
    consumer = _mapping(pipeline.get("consumer"), "pipeline.consumer")
    canary = _mapping(pipeline.get("canary"), "pipeline.canary")
    topology = _mapping(pipeline.get("topology"), "pipeline.topology")
    stream_topology = _mapping(topology.get("stream"), "pipeline.topology.stream")
    business_topology = _mapping(
        topology.get("business_consumer"), "pipeline.topology.business_consumer"
    )
    canary_topology = _mapping(
        topology.get("canary_consumer"), "pipeline.topology.canary_consumer"
    )
    subjects = stream_topology.get("subjects")
    if not isinstance(subjects, list) or not subjects:
        raise GuardError("pipeline.topology.stream.subjects must be a non-empty array")
    normalized_subjects = sorted(
        _normalized_name(subject, "pipeline.topology.stream.subjects")
        for subject in subjects
    )
    return {
        "pipeline": pipeline_id,
        "stream": {
            "name": _normalized_name(transport.get("stream"), "pipeline.transport.stream"),
            "subjects": normalized_subjects,
            "storage": _normalized_name(stream_topology.get("storage"), "stream.storage"),
            "retention": _normalized_name(stream_topology.get("retention"), "stream.retention"),
            "replicas": _positive_int(stream_topology.get("replicas"), "stream.replicas"),
            "duplicate_window_seconds": _positive_number(
                stream_topology.get("duplicate_window_seconds"),
                "stream.duplicate_window_seconds",
            ),
        },
        "business_consumer": _topology_consumer_payload(
            identity=consumer,
            topology=business_topology,
            identity_field="durable",
        ),
        "canary_consumer": _topology_consumer_payload(
            identity=canary,
            topology=canary_topology,
            identity_field="durable",
        ),
    }


def deterministic_reference_path(pipeline_id: str, migration_id: str) -> str:
    pipeline_id = _normalized_name(pipeline_id, "pipeline_id")
    migration_id = _normalized_name(migration_id, "migration_id")
    if "/" in pipeline_id or "/" in migration_id or ".." in (pipeline_id, migration_id):
        raise GuardError("pipeline and migration ids cannot contain path separators")
    return str(FINALIZATIONS_ROOT / pipeline_id / f"{migration_id}.yml")


def build_cleanup_reference(receipt: Mapping[str, Any]) -> dict[str, Any]:
    receipt_id = str(UUID(_normalized_name(receipt.get("receipt_id"), "receipt.receipt_id")))
    pipeline_id = _normalized_name(receipt.get("pipeline"), "receipt.pipeline")
    migration_id = _normalized_name(receipt.get("migration_id"), "receipt.migration_id")
    run_id = str(UUID(_normalized_name(receipt.get("run_id"), "receipt.run_id")))
    lineage = _normalized_name(receipt.get("lineage"), "receipt.lineage")
    if lineage not in ("completed", "resolved_target"):
        raise GuardError("receipt.lineage must be completed or resolved_target")

    reduced_receipt = {
        "receipt_id": receipt_id,
        "pipeline": pipeline_id,
        "migration_id": migration_id,
        "run_id": run_id,
        "lineage": lineage,
        "target_fingerprint": _normalized_name(
            receipt.get("target_fingerprint"), "receipt.target_fingerprint"
        ),
        "live_fingerprint": _normalized_name(
            receipt.get("live_fingerprint"), "receipt.live_fingerprint"
        ),
        "migration_fingerprint": _normalized_name(
            receipt.get("migration_fingerprint"), "receipt.migration_fingerprint"
        ),
        "readiness_status": _normalized_name(
            receipt.get("readiness_status"), "receipt.readiness_status"
        ),
        "finalization_code": _normalized_name(
            receipt.get("finalization_code"), "receipt.finalization_code"
        ),
        "created_at": _normalized_name(receipt.get("created_at"), "receipt.created_at"),
    }
    return {
        "version": 1,
        "kind": "topology_migration_finalization",
        "pipeline": pipeline_id,
        "migration_id": migration_id,
        "receipt": reduced_receipt,
        "receipt_fingerprint": canonical_fingerprint(reduced_receipt),
        "registry_change": {"operation": "remove_topology_migration"},
    }


def _require_hex64(value: Any, field: str) -> str:
    value = _normalized_name(value, field)
    if not _HEX_64.fullmatch(value):
        raise GuardError(f"{field} must be a lowercase SHA-256 hex digest")
    return value


def _require_rfc3339(value: Any, field: str) -> str:
    value = _normalized_name(value, field)
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise GuardError(f"{field} must be an RFC3339 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise GuardError(f"{field} must include a timezone")
    return value


def validate_reference_structure(reference: Mapping[str, Any]) -> None:
    if reference.get("version") != 1:
        raise GuardError("finalization reference version must be 1")
    if reference.get("kind") != "topology_migration_finalization":
        raise GuardError("finalization reference kind is invalid")
    pipeline_id = _normalized_name(reference.get("pipeline"), "reference.pipeline")
    migration_id = _normalized_name(reference.get("migration_id"), "reference.migration_id")
    receipt = _mapping(reference.get("receipt"), "reference.receipt")
    if str(UUID(_normalized_name(receipt.get("receipt_id"), "receipt.receipt_id"))) != receipt.get(
        "receipt_id"
    ):
        raise GuardError("receipt.receipt_id must use canonical UUID spelling")
    if str(UUID(_normalized_name(receipt.get("run_id"), "receipt.run_id"))) != receipt.get(
        "run_id"
    ):
        raise GuardError("receipt.run_id must use canonical UUID spelling")
    if receipt.get("pipeline") != pipeline_id:
        raise GuardError("receipt pipeline must match reference pipeline")
    if receipt.get("migration_id") != migration_id:
        raise GuardError("receipt migration_id must match reference migration_id")
    if receipt.get("lineage") not in ("completed", "resolved_target"):
        raise GuardError("receipt lineage is not finalizable")
    for field in (
        "target_fingerprint",
        "live_fingerprint",
        "migration_fingerprint",
    ):
        _require_hex64(receipt.get(field), f"receipt.{field}")
    if receipt.get("readiness_status") != "ready":
        raise GuardError("receipt readiness_status must be ready")
    if receipt.get("finalization_code") != "topology_migration_finalization_ready":
        raise GuardError("receipt finalization_code is not finalization-ready")
    _require_rfc3339(receipt.get("created_at"), "receipt.created_at")
    fingerprint = _require_hex64(
        reference.get("receipt_fingerprint"), "reference.receipt_fingerprint"
    )
    if fingerprint != canonical_fingerprint(receipt):
        raise GuardError("receipt_fingerprint does not match receipt payload")
    registry_change = _mapping(reference.get("registry_change"), "reference.registry_change")
    if dict(registry_change) != {"operation": "remove_topology_migration"}:
        raise GuardError("registry_change must only remove topology_migration")


def receipt_trailers(pr_body: str | None) -> set[str]:
    if not pr_body:
        return set()
    values: set[str] = set()
    for match in RECEIPT_TRAILER.finditer(pr_body):
        try:
            values.add(str(UUID(match.group(1))))
        except ValueError as exc:
            raise GuardError("Topology-Finalization-Receipt trailer must contain a UUID") from exc
    return values


@dataclass(frozen=True)
class CleanupTransition:
    pipeline_id: str
    migration_id: str
    reference_path: str


@dataclass(frozen=True)
class GuardReport:
    status: str
    code: str
    cleanups: tuple[CleanupTransition, ...]
    errors: tuple[str, ...] = ()

    @property
    def allowed(self) -> bool:
        return self.status in ("ok", "not_applicable")

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "code": self.code,
            "cleanups": [
                {
                    "pipeline": item.pipeline_id,
                    "migration_id": item.migration_id,
                    "reference_path": item.reference_path,
                }
                for item in self.cleanups
            ],
            "errors": list(self.errors),
        }


def evaluate_cleanup_pr(
    *,
    base_registry: Mapping[str, Any],
    head_registry: Mapping[str, Any],
    changed_files: set[str],
    references: Mapping[str, Mapping[str, Any]],
    pr_body: str | None,
) -> GuardReport:
    errors: list[str] = []
    base_pipelines = pipelines_by_id(base_registry)
    head_pipelines = pipelines_by_id(head_registry)
    cleanups: list[CleanupTransition] = []

    for pipeline_id, base_pipeline in base_pipelines.items():
        base_migration = base_pipeline.get("topology_migration")
        if base_migration is None:
            continue
        head_pipeline = head_pipelines.get(pipeline_id)
        if head_pipeline is None:
            errors.append(f"{pipeline_id}: pipeline cannot be removed while topology_migration exists")
            continue
        if head_pipeline.get("topology_migration") is not None:
            continue
        migration = _mapping(base_migration, f"{pipeline_id}.topology_migration")
        migration_id = _normalized_name(
            migration.get("id"), f"{pipeline_id}.topology_migration.id"
        )
        cleanups.append(
            CleanupTransition(
                pipeline_id=pipeline_id,
                migration_id=migration_id,
                reference_path=deterministic_reference_path(pipeline_id, migration_id),
            )
        )

        expected_head = deepcopy(dict(base_pipeline))
        expected_head.pop("topology_migration", None)
        if expected_head != dict(head_pipeline):
            errors.append(
                f"{pipeline_id}: cleanup PR must only remove topology_migration from the pipeline"
            )

    if errors and not cleanups:
        return GuardReport("blocked", "topology_finalization_cleanup_invalid", (), tuple(errors))
    if not cleanups:
        return GuardReport("not_applicable", "topology_finalization_cleanup_not_present", ())

    allowed_files = {REGISTRY_PATH, *(item.reference_path for item in cleanups)}
    unrelated = sorted(changed_files - allowed_files)
    if unrelated:
        errors.append(
            "cleanup PR contains unrelated files: " + ", ".join(unrelated)
        )
    missing_changed = sorted(allowed_files - changed_files)
    if missing_changed:
        errors.append(
            "cleanup PR is missing required changed files: " + ", ".join(missing_changed)
        )

    receipt_ids: set[str] = set()
    for cleanup in cleanups:
        reference = references.get(cleanup.reference_path)
        if reference is None:
            errors.append(f"{cleanup.pipeline_id}: finalization reference file is missing")
            continue
        try:
            validate_reference_structure(reference)
            if reference.get("pipeline") != cleanup.pipeline_id:
                raise GuardError("reference pipeline does not match cleanup pipeline")
            if reference.get("migration_id") != cleanup.migration_id:
                raise GuardError("reference migration_id does not match removed migration")
            receipt = _mapping(reference.get("receipt"), "reference.receipt")
            receipt_id = str(UUID(str(receipt.get("receipt_id"))))
            receipt_ids.add(receipt_id)

            base_pipeline = base_pipelines[cleanup.pipeline_id]
            head_pipeline = head_pipelines[cleanup.pipeline_id]
            target_fingerprint = canonical_fingerprint(target_contract_payload(head_pipeline))
            if receipt.get("target_fingerprint") != target_fingerprint:
                raise GuardError("receipt target_fingerprint does not match cleanup target")
            if receipt.get("live_fingerprint") != target_fingerprint:
                raise GuardError("receipt live_fingerprint does not match cleanup target")
            migration_fingerprint = canonical_fingerprint(
                _mapping(
                    base_pipeline.get("topology_migration"),
                    f"{cleanup.pipeline_id}.topology_migration",
                )
            )
            if receipt.get("migration_fingerprint") != migration_fingerprint:
                raise GuardError("receipt migration_fingerprint does not match removed migration")
        except (GuardError, ValueError) as exc:
            errors.append(f"{cleanup.pipeline_id}: {exc}")

    try:
        trailers = receipt_trailers(pr_body)
    except GuardError as exc:
        errors.append(str(exc))
        trailers = set()
    if trailers != receipt_ids:
        errors.append(
            "PR body receipt trailers must exactly match cleanup references; use one "
            "Topology-Finalization-Receipt: <uuid> line per cleanup"
        )

    if errors:
        return GuardReport(
            "blocked",
            "topology_finalization_cleanup_guard_failed",
            tuple(cleanups),
            tuple(errors),
        )
    return GuardReport(
        "ok",
        "topology_finalization_cleanup_guard_passed",
        tuple(cleanups),
    )
