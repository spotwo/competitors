#!/usr/bin/env python3
from __future__ import annotations

import datetime as dt
import json
import sys
from pathlib import Path
from typing import Any

import yaml
from jsonschema import Draft202012Validator, FormatChecker

ROOT = Path(__file__).resolve().parents[1]
REGISTRY = ROOT / "architecture" / "boundaries.yml"
SCHEMA = ROOT / "schema" / "architecture-boundary-map.schema.json"
DECISIONS = ROOT / "decisions" / "technology" / "registry.yml"
OPEN_SOURCE = ROOT / "research" / "open-source" / "catalog.yml"


def normalize(value: Any) -> Any:
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: normalize(item) for key, item in value.items()}
    if isinstance(value, list):
        return [normalize(item) for item in value]
    return value


def load_yaml(path: Path) -> dict[str, Any]:
    return normalize(yaml.safe_load(path.read_text(encoding="utf-8")))


def main() -> int:
    data = load_yaml(REGISTRY)
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    errors = [
        f"{REGISTRY.relative_to(ROOT)}: {'.'.join(map(str, error.absolute_path)) or '<root>'}: {error.message}"
        for error in sorted(validator.iter_errors(data), key=lambda item: list(item.absolute_path))
    ]

    decisions = load_yaml(DECISIONS)
    decision_by_id = {item["id"]: item for item in decisions.get("decisions", [])}
    open_source = load_yaml(OPEN_SOURCE)
    open_source_ids = {item["id"] for item in open_source.get("projects", [])}
    seen_ids: set[str] = set()

    for boundary in data.get("boundaries", []):
        boundary_id = boundary.get("id", "<missing>")
        if boundary_id in seen_ids:
            errors.append(f"{REGISTRY.relative_to(ROOT)}: duplicate boundary id {boundary_id!r}")
        seen_ids.add(boundary_id)

        contract = boundary.get("contract", {})
        status = contract.get("status")
        primary_ref = contract.get("technology_decision_ref")
        evidence = boundary.get("evidence", {})
        evidence_decisions = evidence.get("technology_decision_refs", [])

        if primary_ref is not None and primary_ref not in evidence_decisions:
            errors.append(
                f"{REGISTRY.relative_to(ROOT)}: boundary {boundary_id!r} primary technology decision "
                f"{primary_ref!r} must also appear in evidence.technology_decision_refs"
            )

        if status == "unresolved" and primary_ref is not None:
            errors.append(
                f"{REGISTRY.relative_to(ROOT)}: unresolved boundary {boundary_id!r} cannot declare a canonical technology decision"
            )
        if status in {"canonical", "candidate"} and primary_ref is None:
            errors.append(
                f"{REGISTRY.relative_to(ROOT)}: {status} boundary {boundary_id!r} must declare technology_decision_ref"
            )

        if primary_ref is not None:
            decision = decision_by_id.get(primary_ref)
            if decision is None:
                errors.append(
                    f"{REGISTRY.relative_to(ROOT)}: boundary {boundary_id!r} references unknown primary technology decision {primary_ref!r}"
                )
            elif status == "canonical" and decision.get("disposition") != "adopt":
                errors.append(
                    f"{REGISTRY.relative_to(ROOT)}: canonical boundary {boundary_id!r} requires an adopted decision; "
                    f"{primary_ref!r} is {decision.get('disposition')!r}"
                )
            elif status == "candidate" and decision.get("disposition") not in {"trial", "adopt"}:
                errors.append(
                    f"{REGISTRY.relative_to(ROOT)}: candidate boundary {boundary_id!r} cannot use "
                    f"{primary_ref!r} with disposition {decision.get('disposition')!r}"
                )

        for ref in evidence_decisions:
            decision = decision_by_id.get(ref)
            if decision is None:
                errors.append(
                    f"{REGISTRY.relative_to(ROOT)}: boundary {boundary_id!r} references unknown technology decision {ref!r}"
                )
            elif decision.get("disposition") == "reject":
                errors.append(
                    f"{REGISTRY.relative_to(ROOT)}: boundary {boundary_id!r} cannot cite rejected technology {ref!r} as supporting evidence"
                )

        for ref in evidence.get("open_source_project_refs", []):
            if ref not in open_source_ids:
                errors.append(
                    f"{REGISTRY.relative_to(ROOT)}: boundary {boundary_id!r} references unknown open-source project {ref!r}"
                )

        for ref in evidence.get("repository_refs", []):
            if not (ROOT / ref).is_file():
                errors.append(
                    f"{REGISTRY.relative_to(ROOT)}: boundary {boundary_id!r} references missing repository file {ref!r}"
                )

        ownership = boundary.get("state_ownership", {})
        if not ownership.get("authoritative") or not ownership.get("forbidden"):
            errors.append(
                f"{REGISTRY.relative_to(ROOT)}: boundary {boundary_id!r} must define both authoritative and forbidden ownership rules"
            )

        flow = boundary.get("flow", {})
        if not (flow.get("commands") or flow.get("events") or flow.get("queries")):
            errors.append(
                f"{REGISTRY.relative_to(ROOT)}: boundary {boundary_id!r} has no commands, events, or queries"
            )

    if errors:
        print("Architecture boundary map validation failed:", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1

    counts = {name: 0 for name in ("canonical", "candidate", "unresolved")}
    gaps = 0
    for boundary in data["boundaries"]:
        counts[boundary["contract"]["status"]] += 1
        gaps += len(boundary["research_gaps"])

    print(
        "Architecture boundary map validation passed: "
        f"{len(seen_ids)} boundaries, "
        + ", ".join(f"{name}={count}" for name, count in counts.items())
        + f", research_gaps={gaps}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
