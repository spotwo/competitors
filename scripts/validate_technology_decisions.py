#!/usr/bin/env python3
from __future__ import annotations

import csv
import datetime as dt
import json
import sys
from pathlib import Path

import yaml
from jsonschema import Draft202012Validator, FormatChecker

ROOT = Path(__file__).resolve().parents[1]
REGISTRY = ROOT / "decisions" / "technology" / "registry.yml"
SCHEMA = ROOT / "schema" / "technology-decision.schema.json"
OPEN_SOURCE_CATALOG = ROOT / "research" / "open-source" / "catalog.yml"
COMPETITOR_MATRIX = ROOT / "technology-intelligence" / "matrix.csv"


def normalize(value):
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: normalize(item) for key, item in value.items()}
    if isinstance(value, list):
        return [normalize(item) for item in value]
    return value


def load_yaml(path: Path):
    return normalize(yaml.safe_load(path.read_text(encoding="utf-8")))


def competitor_products() -> dict[str, dict[str, str]]:
    with COMPETITOR_MATRIX.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    return {f"{row['company_id']}/{row['product_id']}": row for row in rows}


def main() -> int:
    data = load_yaml(REGISTRY)
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema, format_checker=FormatChecker())

    errors = [
        f"{REGISTRY.relative_to(ROOT)}: {'.'.join(map(str, error.absolute_path)) or '<root>'}: {error.message}"
        for error in sorted(validator.iter_errors(data), key=lambda item: list(item.absolute_path))
    ]

    open_source = load_yaml(OPEN_SOURCE_CATALOG)
    open_source_ids = {project["id"] for project in open_source.get("projects", [])}
    competitors = competitor_products()
    seen_ids: set[str] = set()

    for decision in data.get("decisions", []):
        decision_id = decision.get("id", "<missing>")
        if decision_id in seen_ids:
            errors.append(f"{REGISTRY.relative_to(ROOT)}: duplicate decision id {decision_id!r}")
        seen_ids.add(decision_id)

        evidence = decision.get("evidence", {})
        repository_refs = evidence.get("repository_refs", [])
        open_source_refs = evidence.get("open_source_project_refs", [])
        competitor_refs = evidence.get("competitor_product_refs", [])

        if not (repository_refs or open_source_refs or competitor_refs):
            errors.append(f"{REGISTRY.relative_to(ROOT)}: decision {decision_id!r} has no evidence")

        for ref in repository_refs:
            path = ROOT / ref
            if not path.is_file():
                errors.append(
                    f"{REGISTRY.relative_to(ROOT)}: decision {decision_id!r} references missing repository file {ref!r}"
                )

        for ref in open_source_refs:
            if ref not in open_source_ids:
                errors.append(
                    f"{REGISTRY.relative_to(ROOT)}: decision {decision_id!r} references unknown open-source project {ref!r}"
                )

        for ref in competitor_refs:
            row = competitors.get(ref)
            if row is None:
                errors.append(
                    f"{REGISTRY.relative_to(ROOT)}: decision {decision_id!r} references unknown competitor product {ref!r}"
                )
                continue
            if row.get("verification_state") == "research-gap" or int(row.get("source_count") or 0) < 1:
                errors.append(
                    f"{REGISTRY.relative_to(ROOT)}: decision {decision_id!r} uses unverified competitor evidence {ref!r}"
                )

        disposition = decision.get("disposition")
        confidence = decision.get("confidence")
        if disposition == "adopt" and not repository_refs:
            errors.append(
                f"{REGISTRY.relative_to(ROOT)}: adopted decision {decision_id!r} must cite repository decision or lab evidence"
            )
        if disposition in {"adopt", "reject"} and confidence == "low":
            errors.append(
                f"{REGISTRY.relative_to(ROOT)}: {disposition} decision {decision_id!r} cannot have low confidence"
            )
        if disposition == "reject" and not repository_refs:
            errors.append(
                f"{REGISTRY.relative_to(ROOT)}: rejected decision {decision_id!r} must cite repository decision evidence"
            )

    if errors:
        print("Technology decision validation failed:", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1

    counts = {disposition: 0 for disposition in ("adopt", "trial", "watch", "reject")}
    for decision in data["decisions"]:
        counts[decision["disposition"]] += 1

    print(
        "Technology decision validation passed: "
        f"{len(seen_ids)} decisions, "
        + ", ".join(f"{name}={count}" for name, count in counts.items())
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
