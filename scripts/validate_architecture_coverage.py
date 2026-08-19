#!/usr/bin/env python3
from __future__ import annotations

import csv
import datetime as dt
import json
import sys
from pathlib import Path
from typing import Any

import yaml
from jsonschema import Draft202012Validator, FormatChecker

ROOT = Path(__file__).resolve().parents[1]
COVERAGE = ROOT / "architecture" / "coverage-gaps.yml"
BOUNDARIES = ROOT / "architecture" / "boundaries.yml"
SCHEMA = ROOT / "schema" / "architecture-coverage.schema.json"
OPEN_SOURCE = ROOT / "research" / "open-source" / "catalog.yml"
COMPETITOR_MATRIX = ROOT / "technology-intelligence" / "matrix.csv"
LENS_NAMES = ("standards", "open_source", "commercial", "executable")


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


def competitor_products() -> dict[str, dict[str, str]]:
    with COMPETITOR_MATRIX.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    return {f"{row['company_id']}/{row['product_id']}": row for row in rows}


def expected_overall(item: dict[str, Any]) -> str:
    required = [lens for lens in item["lenses"].values() if lens["requirement"] == "required"]
    if required and all(lens["status"] == "covered" for lens in required):
        return "covered"

    relevant = [lens for lens in item["lenses"].values() if lens["requirement"] != "not-required"]
    if any(lens["evidence_refs"] for lens in relevant):
        return "partial"
    return "missing"


def main() -> int:
    data = load_yaml(COVERAGE)
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    errors = [
        f"{COVERAGE.relative_to(ROOT)}: {'.'.join(map(str, error.absolute_path)) or '<root>'}: {error.message}"
        for error in sorted(validator.iter_errors(data), key=lambda item: list(item.absolute_path))
    ]

    boundaries_data = load_yaml(BOUNDARIES)
    boundaries = {item["id"]: item for item in boundaries_data.get("boundaries", [])}
    open_source = load_yaml(OPEN_SOURCE)
    open_source_ids = {item["id"] for item in open_source.get("projects", [])}
    competitors = competitor_products()
    seen: set[str] = set()

    for item in data.get("coverage", []):
        boundary_ref = item.get("boundary_ref", "<missing>")
        if boundary_ref in seen:
            errors.append(f"{COVERAGE.relative_to(ROOT)}: duplicate boundary_ref {boundary_ref!r}")
        seen.add(boundary_ref)

        boundary = boundaries.get(boundary_ref)
        if boundary is None:
            errors.append(f"{COVERAGE.relative_to(ROOT)}: unknown boundary_ref {boundary_ref!r}")
            continue

        calculated = expected_overall(item)
        if item.get("overall") != calculated:
            errors.append(
                f"{COVERAGE.relative_to(ROOT)}: boundary {boundary_ref!r} overall must be {calculated!r}, "
                f"got {item.get('overall')!r}"
            )

        boundary_standards = set(boundary.get("standards", []))
        for lens_name in LENS_NAMES:
            lens = item["lenses"][lens_name]
            requirement = lens["requirement"]
            status = lens["status"]

            if requirement == "not-required":
                if status != "not-required":
                    errors.append(
                        f"{COVERAGE.relative_to(ROOT)}: {boundary_ref}.{lens_name} not-required lens must use not-required status"
                    )
                if lens["evidence_refs"] or lens["research_targets"] or lens["gaps"]:
                    errors.append(
                        f"{COVERAGE.relative_to(ROOT)}: {boundary_ref}.{lens_name} not-required lens must be empty"
                    )
            elif status == "not-required":
                errors.append(
                    f"{COVERAGE.relative_to(ROOT)}: {boundary_ref}.{lens_name} status not-required requires requirement not-required"
                )

            if status == "covered" and not lens["evidence_refs"]:
                errors.append(
                    f"{COVERAGE.relative_to(ROOT)}: {boundary_ref}.{lens_name} covered lens must cite evidence"
                )
            if status == "missing" and lens["evidence_refs"]:
                errors.append(
                    f"{COVERAGE.relative_to(ROOT)}: {boundary_ref}.{lens_name} missing lens cannot cite evidence"
                )

            for evidence in lens["evidence_refs"]:
                kind = evidence["kind"]
                ref = evidence["ref"]
                if kind == "standard":
                    if ref not in boundary_standards:
                        errors.append(
                            f"{COVERAGE.relative_to(ROOT)}: {boundary_ref}.{lens_name} cites standard {ref!r} "
                            "that is not declared on the boundary"
                        )
                elif kind == "open-source-project":
                    if ref not in open_source_ids:
                        errors.append(
                            f"{COVERAGE.relative_to(ROOT)}: {boundary_ref}.{lens_name} cites unknown open-source project {ref!r}"
                        )
                elif kind == "competitor-product":
                    row = competitors.get(ref)
                    if row is None:
                        errors.append(
                            f"{COVERAGE.relative_to(ROOT)}: {boundary_ref}.{lens_name} cites unknown competitor product {ref!r}"
                        )
                    elif row.get("verification_state") == "research-gap" or int(row.get("source_count") or 0) < 1:
                        errors.append(
                            f"{COVERAGE.relative_to(ROOT)}: {boundary_ref}.{lens_name} cites unverified competitor product {ref!r}"
                        )
                elif kind == "repository":
                    if not (ROOT / ref).is_file():
                        errors.append(
                            f"{COVERAGE.relative_to(ROOT)}: {boundary_ref}.{lens_name} cites missing repository file {ref!r}"
                        )

    missing_boundaries = sorted(set(boundaries) - seen)
    extra_boundaries = sorted(seen - set(boundaries))
    if missing_boundaries:
        errors.append(
            f"{COVERAGE.relative_to(ROOT)}: missing coverage records for boundaries: {', '.join(missing_boundaries)}"
        )
    if extra_boundaries:
        errors.append(
            f"{COVERAGE.relative_to(ROOT)}: coverage records without boundaries: {', '.join(extra_boundaries)}"
        )

    if errors:
        print("Architecture reference coverage validation failed:", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1

    overall_counts = {state: 0 for state in ("covered", "partial", "missing")}
    priority_counts = {priority: 0 for priority in ("P0", "P1", "P2")}
    for item in data["coverage"]:
        overall_counts[item["overall"]] += 1
        priority_counts[item["priority"]] += 1

    print(
        "Architecture reference coverage validation passed: "
        f"{len(seen)} boundaries, "
        + ", ".join(f"{state}={count}" for state, count in overall_counts.items())
        + "; "
        + ", ".join(f"{priority}={count}" for priority, count in priority_counts.items())
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
