#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
COVERAGE = ROOT / "architecture" / "coverage-gaps.yml"
BOUNDARIES = ROOT / "architecture" / "boundaries.yml"
PRIORITY_ORDER = {"P0": 0, "P1": 1, "P2": 2}
OVERALL_ORDER = {"missing": 0, "partial": 1, "covered": 2}


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


def contains(value: Any, needle: str | None) -> bool:
    if not needle:
        return True
    folded = needle.casefold()
    if isinstance(value, dict):
        return any(contains(item, needle) for item in value.values())
    if isinstance(value, list):
        return any(contains(item, needle) for item in value)
    return folded in str(value).casefold()


def records() -> list[dict[str, Any]]:
    coverage = load_yaml(COVERAGE)["coverage"]
    boundaries = {item["id"]: item for item in load_yaml(BOUNDARIES)["boundaries"]}
    output: list[dict[str, Any]] = []
    for item in coverage:
        boundary = boundaries[item["boundary_ref"]]
        gap_count = sum(len(lens["gaps"]) for lens in item["lenses"].values())
        target_count = sum(len(lens["research_targets"]) for lens in item["lenses"].values())
        output.append(
            {
                **item,
                "boundary_name": boundary["name"],
                "classification": boundary["classification"],
                "contract_status": boundary["contract"]["status"],
                "contract_mechanism": boundary["contract"]["mechanism"],
                "gap_count": gap_count,
                "target_count": target_count,
            }
        )
    return sorted(
        output,
        key=lambda item: (
            PRIORITY_ORDER[item["priority"]],
            OVERALL_ORDER[item["overall"]],
            item["boundary_ref"],
        ),
    )


def matches(item: dict[str, Any], args: argparse.Namespace) -> bool:
    if args.boundary and not contains([item["boundary_ref"], item["boundary_name"]], args.boundary):
        return False
    if args.priority and item["priority"] != args.priority:
        return False
    if args.overall and item["overall"] != args.overall:
        return False
    if args.contract_status and item["contract_status"] != args.contract_status:
        return False
    if args.classification and not contains(item["classification"], args.classification):
        return False
    if args.lens:
        lens = item["lenses"][args.lens]
        if args.lens_status and lens["status"] != args.lens_status:
            return False
    elif args.lens_status and not any(lens["status"] == args.lens_status for lens in item["lenses"].values()):
        return False
    if args.target:
        targets = [target for lens in item["lenses"].values() for target in lens["research_targets"]]
        if not contains(targets, args.target):
            return False
    if args.gap:
        gaps = [gap for lens in item["lenses"].values() for gap in lens["gaps"]]
        if not contains(gaps, args.gap):
            return False
    if args.evidence:
        refs = [evidence for lens in item["lenses"].values() for evidence in lens["evidence_refs"]]
        if not contains(refs, args.evidence):
            return False
    return True


def table(items: list[dict[str, Any]]) -> str:
    headers = ["PRIORITY", "OVERALL", "BOUNDARY", "CONTRACT", "STANDARDS", "OPEN_SOURCE", "COMMERCIAL", "EXECUTABLE", "GAPS", "TARGETS"]
    rows = []
    for item in items:
        lenses = item["lenses"]
        rows.append(
            [
                item["priority"],
                item["overall"],
                item["boundary_ref"],
                item["contract_status"],
                lenses["standards"]["status"],
                lenses["open_source"]["status"],
                lenses["commercial"]["status"],
                lenses["executable"]["status"],
                str(item["gap_count"]),
                str(item["target_count"]),
            ]
        )
    if not rows:
        return "No matching architecture coverage records."
    widths = [max(len(headers[i]), *(len(row[i]) for row in rows)) for i in range(len(headers))]
    line = "  ".join(headers[i].ljust(widths[i]) for i in range(len(headers)))
    separator = "  ".join("-" * widths[i] for i in range(len(headers)))
    body = ["  ".join(row[i].ljust(widths[i]) for i in range(len(headers))) for row in rows]
    return "\n".join([line, separator, *body])


def main() -> int:
    parser = argparse.ArgumentParser(description="Query the Spotwo architecture reference coverage backlog.")
    parser.add_argument("--boundary")
    parser.add_argument("--priority", choices=("P0", "P1", "P2"))
    parser.add_argument("--overall", choices=("covered", "partial", "missing"))
    parser.add_argument("--contract-status", choices=("canonical", "candidate", "unresolved"))
    parser.add_argument("--classification")
    parser.add_argument("--lens", choices=("standards", "open_source", "commercial", "executable"))
    parser.add_argument("--lens-status", choices=("covered", "partial", "missing", "not-required"))
    parser.add_argument("--target")
    parser.add_argument("--gap")
    parser.add_argument("--evidence")
    parser.add_argument("--format", choices=("table", "json", "ndjson"), default="table")
    args = parser.parse_args()

    items = [item for item in records() if matches(item, args)]
    if args.format == "json":
        print(json.dumps(items, indent=2, ensure_ascii=False))
    elif args.format == "ndjson":
        for item in items:
            print(json.dumps(item, ensure_ascii=False))
    else:
        print(table(items))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
