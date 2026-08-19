#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
REGISTRY = ROOT / "architecture" / "boundaries.yml"


def normalize(value: Any) -> Any:
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: normalize(item) for key, item in value.items()}
    if isinstance(value, list):
        return [normalize(item) for item in value]
    return value


def load() -> list[dict[str, Any]]:
    data = normalize(yaml.safe_load(REGISTRY.read_text(encoding="utf-8")))
    return data["boundaries"]


def contains(value: Any, needle: str | None) -> bool:
    if needle is None:
        return True
    return needle.casefold() in json.dumps(value, ensure_ascii=False, sort_keys=True).casefold()


def matches(boundary: dict[str, Any], args: argparse.Namespace) -> bool:
    if args.id and args.id.casefold() not in boundary["id"].casefold():
        return False
    if args.classification and args.classification.casefold() not in boundary["classification"].casefold():
        return False
    if args.status and args.status != boundary["contract"]["status"]:
        return False
    if args.technology:
        searchable = {
            "mechanism": boundary["contract"]["mechanism"],
            "technology_decision_refs": boundary["evidence"]["technology_decision_refs"],
            "open_source_project_refs": boundary["evidence"]["open_source_project_refs"],
        }
        if not contains(searchable, args.technology):
            return False
    if args.standard and not contains(boundary["standards"], args.standard):
        return False
    if args.gap and not contains(boundary["research_gaps"], args.gap):
        return False
    if args.has_gaps and not boundary["research_gaps"]:
        return False
    return True


def compact(boundary: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": boundary["id"],
        "name": boundary["name"],
        "classification": boundary["classification"],
        "from": boundary["from"],
        "to": boundary["to"],
        "contract": boundary["contract"],
        "interaction": boundary["flow"]["interaction"],
        "direction": boundary["flow"]["direction"],
        "standards": boundary["standards"],
        "technology_decision_refs": boundary["evidence"]["technology_decision_refs"],
        "research_gaps": boundary["research_gaps"],
    }


def table(records: list[dict[str, Any]]) -> str:
    rows = [(item["id"], item["contract"]["status"], item["classification"], item["contract"]["mechanism"], str(len(item["research_gaps"]))) for item in records]
    headers = ("BOUNDARY", "STATUS", "CLASSIFICATION", "CONTRACT", "GAPS")
    widths = [len(value) for value in headers]
    for row in rows:
        for index, value in enumerate(row):
            widths[index] = max(widths[index], len(value))
    output = ["  ".join(headers[index].ljust(widths[index]) for index in range(len(headers)))]
    output.append("  ".join("-" * width for width in widths))
    output.extend("  ".join(row[index].ljust(widths[index]) for index in range(len(row))) for row in rows)
    return "\n".join(output)


def main() -> int:
    parser = argparse.ArgumentParser(description="Query the Spotwo architecture boundary registry")
    parser.add_argument("--id")
    parser.add_argument("--classification")
    parser.add_argument("--status", choices=("canonical", "candidate", "unresolved"))
    parser.add_argument("--technology")
    parser.add_argument("--standard")
    parser.add_argument("--gap")
    parser.add_argument("--has-gaps", action="store_true")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--format", choices=("table", "json", "ndjson"), default="table")
    args = parser.parse_args()

    records = [compact(item) for item in load() if matches(item, args)]
    records.sort(key=lambda item: item["id"])
    records = records[: max(args.limit, 0)]

    if args.format == "json":
        print(json.dumps(records, indent=2, ensure_ascii=False, sort_keys=True))
    elif args.format == "ndjson":
        for record in records:
            print(json.dumps(record, ensure_ascii=False, sort_keys=True))
    else:
        print(table(records))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
