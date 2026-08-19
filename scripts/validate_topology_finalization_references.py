#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml
from jsonschema import Draft202012Validator, FormatChecker

from topology_finalization_pr_guard import (
    GuardError,
    deterministic_reference_path,
    validate_reference_structure,
)

ROOT = Path(__file__).resolve().parents[1]
REFERENCES_ROOT = ROOT / "operations" / "event-pipelines" / "finalizations"
SCHEMA_PATH = ROOT / "schema" / "topology-finalization-reference.schema.json"


def load_schema() -> dict[str, Any]:
    with SCHEMA_PATH.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def main() -> int:
    schema = load_schema()
    errors: list[str] = []
    if not REFERENCES_ROOT.exists():
        print("Topology finalization references valid: 0 files")
        return 0

    files = sorted(REFERENCES_ROOT.rglob("*.yml"))
    for path in files:
        with path.open("r", encoding="utf-8") as handle:
            value = yaml.safe_load(handle)
        if not isinstance(value, dict):
            errors.append(f"{path.relative_to(ROOT)}: root must be an object")
            continue
        schema_errors = sorted(
            Draft202012Validator(
                schema, format_checker=FormatChecker()
            ).iter_errors(value),
            key=lambda error: tuple(str(part) for part in error.absolute_path),
        )
        for error in schema_errors:
            location = ".".join(str(part) for part in error.absolute_path) or "$"
            errors.append(f"{path.relative_to(ROOT)}:{location}: {error.message}")
        try:
            validate_reference_structure(value)
            expected = deterministic_reference_path(value["pipeline"], value["migration_id"])
            actual = str(path.relative_to(ROOT))
            if actual != expected:
                errors.append(f"{actual}: expected deterministic path {expected}")
        except (GuardError, KeyError, ValueError) as exc:
            errors.append(f"{path.relative_to(ROOT)}: {exc}")

    if errors:
        for error in errors:
            print(error)
        return 1
    print(f"Topology finalization references valid: {len(files)} files")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
