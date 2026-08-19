#!/usr/bin/env python3
from __future__ import annotations

import datetime as dt
import json
import sys
from pathlib import Path

import yaml
from jsonschema import Draft202012Validator, FormatChecker

ROOT = Path(__file__).resolve().parents[1]
CATALOG = ROOT / "research/open-source/catalog.yml"
SCHEMA = ROOT / "schema/open-source-project.schema.json"


def normalize(value):
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: normalize(item) for key, item in value.items()}
    if isinstance(value, list):
        return [normalize(item) for item in value]
    return value


def main() -> int:
    data = normalize(yaml.safe_load(CATALOG.read_text(encoding="utf-8")))
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema, format_checker=FormatChecker())

    errors = [
        f"{CATALOG.relative_to(ROOT)}: {'.'.join(map(str, error.absolute_path)) or '<root>'}: {error.message}"
        for error in sorted(validator.iter_errors(data), key=lambda item: list(item.absolute_path))
    ]

    categories = set(data.get("categories", []))
    seen_ids: set[str] = set()

    for project in data.get("projects", []):
        project_id = project.get("id", "<missing>")
        category = project.get("category")
        assessment = project.get("spotwo", {})

        if project_id in seen_ids:
            errors.append(f"{CATALOG.relative_to(ROOT)}: duplicate project id {project_id!r}")
        seen_ids.add(project_id)

        if category not in categories:
            errors.append(
                f"{CATALOG.relative_to(ROOT)}: project {project_id!r} uses undeclared category {category!r}"
            )

        if assessment.get("priority") == "P0" and assessment.get("relevance") != 5:
            errors.append(
                f"{CATALOG.relative_to(ROOT)}: P0 project {project_id!r} must have relevance 5"
            )

        if assessment.get("priority") == "P0" and not assessment.get("study_architecture"):
            errors.append(
                f"{CATALOG.relative_to(ROOT)}: P0 project {project_id!r} must be marked for architecture study"
            )

        if project.get("status") == "archived" and assessment.get("use_directly") is True:
            errors.append(
                f"{CATALOG.relative_to(ROOT)}: archived project {project_id!r} cannot be marked use_directly=true"
            )

    if errors:
        print("Open-source research validation failed:", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1

    priorities: dict[str, int] = {priority: 0 for priority in ("P0", "P1", "P2", "P3")}
    for project in data.get("projects", []):
        priorities[project["spotwo"]["priority"]] += 1

    print(
        "Open-source research validation passed: "
        f"{len(seen_ids)} projects, "
        + ", ".join(f"{priority}={count}" for priority, count in priorities.items())
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
