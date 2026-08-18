#!/usr/bin/env python3
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import yaml
from jsonschema import Draft202012Validator, FormatChecker

ROOT = Path(__file__).resolve().parents[1]
SCHEMA = ROOT / "schema/scenario-registry.schema.json"
TEST_ROOT = ROOT / "labs/postgres-inventory-kernel"
TEST_REF_RE = re.compile(r"^(?P<path>tests/[^:]+\.py)::(?P<function>test_[a-z0-9_]+)$")


def load_yaml(path: Path):
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def main() -> int:
    errors: list[str] = []
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    scenario_ids: set[str] = set()
    test_refs: set[str] = set()
    registry_count = 0
    scenario_count = 0

    for path in sorted(ROOT.glob("scenarios/*.yml")):
        registry_count += 1
        data = load_yaml(path)
        for error in sorted(validator.iter_errors(data), key=lambda item: list(item.absolute_path)):
            location = ".".join(map(str, error.absolute_path)) or "<root>"
            errors.append(f"{path.relative_to(ROOT)}: {location}: {error.message}")

        if not isinstance(data, dict):
            continue

        for scenario in data.get("scenarios", []) or []:
            scenario_count += 1
            scenario_id = scenario.get("id")
            if scenario_id in scenario_ids:
                errors.append(f"{path.relative_to(ROOT)}: duplicate golden scenario id {scenario_id!r}")
            if scenario_id:
                scenario_ids.add(scenario_id)

            step_ids: set[str] = set()
            for step in scenario.get("steps", []) or []:
                step_id = step.get("id")
                if step_id in step_ids:
                    errors.append(
                        f"{path.relative_to(ROOT)}: scenario {scenario_id!r} duplicate step id {step_id!r}"
                    )
                if step_id:
                    step_ids.add(step_id)

            for checkpoint in scenario.get("checkpoints", []) or []:
                after = checkpoint.get("after")
                if after not in step_ids:
                    errors.append(
                        f"{path.relative_to(ROOT)}: scenario {scenario_id!r} checkpoint references unknown step {after!r}"
                    )

            test_ref = scenario.get("executable_test", "")
            if test_ref in test_refs:
                errors.append(
                    f"{path.relative_to(ROOT)}: executable test {test_ref!r} is reused by multiple scenarios"
                )
            if test_ref:
                test_refs.add(test_ref)

            match = TEST_REF_RE.fullmatch(test_ref)
            if not match:
                continue

            test_path = TEST_ROOT / match.group("path")
            if not test_path.exists():
                errors.append(
                    f"{path.relative_to(ROOT)}: scenario {scenario_id!r} executable test file does not exist: {match.group('path')}"
                )
                continue

            function_name = match.group("function")
            source = test_path.read_text(encoding="utf-8")
            if not re.search(rf"^def\s+{re.escape(function_name)}\s*\(", source, re.MULTILINE):
                errors.append(
                    f"{path.relative_to(ROOT)}: scenario {scenario_id!r} executable test function not found: {function_name}"
                )

    if registry_count == 0:
        errors.append("scenarios/: no golden scenario registry found")

    if errors:
        print(f"Golden scenario validation failed with {len(errors)} error(s):")
        for error in errors:
            print(f"- {error}")
        return 1

    print(f"Golden scenario validation OK: {registry_count} registries, {scenario_count} scenarios")
    return 0


if __name__ == "__main__":
    sys.exit(main())
