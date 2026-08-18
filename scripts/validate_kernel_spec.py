#!/usr/bin/env python3
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import yaml
from jsonschema import Draft202012Validator, FormatChecker

ROOT = Path(__file__).resolve().parents[1]
SPEC_PATH = ROOT / "kernel/spec.yml"
SCHEMA_PATH = ROOT / "schema/kernel-spec.schema.json"
TEST_ROOT = ROOT / "labs/postgres-inventory-kernel/tests"
TEST_RE = re.compile(r"^def\s+test_[a-zA-Z0-9_]+\s*\(", re.MULTILINE)


def load_yaml(path: Path):
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def relative(path: Path) -> str:
    return str(path.relative_to(ROOT))


def require_path(errors: list[str], value: str, label: str) -> Path:
    path = ROOT / value
    if not path.exists():
        errors.append(f"{label} does not exist: {value}")
    return path


def main() -> int:
    errors: list[str] = []

    if not SPEC_PATH.exists():
        print("WMS Kernel spec validation failed: kernel/spec.yml is missing")
        return 1

    spec = load_yaml(SPEC_PATH)
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    for error in sorted(validator.iter_errors(spec), key=lambda item: list(item.absolute_path)):
        location = ".".join(map(str, error.absolute_path)) or "<root>"
        errors.append(f"kernel/spec.yml: {location}: {error.message}")

    if not isinstance(spec, dict):
        errors.append("kernel/spec.yml must contain an object")
        spec = {}

    contract_ids: set[str] = set()
    for contract in spec.get("stable_contracts", []) or []:
        if not isinstance(contract, dict):
            continue
        contract_id = contract.get("id")
        if contract_id in contract_ids:
            errors.append(f"duplicate stable contract id: {contract_id!r}")
        if contract_id:
            contract_ids.add(contract_id)
        adr = contract.get("adr")
        if isinstance(adr, str):
            require_path(errors, adr, f"stable contract {contract_id!r} ADR")

    executable = spec.get("executable_contract", {}) or {}
    postgres_lab = executable.get("postgres_lab")
    if isinstance(postgres_lab, str):
        require_path(errors, postgres_lab, "PostgreSQL lab")

    for migration in executable.get("migrations", []) or []:
        if isinstance(migration, str):
            require_path(errors, migration, "frozen migration")

    test_count = 0
    for path in sorted(TEST_ROOT.glob("test_*.py")):
        test_count += len(TEST_RE.findall(path.read_text(encoding="utf-8")))
    minimum_tests = executable.get("minimum_functional_tests", 0)
    if isinstance(minimum_tests, int) and test_count < minimum_tests:
        errors.append(
            f"functional test floor regressed: found {test_count}, frozen minimum is {minimum_tests}"
        )

    golden = spec.get("required_golden_scenarios", {}) or {}
    registry_value = golden.get("registry")
    scenario_ids: set[str] = set()
    if isinstance(registry_value, str):
        registry_path = require_path(errors, registry_value, "Golden Scenario registry")
        if registry_path.exists():
            registry = load_yaml(registry_path) or {}
            if isinstance(registry, dict):
                for scenario in registry.get("scenarios", []) or []:
                    if isinstance(scenario, dict) and scenario.get("id"):
                        scenario_ids.add(str(scenario["id"]))
    for required_id in golden.get("ids", []) or []:
        if required_id not in scenario_ids:
            errors.append(f"frozen Golden Scenario is missing: {required_id!r}")

    evidence = spec.get("benchmark_evidence", {}) or {}
    methodology = evidence.get("methodology")
    if isinstance(methodology, str):
        require_path(errors, methodology, "benchmark methodology")

    baseline_profiles: set[str] = set()
    for baseline in evidence.get("baselines", []) or []:
        if not isinstance(baseline, str):
            continue
        path = require_path(errors, baseline, "benchmark baseline")
        if not path.exists():
            continue
        try:
            document = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            errors.append(f"{baseline}: invalid JSON: {exc}")
            continue
        report = document.get("report", {}) if isinstance(document, dict) else {}
        profile = report.get("profile")
        if profile:
            baseline_profiles.add(str(profile))
        plan = report.get("candidate_plan", {}) or {}
        allocation = report.get("hot_allocation", {}) or {}
        transfers = report.get("opposite_transfers", {}) or {}
        if not plan.get("index_names"):
            errors.append(f"{baseline}: frozen source-selection evidence is not index-backed")
        if allocation.get("deadlocks") != 0:
            errors.append(f"{baseline}: frozen allocation evidence contains deadlocks")
        if allocation.get("successes") != allocation.get("capacity"):
            errors.append(f"{baseline}: allocation successes do not equal tested capacity")
        if allocation.get("final_allocated_qty") != float(allocation.get("capacity", -1)):
            errors.append(f"{baseline}: final allocated quantity does not equal tested capacity")
        if transfers.get("deadlocks_seen_by_clients") != 0 or transfers.get("deadlocks_pg_stat_delta") != 0:
            errors.append(f"{baseline}: frozen movement evidence contains deadlocks")
        if transfers.get("conserved_physical_qty") != 200000.0:
            errors.append(f"{baseline}: frozen movement evidence does not conserve physical quantity")

    for expected_profile in ("standard", "scale"):
        if expected_profile not in baseline_profiles:
            errors.append(f"benchmark evidence is missing required profile: {expected_profile}")

    if errors:
        print(f"WMS Kernel spec validation failed with {len(errors)} error(s):")
        for error in errors:
            print(f"- {error}")
        return 1

    print(
        "WMS Kernel spec validation OK: "
        f"version {spec.get('version')}, {len(contract_ids)} stable contracts, "
        f"{len(scenario_ids)} Golden Scenarios, {test_count} functional tests, "
        f"benchmark profiles {', '.join(sorted(baseline_profiles))}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
