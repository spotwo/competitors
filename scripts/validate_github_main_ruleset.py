#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
RULESET_PATH = ROOT / ".github" / "rulesets" / "main.json"
WORKFLOWS = {
    "validate": ROOT / ".github" / "workflows" / "validate.yml",
    "postgres-kernel-lab": ROOT / ".github" / "workflows" / "kernel-lab.yml",
    "cleanup-guard": ROOT / ".github" / "workflows" / "topology-finalization-pr-guard.yml",
}
EXPECTED_CONTEXTS = tuple(WORKFLOWS)


def fail(message: str) -> None:
    raise SystemExit(f"GitHub main ruleset invalid: {message}")


def load_ruleset() -> dict:
    try:
        value = json.loads(RULESET_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        fail(str(exc))
    if not isinstance(value, dict):
        fail("ruleset root must be an object")
    return value


def workflow_mapping(path: Path) -> dict:
    try:
        value = yaml.load(path.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)
    except (OSError, yaml.YAMLError) as exc:
        fail(f"{path.relative_to(ROOT)}: {exc}")
    if not isinstance(value, dict):
        fail(f"{path.relative_to(ROOT)}: workflow root must be an object")
    return value


def rule_by_type(ruleset: dict, rule_type: str) -> dict:
    matches = [rule for rule in ruleset.get("rules", []) if isinstance(rule, dict) and rule.get("type") == rule_type]
    if len(matches) != 1:
        fail(f"expected exactly one {rule_type!r} rule")
    return matches[0]


def validate_ruleset(ruleset: dict) -> None:
    if ruleset.get("name") != "Protect main merge gates":
        fail("unexpected ruleset name")
    if ruleset.get("target") != "branch":
        fail("target must be branch")
    if ruleset.get("enforcement") != "active":
        fail("enforcement must be active")

    conditions = ruleset.get("conditions")
    if not isinstance(conditions, dict):
        fail("conditions must be an object")
    ref_name = conditions.get("ref_name")
    if not isinstance(ref_name, dict):
        fail("conditions.ref_name must be an object")
    if ref_name.get("include") != ["refs/heads/main"] or ref_name.get("exclude") != []:
        fail("ruleset must target only refs/heads/main")

    for rule_type in ("deletion", "non_fast_forward", "required_linear_history"):
        rule_by_type(ruleset, rule_type)

    pull_request = rule_by_type(ruleset, "pull_request")
    pr_parameters = pull_request.get("parameters")
    if not isinstance(pr_parameters, dict):
        fail("pull_request.parameters must be an object")
    if pr_parameters.get("allowed_merge_methods") != ["squash"]:
        fail("main must allow squash merge only")
    if pr_parameters.get("required_approving_review_count") != 0:
        fail("solo-agent workflow must not require an approval count")

    status_rule = rule_by_type(ruleset, "required_status_checks")
    status_parameters = status_rule.get("parameters")
    if not isinstance(status_parameters, dict):
        fail("required_status_checks.parameters must be an object")
    if status_parameters.get("strict_required_status_checks_policy") is not True:
        fail("required status checks must use strict mode")
    contexts = status_parameters.get("required_status_checks")
    if not isinstance(contexts, list):
        fail("required_status_checks list missing")
    actual_contexts = tuple(
        item.get("context") for item in contexts if isinstance(item, dict) and isinstance(item.get("context"), str)
    )
    if actual_contexts != EXPECTED_CONTEXTS:
        fail(f"required contexts must be {EXPECTED_CONTEXTS!r}, got {actual_contexts!r}")


def validate_required_workflows() -> None:
    for job_name, path in WORKFLOWS.items():
        workflow = workflow_mapping(path)
        triggers = workflow.get("on")
        if not isinstance(triggers, dict) or "pull_request" not in triggers:
            fail(f"{path.relative_to(ROOT)} must run on pull_request")
        pull_request = triggers["pull_request"]
        if isinstance(pull_request, dict) and any(key in pull_request for key in ("paths", "paths-ignore")):
            fail(f"{path.relative_to(ROOT)} cannot path-filter a required pull_request check")
        jobs = workflow.get("jobs")
        if not isinstance(jobs, dict) or job_name not in jobs:
            fail(f"{path.relative_to(ROOT)} must expose required job context {job_name!r}")


def main() -> int:
    validate_ruleset(load_ruleset())
    validate_required_workflows()
    print("GitHub main ruleset contract valid")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
