#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import yaml
from jsonschema import Draft202012Validator, FormatChecker

from topology_finalization_pr_guard import (
    REGISTRY_PATH,
    GuardError,
    evaluate_cleanup_pr,
)

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "schema" / "topology-finalization-reference.schema.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Guard topology_migration cleanup pull requests with immutable receipt references"
    )
    parser.add_argument("--base", required=True, help="Base commit SHA")
    parser.add_argument("--head", required=True, help="Pull request head commit SHA")
    parser.add_argument(
        "--pr-body-env",
        default="PR_BODY",
        help="Environment variable containing the pull request body",
    )
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def git_output(*args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=ROOT,
        check=False,
        text=True,
        capture_output=True,
    )
    if result.returncode != 0:
        raise GuardError(result.stderr.strip() or f"git {' '.join(args)} failed")
    return result.stdout


def git_yaml(ref: str, path: str) -> dict[str, Any]:
    raw = git_output("show", f"{ref}:{path}")
    value = yaml.safe_load(raw)
    if not isinstance(value, dict):
        raise GuardError(f"{path} at {ref} must contain a YAML object")
    return value


def changed_files(base: str, head: str) -> set[str]:
    return {
        line.strip()
        for line in git_output("diff", "--name-only", f"{base}...{head}").splitlines()
        if line.strip()
    }


def load_schema() -> dict[str, Any]:
    with SCHEMA_PATH.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def validate_schema(reference: dict[str, Any], path: str) -> None:
    errors = sorted(
        Draft202012Validator(
            load_schema(), format_checker=FormatChecker()
        ).iter_errors(reference),
        key=lambda error: tuple(str(part) for part in error.absolute_path),
    )
    if not errors:
        return
    messages = []
    for error in errors:
        location = ".".join(str(part) for part in error.absolute_path) or "$"
        messages.append(f"{path}:{location}: {error.message}")
    raise GuardError("; ".join(messages))


def main() -> int:
    args = parse_args()
    try:
        base_registry = git_yaml(args.base, REGISTRY_PATH)
        head_registry = git_yaml(args.head, REGISTRY_PATH)
        files = changed_files(args.base, args.head)

        references: dict[str, dict[str, Any]] = {}
        for path in sorted(files):
            if not path.startswith("operations/event-pipelines/finalizations/"):
                continue
            if not path.endswith(".yml"):
                continue
            reference = git_yaml(args.head, path)
            validate_schema(reference, path)
            references[path] = reference

        report = evaluate_cleanup_pr(
            base_registry=base_registry,
            head_registry=head_registry,
            changed_files=files,
            references=references,
            pr_body=os.getenv(args.pr_body_env),
        )
    except GuardError as exc:
        payload = {
            "status": "blocked",
            "code": "topology_finalization_cleanup_guard_error",
            "cleanups": [],
            "errors": [str(exc)],
        }
        print(json.dumps(payload, indent=2 if args.pretty else None, sort_keys=True))
        return 2

    print(json.dumps(report.to_dict(), indent=2 if args.pretty else None, sort_keys=True))
    return 0 if report.allowed else 2


if __name__ == "__main__":
    raise SystemExit(main())
