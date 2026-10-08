#!/usr/bin/env python3
"""Generate a reviewed, disabled Event Pipeline contract with fail-closed code stubs."""

from __future__ import annotations

import argparse
import copy
import json
import os
import re
import stat
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import jsonschema
import yaml

ROOT = Path(__file__).resolve().parents[1]
LAB = ROOT / "labs" / "postgres-inventory-kernel"
DEFAULT_REGISTRY = ROOT / "operations" / "event-pipelines" / "registry.yml"
DEFAULT_HANDLERS = LAB / "event_pipeline_handlers"
DEFAULT_TESTS = LAB / "tests"

sys.path.insert(0, str(LAB))

from event_pipeline_readiness import EventPipelineDeploymentRegistry  # noqa: E402
from event_pipeline_registry_validation import (  # noqa: E402
    validate_global_consumer_identities,
)
from event_pipeline_topology_config import DeploymentTopologyConfig  # noqa: E402


PIPELINE_ID = re.compile(r"^[a-z][a-z0-9]*(?:-[a-z0-9]+)*$")
EVENT_TYPE = re.compile(r"^[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)+$")
MAX_PIPELINE_ID_LENGTH = 48


@dataclass(frozen=True)
class ScaffoldRequest:
    pipeline_id: str
    event_type: str
    from_pipeline: str = "warehouse-work-state-projection"

    def __post_init__(self) -> None:
        if (
            not PIPELINE_ID.fullmatch(self.pipeline_id)
            or len(self.pipeline_id) > MAX_PIPELINE_ID_LENGTH
        ):
            raise ValueError(
                "pipeline id must be a lowercase hyphenated slug of at most 48 characters"
            )
        if not EVENT_TYPE.fullmatch(self.event_type):
            raise ValueError(
                "event type must contain at least two lowercase dotted tokens without wildcards"
            )
        if not PIPELINE_ID.fullmatch(self.from_pipeline):
            raise ValueError("source pipeline must be a lowercase hyphenated slug")

    @property
    def slug(self) -> str:
        return self.pipeline_id.replace("-", "_")

    @property
    def handler_module(self) -> str:
        return f"event_pipeline_handlers.{self.slug}"

    @property
    def handler_class(self) -> str:
        return "".join(segment.capitalize() for segment in self.pipeline_id.split("-")) + "Projector"


def _registry_yaml(data: str) -> dict[str, Any]:
    parsed = yaml.safe_load(data)
    if not isinstance(parsed, dict) or not isinstance(parsed.get("pipelines"), list):
        raise ValueError("event pipeline registry must be an object with a pipelines list")
    return parsed


def _template(registry: dict[str, Any], name: str) -> dict[str, Any]:
    for pipeline in registry["pipelines"]:
        if isinstance(pipeline, dict) and pipeline.get("id") == name:
            return pipeline
    raise ValueError(f"source pipeline {name} was not found")


def build_pipeline(registry: dict[str, Any], request: ScaffoldRequest) -> dict[str, Any]:
    if any(
        isinstance(item, dict) and item.get("id") == request.pipeline_id
        for item in registry["pipelines"]
    ):
        raise ValueError(f"pipeline {request.pipeline_id} already exists")

    source = _template(registry, request.from_pipeline)
    result = copy.deepcopy(source)
    result.pop("topology_migration", None)
    result["id"] = request.pipeline_id
    result["enabled"] = False

    names = request.slug.upper()
    result["runtime"]["watchdog_state_path_env"] = (
        f"KERNEL_LAB_{names}_CANARY_WATCHDOG_STATE_PATH"
    )
    result["consumer"].update(
        durable=f"{names}_PROJECTOR",
        inbox_consumer_name=request.slug,
        projection_gap_monitor="none",
        handler_module=request.handler_module,
        handler_class=request.handler_class,
    )
    result["canary"].update(
        durable=f"{names}_CANARY",
        consumer_name=f"{request.slug}_canary",
    )
    result["topology"]["business_consumer"]["filter_subject"] = (
        f"{result['transport']['subject_prefix']}.{request.event_type}"
    )

    # One JetStream business subject must belong to exactly one durable projector.
    new_subject = result["topology"]["business_consumer"]["filter_subject"]
    for pipeline in registry["pipelines"]:
        existing_subject = pipeline["topology"]["business_consumer"]["filter_subject"]
        if existing_subject == new_subject:
            raise ValueError(f"business event subject already registered: {new_subject}")
    return result


def validate_candidate(registry: dict[str, Any], candidate: dict[str, Any]) -> None:
    proposed = copy.deepcopy(registry)
    proposed["pipelines"].append(candidate)

    with (ROOT / "schema" / "event-pipeline-deployment.schema.json").open(
        encoding="utf-8"
    ) as handle:
        schema = json.load(handle)
    jsonschema.Draft202012Validator(schema).validate(proposed)

    parsed = EventPipelineDeploymentRegistry.from_mapping(proposed)
    validate_global_consumer_identities(parsed)
    for item in proposed["pipelines"]:
        spec = parsed.get(item["id"])
        DeploymentTopologyConfig.from_mapping(item["topology"]).validate(spec)


def yaml_fragment(candidate: dict[str, Any]) -> str:
    """Append under the existing pipelines list without rewriting reviewed comments."""
    item = yaml.safe_dump([candidate], sort_keys=False, allow_unicode=True)
    return "".join(f"  {line}\n" for line in item.rstrip("\n").split("\n"))


def handler_stub(request: ScaffoldRequest) -> str:
    return (
        '"""Scaffold only: implement the domain projection before enabling this pipeline."""\n'
        "\n"
        "from __future__ import annotations\n"
        "\n"
        "from consumer_runtime import ConsumedEvent, InboxTransaction\n"
        "\n"
        "\n"
        f"class {request.handler_class}:\n"
        "    def __init__(self, *, consumer_name: str):\n"
        "        if not consumer_name or consumer_name != consumer_name.strip():\n"
        '            raise ValueError("consumer_name is required")\n'
        "        self.consumer_name = consumer_name\n"
        "\n"
        "    def __call__(self, event: ConsumedEvent, transaction: InboxTransaction) -> None:\n"
        f'        if event.event_type != "{request.event_type}":\n'
        '            raise ValueError("unexpected event type for this projector")\n'
        "        raise NotImplementedError(\n"
        f'            "{request.pipeline_id} needs a domain projection and JetStream E2E proof"\n'
        "        )\n"
    )


def test_stub(request: ScaffoldRequest) -> str:
    return (
        '"""Contract guard: replace the skipped E2E test before enabling the pipeline."""\n'
        "\n"
        "from __future__ import annotations\n"
        "\n"
        "from pathlib import Path\n"
        "from types import SimpleNamespace\n"
        "\n"
        "import pytest\n"
        "\n"
        "from event_pipeline_handler_loader import load_pipeline_handler_type\n"
        "from event_pipeline_readiness import load_registry\n"
        "\n"
        "ROOT = Path(__file__).resolve().parents[3]\n"
        "REGISTRY = ROOT / 'operations' / 'event-pipelines' / 'registry.yml'\n"
        "\n"
        "\n"
        "def test_scaffold_stays_disabled_and_handler_fails_closed():\n"
        f"    spec = load_registry(REGISTRY).get('{request.pipeline_id}')\n"
        "    assert spec.enabled is False\n"
        f"    assert spec.consumer.handler_module == '{request.handler_module}'\n"
        f"    assert spec.consumer.handler_class == '{request.handler_class}'\n"
        "    handler = load_pipeline_handler_type(spec.consumer)(\n"
        "        consumer_name=spec.consumer.inbox_consumer_name\n"
        "    )\n"
        "    with pytest.raises(NotImplementedError):\n"
        f"        handler(SimpleNamespace(event_type='{request.event_type}'), None)\n"
        "\n"
        "\n"
        "@pytest.mark.skip(reason='Replace with PostgreSQL/JetStream E2E proof before enablement')\n"
        "def test_domain_outbox_to_jetstream_inbox_projection_e2e():\n"
        "    # Prove ordered apply, redelivery deduplication, gap handling, and ACK after commit.\n"
        "    raise AssertionError('domain E2E test not implemented')\n"
    )


def scaffold(
    request: ScaffoldRequest,
    *,
    registry_path: Path = DEFAULT_REGISTRY,
    handlers_dir: Path = DEFAULT_HANDLERS,
    tests_dir: Path = DEFAULT_TESTS,
    write: bool = False,
) -> tuple[dict[str, Any], str]:
    original = registry_path.read_text(encoding="utf-8")
    registry = _registry_yaml(original)
    candidate = build_pipeline(registry, request)
    validate_candidate(registry, candidate)
    fragment = yaml_fragment(candidate)
    if not write:
        return candidate, fragment

    handler_path = handlers_dir / f"{request.slug}.py"
    test_path = tests_dir / f"test_{request.slug}_scaffold.py"
    if not handlers_dir.is_dir() or not tests_dir.is_dir():
        raise ValueError("handler and test directories must already exist")
    if handler_path.exists() or test_path.exists():
        raise ValueError("a handler or test file already exists; refusing overwrite")

    # Creation is exclusive; a concurrent/repeated scaffold cannot clobber code.
    created: list[Path] = []
    temp_path: Path | None = None
    try:
        for path, body in (
            (handler_path, handler_stub(request)),
            (test_path, test_stub(request)),
        ):
            with path.open("x", encoding="utf-8") as target:
                created.append(path)
                target.write(body)

        if registry_path.read_text(encoding="utf-8") != original:
            raise RuntimeError("registry changed during scaffolding; abort and retry")
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=registry_path.parent,
            prefix=".event-pipeline-scaffold-",
            delete=False,
        ) as output:
            temp_path = Path(output.name)
            output.write(original.rstrip("\n") + "\n\n" + fragment)
        os.chmod(temp_path, stat.S_IMODE(registry_path.stat().st_mode))
        os.replace(temp_path, registry_path)
        temp_path = None
    except Exception:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)
        for path in reversed(created):
            path.unlink(missing_ok=True)
        raise
    return candidate, fragment


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Scaffold a disabled event pipeline; dry-run unless --write is given"
    )
    parser.add_argument("--pipeline-id", required=True)
    parser.add_argument("--event-type", required=True)
    parser.add_argument(
        "--from-pipeline", default="warehouse-work-state-projection"
    )
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()

    try:
        request = ScaffoldRequest(args.pipeline_id, args.event_type, args.from_pipeline)
        candidate, fragment = scaffold(
            request, registry_path=args.registry, write=args.write
        )
    except (ValueError, OSError, RuntimeError, jsonschema.ValidationError) as exc:
        parser.error(str(exc))

    if args.write:
        print(f"Scaffolded disabled pipeline {candidate['id']}.")
        print("Implement the handler and replace the skipped E2E proof before enabling.")
    else:
        print("# Dry run. No files changed. Append to pipelines only with --write:")
        print(fragment, end="")
        print("# --write creates a fail-closed handler and an explicit E2E TODO.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
