#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from pathlib import Path

import jsonschema
import yaml

ROOT = Path(__file__).resolve().parents[1]
REGISTRY = ROOT / "operations" / "event-pipelines" / "registry.yml"
SCHEMA = ROOT / "schema" / "event-pipeline-deployment.schema.json"
LAB = ROOT / "labs" / "postgres-inventory-kernel"
sys.path.insert(0, str(LAB))

from event_pipeline_readiness import EventPipelineDeploymentRegistry  # noqa: E402
from event_pipeline_registry_validation import (  # noqa: E402
    validate_global_consumer_identities,
)
from event_pipeline_topology_config import (  # noqa: E402
    DeploymentTopologyConfig,
    pipeline_topology_mapping,
)
from event_pipeline_topology_migration import (  # noqa: E402
    TopologyMigrationConfig,
    pipeline_topology_migration_mapping,
)


def main() -> int:
    with REGISTRY.open("r", encoding="utf-8") as handle:
        registry_data = yaml.safe_load(handle)
    with SCHEMA.open("r", encoding="utf-8") as handle:
        schema = json.load(handle)

    validator = jsonschema.Draft202012Validator(
        schema,
        format_checker=jsonschema.FormatChecker(),
    )
    errors = sorted(
        validator.iter_errors(registry_data),
        key=lambda error: tuple(str(part) for part in error.absolute_path),
    )
    if errors:
        for error in errors:
            path = ".".join(str(part) for part in error.absolute_path) or "<root>"
            print(
                f"event pipeline deployment schema error at {path}: {error.message}",
                file=sys.stderr,
            )
        return 1

    try:
        registry = EventPipelineDeploymentRegistry.from_mapping(registry_data)
        validate_global_consumer_identities(registry)
        for spec in registry.pipelines:
            topology = pipeline_topology_mapping(registry_data, spec.pipeline_id)
            DeploymentTopologyConfig.from_mapping(topology).validate(spec)
            migration = pipeline_topology_migration_mapping(
                registry_data,
                spec.pipeline_id,
            )
            if migration is not None:
                TopologyMigrationConfig.from_mapping(migration).source_topology(
                    topology,
                    spec,
                )
    except (KeyError, ValueError) as exc:
        print(f"event pipeline deployment invariant error: {exc}", file=sys.stderr)
        return 1

    enabled = sum(1 for pipeline in registry.pipelines if pipeline.enabled)
    print(
        "event pipeline deployments valid: "
        f"{len(registry.pipelines)} configured, {enabled} enabled"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
