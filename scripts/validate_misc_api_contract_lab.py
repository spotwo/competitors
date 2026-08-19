#!/usr/bin/env python3
from __future__ import annotations

import re
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
LAB = ROOT / "misc" / "api-contract-lab"
OPENAPI = LAB / "openapi.yaml"
PROTO = LAB / "spotwo.proto"
SCENARIOS = LAB / "scenarios.yml"
COMPARISON = LAB / "comparison.yml"
HTTP_METHODS = {"get", "post", "put", "patch", "delete", "options", "head", "trace"}
CANDIDATES = {"rest-openapi", "grpc"}


def load_yaml(path: Path) -> dict:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"{path.relative_to(ROOT)} must contain a mapping")
    return data


def proto_messages(text: str) -> dict[str, set[str]]:
    messages: dict[str, set[str]] = {}
    for match in re.finditer(r"message\s+(\w+)\s*\{(.*?)\n\}", text, flags=re.DOTALL):
        name, body = match.groups()
        fields = set(
            re.findall(
                r"^\s*(?:(?:optional|repeated)\s+)?(?:[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*)\s+([A-Za-z_]\w*)\s*=\s*\d+\s*;",
                body,
                flags=re.MULTILINE,
            )
        )
        messages[name] = fields
    return messages


def main() -> int:
    errors: list[str] = []

    try:
        openapi = load_yaml(OPENAPI)
        scenarios = load_yaml(SCENARIOS)
        comparison = load_yaml(COMPARISON)
    except (OSError, ValueError, yaml.YAMLError) as exc:
        print(f"MISC API contract lab validation failed: {exc}", file=sys.stderr)
        return 1

    proto = PROTO.read_text(encoding="utf-8")

    if not str(openapi.get("openapi", "")).startswith("3.1."):
        errors.append("openapi.yaml must use OpenAPI 3.1.x")

    operations: dict[tuple[str, str], str] = {}
    operation_ids: set[str] = set()
    for path, path_item in openapi.get("paths", {}).items():
        if not isinstance(path_item, dict):
            continue
        for method, operation in path_item.items():
            if method.lower() not in HTTP_METHODS or not isinstance(operation, dict):
                continue
            operation_id = operation.get("operationId")
            if not operation_id:
                errors.append(f"openapi operation {method.upper()} {path} has no operationId")
                continue
            key = (method.upper(), path)
            operations[key] = operation_id
            if operation_id in operation_ids:
                errors.append(f"duplicate OpenAPI operationId {operation_id!r}")
            operation_ids.add(operation_id)

    service_name = scenarios.get("service")
    service_match = re.search(rf"service\s+{re.escape(str(service_name))}\s*\{{(.*?)\n\}}", proto, flags=re.DOTALL)
    if not service_match:
        grpc_methods: set[str] = set()
        errors.append(f"gRPC service {service_name!r} is missing")
    else:
        grpc_methods = set(re.findall(r"rpc\s+(\w+)\s*\(", service_match.group(1)))

    seen_scenarios: set[str] = set()
    for scenario in scenarios.get("operations", []):
        scenario_id = scenario.get("id")
        if scenario_id in seen_scenarios:
            errors.append(f"duplicate scenario id {scenario_id!r}")
        seen_scenarios.add(scenario_id)

        rest = scenario.get("rest", {})
        key = (str(rest.get("method", "")).upper(), rest.get("path"))
        expected_operation_id = rest.get("operation_id")
        actual_operation_id = operations.get(key)
        if actual_operation_id != expected_operation_id:
            errors.append(
                f"scenario {scenario_id!r} expects REST {key[0]} {key[1]} operationId {expected_operation_id!r}, got {actual_operation_id!r}"
            )

        grpc_method = scenario.get("grpc", {}).get("method")
        if grpc_method not in grpc_methods:
            errors.append(f"scenario {scenario_id!r} references missing gRPC method {grpc_method!r}")

        if scenario.get("mutating") and scenario.get("idempotency") != "required":
            errors.append(f"mutating scenario {scenario_id!r} must require idempotency")

    declared_operation_ids = {
        scenario.get("rest", {}).get("operation_id") for scenario in scenarios.get("operations", [])
    }
    undeclared_openapi = operation_ids - declared_operation_ids
    if undeclared_openapi:
        errors.append(f"OpenAPI operations missing from scenarios.yml: {sorted(undeclared_openapi)}")

    declared_grpc = {scenario.get("grpc", {}).get("method") for scenario in scenarios.get("operations", [])}
    undeclared_grpc = grpc_methods - declared_grpc
    if undeclared_grpc:
        errors.append(f"gRPC methods missing from scenarios.yml: {sorted(undeclared_grpc)}")

    schemas = openapi.get("components", {}).get("schemas", {})
    messages = proto_messages(proto)
    for model_name, contract in scenarios.get("models", {}).items():
        expected = set(contract.get("fields", []))
        openapi_fields = set(schemas.get(model_name, {}).get("properties", {}))
        proto_fields = messages.get(model_name)
        if openapi_fields != expected:
            errors.append(
                f"OpenAPI model {model_name} fields differ: expected {sorted(expected)}, got {sorted(openapi_fields)}"
            )
        if proto_fields is None:
            errors.append(f"gRPC message {model_name!r} is missing")
        elif proto_fields != expected:
            errors.append(
                f"gRPC model {model_name} fields differ: expected {sorted(expected)}, got {sorted(proto_fields)}"
            )

    create_task = openapi.get("paths", {}).get("/v1/tasks", {}).get("post", {})
    parameter_refs = {
        parameter.get("$ref")
        for parameter in create_task.get("parameters", [])
        if isinstance(parameter, dict) and "$ref" in parameter
    }
    if "#/components/parameters/IdempotencyKey" not in parameter_refs:
        errors.append("REST createTask must require the shared IdempotencyKey parameter")
    idempotency_parameter = openapi.get("components", {}).get("parameters", {}).get("IdempotencyKey", {})
    if idempotency_parameter.get("name") != "Idempotency-Key" or idempotency_parameter.get("required") is not True:
        errors.append("OpenAPI IdempotencyKey must be the required Idempotency-Key header")
    if "idempotency_key" not in messages.get("CreateTaskRequest", set()):
        errors.append("gRPC CreateTaskRequest must contain idempotency_key")

    required_errors = {
        "invalid_argument": (400, "INVALID_ARGUMENT"),
        "not_found": (404, "NOT_FOUND"),
        "conflict": (409, "ABORTED"),
        "unavailable": (503, "UNAVAILABLE"),
    }
    for error_id, (http_status, grpc_status) in required_errors.items():
        actual = scenarios.get("errors", {}).get(error_id, {})
        if actual.get("http_status") != http_status or actual.get("grpc_status") != grpc_status:
            errors.append(f"error mapping {error_id!r} must remain HTTP {http_status} / gRPC {grpc_status}")

    if comparison.get("measurement_basis") != "structural-only":
        errors.append("comparison measurement_basis must remain structural-only until runtime measurements exist")

    totals_by_profile: dict[str, dict[str, float]] = {}
    for profile_id, profile in comparison.get("profiles", {}).items():
        criteria = profile.get("criteria", [])
        weights = {criterion.get("id"): criterion.get("weight") for criterion in criteria}
        if None in weights or sum(weights.values()) != 100:
            errors.append(f"comparison profile {profile_id!r} criterion weights must sum to 100")
            continue

        candidates = profile.get("candidates", {})
        if set(candidates) != CANDIDATES:
            errors.append(f"comparison profile {profile_id!r} must score exactly {sorted(CANDIDATES)}")
            continue

        totals: dict[str, float] = {}
        for candidate_id, candidate in candidates.items():
            scores = candidate.get("scores", {})
            if set(scores) != set(weights):
                errors.append(f"comparison profile {profile_id!r} candidate {candidate_id!r} has incomplete criteria")
                continue
            invalid = {key: value for key, value in scores.items() if not isinstance(value, int) or not 1 <= value <= 5}
            if invalid:
                errors.append(f"comparison profile {profile_id!r} candidate {candidate_id!r} has invalid scores {invalid}")
                continue
            totals[candidate_id] = sum(weights[key] * scores[key] for key in weights) / 100.0
        totals_by_profile[profile_id] = totals
        if len(totals) != 2:
            continue

        ranking = sorted(totals, key=lambda candidate: (-totals[candidate], candidate))
        declared_preferred = profile.get("decision", {}).get("preferred")
        if declared_preferred != ranking[0]:
            errors.append(
                f"comparison profile {profile_id!r} declares {declared_preferred!r} but scores select {ranking[0]!r}"
            )
        minimum_margin = float(profile.get("decision", {}).get("minimum_margin", 0))
        actual_margin = totals[ranking[0]] - totals[ranking[1]]
        if actual_margin + 1e-9 < minimum_margin:
            errors.append(
                f"comparison profile {profile_id!r} margin {actual_margin:.2f} is below required {minimum_margin:.2f}"
            )

    expected_preferred = {
        "external_business_api": "rest-openapi",
        "internal_realtime_candidate": "grpc",
    }
    for profile_id, candidate_id in expected_preferred.items():
        declared = comparison.get("profiles", {}).get(profile_id, {}).get("decision", {}).get("preferred")
        if declared != candidate_id:
            errors.append(f"comparison profile {profile_id!r} must currently prefer {candidate_id!r}")

    if errors:
        print("MISC API contract lab validation failed:", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1

    summaries = []
    for profile_id in sorted(totals_by_profile):
        totals = totals_by_profile[profile_id]
        if totals:
            summaries.append(
                f"{profile_id}:rest={totals.get('rest-openapi', 0):.2f},grpc={totals.get('grpc', 0):.2f}"
            )
    print(
        "MISC API contract lab validation passed: "
        f"{len(seen_scenarios)} operations, {len(scenarios.get('models', {}))} shared models, "
        + "; ".join(summaries)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
