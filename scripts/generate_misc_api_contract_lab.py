#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
LAB = ROOT / "misc" / "api-contract-lab"
COMPARISON = LAB / "comparison.yml"
SCENARIOS = LAB / "scenarios.yml"
OUTPUT = LAB / "comparison.md"


def score(profile: dict, candidate_id: str) -> float:
    weights = {criterion["id"]: criterion["weight"] for criterion in profile["criteria"]}
    scores = profile["candidates"][candidate_id]["scores"]
    return sum(weights[key] * scores[key] for key in weights) / 100.0


def render() -> str:
    comparison = yaml.safe_load(COMPARISON.read_text(encoding="utf-8"))
    scenarios = yaml.safe_load(SCENARIOS.read_text(encoding="utf-8"))

    lines = [
        "# MISC API Contract Lab Comparison",
        "",
        "> Generated from `misc/api-contract-lab/comparison.yml` and `scenarios.yml`. Do not edit manually.",
        "",
        "This is a structural experiment, not a production benchmark or canonical API decision.",
        "",
        "## Profile results",
        "",
        "| Profile | Status | REST + OpenAPI | gRPC | Preferred | Margin |",
        "|---|---|---:|---:|---|---:|",
    ]

    for profile_id, profile in comparison["profiles"].items():
        rest = score(profile, "rest-openapi")
        grpc = score(profile, "grpc")
        preferred = profile["decision"]["preferred"]
        other = "grpc" if preferred == "rest-openapi" else "rest-openapi"
        totals = {"rest-openapi": rest, "grpc": grpc}
        margin = totals[preferred] - totals[other]
        lines.append(
            f"| `{profile_id}` | `{profile['status']}` | {rest:.2f} / 5 | {grpc:.2f} / 5 | `{preferred}` | {margin:.2f} |"
        )

    lines.extend(
        [
            "",
            "## Semantic operation parity",
            "",
            "| Operation | REST | gRPC | Mutating | Idempotency |",
            "|---|---|---|---|---|",
        ]
    )
    for operation in scenarios["operations"]:
        rest = operation["rest"]
        grpc = operation["grpc"]
        mutating = "yes" if operation["mutating"] else "no"
        lines.append(
            f"| `{operation['id']}` | `{rest['method']} {rest['path']}` (`{rest['operation_id']}`) | `{grpc['method']}` | {mutating} | `{operation['idempotency']}` |"
        )

    lines.extend(
        [
            "",
            "## Shared model parity",
            "",
            "| Model | Canonical fields in both contracts |",
            "|---|---|",
        ]
    )
    for model_name in sorted(scenarios["models"]):
        fields = ", ".join(f"`{field}`" for field in scenarios["models"][model_name]["fields"])
        lines.append(f"| `{model_name}` | {fields} |")

    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "For the external business API scope, REST/OpenAPI currently wins on HTTP/browser ubiquity, customer tooling, human debugging, agent tooling, and ordinary edge/proxy compatibility. gRPC remains stronger in the structural internal-realtime hypothesis because typed generated RPC contracts, wire efficiency, and bidirectional streaming receive more weight there.",
            "",
            "The internal result is intentionally only a hypothesis. No production latency, throughput, CPU, memory, connection-scale, or failure-recovery benchmark is recorded by this lab.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    rendered = render()

    if args.check:
        current = OUTPUT.read_text(encoding="utf-8") if OUTPUT.exists() else ""
        if current != rendered:
            print(f"{OUTPUT.relative_to(ROOT)} is stale")
            return 1
        print("MISC API contract lab comparison is current")
        return 0

    OUTPUT.write_text(rendered, encoding="utf-8")
    print(f"Wrote {OUTPUT.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
