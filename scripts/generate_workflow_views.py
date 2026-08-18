#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parents[1]


def load(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def picking_claims() -> list[dict]:
    claims = []
    for path in sorted(ROOT.glob("claims/*.yml")):
        claim = load(path)
        if claim.get("predicate") == "picking-model" and claim.get("status") == "current":
            claims.append(claim)
    return claims


def summary_markdown(workflows: list[dict]) -> str:
    lines = [
        "# Workflow Decomposition Matrix",
        "",
        "> Generated from `workflows/*.yml`. Do not edit by hand.",
        "",
        "| Workflow | Capability | Status | Stages | Objects | States | Strategies | Exceptions | Vendor models | Evidence |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for workflow in workflows:
        lines.append(
            f"| {workflow['title']} | {workflow['capability_id']} | {workflow['status']} | "
            f"{len(workflow.get('stages', []))} | {len(workflow.get('objects', []))} | "
            f"{len(workflow.get('states', []))} | {len(workflow.get('strategies', []))} | "
            f"{len(workflow.get('exceptions', []))} | {len(workflow.get('vendor_models', []))} | "
            f"{len(workflow.get('evidence_refs', []))} |"
        )
    return "\n".join(lines) + "\n"


def render_claim_value(value) -> str:
    if isinstance(value, dict):
        parts = []
        for key, item in value.items():
            rendered = ", ".join(str(v) for v in item) if isinstance(item, list) else str(item)
            parts.append(f"{key}: {rendered}")
        return "; ".join(parts)
    return str(value)


def supplemental_markdown(claims: list[dict]) -> str:
    lines = [
        "# Picking Supplemental Vendor Observations",
        "",
        "> Generated from current claims with `predicate: picking-model`. These broaden the market sample without pretending every observation has a full deep-workflow mapping.",
        "",
        "| Vendor | Observation | Confidence | Evidence refs |",
        "|---|---|---|---:|",
    ]
    for claim in sorted(claims, key=lambda c: c["subject"]):
        lines.append(
            f"| {claim['subject']} | {render_claim_value(claim['value'])} | "
            f"{claim['confidence']} | {len(claim.get('evidence_refs', []))} |"
        )
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()

    workflows = [load(path) for path in sorted(ROOT.glob("workflows/*.yml"))]
    outputs = {
        ROOT / "matrices/workflows.md": summary_markdown(workflows),
        ROOT / "matrices/picking-supplemental-observations.md": supplemental_markdown(picking_claims()),
    }

    stale = []
    for path, content in outputs.items():
        if args.check:
            if not path.exists() or path.read_text(encoding="utf-8") != content:
                stale.append(str(path.relative_to(ROOT)))
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")
    if stale:
        raise SystemExit("Generated workflow views are stale: " + ", ".join(stale))


if __name__ == "__main__":
    main()
