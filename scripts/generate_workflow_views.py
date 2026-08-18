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


def summary_markdown(workflows: list[dict], supplemental: list[dict]) -> str:
    lines = [
        "# Workflow Decomposition Matrix",
        "",
        "> Generated from `workflows/*.yml` and supplemental workflow claims. Do not edit by hand.",
        "",
        "| Workflow | Capability | Status | Stages | Objects | States | Strategies | Exceptions | Deep vendor models | Supplemental vendor claims | Evidence |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for workflow in workflows:
        extra = supplemental if workflow["capability_id"] == "picking" else []
        lines.append(
            f"| {workflow['title']} | {workflow['capability_id']} | {workflow['status']} | "
            f"{len(workflow.get('stages', []))} | {len(workflow.get('objects', []))} | "
            f"{len(workflow.get('states', []))} | {len(workflow.get('strategies', []))} | "
            f"{len(workflow.get('exceptions', []))} | {len(workflow.get('vendor_models', []))} | "
            f"{len(extra)} | {len(workflow.get('evidence_refs', []))} |"
        )
    return "\n".join(lines) + "\n"


def render_claim_value(value) -> str:
    if isinstance(value, dict):
        parts = []
        for key, item in value.items():
            if isinstance(item, list):
                rendered = ", ".join(str(v) for v in item)
            else:
                rendered = str(item)
            parts.append(f"{key}: {rendered}")
        return "; ".join(parts)
    return str(value)


def detail_markdown(workflow: dict, supplemental: list[dict]) -> str:
    lines = [
        f"# {workflow['title']}",
        "",
        "> Generated from canonical workflow data and supplemental evidence-backed claims. Do not edit by hand.",
        "",
        workflow["definition"],
        "",
        "## Canonical spine",
        "",
        "| # | Stage | Purpose |",
        "|---:|---|---|",
    ]
    for index, stage in enumerate(workflow["stages"], start=1):
        lines.append(f"| {index} | {stage['label']} | {stage['purpose']} |")

    lines += ["", "## Canonical objects", "", "| Object | Role |", "|---|---|"]
    for obj in workflow["objects"]:
        lines.append(f"| {obj['term']} | {obj['role']} |")

    lines += ["", "## Task states", "", "| State | Meaning | Terminal |", "|---|---|---|"]
    for state in workflow["states"]:
        lines.append(f"| {state['label']} | {state['meaning']} | {'yes' if state.get('terminal') else ''} |")

    lines += ["", "## Strategies", "", "| Category | Strategy | Status | Evidence |", "|---|---|---|---:|"]
    for strategy in workflow.get("strategies", []):
        lines.append(f"| {strategy['category']} | {strategy['label']} | {strategy.get('status', '')} | {len(strategy.get('evidence_refs', []))} |")

    lines += ["", "## Deep vendor models", "", "| Vendor | Workflow | Strategies | Channels | Evidence |", "|---|---|---|---|---:|"]
    for model in workflow.get("vendor_models", []):
        lines.append(
            f"| {model['company_id']} | {' -> '.join(model['workflow'])} | "
            f"{', '.join(model.get('strategies', []))} | {', '.join(model.get('execution_channels', []))} | "
            f"{len(model.get('evidence_refs', []))} |"
        )

    if workflow["capability_id"] == "picking" and supplemental:
        lines += ["", "## Supplemental vendor observations", "", "| Vendor | Observation | Confidence | Evidence |", "|---|---|---|---:|"]
        for claim in sorted(supplemental, key=lambda c: c["subject"]):
            lines.append(
                f"| {claim['subject']} | {render_claim_value(claim['value'])} | "
                f"{claim['confidence']} | {len(claim.get('evidence_refs', []))} |"
            )

    lines += ["", "## Exceptions", "", "| Exception | Status | Trigger | Outcomes |", "|---|---|---|---|"]
    for item in workflow.get("exceptions", []):
        lines.append(f"| {item['label']} | {item.get('status', '')} | {item['trigger']} | {', '.join(item['outcomes'])} |")

    spotwo = workflow["spotwo"]
    lines += [
        "",
        "## Spotwo candidate model",
        "",
        f"Status: **{spotwo['status']}**",
        "",
        "Canonical spine: `" + " -> ".join(spotwo["canonical_spine"]) + "`",
        "",
        "Task states: `" + " -> ".join(spotwo["task_states"]) + "`",
        "",
        "### Commands",
        "",
    ]
    lines.extend(f"- `{command}`" for command in spotwo.get("commands", []))
    lines += ["", "### Events", ""]
    lines.extend(f"- `{event}`" for event in spotwo.get("events", []))

    if workflow.get("research_gaps"):
        lines += ["", "## Research gaps", ""]
        lines.extend(f"- {gap}" for gap in workflow["research_gaps"])

    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()

    workflows = [load(path) for path in sorted(ROOT.glob("workflows/*.yml"))]
    supplemental = picking_claims()
    outputs = {ROOT / "matrices/workflows.md": summary_markdown(workflows, supplemental)}
    for workflow in workflows:
        outputs[ROOT / f"matrices/workflow-{workflow['id']}.md"] = detail_markdown(workflow, supplemental)

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
