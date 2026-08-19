#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
MODEL = ROOT / "decisions" / "technology" / "registry.yml"
OUTPUT = ROOT / "matrices" / "spotwo-technology-decisions.md"
DISPOSITION_ORDER = {"adopt": 0, "trial": 1, "watch": 2, "reject": 3}


def cell(value: object) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ")


def evidence_summary(decision: dict) -> str:
    evidence = decision["evidence"]
    return (
        f"repo:{len(evidence['repository_refs'])} / "
        f"oss:{len(evidence['open_source_project_refs'])} / "
        f"market:{len(evidence['competitor_product_refs'])}"
    )


def render() -> str:
    data = yaml.safe_load(MODEL.read_text(encoding="utf-8"))
    decisions = sorted(
        data["decisions"],
        key=lambda item: (DISPOSITION_ORDER[item["disposition"]], item["category"], item["id"]),
    )
    counts = {name: 0 for name in DISPOSITION_ORDER}
    for decision in decisions:
        counts[decision["disposition"]] += 1

    lines = [
        "# Spotwo Technology Decision Matrix",
        "",
        "> Generated from `decisions/technology/registry.yml`. Do not edit manually.",
        "",
        "Competitor adoption and open-source popularity are evidence signals, not automatic authorization. Every decision is scoped and can be revisited when its explicit triggers become true.",
        "",
        "## Summary",
        "",
        "| Disposition | Count | Meaning |",
        "|---|---:|---|",
    ]
    for disposition in ("adopt", "trial", "watch", "reject"):
        lines.append(
            f"| `{disposition}` | {counts[disposition]} | {cell(data['policy']['dispositions'][disposition])} |"
        )

    lines.extend([
        "",
        "## Decisions",
        "",
        "| Technology | Category | Disposition | Confidence | Evidence | Next action |",
        "|---|---|---|---|---|---|",
    ])
    for decision in decisions:
        lines.append(
            "| "
            + " | ".join(
                [
                    f"**{cell(decision['name'])}** (`{decision['id']}`)",
                    f"`{decision['category']}`",
                    f"`{decision['disposition']}`",
                    f"`{decision['confidence']}`",
                    f"`{evidence_summary(decision)}`",
                    f"`{decision['next_action']['type']}`",
                ]
            )
            + " |"
        )

    lines.extend([
        "",
        "Full scope, rationale, evidence references, constraints, alternatives, next action text, and revisit triggers live in `decisions/technology/registry.yml`.",
        "",
    ])
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
        print("Technology decision matrix is current")
        return 0

    OUTPUT.write_text(rendered, encoding="utf-8")
    print(f"Wrote {OUTPUT.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
