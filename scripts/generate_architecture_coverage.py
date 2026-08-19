#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
from collections import Counter
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
COVERAGE = ROOT / "architecture" / "coverage-gaps.yml"
BOUNDARIES = ROOT / "architecture" / "boundaries.yml"
OUTPUT = ROOT / "matrices" / "spotwo-architecture-reference-coverage.md"
PRIORITY_ORDER = {"P0": 0, "P1": 1, "P2": 2}
OVERALL_ORDER = {"missing": 0, "partial": 1, "covered": 2}
LENS_NAMES = ("standards", "open_source", "commercial", "executable")


def normalize(value: Any) -> Any:
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: normalize(item) for key, item in value.items()}
    if isinstance(value, list):
        return [normalize(item) for item in value]
    return value


def load_yaml(path: Path) -> dict[str, Any]:
    return normalize(yaml.safe_load(path.read_text(encoding="utf-8")))


def cell(value: Any) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ")


def render() -> str:
    data = load_yaml(COVERAGE)
    boundaries = {item["id"]: item for item in load_yaml(BOUNDARIES)["boundaries"]}
    items = sorted(
        data["coverage"],
        key=lambda item: (
            PRIORITY_ORDER[item["priority"]],
            OVERALL_ORDER[item["overall"]],
            item["boundary_ref"],
        ),
    )
    overall_counts = Counter(item["overall"] for item in items)
    priority_counts = Counter(item["priority"] for item in items)
    gap_count = sum(len(lens["gaps"]) for item in items for lens in item["lenses"].values())
    target_count = sum(len(lens["research_targets"]) for item in items for lens in item["lenses"].values())

    lines = [
        "# Spotwo Architecture Reference Coverage",
        "",
        "> Generated from `architecture/coverage-gaps.yml`. Do not edit manually.",
        "",
        "This view turns the Architecture Boundary Map into a bounded research backlog. Evidence and future research targets are deliberately separate.",
        "",
        "## Summary",
        "",
        f"- Boundaries: **{len(items)}**",
        f"- Coverage: **{overall_counts['covered']} covered**, **{overall_counts['partial']} partial**, **{overall_counts['missing']} missing**",
        f"- Priority: **{priority_counts['P0']} P0**, **{priority_counts['P1']} P1**, **{priority_counts['P2']} P2**",
        f"- Explicit gaps: **{gap_count}**",
        f"- Research targets: **{target_count}**",
        "",
        "## Coverage matrix",
        "",
        "| Priority | Boundary | Contract | Overall | Standards | Open source | Commercial | Executable | Gaps | Targets |",
        "|---|---|---|---|---|---|---|---|---:|---:|",
    ]

    for item in items:
        boundary = boundaries[item["boundary_ref"]]
        lenses = item["lenses"]
        gaps = sum(len(lens["gaps"]) for lens in lenses.values())
        targets = sum(len(lens["research_targets"]) for lens in lenses.values())
        lines.append(
            "| "
            + " | ".join(
                [
                    f"`{item['priority']}`",
                    f"**{cell(boundary['name'])}** (`{item['boundary_ref']}`)",
                    f"`{boundary['contract']['status']}`",
                    f"`{item['overall']}`",
                    f"`{lenses['standards']['status']}`",
                    f"`{lenses['open_source']['status']}`",
                    f"`{lenses['commercial']['status']}`",
                    f"`{lenses['executable']['status']}`",
                    str(gaps),
                    str(targets),
                ]
            )
            + " |"
        )

    lines.extend(["", "## P0 research queue", ""])
    for item in [item for item in items if item["priority"] == "P0"]:
        boundary = boundaries[item["boundary_ref"]]
        lines.extend([
            f"### {boundary['name']} (`{item['boundary_ref']}`)",
            "",
            f"**Coverage:** `{item['overall']}`  ",
            f"**Contract:** `{boundary['contract']['status']}` - {boundary['contract']['mechanism']}",
            "",
            item["rationale"],
            "",
        ])
        for lens_name in LENS_NAMES:
            lens = item["lenses"][lens_name]
            if lens["requirement"] == "not-required":
                continue
            lines.append(
                f"**{lens_name.replace('_', ' ').title()}**: `{lens['status']}` ({lens['requirement']})"
            )
            if lens["gaps"]:
                for gap in lens["gaps"]:
                    lines.append(f"- Gap: {gap}")
            if lens["research_targets"]:
                for target in lens["research_targets"]:
                    lines.append(f"- Target: {target}")
            lines.append("")

    lines.extend([
        "## Interpretation",
        "",
        "`covered` means all required reference lenses are covered for the current architecture question. `partial` means relevant evidence exists but at least one required lens is still incomplete. `missing` means required research has no captured evidence yet. A research target is not evidence and must never be treated as a claim about a product, standard, or implementation.",
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
        print("Architecture reference coverage view is current")
        return 0
    OUTPUT.write_text(rendered, encoding="utf-8")
    print(f"Wrote {OUTPUT.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
