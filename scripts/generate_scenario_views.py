#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def load(path: Path):
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def scenarios_markdown() -> str:
    rows = []
    for path in sorted(ROOT.glob("scenarios/*.yml")):
        data = load(path)
        for scenario in data.get("scenarios", []) or []:
            rows.append(
                (
                    scenario["id"],
                    scenario["title"],
                    len(scenario.get("steps", []) or []),
                    len(scenario.get("checkpoints", []) or []),
                    len(scenario.get("invariants", []) or []),
                    scenario["executable_test"],
                )
            )

    rows.sort(key=lambda row: row[0])
    lines = [
        "# Golden Warehouse Scenarios",
        "",
        "> Generated from `scenarios/*.yml`. Do not edit by hand.",
        "",
        "| Scenario | Title | Steps | Checkpoints | Invariants | Executable test |",
        "|---|---|---:|---:|---:|---|",
    ]
    lines.extend(
        f"| `{scenario_id}` | {title} | {steps} | {checkpoints} | {invariants} | `{test}` |"
        for scenario_id, title, steps, checkpoints, invariants, test in rows
    )
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()

    output = ROOT / "matrices/scenarios.md"
    content = scenarios_markdown()
    if args.check:
        if not output.exists() or output.read_text(encoding="utf-8") != content:
            raise SystemExit("Generated scenario view is stale: matrices/scenarios.md")
    else:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(content, encoding="utf-8")


if __name__ == "__main__":
    main()
