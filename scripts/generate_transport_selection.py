#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
MODEL = ROOT / "kernel" / "transports.yml"
OUTPUT = ROOT / "matrices" / "transport-selection.md"


def render() -> str:
    data = yaml.safe_load(MODEL.read_text(encoding="utf-8"))
    criteria = data["profile"]["criteria"]
    weights = {criterion["id"]: criterion["weight"] for criterion in criteria}
    candidates = data["candidates"]

    totals = {
        candidate["id"]: sum(
            weights[criterion_id] * candidate["scores"][criterion_id]
            for criterion_id in weights
        ) / 100.0
        for candidate in candidates
    }
    ranking = sorted(candidates, key=lambda c: (-totals[c["id"]], c["id"]))

    lines = [
        "# Transport Selection Matrix",
        "",
        "> Generated from `kernel/transports.yml`. Do not edit manually.",
        "",
        f"Profile: `{data['profile']['id']}`",
        "",
        "## Weighted ranking",
        "",
        "| Rank | Candidate | Disposition | Weighted score |",
        "|---:|---|---|---:|",
    ]
    for index, candidate in enumerate(ranking, start=1):
        lines.append(
            f"| {index} | {candidate['name']} (`{candidate['id']}`) | `{candidate['disposition']}` | {totals[candidate['id']]:.2f} / 5 |"
        )

    lines.extend([
        "",
        "## Criteria scores",
        "",
        "| Criterion | Weight | " + " | ".join(candidate["name"] for candidate in candidates) + " |",
        "|---|---:|" + "---:|" * len(candidates),
    ])
    for criterion in criteria:
        criterion_id = criterion["id"]
        score_cells = " | ".join(str(candidate["scores"][criterion_id]) for candidate in candidates)
        lines.append(f"| `{criterion_id}` | {criterion['weight']}% | {score_cells} |")

    lines.extend([
        "",
        "## Decision",
        "",
        f"Canonical default: **`{data['decision']['canonical_default']}`**.",
        "",
        "Profile-specific exceptions:",
        "",
    ])
    for name, target in data["decision"]["exceptions"].items():
        lines.append(f"- `{name}` -> `{target}`")

    lines.extend([
        "",
        "The ranking is a Spotwo WMS project-fit decision, not a universal broker benchmark. Broker-specific semantics remain outside the frozen kernel contract.",
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
        print("Transport selection matrix is current")
        return 0

    OUTPUT.write_text(rendered, encoding="utf-8")
    print(f"Wrote {OUTPUT.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
