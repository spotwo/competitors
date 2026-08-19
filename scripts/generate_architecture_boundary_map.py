#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
from collections import Counter
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
REGISTRY = ROOT / "architecture" / "boundaries.yml"
OUTPUT = ROOT / "matrices" / "spotwo-architecture-boundaries.md"
STATUS_ORDER = {"canonical": 0, "candidate": 1, "unresolved": 2}


def normalize(value: Any) -> Any:
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: normalize(item) for key, item in value.items()}
    if isinstance(value, list):
        return [normalize(item) for item in value]
    return value


def load() -> dict[str, Any]:
    return normalize(yaml.safe_load(REGISTRY.read_text(encoding="utf-8")))


def cell(value: Any) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ")


def render() -> str:
    data = load()
    boundaries = sorted(
        data["boundaries"],
        key=lambda item: (STATUS_ORDER[item["contract"]["status"]], item["id"]),
    )
    counts = Counter(item["contract"]["status"] for item in boundaries)
    gap_count = sum(len(item["research_gaps"]) for item in boundaries)

    lines = [
        "# Spotwo Architecture Boundary Map",
        "",
        "> Generated from `architecture/boundaries.yml`. Do not edit manually.",
        "",
        "This map defines ownership and contracts at architecture boundaries. A technology name is never allowed to define the domain model implicitly.",
        "",
        "## Summary",
        "",
        f"- Boundaries: **{len(boundaries)}**",
        f"- Canonical contracts: **{counts['canonical']}**",
        f"- Candidate contracts: **{counts['candidate']}**",
        f"- Unresolved contracts: **{counts['unresolved']}**",
        f"- Explicit research gaps: **{gap_count}**",
        "",
        "## Boundary matrix",
        "",
        "| Boundary | Classification | From | To | Contract | Status | Decision | Gaps |",
        "|---|---|---|---|---|---|---|---:|",
    ]

    for boundary in boundaries:
        contract = boundary["contract"]
        decision = contract["technology_decision_ref"] or "-"
        lines.append(
            "| "
            + " | ".join(
                [
                    f"**{cell(boundary['name'])}** (`{boundary['id']}`)",
                    f"`{boundary['classification']}`",
                    cell(boundary["from"]),
                    cell(boundary["to"]),
                    cell(contract["mechanism"]),
                    f"`{contract['status']}`",
                    f"`{decision}`" if decision != "-" else "-",
                    str(len(boundary["research_gaps"])),
                ]
            )
            + " |"
        )

    lines.extend([
        "",
        "## System flow",
        "",
        "```text",
        "External ERP / OMS / TMS / customers",
        "        | REST + OpenAPI 3.1",
        "        v",
        "Spotwo application + canonical domain",
        "        |-- NATS JetStream domain events --> internal consumers / Edge consumers",
        "        |-- [UNRESOLVED] external async --> customers / partners",
        "        |-- GS1 EPCIS candidate ---------> visibility consumers",
        "        |-- OR-Tools candidate ----------> optimization",
        "        |-- real/sim executor port ------> execution",
        "        v",
        "Spotwo WES / WCS / MFC / Edge",
        "        |-- VDA 5050 candidate ----------> robot fleet",
        "        |-- PLC4X / OPC UA candidate ----> PLC / machine",
        "        |<- MQTT / Sparkplug candidate --- devices / gateways",
        "        |<-> [UNRESOLVED] Edge control --- control plane",
        "        v",
        "Physical world",
        "```",
        "",
        "## Rule of interpretation",
        "",
        "`canonical` means the contract mechanism is adopted for this exact boundary. `candidate` means the boundary and ownership are defined but the mechanism is still under trial. `unresolved` means the map deliberately refuses to guess a mechanism. Detailed state ownership, reliability, security, versioning, simulation rules, evidence, standards, and research gaps live in `architecture/boundaries.yml`.",
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
        print("Architecture boundary map view is current")
        return 0

    OUTPUT.write_text(rendered, encoding="utf-8")
    print(f"Wrote {OUTPUT.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
