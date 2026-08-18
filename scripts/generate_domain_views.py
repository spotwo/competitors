#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parents[1]


def load(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def ontology_markdown() -> str:
    rows = []
    for path in sorted(ROOT.glob("ontology/*.yml")):
        data = load(path)
        rows.append((data["title"], data["kind"], data["status"], len(data["entities"]), len(data.get("evidence_refs", []))))
    lines = ["# Ontology Matrix", "", "> Generated from `ontology/*.yml`. Do not edit by hand.", "", "| Ontology | Kind | Status | Entities | Evidence |", "|---|---|---|---:|---:|"]
    lines.extend(f"| {title} | {kind} | {status} | {entities} | {evidence} |" for title, kind, status, entities, evidence in rows)
    return "\n".join(lines) + "\n"


def kpis_markdown() -> str:
    rows = []
    for path in sorted(ROOT.glob("kpis/*.yml")):
        data = load(path)
        for metric in data["metrics"]:
            rows.append((metric["name"], metric["category"], metric["direction"], metric.get("unit", ""), metric["status"]))
    lines = ["# KPI Matrix", "", "> Generated from `kpis/*.yml`. Do not edit by hand.", "", "| KPI | Category | Direction | Unit | Status |", "|---|---|---|---|---|"]
    lines.extend(f"| {name} | {category} | {direction} | {unit} | {status} |" for name, category, direction, unit, status in rows)
    return "\n".join(lines) + "\n"


def integrations_markdown() -> str:
    rows = []
    for path in sorted(ROOT.glob("integrations/*.yml")):
        data = load(path)
        for pattern in data["patterns"]:
            rows.append((pattern["name"], pattern["category"], pattern["directionality"], ", ".join(pattern.get("typical_protocols", [])), pattern["status"]))
    lines = ["# Integration Pattern Matrix", "", "> Generated from `integrations/*.yml`. Do not edit by hand.", "", "| Pattern | Category | Direction | Typical protocols | Status |", "|---|---|---|---|---|"]
    lines.extend(f"| {name} | {category} | {direction} | {protocols} | {status} |" for name, category, direction, protocols, status in rows)
    return "\n".join(lines) + "\n"


def capabilities_markdown() -> str:
    rows = []
    for path in sorted(ROOT.glob("capabilities/*.yml")):
        data = load(path)
        vendors = sorted({item["company_id"] for item in data.get("vendor_observations", []) or []})
        rows.append((data["group"], data["label"], data["status"], data["spotwo"]["canonical_term"], ", ".join(vendors), len(data.get("evidence_refs", []))))
    rows.sort(key=lambda row: (row[0], row[1]))
    lines = ["# Capability Matrix", "", "> Generated from `capabilities/*.yml`. Do not edit by hand.", "", "| Group | Capability | Status | Spotwo term | Observed vendors | Evidence |", "|---|---|---|---|---|---:|"]
    lines.extend(f"| {group} | {label} | {status} | {term} | {vendors} | {evidence} |" for group, label, status, term, vendors, evidence in rows)
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    outputs = {
        ROOT / "matrices/ontology.md": ontology_markdown(),
        ROOT / "matrices/kpis.md": kpis_markdown(),
        ROOT / "matrices/integrations.md": integrations_markdown(),
        ROOT / "matrices/capabilities.md": capabilities_markdown(),
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
        raise SystemExit("Generated domain views are stale: " + ", ".join(stale))


if __name__ == "__main__":
    main()
