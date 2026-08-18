#!/usr/bin/env python3
from __future__ import annotations

import argparse
from collections import defaultdict
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def load_yaml(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def companies():
    return [load_yaml(p) for p in sorted(ROOT.glob("companies/*/company.yml"))]


def terms():
    return [load_yaml(p) for p in sorted(ROOT.glob("terminology/*.yml"))]


def vendor_matrix(records):
    lines = ["# Vendor Matrix", "", "> Generated from `companies/*/company.yml`. Do not edit by hand.", "", "| Vendor | Segments | Solution layers | Markets | Deployment | Products | Verified |", "|---|---|---|---|---|---|---|"]
    for c in sorted(records, key=lambda x: x["name"].lower()):
        lines.append(f"| {c['name']} | {', '.join(c['segments'])} | {', '.join(c['solution_layers'])} | {', '.join(c['markets'])} | {', '.join(c['deployment_models'])} | {', '.join(p['name'] for p in c['products'])} | {c['last_verified']} |")
    return "\n".join(lines) + "\n"


def layer_matrix(records):
    layer_map = defaultdict(list)
    for c in records:
        for layer in c["solution_layers"]:
            layer_map[layer].append(c["name"])
    lines = ["# Solution Layer Matrix", "", "> Generated from company records. Do not edit by hand.", "", "| Layer | Vendors | Count |", "|---|---|---:|"]
    for layer in sorted(layer_map):
        vendors = sorted(layer_map[layer])
        lines.append(f"| {layer} | {', '.join(vendors)} | {len(vendors)} |")
    return "\n".join(lines) + "\n"


def terminology_matrix(records):
    lines = ["# Terminology Matrix", "", "> Generated from `terminology/*.yml`. Do not edit by hand.", "", "| Term | Status | Spotwo term | Aliases | Evidence |", "|---|---|---|---|---:|"]
    for t in sorted(records, key=lambda x: x["preferred_term"].lower()):
        lines.append(f"| {t['preferred_term']} | {t['status']} | {t.get('spotwo_term') or ''} | {', '.join(t.get('aliases', []))} | {len(t.get('evidence_refs', []))} |")
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    outputs = {ROOT / "matrices/vendors.md": vendor_matrix(companies()), ROOT / "matrices/solution-layers.md": layer_matrix(companies()), ROOT / "matrices/terminology.md": terminology_matrix(terms())}
    stale = []
    for path, content in outputs.items():
        if args.check:
            if not path.exists() or path.read_text(encoding="utf-8") != content:
                stale.append(str(path.relative_to(ROOT)))
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")
    if stale:
        raise SystemExit("Generated matrices are stale: " + ", ".join(stale))


if __name__ == "__main__":
    main()
