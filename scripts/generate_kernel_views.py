#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parents[1]


def load(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def ledger_markdown() -> str:
    lines = [
        "# Inventory Ledger Matrix",
        "",
        "> Generated from `ledger/*.yml`. Do not edit by hand.",
        "",
        "| Transaction type | Category | Physical | Conserves physical | Posting | Evidence |",
        "|---|---|---|---|---|---:|",
    ]
    for path in sorted(ROOT.glob("ledger/*.yml")):
        registry = load(path)
        for tx in registry.get("transaction_types", []):
            posting = "; ".join(tx.get("posting", []))
            lines.append(
                f"| {tx['label']} | {tx['category']} | {'yes' if tx['affects_physical'] else 'no'} | "
                f"{'yes' if tx['conserves_physical'] else 'no'} | {posting} | {len(tx.get('evidence_refs', []))} |"
            )
        lines += ["", "## Candidate invariants", "", "| Invariant | Status | Statement |", "|---|---|---|"]
        for invariant in registry.get("invariants", []):
            lines.append(f"| {invariant['id']} | {invariant['status']} | {invariant['statement']} |")
    return "\n".join(lines) + "\n"


def events_markdown() -> str:
    lines = [
        "# Inventory Domain Event Matrix",
        "",
        "> Generated from `events/*.yml`. Do not edit by hand.",
        "",
        "| Event type | Category | External visibility | Ledger relation | Evidence |",
        "|---|---|---|---|---:|",
    ]
    for path in sorted(ROOT.glob("events/*.yml")):
        registry = load(path)
        for event in registry.get("events", []):
            lines.append(
                f"| `{event['type']}` | {event['category']} | {'yes' if event.get('external_visibility') else 'no'} | "
                f"{event['ledger_relation']} | {len(event.get('evidence_refs', []))} |"
            )
        lines += ["", "## Event envelope", "", "| Field | Required | Meaning |", "|---|---|---|"]
        for field in registry.get("envelope", {}).get("fields", []):
            lines.append(f"| `{field['name']}` | {'yes' if field['required'] else 'no'} | {field['description']} |")
        lines += ["", "## External mappings", "", "| Standard | Scope | Evidence |", "|---|---|---:|"]
        for mapping in registry.get("external_mappings", []):
            lines.append(f"| {mapping['standard']} | {mapping['scope']} | {len(mapping.get('evidence_refs', []))} |")
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    outputs = {
        ROOT / "matrices/ledger.md": ledger_markdown(),
        ROOT / "matrices/events.md": events_markdown(),
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
        raise SystemExit("Generated kernel views are stale: " + ", ".join(stale))


if __name__ == "__main__":
    main()
