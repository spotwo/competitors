#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parents[1]


def load(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def normalized(value: str) -> str:
    return " ".join(value.strip().lower().replace("_", "-").split())


def dump_json(value):
    print(json.dumps(value, indent=2, ensure_ascii=False, default=str))


def ledger(args):
    needle = normalized(args.query) if args.query else None
    rows = []
    for path in sorted(ROOT.glob("ledger/*.yml")):
        registry = load(path)
        for tx in registry.get("transaction_types", []):
            candidates = [tx["id"], tx["label"], tx["category"], tx["description"]]
            if needle and not any(needle in normalized(value) for value in candidates):
                continue
            rows.append(tx)
    if args.json:
        return dump_json(rows)
    print("| Transaction | Category | Physical | Conserves | Posting |")
    print("|---|---|---|---|---|")
    for tx in rows:
        print(f"| {tx['label']} | {tx['category']} | {'yes' if tx['affects_physical'] else 'no'} | {'yes' if tx['conserves_physical'] else 'no'} | {'; '.join(tx['posting'])} |")
    print(f"\n{len(rows)} result(s)")


def invariants(args):
    rows = []
    for path in sorted(ROOT.glob("ledger/*.yml")):
        rows.extend(load(path).get("invariants", []))
    if args.json:
        return dump_json(rows)
    print("| Invariant | Status | Statement |")
    print("|---|---|---|")
    for item in rows:
        print(f"| {item['id']} | {item['status']} | {item['statement']} |")


def events(args):
    needle = normalized(args.query) if args.query else None
    rows = []
    for path in sorted(ROOT.glob("events/*.yml")):
        registry = load(path)
        for event in registry.get("events", []):
            candidates = [event["id"], event["type"], event["category"], event["meaning"]]
            if needle and not any(needle in normalized(value) for value in candidates):
                continue
            rows.append(event)
    if args.json:
        return dump_json(rows)
    print("| Event type | Category | External visibility | Meaning |")
    print("|---|---|---|---|")
    for event in rows:
        print(f"| {event['type']} | {event['category']} | {'yes' if event.get('external_visibility') else 'no'} | {event['meaning']} |")
    print(f"\n{len(rows)} result(s)")


def envelope(args):
    registries = [load(path) for path in sorted(ROOT.glob("events/*.yml"))]
    fields = [field for registry in registries for field in registry.get("envelope", {}).get("fields", [])]
    if args.json:
        return dump_json(fields)
    print("| Field | Required | Meaning |")
    print("|---|---|---|")
    for field in fields:
        print(f"| {field['name']} | {'yes' if field['required'] else 'no'} | {field['description']} |")


def main():
    parser = argparse.ArgumentParser(description="Query the Spotwo inventory kernel model")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("ledger")
    p.add_argument("query", nargs="?")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=ledger)

    p = sub.add_parser("invariants")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=invariants)

    p = sub.add_parser("events")
    p.add_argument("query", nargs="?")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=events)

    p = sub.add_parser("envelope")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=envelope)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
