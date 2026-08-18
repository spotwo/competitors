#!/usr/bin/env python3
from __future__ import annotations

import re
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
EVENT_TYPE_RE = re.compile(r"^[a-z0-9]+(?:\.[a-z0-9-]+)+$")
REQUIRED_EVENT_FIELDS = {"event_id", "type", "occurred_at", "recorded_at", "schema_version", "data"}
REQUIRED_LEDGER_INVARIANTS = {
    "immutable-posted-history",
    "commitment-does-not-create-stock",
    "available-is-derived",
    "no-double-post",
}
REQUIRED_INVENTORY_KEY_ATTRIBUTES = {"warehouse", "anchor", "item", "owner", "condition"}
FORBIDDEN_INVENTORY_KEY_ATTRIBUTES = {
    "location",
    "handling_unit",
    "serial",
    "physical_quantity",
    "reserved_quantity",
    "allocated_quantity",
    "available_quantity",
    "uom",
}
REQUIRED_POSITION_ENTITIES = {"inventory-key", "inventory-anchor", "serial-membership"}


def load(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def validate_inventory_key(errors: list[str]) -> None:
    path = ROOT / "ontology/inventory-transaction.yml"
    if not path.exists():
        errors.append("ontology/inventory-transaction.yml: missing inventory transaction ontology")
        return

    ontology = load(path)
    entities = {item["id"]: item for item in ontology.get("entities", []) or []}

    for required in sorted(REQUIRED_POSITION_ENTITIES - set(entities)):
        errors.append(f"{path.relative_to(ROOT)}: missing core position entity {required!r}")

    key = entities.get("inventory-key")
    if not key:
        return

    attributes = set(key.get("attributes", []) or [])
    for required in sorted(REQUIRED_INVENTORY_KEY_ATTRIBUTES - attributes):
        errors.append(f"{path.relative_to(ROOT)}: Inventory Key missing required semantic dimension {required!r}")
    for forbidden in sorted(FORBIDDEN_INVENTORY_KEY_ATTRIBUTES & attributes):
        errors.append(f"{path.relative_to(ROOT)}: Inventory Key must not directly contain {forbidden!r}; use canonical anchor/membership/state semantics")

    anchor = entities.get("inventory-anchor")
    if anchor:
        examples = set(anchor.get("examples", []) or [])
        if not {"location", "handling-unit"}.issubset(examples):
            errors.append(f"{path.relative_to(ROOT)}: Inventory Anchor must explicitly support location and handling-unit alternatives")

    serial_membership = entities.get("serial-membership")
    if serial_membership and "inventory-key" in set(serial_membership.get("parents", []) or []):
        errors.append(f"{path.relative_to(ROOT)}: Serial Membership must not be modeled as an Inventory Key subtype")


def main() -> int:
    errors: list[str] = []

    validate_inventory_key(errors)

    for path in sorted(ROOT.glob("ledger/*.yml")):
        ledger = load(path)
        transaction_types = ledger.get("transaction_types", []) or []
        by_id = {item["id"]: item for item in transaction_types}

        for tx in transaction_types:
            tx_id = tx["id"]
            category = tx["category"]
            affects_physical = tx["affects_physical"]
            conserves_physical = tx["conserves_physical"]

            if category == "commitment" and affects_physical:
                errors.append(f"{path.relative_to(ROOT)}: commitment transaction {tx_id!r} cannot affect physical quantity")
            if category == "commitment" and not conserves_physical:
                errors.append(f"{path.relative_to(ROOT)}: commitment transaction {tx_id!r} must conserve physical quantity")
            if tx_id in {"movement", "posting-change"} and (not affects_physical or not conserves_physical):
                errors.append(f"{path.relative_to(ROOT)}: {tx_id!r} must affect and conserve physical quantity")
            if tx_id in {"receipt", "issue"} and conserves_physical:
                errors.append(f"{path.relative_to(ROOT)}: boundary transaction {tx_id!r} must not claim internal physical conservation")
            if tx_id == "reversal" and "inverse" not in " ".join(tx.get("posting", [])).lower():
                errors.append(f"{path.relative_to(ROOT)}: reversal posting must explicitly describe inverse prior legs")

        for required in ("receipt", "issue", "movement", "reservation", "reservation-release", "allocation", "allocation-release", "reversal"):
            if required not in by_id:
                errors.append(f"{path.relative_to(ROOT)}: missing core transaction type {required!r}")

        invariant_ids = {item["id"] for item in ledger.get("invariants", []) or []}
        for required in sorted(REQUIRED_LEDGER_INVARIANTS - invariant_ids):
            errors.append(f"{path.relative_to(ROOT)}: missing core ledger invariant {required!r}")

        controls = {str(value).lower() for value in ledger.get("concurrency", {}).get("controls", []) or []}
        if not any("idempotency" in value for value in controls):
            errors.append(f"{path.relative_to(ROOT)}: concurrency controls must include idempotency protection")

    global_event_types: set[str] = set()
    for path in sorted(ROOT.glob("events/*.yml")):
        registry = load(path)
        fields = {item["name"] for item in registry.get("envelope", {}).get("fields", []) if item.get("required")}
        for required in sorted(REQUIRED_EVENT_FIELDS - fields):
            errors.append(f"{path.relative_to(ROOT)}: required event-envelope field {required!r} is missing or optional")

        for event in registry.get("events", []) or []:
            event_type = event["type"]
            if not EVENT_TYPE_RE.fullmatch(event_type):
                errors.append(f"{path.relative_to(ROOT)}: invalid semantic event type {event_type!r}")
            if event_type in global_event_types:
                errors.append(f"{path.relative_to(ROOT)}: duplicate semantic event type {event_type!r}")
            global_event_types.add(event_type)
            if event.get("external_visibility") and not event.get("evidence_refs"):
                errors.append(f"{path.relative_to(ROOT)}: externally visible event {event_type!r} requires evidence refs")

        publication = registry.get("publication", {})
        if "outbox" not in publication.get("atomicity", "").lower():
            errors.append(f"{path.relative_to(ROOT)}: publication atomicity must explicitly use an outbox")
        if "at-least-once" not in publication.get("delivery", "").lower():
            errors.append(f"{path.relative_to(ROOT)}: delivery policy must explicitly state at-least-once")
        if "event_id" not in publication.get("deduplication", ""):
            errors.append(f"{path.relative_to(ROOT)}: deduplication policy must explicitly reference event_id")

    if errors:
        print(f"Kernel semantic validation failed with {len(errors)} error(s):")
        for error in errors:
            print(f"- {error}")
        return 1

    print("Kernel semantic validation OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
