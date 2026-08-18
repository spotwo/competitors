#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from pathlib import Path

import yaml
from jsonschema import Draft202012Validator, FormatChecker

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_BY_GLOB = {"companies/*/company.yml": "schema/company.schema.json", "evidence/*.yml": "schema/evidence.schema.json", "terminology/*.yml": "schema/term.schema.json", "authorities/*.yml": "schema/authority.schema.json"}
TAXONOMY_FIELDS = {"markets": "taxonomy/regions.yml", "segments": "taxonomy/segments.yml", "solution_layers": "taxonomy/solution-layers.yml", "deployment_models": "taxonomy/deployment-models.yml", "industries": "taxonomy/industries.yml", "operating_environments": "taxonomy/operating-environments.yml", "capabilities": "taxonomy/processes.yml"}

def load_yaml(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def taxonomy_values(rel_path: str) -> set[str]:
    data = load_yaml(ROOT / rel_path)
    return {item["id"] for item in data["values"]}

def validate_schema(path: Path, schema_path: Path) -> list[str]:
    data = load_yaml(path)
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    return [f"{path.relative_to(ROOT)}: {'.'.join(map(str, err.absolute_path)) or '<root>'}: {err.message}" for err in sorted(validator.iter_errors(data), key=lambda e: list(e.absolute_path))]

def main() -> int:
    errors: list[str] = []
    for pattern, schema_rel in SCHEMA_BY_GLOB.items():
        for path in sorted(ROOT.glob(pattern)):
            errors.extend(validate_schema(path, ROOT / schema_rel))

    evidence_ids: set[str] = set()
    for path in ROOT.glob("evidence/*.yml"):
        data = load_yaml(path)
        if isinstance(data, dict) and "id" in data:
            if data["id"] in evidence_ids:
                errors.append(f"{path.relative_to(ROOT)}: duplicate evidence id {data['id']}")
            evidence_ids.add(data["id"])

    allowed = {field: taxonomy_values(rel) for field, rel in TAXONOMY_FIELDS.items()}
    country_codes = taxonomy_values("taxonomy/countries.yml")
    company_ids: set[str] = set()
    company_records: list[tuple[Path, dict]] = []
    product_ids: set[str] = set()

    for path in sorted(ROOT.glob("companies/*/company.yml")):
        data = load_yaml(path)
        if not isinstance(data, dict):
            continue
        company_records.append((path, data))
        company_id = data.get("id")
        if company_id:
            if company_id in company_ids:
                errors.append(f"{path.relative_to(ROOT)}: duplicate company id {company_id}")
            company_ids.add(company_id)
        for field, values in allowed.items():
            for value in data.get(field, []) or []:
                if value not in values:
                    errors.append(f"{path.relative_to(ROOT)}: unknown {field} value {value!r}")
        value = data.get("hq_country")
        if value and value not in country_codes:
            errors.append(f"{path.relative_to(ROOT)}: unknown ISO country code {value!r}")
        for field in ("served_countries", "local_presence_countries"):
            for value in data.get(field, []) or []:
                if value not in country_codes:
                    errors.append(f"{path.relative_to(ROOT)}: unknown {field} country code {value!r}")
        for product in data.get("products", []) or []:
            pid = product.get("id")
            if pid:
                global_id = f"{company_id}/{pid}"
                if global_id in product_ids:
                    errors.append(f"{path.relative_to(ROOT)}: duplicate product id {global_id}")
                product_ids.add(global_id)
            for category in product.get("categories", []) or []:
                if category not in allowed["solution_layers"]:
                    errors.append(f"{path.relative_to(ROOT)}: product {pid!r} has unknown category {category!r}")
            for field in ("segments", "deployment_models"):
                for value in product.get(field, []) or []:
                    if value not in allowed[field]:
                        errors.append(f"{path.relative_to(ROOT)}: product {pid!r} has unknown {field} value {value!r}")
        for ref in data.get("evidence_refs", []) or []:
            if ref not in evidence_ids:
                errors.append(f"{path.relative_to(ROOT)}: missing evidence ref {ref!r}")

    for path, data in company_records:
        parent = data.get("parent_company_id")
        if parent and parent not in company_ids:
            errors.append(f"{path.relative_to(ROOT)}: missing parent company {parent!r}")
        if parent and parent == data.get("id"):
            errors.append(f"{path.relative_to(ROOT)}: company cannot be its own parent")

    for pattern in ("terminology/*.yml", "authorities/*.yml"):
        for path in sorted(ROOT.glob(pattern)):
            data = load_yaml(path)
            if isinstance(data, dict):
                for ref in data.get("evidence_refs", []) or []:
                    if ref not in evidence_ids:
                        errors.append(f"{path.relative_to(ROOT)}: missing evidence ref {ref!r}")

    if errors:
        print(f"Validation failed with {len(errors)} error(s):")
        for error in errors:
            print(f"- {error}")
        return 1
    print(f"Validation OK: {len(company_ids)} companies, {len(product_ids)} products, {len(evidence_ids)} evidence records")
    return 0

if __name__ == "__main__":
    sys.exit(main())
