#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from pathlib import Path

import yaml
from jsonschema import Draft202012Validator, FormatChecker

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_BY_GLOB = {
    "companies/*/company.yml": "schema/company.schema.json",
    "evidence/*.yml": "schema/evidence.schema.json",
    "terminology/*.yml": "schema/term.schema.json",
    "authorities/*.yml": "schema/authority.schema.json",
    "claims/*.yml": "schema/claim.schema.json",
    "taxonomy/*.yml": "schema/taxonomy.schema.json",
    "ontology/*.yml": "schema/ontology.schema.json",
    "kpis/*.yml": "schema/kpi-registry.schema.json",
    "integrations/*.yml": "schema/integration-registry.schema.json",
}
TAXONOMY_FIELDS = {
    "markets": "taxonomy/regions.yml",
    "segments": "taxonomy/segments.yml",
    "solution_layers": "taxonomy/solution-layers.yml",
    "deployment_models": "taxonomy/deployment-models.yml",
    "industries": "taxonomy/industries.yml",
    "operating_environments": "taxonomy/operating-environments.yml",
    "capabilities": "taxonomy/processes.yml",
}


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
    return [
        f"{path.relative_to(ROOT)}: {'.'.join(map(str, err.absolute_path)) or '<root>'}: {err.message}"
        for err in sorted(validator.iter_errors(data), key=lambda e: list(e.absolute_path))
    ]


def record_ids(pattern: str, errors: list[str]) -> set[str]:
    result: set[str] = set()
    for path in sorted(ROOT.glob(pattern)):
        data = load_yaml(path)
        if not isinstance(data, dict) or not data.get("id"):
            continue
        record_id = data["id"]
        if record_id in result:
            errors.append(f"{path.relative_to(ROOT)}: duplicate id {record_id!r}")
        result.add(record_id)
    return result


def check_evidence_refs(path: Path, data: dict, evidence_ids: set[str], errors: list[str]) -> None:
    for ref in data.get("evidence_refs", []) or []:
        if ref not in evidence_ids:
            errors.append(f"{path.relative_to(ROOT)}: missing evidence ref {ref!r}")


def main() -> int:
    errors: list[str] = []

    for pattern, schema_rel in SCHEMA_BY_GLOB.items():
        for path in sorted(ROOT.glob(pattern)):
            errors.extend(validate_schema(path, ROOT / schema_rel))

    for path in sorted(ROOT.glob("taxonomy/*.yml")):
        data = load_yaml(path)
        seen: set[str] = set()
        for item in (data or {}).get("values", []):
            value_id = item.get("id")
            if value_id in seen:
                errors.append(f"{path.relative_to(ROOT)}: duplicate taxonomy id {value_id!r}")
            seen.add(value_id)

    evidence_ids = record_ids("evidence/*.yml", errors)
    term_ids = record_ids("terminology/*.yml", errors)
    authority_ids = record_ids("authorities/*.yml", errors)
    claim_ids = record_ids("claims/*.yml", errors)
    ontology_ids = record_ids("ontology/*.yml", errors)

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
                errors.append(f"{path.relative_to(ROOT)}: duplicate company id {company_id!r}")
            company_ids.add(company_id)
        for field, values in allowed.items():
            for value in data.get(field, []) or []:
                if value not in values:
                    errors.append(f"{path.relative_to(ROOT)}: unknown {field} value {value!r}")
        hq_country = data.get("hq_country")
        if hq_country and hq_country not in country_codes:
            errors.append(f"{path.relative_to(ROOT)}: unknown ISO country code {hq_country!r}")
        for field in ("served_countries", "local_presence_countries"):
            for value in data.get(field, []) or []:
                if value not in country_codes:
                    errors.append(f"{path.relative_to(ROOT)}: unknown {field} country code {value!r}")
        for product in data.get("products", []) or []:
            pid = product.get("id")
            if pid:
                global_id = f"{company_id}/{pid}"
                if global_id in product_ids:
                    errors.append(f"{path.relative_to(ROOT)}: duplicate product id {global_id!r}")
                product_ids.add(global_id)
            for category in product.get("categories", []) or []:
                if category not in allowed["solution_layers"]:
                    errors.append(f"{path.relative_to(ROOT)}: product {pid!r} has unknown category {category!r}")
            for field in ("segments", "deployment_models"):
                for value in product.get(field, []) or []:
                    if value not in allowed[field]:
                        errors.append(f"{path.relative_to(ROOT)}: product {pid!r} has unknown {field} value {value!r}")
        check_evidence_refs(path, data, evidence_ids, errors)

    for path, data in company_records:
        parent = data.get("parent_company_id")
        if parent and parent not in company_ids:
            errors.append(f"{path.relative_to(ROOT)}: missing parent company {parent!r}")
        if parent and parent == data.get("id"):
            errors.append(f"{path.relative_to(ROOT)}: company cannot be its own parent")

    for pattern in ("terminology/*.yml", "authorities/*.yml", "ontology/*.yml", "kpis/*.yml", "integrations/*.yml"):
        for path in sorted(ROOT.glob(pattern)):
            data = load_yaml(path)
            if isinstance(data, dict):
                check_evidence_refs(path, data, evidence_ids, errors)

    ontology_entity_count = 0
    for path in sorted(ROOT.glob("ontology/*.yml")):
        data = load_yaml(path)
        seen: set[str] = set()
        for entity in data.get("entities", []) or []:
            entity_id = entity["id"]
            if entity_id in seen:
                errors.append(f"{path.relative_to(ROOT)}: duplicate ontology entity {entity_id!r}")
            seen.add(entity_id)
        ontology_entity_count += len(seen)

    kpi_ids: set[str] = set()
    for path in sorted(ROOT.glob("kpis/*.yml")):
        data = load_yaml(path)
        for metric in data.get("metrics", []) or []:
            metric_id = metric["id"]
            if metric_id in kpi_ids:
                errors.append(f"{path.relative_to(ROOT)}: duplicate KPI id {metric_id!r}")
            kpi_ids.add(metric_id)
            for ref in metric.get("evidence_refs", []) or []:
                if ref not in evidence_ids:
                    errors.append(f"{path.relative_to(ROOT)}: KPI {metric_id!r} missing evidence ref {ref!r}")

    integration_ids: set[str] = set()
    for path in sorted(ROOT.glob("integrations/*.yml")):
        data = load_yaml(path)
        for pattern in data.get("patterns", []) or []:
            pattern_id = pattern["id"]
            if pattern_id in integration_ids:
                errors.append(f"{path.relative_to(ROOT)}: duplicate integration pattern {pattern_id!r}")
            integration_ids.add(pattern_id)

    resolvers = {
        "company": company_ids,
        "product": product_ids,
        "terminology": term_ids,
        "authority": authority_ids,
    }
    for path in sorted(ROOT.glob("claims/*.yml")):
        data = load_yaml(path)
        if not isinstance(data, dict):
            continue
        subject_type = data.get("subject_type")
        subject = data.get("subject")
        if subject_type in resolvers and subject not in resolvers[subject_type]:
            errors.append(f"{path.relative_to(ROOT)}: unresolved {subject_type} subject {subject!r}")
        check_evidence_refs(path, data, evidence_ids, errors)

    if errors:
        print(f"Validation failed with {len(errors)} error(s):")
        for error in errors:
            print(f"- {error}")
        return 1

    print(
        f"Validation OK: {len(company_ids)} companies, {len(product_ids)} products, "
        f"{len(evidence_ids)} evidence records, {len(claim_ids)} claims, {len(term_ids)} terms, "
        f"{len(authority_ids)} authorities, {len(ontology_ids)} ontologies/{ontology_entity_count} entities, "
        f"{len(kpi_ids)} KPIs, {len(integration_ids)} integration patterns"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
