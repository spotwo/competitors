#!/usr/bin/env python3
from __future__ import annotations

import datetime as dt
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
    "capabilities/*.yml": "schema/capability.schema.json",
    "workflows/*.yml": "schema/workflow.schema.json",
    "ledger/*.yml": "schema/ledger-registry.schema.json",
    "events/*.yml": "schema/event-registry.schema.json",
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


def normalize(value):
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: normalize(v) for k, v in value.items()}
    if isinstance(value, list):
        return [normalize(v) for v in value]
    return value


def load_yaml(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return normalize(yaml.safe_load(f))


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


def check_nested_evidence_refs(path: Path, records: list[dict], evidence_ids: set[str], label: str, errors: list[str]) -> None:
    for record in records or []:
        for ref in record.get("evidence_refs", []) or []:
            if ref not in evidence_ids:
                errors.append(f"{path.relative_to(ROOT)}: {label} {record.get('id', record.get('standard', '<unknown>'))!r} missing evidence ref {ref!r}")


def check_unique_nested_ids(path: Path, records: list[dict], label: str, errors: list[str]) -> set[str]:
    seen: set[str] = set()
    for record in records or []:
        record_id = record.get("id")
        if not record_id:
            continue
        if record_id in seen:
            errors.append(f"{path.relative_to(ROOT)}: duplicate {label} id {record_id!r}")
        seen.add(record_id)
    return seen


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
    capability_ids = record_ids("capabilities/*.yml", errors)
    workflow_ids = record_ids("workflows/*.yml", errors)
    ledger_ids = record_ids("ledger/*.yml", errors)
    event_registry_ids = record_ids("events/*.yml", errors)
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

    for pattern in (
        "terminology/*.yml", "authorities/*.yml", "ontology/*.yml", "kpis/*.yml",
        "integrations/*.yml", "capabilities/*.yml", "workflows/*.yml", "ledger/*.yml", "events/*.yml"
    ):
        for path in sorted(ROOT.glob(pattern)):
            data = load_yaml(path)
            if isinstance(data, dict):
                check_evidence_refs(path, data, evidence_ids, errors)

    expected_capabilities = allowed["capabilities"]
    for missing in sorted(expected_capabilities - capability_ids):
        errors.append(f"capabilities/: missing canonical capability record {missing!r}")
    for unknown in sorted(capability_ids - expected_capabilities):
        errors.append(f"capabilities/: capability {unknown!r} is not in taxonomy/processes.yml")
    for path in sorted(ROOT.glob("capabilities/*.yml")):
        data = load_yaml(path)
        for related in data.get("related_capabilities", []) or []:
            if related not in expected_capabilities:
                errors.append(f"{path.relative_to(ROOT)}: unknown related capability {related!r}")
        for observation in data.get("vendor_observations", []) or []:
            company_id = observation.get("company_id")
            if company_id not in company_ids:
                errors.append(f"{path.relative_to(ROOT)}: unresolved vendor company {company_id!r}")
            product_id = observation.get("product_id")
            if product_id and product_id not in product_ids:
                errors.append(f"{path.relative_to(ROOT)}: unresolved vendor product {product_id!r}")
            check_nested_evidence_refs(path, [observation], evidence_ids, "vendor observation", errors)

    workflow_stage_count = 0
    workflow_strategy_count = 0
    workflow_vendor_model_count = 0
    for path in sorted(ROOT.glob("workflows/*.yml")):
        data = load_yaml(path)
        capability_id = data.get("capability_id")
        if capability_id not in capability_ids:
            errors.append(f"{path.relative_to(ROOT)}: unresolved capability {capability_id!r}")
        stage_ids = check_unique_nested_ids(path, data.get("stages", []), "workflow stage", errors)
        object_ids = check_unique_nested_ids(path, data.get("objects", []), "workflow object", errors)
        state_ids = check_unique_nested_ids(path, data.get("states", []), "workflow state", errors)
        strategy_ids = check_unique_nested_ids(path, data.get("strategies", []), "workflow strategy", errors)
        check_unique_nested_ids(path, data.get("exceptions", []), "workflow exception", errors)
        workflow_stage_count += len(stage_ids)
        workflow_strategy_count += len(strategy_ids)
        check_nested_evidence_refs(path, data.get("strategies", []), evidence_ids, "strategy", errors)
        check_nested_evidence_refs(path, data.get("exceptions", []), evidence_ids, "exception", errors)
        for model in data.get("vendor_models", []) or []:
            workflow_vendor_model_count += 1
            company_id = model.get("company_id")
            if company_id not in company_ids:
                errors.append(f"{path.relative_to(ROOT)}: workflow vendor model unresolved company {company_id!r}")
            product_id = model.get("product_id")
            if product_id and product_id not in product_ids:
                errors.append(f"{path.relative_to(ROOT)}: workflow vendor model unresolved product {product_id!r}")
        check_nested_evidence_refs(path, data.get("vendor_models", []), evidence_ids, "workflow vendor model", errors)
        spotwo = data.get("spotwo", {})
        for stage in spotwo.get("canonical_spine", []) or []:
            if stage not in stage_ids:
                errors.append(f"{path.relative_to(ROOT)}: Spotwo spine references unknown stage {stage!r}")
        for object_id in spotwo.get("canonical_objects", []) or []:
            if object_id not in object_ids:
                errors.append(f"{path.relative_to(ROOT)}: Spotwo canonical_objects references unknown object {object_id!r}")
        for state_id in spotwo.get("task_states", []) or []:
            if state_id not in state_ids:
                errors.append(f"{path.relative_to(ROOT)}: Spotwo task_states references unknown state {state_id!r}")

    ontology_entity_count = 0
    ontology_records: list[tuple[Path, dict, set[str]]] = []
    ontology_entity_ids_global: set[str] = set()
    for path in sorted(ROOT.glob("ontology/*.yml")):
        data = load_yaml(path)
        entity_ids = check_unique_nested_ids(path, data.get("entities", []), "ontology entity", errors)
        ontology_records.append((path, data, entity_ids))
        ontology_entity_ids_global.update(entity_ids)
        ontology_entity_count += len(entity_ids)

    for path, data, _entity_ids in ontology_records:
        for entity in data.get("entities", []) or []:
            for parent in entity.get("parents", []) or []:
                if parent not in ontology_entity_ids_global:
                    errors.append(f"{path.relative_to(ROOT)}: ontology entity {entity.get('id')!r} unresolved parent {parent!r}")
        for relationship in data.get("relationships", []) or []:
            subject = relationship.get("subject")
            object_id = relationship.get("object")
            if subject not in ontology_entity_ids_global:
                errors.append(f"{path.relative_to(ROOT)}: ontology relationship unresolved subject {subject!r}")
            if object_id not in ontology_entity_ids_global:
                errors.append(f"{path.relative_to(ROOT)}: ontology relationship unresolved object {object_id!r}")

    kpi_ids: set[str] = set()
    for path in sorted(ROOT.glob("kpis/*.yml")):
        data = load_yaml(path)
        for metric in data.get("metrics", []) or []:
            metric_id = metric["id"]
            if metric_id in kpi_ids:
                errors.append(f"{path.relative_to(ROOT)}: duplicate KPI id {metric_id!r}")
            kpi_ids.add(metric_id)
            check_nested_evidence_refs(path, [metric], evidence_ids, "KPI", errors)

    integration_ids: set[str] = set()
    for path in sorted(ROOT.glob("integrations/*.yml")):
        data = load_yaml(path)
        for pattern in data.get("patterns", []) or []:
            pattern_id = pattern["id"]
            if pattern_id in integration_ids:
                errors.append(f"{path.relative_to(ROOT)}: duplicate integration pattern {pattern_id!r}")
            integration_ids.add(pattern_id)

    ledger_transaction_type_count = 0
    for path in sorted(ROOT.glob("ledger/*.yml")):
        data = load_yaml(path)
        transaction_type_ids = check_unique_nested_ids(path, data.get("transaction_types", []), "ledger transaction type", errors)
        check_unique_nested_ids(path, data.get("invariants", []), "ledger invariant", errors)
        ledger_transaction_type_count += len(transaction_type_ids)
        check_nested_evidence_refs(path, data.get("transaction_types", []), evidence_ids, "ledger transaction type", errors)

    domain_event_type_count = 0
    domain_event_types_global: set[str] = set()
    for path in sorted(ROOT.glob("events/*.yml")):
        data = load_yaml(path)
        event_ids = check_unique_nested_ids(path, data.get("events", []), "domain event", errors)
        domain_event_type_count += len(event_ids)
        for event in data.get("events", []) or []:
            event_type = event.get("type")
            if event_type in domain_event_types_global:
                errors.append(f"{path.relative_to(ROOT)}: duplicate domain event type {event_type!r}")
            if event_type:
                domain_event_types_global.add(event_type)
        check_nested_evidence_refs(path, data.get("events", []), evidence_ids, "domain event", errors)
        check_nested_evidence_refs(path, data.get("external_mappings", []), evidence_ids, "external mapping", errors)

    resolvers = {"company": company_ids, "product": product_ids, "terminology": term_ids, "authority": authority_ids}
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
        f"Validation OK: {len(company_ids)} companies, {len(product_ids)} products, {len(evidence_ids)} evidence records, "
        f"{len(claim_ids)} claims, {len(term_ids)} terms, {len(authority_ids)} authorities, "
        f"{len(ontology_ids)} ontologies/{ontology_entity_count} entities, {len(kpi_ids)} KPIs, "
        f"{len(integration_ids)} integration patterns, {len(capability_ids)} capabilities, "
        f"{len(workflow_ids)} workflows/{workflow_stage_count} stages/{workflow_strategy_count} strategies/{workflow_vendor_model_count} vendor models, "
        f"{len(ledger_ids)} ledger registries/{ledger_transaction_type_count} transaction types, "
        f"{len(event_registry_ids)} event registries/{domain_event_type_count} event types"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
