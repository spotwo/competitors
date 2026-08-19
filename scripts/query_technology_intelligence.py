#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable

import yaml

ROOT = Path(__file__).resolve().parents[1]
MATRIX = ROOT / "technology-intelligence" / "matrix.csv"
DEPLOYMENT_MATRIX = ROOT / "technology-intelligence" / "deployment-matrix.csv"
SOURCES = ROOT / "technology-intelligence" / "sources.csv"
DECISIONS = ROOT / "decisions" / "technology" / "registry.yml"
OPEN_SOURCE = ROOT / "research" / "open-source" / "catalog.yml"

TECHNOLOGY_FIELDS = (
    "primary_stack",
    "secondary_stack",
    "languages",
    "frameworks_platforms",
    "databases",
    "data_search_cache",
    "cloud",
    "containers_orchestration",
    "api_integration",
    "architecture",
)
DEPLOYMENT_FIELDS = (
    "canonical_deployment_models",
    "deployment_class",
    "hosting_topology",
    "warehouse_site_installation",
    "customer_datacenter_installation",
    "vendor_cloud_required",
    "cloud_independent_customer_hosted_core",
    "deployment_confidence",
    "notes",
)
KIND_ORDER = {"decision": 0, "product": 1, "open-source": 2}


def normalize(value: Any) -> Any:
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: normalize(item) for key, item in value.items()}
    if isinstance(value, list):
        return [normalize(item) for item in value]
    return value


def load_yaml(path: Path) -> dict[str, Any]:
    return normalize(yaml.safe_load(path.read_text(encoding="utf-8")))


def load_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def split_pipe(value: str | None, *, keep_unknown: bool = True) -> list[str]:
    if not value:
        return []
    values = [item.strip() for item in value.split("|") if item.strip()]
    if not keep_unknown:
        values = [item for item in values if item.casefold() != "unknown"]
    return values


def search_key(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, dict):
        value = " ".join(search_key(item) for item in value.values())
    elif isinstance(value, (list, tuple, set)):
        value = " ".join(search_key(item) for item in value)
    else:
        value = str(value)
    return " ".join(re.sub(r"[\W_]+", " ", value.casefold(), flags=re.UNICODE).split())


def matches_terms(values: Iterable[Any], terms: list[str]) -> bool:
    if not terms:
        return True
    haystacks = [search_key(value) for value in values]
    return all(any(search_key(term) in haystack for haystack in haystacks) for term in terms)


def unique_dicts(items: Iterable[dict[str, Any]], key: str = "id") -> list[dict[str, Any]]:
    seen: set[str] = set()
    result: list[dict[str, Any]] = []
    for item in items:
        identity = str(item.get(key, ""))
        if identity and identity not in seen:
            seen.add(identity)
            result.append(item)
    return result


def decision_summary(decision: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": decision["id"],
        "name": decision["name"],
        "category": decision["category"],
        "disposition": decision["disposition"],
        "confidence": decision["confidence"],
        "scope": decision["scope"],
    }


def source_summary(source: dict[str, str]) -> dict[str, str]:
    return {
        "id": source["id"],
        "publisher": source["publisher"],
        "source_type": source["source_type"],
        "title": source["title"],
        "url": source["url"],
        "captured_at": source["captured_at"],
        "supports": source["supports"],
    }


class TechnologyIntelligenceIndex:
    def __init__(self, root: Path = ROOT) -> None:
        self.root = root
        self.matrix_rows = load_csv(root / "technology-intelligence" / "matrix.csv")
        self.deployment_rows = load_csv(root / "technology-intelligence" / "deployment-matrix.csv")
        self.source_rows = load_csv(root / "technology-intelligence" / "sources.csv")
        self.decision_data = load_yaml(root / "decisions" / "technology" / "registry.yml")
        self.open_source_data = load_yaml(root / "research" / "open-source" / "catalog.yml")

        self.sources = {row["id"]: row for row in self.source_rows}
        self.deployments = {
            f"{row['company_id']}/{row['product_id']}": row for row in self.deployment_rows
        }
        self.decisions = {decision["id"]: decision for decision in self.decision_data["decisions"]}
        self.open_source_projects = {
            project["id"]: project for project in self.open_source_data["projects"]
        }
        self.products = {
            f"{row['company_id']}/{row['product_id']}": row for row in self.matrix_rows
        }

        self.product_decisions: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self.open_source_decisions: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for decision in self.decisions.values():
            evidence = decision["evidence"]
            for product_id in evidence["competitor_product_refs"]:
                self.product_decisions[product_id].append(decision)
            for project_id in evidence["open_source_project_refs"]:
                self.open_source_decisions[project_id].append(decision)

    def validate_joins(self) -> list[str]:
        errors: list[str] = []
        matrix_ids = set(self.products)
        deployment_ids = set(self.deployments)
        if matrix_ids != deployment_ids:
            for missing in sorted(matrix_ids - deployment_ids):
                errors.append(f"deployment matrix is missing product {missing}")
            for extra in sorted(deployment_ids - matrix_ids):
                errors.append(f"deployment matrix contains unknown product {extra}")

        for product_id, row in self.products.items():
            refs = split_pipe(row.get("source_refs"), keep_unknown=False)
            declared_count = int(row.get("source_count") or 0)
            if declared_count != len(refs):
                errors.append(
                    f"{product_id}: source_count={declared_count} but source_refs contains {len(refs)} ids"
                )
            for ref in refs:
                if ref not in self.sources:
                    errors.append(f"{product_id}: unknown source ref {ref}")

        for decision in self.decisions.values():
            evidence = decision["evidence"]
            for product_id in evidence["competitor_product_refs"]:
                if product_id not in self.products:
                    errors.append(f"decision {decision['id']}: unknown competitor product {product_id}")
            for project_id in evidence["open_source_project_refs"]:
                if project_id not in self.open_source_projects:
                    errors.append(f"decision {decision['id']}: unknown open-source project {project_id}")

        return errors

    def product_record(self, product_id: str, row: dict[str, str]) -> dict[str, Any]:
        deployment = self.deployments[product_id]
        refs = split_pipe(row.get("source_refs"), keep_unknown=False)
        source_records = [source_summary(self.sources[ref]) for ref in refs]
        technology = {
            field: split_pipe(row.get(field), keep_unknown=True) for field in TECHNOLOGY_FIELDS
        }
        deployment_record = {
            "canonical_deployment_models": split_pipe(deployment["canonical_deployment_models"]),
            "deployment_class": deployment["deployment_class"],
            "hosting_topology": deployment["hosting_topology"],
            "warehouse_site_installation": deployment["warehouse_site_installation"],
            "customer_datacenter_installation": deployment["customer_datacenter_installation"],
            "vendor_cloud_required": deployment["vendor_cloud_required"],
            "cloud_independent_customer_hosted_core": deployment[
                "cloud_independent_customer_hosted_core"
            ],
            "confidence": deployment["deployment_confidence"],
            "canonical_record": deployment["canonical_record"],
            "notes": deployment["notes"],
        }
        return {
            "kind": "product",
            "id": product_id,
            "company": {"id": row["company_id"], "name": row["company"]},
            "product": {"id": row["product_id"], "name": row["product"]},
            "technology": technology,
            "verification": {
                "state": row["verification_state"],
                "confidence": row["overall_confidence"],
            },
            "deployment": deployment_record,
            "evidence": {
                "source_refs": refs,
                "source_count": len(source_records),
                "sources": source_records,
            },
            "spotwo_decisions": [
                decision_summary(decision)
                for decision in sorted(
                    self.product_decisions.get(product_id, []), key=lambda item: item["id"]
                )
            ],
            "research_gaps": split_pipe(row.get("research_gaps"), keep_unknown=True),
        }

    def decision_record(self, decision: dict[str, Any]) -> dict[str, Any]:
        evidence = decision["evidence"]
        market_products: list[dict[str, Any]] = []
        market_sources: list[dict[str, Any]] = []
        for product_id in evidence["competitor_product_refs"]:
            product = self.products[product_id]
            refs = split_pipe(product.get("source_refs"), keep_unknown=False)
            market_products.append(
                {
                    "id": product_id,
                    "company": product["company"],
                    "product": product["product"],
                    "verification_state": product["verification_state"],
                    "confidence": product["overall_confidence"],
                    "source_refs": refs,
                }
            )
            market_sources.extend(source_summary(self.sources[ref]) for ref in refs)

        open_source_projects = []
        for project_id in evidence["open_source_project_refs"]:
            project = self.open_source_projects[project_id]
            open_source_projects.append(
                {
                    "id": project_id,
                    "name": project["name"],
                    "organization": project["organization"],
                    "category": project["category"],
                    "kind": project["kind"],
                    "upstream_url": project["upstream_url"],
                }
            )

        return {
            "kind": "decision",
            "id": decision["id"],
            "name": decision["name"],
            "category": decision["category"],
            "scope": decision["scope"],
            "disposition": decision["disposition"],
            "confidence": decision["confidence"],
            "rationale": decision["rationale"],
            "evidence": {
                "repository_refs": evidence["repository_refs"],
                "open_source_projects": open_source_projects,
                "competitor_products": market_products,
                "market_sources": unique_dicts(market_sources),
            },
            "constraints": decision["constraints"],
            "alternatives": decision["alternatives"],
            "next_action": decision["next_action"],
            "revisit_triggers": decision["revisit_triggers"],
        }

    def open_source_record(self, project: dict[str, Any]) -> dict[str, Any]:
        spotwo = project["spotwo"]
        return {
            "kind": "open-source",
            "id": project["id"],
            "name": project["name"],
            "organization": project["organization"],
            "category": project["category"],
            "project_kind": project["kind"],
            "upstream_url": project["upstream_url"],
            "layers": project["layers"],
            "status": project["status"],
            "spotwo_research": spotwo,
            "spotwo_decisions": [
                decision_summary(decision)
                for decision in sorted(
                    self.open_source_decisions.get(project["id"], []), key=lambda item: item["id"]
                )
            ],
        }

    def records(self) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        records.extend(self.decision_record(decision) for decision in self.decisions.values())
        records.extend(self.product_record(product_id, row) for product_id, row in self.products.items())
        records.extend(self.open_source_record(project) for project in self.open_source_projects.values())
        return sorted(records, key=lambda item: (KIND_ORDER[item["kind"]], item["id"]))


def decision_links(record: dict[str, Any]) -> list[dict[str, Any]]:
    if record["kind"] == "decision":
        return [record]
    return record.get("spotwo_decisions", [])


def values_for_filter(record: dict[str, Any], name: str) -> list[Any]:
    kind = record["kind"]
    if name == "company":
        if kind == "product":
            return [record["company"]]
        if kind == "decision":
            return record["evidence"]["competitor_products"]
        return []
    if name == "product":
        if kind == "product":
            return [record["product"]]
        if kind == "decision":
            return record["evidence"]["competitor_products"]
        return []
    if name == "database":
        return record["technology"]["databases"] if kind == "product" else []
    if name == "api":
        if kind == "product":
            return record["technology"]["api_integration"]
        if kind == "decision" and record["category"] == "api-contract":
            return [record["name"], record["scope"], record["alternatives"]]
        if kind == "open-source" and record["category"] == "api-governance":
            return [record["name"], record["layers"]]
        return []
    if name == "deployment":
        if kind == "product":
            deployment = record["deployment"]
            return [deployment[field] for field in deployment if field != "canonical_record"]
        if kind == "decision" and record["category"] == "deployment-orchestration":
            return [record["name"], record["scope"], record["alternatives"]]
        return []
    if name == "technology":
        if kind == "product":
            return [record["technology"][field] for field in TECHNOLOGY_FIELDS]
        if kind == "decision":
            return [record["name"], record["category"], record["scope"], record["alternatives"]]
        return [
            record["name"],
            record["organization"],
            record["category"],
            record["project_kind"],
            record["layers"],
        ]
    if name == "source":
        if kind == "product":
            return record["evidence"]["sources"]
        if kind == "decision":
            return [
                record["evidence"]["repository_refs"],
                record["evidence"]["market_sources"],
                record["evidence"]["open_source_projects"],
            ]
        return [record["upstream_url"]]
    raise ValueError(f"unknown filter field: {name}")


def record_matches(record: dict[str, Any], args: argparse.Namespace) -> bool:
    if args.kind and record["kind"] not in args.kind:
        return False

    for name in ("company", "product", "database", "api", "deployment", "technology", "source"):
        terms = getattr(args, name)
        if terms and not matches_terms(values_for_filter(record, name), terms):
            return False

    if args.decision:
        dispositions = {item["disposition"] for item in decision_links(record)}
        if not all(disposition in dispositions for disposition in args.decision):
            return False

    if args.verification:
        if record["kind"] != "product" or record["verification"]["state"] not in args.verification:
            return False

    if args.confidence:
        if record["kind"] == "product":
            value = record["verification"]["confidence"]
        elif record["kind"] == "decision":
            value = record["confidence"]
        else:
            value = ""
        if value not in args.confidence:
            return False

    if args.research_gaps and (record["kind"] != "product" or not record["research_gaps"]):
        return False

    return True


def query_dict(args: argparse.Namespace) -> dict[str, Any]:
    return {
        key: value
        for key, value in {
            "kind": args.kind,
            "company": args.company,
            "product": args.product,
            "database": args.database,
            "api": args.api,
            "deployment": args.deployment,
            "technology": args.technology,
            "source": args.source,
            "decision": args.decision,
            "verification": args.verification,
            "confidence": args.confidence,
            "research_gaps": args.research_gaps or None,
        }.items()
        if value
    }


def record_name(record: dict[str, Any]) -> str:
    if record["kind"] == "product":
        return f"{record['company']['name']} / {record['product']['name']}"
    return record["name"]


def table_state(record: dict[str, Any]) -> tuple[str, str]:
    if record["kind"] == "product":
        return record["verification"]["state"], record["verification"]["confidence"]
    if record["kind"] == "decision":
        return record["disposition"], record["confidence"]
    return record["status"], str(record["spotwo_research"].get("priority", ""))


def table_evidence(record: dict[str, Any]) -> str:
    if record["kind"] == "product":
        return f"sources:{record['evidence']['source_count']}"
    if record["kind"] == "decision":
        evidence = record["evidence"]
        return (
            f"repo:{len(evidence['repository_refs'])} "
            f"oss:{len(evidence['open_source_projects'])} "
            f"market:{len(evidence['competitor_products'])}"
        )
    return "upstream:1"


def render_table(records: list[dict[str, Any]], total: int) -> str:
    lines = [
        "KIND\tID\tNAME\tSTATE\tCONFIDENCE/PRIORITY\tEVIDENCE\tSPOTWO_DECISIONS\tRESEARCH_GAPS"
    ]
    for record in records:
        state, confidence = table_state(record)
        links = decision_links(record)
        decisions = ",".join(f"{item['id']}:{item['disposition']}" for item in links) or "-"
        gaps = str(len(record.get("research_gaps", []))) if record["kind"] == "product" else "-"
        lines.append(
            "\t".join(
                [
                    record["kind"],
                    record["id"],
                    record_name(record),
                    state,
                    confidence,
                    table_evidence(record),
                    decisions,
                    gaps,
                ]
            )
        )
    lines.append(f"matched={total} returned={len(records)}")
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Query Spotwo competitor, open-source, deployment, evidence, and technology-decision data."
    )
    parser.add_argument("--kind", action="append", choices=sorted(KIND_ORDER))
    parser.add_argument("--company", action="append", help="Company id or name; repeat for AND matching.")
    parser.add_argument("--product", action="append", help="Product id or name; repeat for AND matching.")
    parser.add_argument("--database", action="append", help="Database technology; repeat for AND matching.")
    parser.add_argument("--api", action="append", help="API/integration technology; repeat for AND matching.")
    parser.add_argument("--deployment", action="append", help="Deployment model/topology term; repeat for AND matching.")
    parser.add_argument("--technology", action="append", help="Broad technology term; repeat for AND matching.")
    parser.add_argument("--source", action="append", help="Evidence source id, publisher, title, URL, or repository ref.")
    parser.add_argument("--decision", action="append", choices=["adopt", "trial", "watch", "reject"])
    parser.add_argument("--verification", action="append", choices=["verified", "partial", "research-gap"])
    parser.add_argument("--confidence", action="append", choices=["unknown", "low", "medium", "high"])
    parser.add_argument("--research-gaps", action="store_true", help="Return only product records with unresolved research gaps.")
    parser.add_argument("--format", choices=["table", "json", "ndjson"], default="table")
    parser.add_argument("--limit", type=int, default=100, help="Maximum records to return after filtering. Default: 100.")
    parser.add_argument("--validate", action="store_true", help="Validate all cross-file joins before querying.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.limit < 1:
        parser.error("--limit must be at least 1")

    index = TechnologyIntelligenceIndex()
    if args.validate:
        errors = index.validate_joins()
        if errors:
            print("Technology intelligence query index validation failed:", file=sys.stderr)
            for error in errors:
                print(f"- {error}", file=sys.stderr)
            return 1

    matched = [record for record in index.records() if record_matches(record, args)]
    total = len(matched)
    returned = matched[: args.limit]
    by_kind = Counter(record["kind"] for record in matched)

    if args.format == "json":
        payload = {
            "query": query_dict(args),
            "summary": {
                "matched": total,
                "returned": len(returned),
                "by_kind": dict(sorted(by_kind.items())),
            },
            "records": returned,
        }
        print(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False))
    elif args.format == "ndjson":
        for record in returned:
            print(json.dumps(record, sort_keys=True, ensure_ascii=False))
    else:
        print(render_table(returned, total))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
