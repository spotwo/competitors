#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]


def load_yaml(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_records(pattern: str) -> list[dict[str, Any]]:
    return [load_yaml(path) for path in sorted(ROOT.glob(pattern))]


def contains_all(values: list[str] | None, required: list[str] | None) -> bool:
    if not required:
        return True
    actual = set(values or [])
    return all(value in actual for value in required)


def company_matches(company: dict[str, Any], args: argparse.Namespace) -> bool:
    if args.name and args.name.lower() not in company["name"].lower():
        return False
    if not contains_all(company.get("markets"), args.market):
        return False
    if not contains_all(company.get("segments"), args.segment):
        return False
    if not contains_all(company.get("solution_layers"), args.layer):
        return False
    if not contains_all(company.get("industries"), args.industry):
        return False
    if not contains_all(company.get("operating_environments"), args.environment):
        return False
    if not contains_all(company.get("capabilities"), args.capability):
        return False
    if not contains_all(company.get("deployment_models"), args.deployment):
        return False
    if args.country:
        countries = set()
        if company.get("hq_country"):
            countries.add(company["hq_country"])
        countries.update(company.get("served_countries", []) or [])
        countries.update(company.get("local_presence_countries", []) or [])
        if not all(country.upper() in countries for country in args.country):
            return False
    return True


def print_vendor_table(companies: list[dict[str, Any]]) -> None:
    print("| Vendor | Segments | Layers | Markets | Deployment | Products |")
    print("|---|---|---|---|---|---|")
    for company in companies:
        products = ", ".join(product["name"] for product in company.get("products", []))
        print(
            f"| {company['name']} | {', '.join(company.get('segments', []))} | "
            f"{', '.join(company.get('solution_layers', []))} | {', '.join(company.get('markets', []))} | "
            f"{', '.join(company.get('deployment_models', []))} | {products} |"
        )


def command_vendors(args: argparse.Namespace) -> None:
    companies = [
        company
        for company in load_records("companies/*/company.yml")
        if company_matches(company, args)
    ]
    companies.sort(key=lambda company: company["name"].lower())
    if args.json:
        print(json.dumps(companies, indent=2, ensure_ascii=False))
    else:
        print_vendor_table(companies)
        print(f"\n{len(companies)} result(s)")


def normalized(value: str) -> str:
    return " ".join(value.strip().lower().replace("_", "-").split())


def command_term(args: argparse.Namespace) -> None:
    needle = normalized(args.query)
    matches = []
    for term in load_records("terminology/*.yml"):
        candidates = [term["id"], term["preferred_term"], *(term.get("aliases", []) or [])]
        if any(needle in normalized(candidate) for candidate in candidates):
            matches.append(term)
    if args.json:
        print(json.dumps(matches, indent=2, ensure_ascii=False))
    else:
        for index, term in enumerate(matches):
            if index:
                print("---")
            print(yaml.safe_dump(term, sort_keys=False, allow_unicode=True).rstrip())
        if not matches:
            print("No terminology matches.")


def command_claims(args: argparse.Namespace) -> None:
    claims = load_records("claims/*.yml")
    if args.subject:
        claims = [claim for claim in claims if claim.get("subject") == args.subject]
    if args.predicate:
        claims = [claim for claim in claims if claim.get("predicate") == args.predicate]
    if args.confidence:
        claims = [claim for claim in claims if claim.get("confidence") == args.confidence]
    claims.sort(key=lambda claim: claim["id"])
    if args.json:
        print(json.dumps(claims, indent=2, ensure_ascii=False))
    else:
        for claim in claims:
            print(yaml.safe_dump(claim, sort_keys=False, allow_unicode=True).rstrip())
            print("---")
        if not claims:
            print("No claims matched.")


def command_evidence(args: argparse.Namespace) -> None:
    evidence = load_records("evidence/*.yml")
    if args.subject:
        evidence = [item for item in evidence if item.get("subject") == args.subject]
    if args.grade:
        evidence = [item for item in evidence if item.get("grade") == args.grade]
    if args.publisher:
        evidence = [item for item in evidence if args.publisher.lower() in item.get("publisher", "").lower()]
    evidence.sort(key=lambda item: item["id"])
    if args.json:
        print(json.dumps(evidence, indent=2, ensure_ascii=False))
    else:
        print("| ID | Grade | Publisher | Captured | Title |")
        print("|---|---|---|---|---|")
        for item in evidence:
            print(f"| {item['id']} | {item['grade']} | {item['publisher']} | {item['captured_at']} | {item['title']} |")
        print(f"\n{len(evidence)} result(s)")


def command_gaps(args: argparse.Namespace) -> None:
    gaps = []
    for company in load_records("companies/*/company.yml"):
        for gap in company.get("research_gaps", []) or []:
            gaps.append({"type": "company", "subject": company["name"], "gap": gap})
    for term in load_records("terminology/*.yml"):
        if term.get("status") in {"candidate", "observed", "needs-research"}:
            gaps.append(
                {
                    "type": "terminology",
                    "subject": term["preferred_term"],
                    "gap": f"status={term['status']}; evidence_refs={len(term.get('evidence_refs', []))}",
                }
            )
    if args.json:
        print(json.dumps(gaps, indent=2, ensure_ascii=False))
    else:
        print("| Type | Subject | Gap |")
        print("|---|---|---|")
        for gap in gaps:
            print(f"| {gap['type']} | {gap['subject']} | {gap['gap']} |")
        print(f"\n{len(gaps)} open item(s)")


def command_stats(args: argparse.Namespace) -> None:
    companies = load_records("companies/*/company.yml")
    stats = {
        "companies": len(companies),
        "products": sum(len(company.get("products", [])) for company in companies),
        "evidence": len(load_records("evidence/*.yml")),
        "claims": len(load_records("claims/*.yml")),
        "terminology": len(load_records("terminology/*.yml")),
        "authorities": len(load_records("authorities/*.yml")),
        "taxonomy_vocabularies": len(list(ROOT.glob("taxonomy/*.yml"))),
    }
    if args.json:
        print(json.dumps(stats, indent=2))
    else:
        for key, value in stats.items():
            print(f"{key}: {value}")


def add_repeatable(parser: argparse.ArgumentParser, flag: str, dest: str, help_text: str) -> None:
    parser.add_argument(flag, dest=dest, action="append", help=help_text)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Query the Spotwo intralogistics knowledge base.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    vendors = subparsers.add_parser("vendors", help="Filter vendor/company records.")
    vendors.add_argument("--name")
    add_repeatable(vendors, "--market", "market", "Require a market taxonomy value. Repeatable.")
    add_repeatable(vendors, "--segment", "segment", "Require a segment taxonomy value. Repeatable.")
    add_repeatable(vendors, "--layer", "layer", "Require a solution-layer taxonomy value. Repeatable.")
    add_repeatable(vendors, "--industry", "industry", "Require an industry taxonomy value. Repeatable.")
    add_repeatable(vendors, "--environment", "environment", "Require an operating-environment value. Repeatable.")
    add_repeatable(vendors, "--capability", "capability", "Require a process capability. Repeatable.")
    add_repeatable(vendors, "--deployment", "deployment", "Require a deployment model. Repeatable.")
    add_repeatable(vendors, "--country", "country", "Require an ISO alpha-2 HQ/served/local-presence country. Repeatable.")
    vendors.add_argument("--json", action="store_true")
    vendors.set_defaults(func=command_vendors)

    term = subparsers.add_parser("term", help="Find terminology by id, preferred term, or alias.")
    term.add_argument("query")
    term.add_argument("--json", action="store_true")
    term.set_defaults(func=command_term)

    claims = subparsers.add_parser("claims", help="Query reusable claims.")
    claims.add_argument("--subject")
    claims.add_argument("--predicate")
    claims.add_argument("--confidence", choices=["confirmed", "high", "medium", "low"])
    claims.add_argument("--json", action="store_true")
    claims.set_defaults(func=command_claims)

    evidence = subparsers.add_parser("evidence", help="Query evidence records.")
    evidence.add_argument("--subject")
    evidence.add_argument("--grade", choices=list("ABCDEF"))
    evidence.add_argument("--publisher")
    evidence.add_argument("--json", action="store_true")
    evidence.set_defaults(func=command_evidence)

    gaps = subparsers.add_parser("gaps", help="Show explicit research gaps and unresolved terminology.")
    gaps.add_argument("--json", action="store_true")
    gaps.set_defaults(func=command_gaps)

    stats = subparsers.add_parser("stats", help="Show live repository counts.")
    stats.add_argument("--json", action="store_true")
    stats.set_defaults(func=command_stats)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
