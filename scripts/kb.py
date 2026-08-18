#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
from typing import Any
import yaml

ROOT = Path(__file__).resolve().parents[1]


def normalize(value):
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: normalize(v) for k, v in value.items()}
    if isinstance(value, list):
        return [normalize(v) for v in value]
    return value


def load_yaml(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return normalize(yaml.safe_load(f))


def load_records(pattern: str) -> list[dict[str, Any]]:
    return [load_yaml(path) for path in sorted(ROOT.glob(pattern))]


def contains_all(values: list[str] | None, required: list[str] | None) -> bool:
    if not required:
        return True
    actual = set(values or [])
    return all(value in actual for value in required)


def dump_json(value) -> None:
    print(json.dumps(value, indent=2, ensure_ascii=False))


def company_matches(company: dict[str, Any], args: argparse.Namespace) -> bool:
    if args.name and args.name.lower() not in company["name"].lower(): return False
    if not contains_all(company.get("markets"), args.market): return False
    if not contains_all(company.get("segments"), args.segment): return False
    if not contains_all(company.get("solution_layers"), args.layer): return False
    if not contains_all(company.get("industries"), args.industry): return False
    if not contains_all(company.get("operating_environments"), args.environment): return False
    if not contains_all(company.get("capabilities"), args.capability): return False
    if not contains_all(company.get("deployment_models"), args.deployment): return False
    if args.country:
        countries = set(filter(None, [company.get("hq_country")]))
        countries.update(company.get("served_countries", []) or [])
        countries.update(company.get("local_presence_countries", []) or [])
        if not all(country.upper() in countries for country in args.country): return False
    return True


def command_vendors(args):
    companies = [c for c in load_records("companies/*/company.yml") if company_matches(c, args)]
    companies.sort(key=lambda c: c["name"].lower())
    if args.json: return dump_json(companies)
    print("| Vendor | Segments | Layers | Markets | Deployment | Products |")
    print("|---|---|---|---|---|---|")
    for c in companies:
        products = ", ".join(p["name"] for p in c.get("products", []))
        print(f"| {c['name']} | {', '.join(c.get('segments', []))} | {', '.join(c.get('solution_layers', []))} | {', '.join(c.get('markets', []))} | {', '.join(c.get('deployment_models', []))} | {products} |")
    print(f"\n{len(companies)} result(s)")


def normalized(value: str) -> str:
    return " ".join(value.strip().lower().replace("_", "-").split())


def command_term(args):
    needle = normalized(args.query)
    matches = []
    for term in load_records("terminology/*.yml"):
        candidates = [term["id"], term["preferred_term"], *(term.get("aliases", []) or [])]
        if any(needle in normalized(candidate) for candidate in candidates): matches.append(term)
    if args.json: return dump_json(matches)
    for term in matches:
        print(yaml.safe_dump(term, sort_keys=False, allow_unicode=True).rstrip())
        print("---")
    if not matches: print("No terminology matches.")


def command_capability(args):
    needle = normalized(args.query) if args.query else None
    matches = []
    for capability in load_records("capabilities/*.yml"):
        candidates = [capability["id"], capability["label"], capability["spotwo"]["canonical_term"]]
        if needle and not any(needle in normalized(candidate) for candidate in candidates):
            continue
        if args.group and capability["group"] != args.group:
            continue
        if args.vendor and not any(obs.get("company_id") == args.vendor for obs in capability.get("vendor_observations", [])):
            continue
        matches.append(capability)
    matches.sort(key=lambda c: (c["group"], c["label"]))
    if args.json: return dump_json(matches)
    for capability in matches:
        print(f"## {capability['label']} [{capability['status']}]\n")
        print(capability["definition"])
        print(f"\nSpotwo: **{capability['spotwo']['canonical_term']}** [{capability['spotwo']['status']}]")
        if capability.get("strategies"):
            print("Strategies: " + ", ".join(capability["strategies"]))
        observations = capability.get("vendor_observations", []) or []
        if observations:
            print("\n| Vendor | Level | Terminology / note |")
            print("|---|---|---|")
            for obs in observations:
                text = ", ".join(obs.get("terminology", [])) or obs.get("notes", "")
                print(f"| {obs['company_id']} | {obs['level']} | {text} |")
        print()
    if not matches: print("No capability matches.")


def command_ontology(args):
    needle = normalized(args.query) if args.query else None
    matches = []
    for ontology in load_records("ontology/*.yml"):
        entity_matches = []
        for entity in ontology.get("entities", []):
            candidates = [entity["id"], entity["term"], *(entity.get("aliases", []) or [])]
            if not needle or any(needle in normalized(candidate) for candidate in candidates): entity_matches.append(entity)
        if (not needle or needle in normalized(ontology["id"]) or needle in normalized(ontology["title"]) or entity_matches):
            copy = dict(ontology)
            if needle: copy["entities"] = entity_matches
            matches.append(copy)
    if args.json: return dump_json(matches)
    for item in matches:
        print(f"## {item['title']} [{item['status']}]\n")
        for entity in item.get("entities", []):
            aliases = f" ({', '.join(entity.get('aliases', []))})" if entity.get("aliases") else ""
            print(f"- **{entity['term']}**{aliases}: {entity['definition']}")
        print()
    if not matches: print("No ontology matches.")


def command_kpis(args):
    metrics = []
    for registry in load_records("kpis/*.yml"):
        for metric in registry.get("metrics", []):
            if args.category and metric["category"] != args.category: continue
            if args.name and args.name.lower() not in metric["name"].lower(): continue
            metrics.append(metric)
    if args.json: return dump_json(metrics)
    print("| KPI | Category | Direction | Unit | Status |")
    print("|---|---|---|---|---|")
    for m in metrics: print(f"| {m['name']} | {m['category']} | {m['direction']} | {m.get('unit', '')} | {m['status']} |")
    print(f"\n{len(metrics)} result(s)")


def command_integrations(args):
    patterns = []
    for registry in load_records("integrations/*.yml"):
        for pattern in registry.get("patterns", []):
            if args.category and pattern["category"] != args.category: continue
            if args.protocol and not any(args.protocol.lower() in p.lower() for p in pattern.get("typical_protocols", [])): continue
            patterns.append(pattern)
    if args.json: return dump_json(patterns)
    print("| Pattern | Category | Direction | Protocols | Status |")
    print("|---|---|---|---|---|")
    for p in patterns: print(f"| {p['name']} | {p['category']} | {p['directionality']} | {', '.join(p.get('typical_protocols', []))} | {p['status']} |")
    print(f"\n{len(patterns)} result(s)")


def command_claims(args):
    claims = load_records("claims/*.yml")
    if args.subject: claims = [c for c in claims if c.get("subject") == args.subject]
    if args.predicate: claims = [c for c in claims if c.get("predicate") == args.predicate]
    if args.confidence: claims = [c for c in claims if c.get("confidence") == args.confidence]
    claims.sort(key=lambda c: c["id"])
    if args.json: return dump_json(claims)
    for claim in claims:
        print(yaml.safe_dump(claim, sort_keys=False, allow_unicode=True).rstrip())
        print("---")
    if not claims: print("No claims matched.")


def command_evidence(args):
    evidence = load_records("evidence/*.yml")
    if args.subject: evidence = [e for e in evidence if e.get("subject") == args.subject]
    if args.grade: evidence = [e for e in evidence if e.get("grade") == args.grade]
    if args.publisher: evidence = [e for e in evidence if args.publisher.lower() in e.get("publisher", "").lower()]
    evidence.sort(key=lambda e: e["id"])
    if args.json: return dump_json(evidence)
    print("| ID | Grade | Publisher | Captured | Title |")
    print("|---|---|---|---|---|")
    for e in evidence: print(f"| {e['id']} | {e['grade']} | {e['publisher']} | {e['captured_at']} | {e['title']} |")
    print(f"\n{len(evidence)} result(s)")


def command_gaps(args):
    gaps = []
    for company in load_records("companies/*/company.yml"):
        for gap in company.get("research_gaps", []) or []: gaps.append({"type": "company", "subject": company["name"], "gap": gap})
    for term in load_records("terminology/*.yml"):
        if term.get("status") in {"candidate", "observed", "needs-research"}:
            gaps.append({"type": "terminology", "subject": term["preferred_term"], "gap": f"status={term['status']}; evidence_refs={len(term.get('evidence_refs', []))}"})
    for capability in load_records("capabilities/*.yml"):
        if capability.get("status") in {"candidate", "observed", "needs-research"} or not capability.get("evidence_refs"):
            gaps.append({"type": "capability", "subject": capability["label"], "gap": f"status={capability['status']}; vendors={len(capability.get('vendor_observations', []))}; evidence_refs={len(capability.get('evidence_refs', []))}"})
        for gap in capability.get("research_gaps", []) or []: gaps.append({"type": "capability", "subject": capability["label"], "gap": gap})
    for pattern, kind in (("ontology/*.yml", "ontology"), ("kpis/*.yml", "kpi-registry"), ("integrations/*.yml", "integration-registry")):
        for record in load_records(pattern):
            for gap in record.get("research_gaps", []) or []: gaps.append({"type": kind, "subject": record["title"], "gap": gap})
    if args.json: return dump_json(gaps)
    print("| Type | Subject | Gap |")
    print("|---|---|---|")
    for gap in gaps: print(f"| {gap['type']} | {gap['subject']} | {gap['gap']} |")
    print(f"\n{len(gaps)} open item(s)")


def command_stats(args):
    companies = load_records("companies/*/company.yml")
    ontologies = load_records("ontology/*.yml")
    capabilities = load_records("capabilities/*.yml")
    stats = {
        "companies": len(companies),
        "products": sum(len(c.get("products", [])) for c in companies),
        "evidence": len(load_records("evidence/*.yml")),
        "claims": len(load_records("claims/*.yml")),
        "terminology": len(load_records("terminology/*.yml")),
        "authorities": len(load_records("authorities/*.yml")),
        "taxonomy_vocabularies": len(list(ROOT.glob("taxonomy/*.yml"))),
        "ontologies": len(ontologies),
        "ontology_entities": sum(len(o.get("entities", [])) for o in ontologies),
        "kpis": sum(len(r.get("metrics", [])) for r in load_records("kpis/*.yml")),
        "integration_patterns": sum(len(r.get("patterns", [])) for r in load_records("integrations/*.yml")),
        "capabilities": len(capabilities),
        "vendor_capability_observations": sum(len(c.get("vendor_observations", [])) for c in capabilities),
    }
    if args.json: return dump_json(stats)
    for key, value in stats.items(): print(f"{key}: {value}")


def add_repeatable(parser, flag, dest, help_text):
    parser.add_argument(flag, dest=dest, action="append", help=help_text)


def build_parser():
    parser = argparse.ArgumentParser(description="Query the Spotwo intralogistics knowledge base.")
    sub = parser.add_subparsers(dest="command", required=True)
    vendors = sub.add_parser("vendors")
    vendors.add_argument("--name")
    for flag, dest in (("--market","market"),("--segment","segment"),("--layer","layer"),("--industry","industry"),("--environment","environment"),("--capability","capability"),("--deployment","deployment"),("--country","country")):
        add_repeatable(vendors, flag, dest, f"Require {dest}; repeatable.")
    vendors.add_argument("--json", action="store_true"); vendors.set_defaults(func=command_vendors)
    term = sub.add_parser("term"); term.add_argument("query"); term.add_argument("--json", action="store_true"); term.set_defaults(func=command_term)
    capability = sub.add_parser("capability"); capability.add_argument("query", nargs="?"); capability.add_argument("--group"); capability.add_argument("--vendor"); capability.add_argument("--json", action="store_true"); capability.set_defaults(func=command_capability)
    ontology = sub.add_parser("ontology"); ontology.add_argument("query", nargs="?"); ontology.add_argument("--json", action="store_true"); ontology.set_defaults(func=command_ontology)
    kpis = sub.add_parser("kpis"); kpis.add_argument("--category"); kpis.add_argument("--name"); kpis.add_argument("--json", action="store_true"); kpis.set_defaults(func=command_kpis)
    integrations = sub.add_parser("integrations"); integrations.add_argument("--category"); integrations.add_argument("--protocol"); integrations.add_argument("--json", action="store_true"); integrations.set_defaults(func=command_integrations)
    claims = sub.add_parser("claims"); claims.add_argument("--subject"); claims.add_argument("--predicate"); claims.add_argument("--confidence", choices=["confirmed","high","medium","low"]); claims.add_argument("--json", action="store_true"); claims.set_defaults(func=command_claims)
    evidence = sub.add_parser("evidence"); evidence.add_argument("--subject"); evidence.add_argument("--grade", choices=list("ABCDEF")); evidence.add_argument("--publisher"); evidence.add_argument("--json", action="store_true"); evidence.set_defaults(func=command_evidence)
    gaps = sub.add_parser("gaps"); gaps.add_argument("--json", action="store_true"); gaps.set_defaults(func=command_gaps)
    stats = sub.add_parser("stats"); stats.add_argument("--json", action="store_true"); stats.set_defaults(func=command_stats)
    return parser


def main():
    parser = build_parser(); args = parser.parse_args(); args.func(args)


if __name__ == "__main__":
    main()
