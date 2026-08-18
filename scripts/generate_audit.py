#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parents[1]


def load_yaml(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def records(pattern: str):
    return [load_yaml(p) for p in sorted(ROOT.glob(pattern))]


def stats_markdown() -> str:
    companies = records("companies/*/company.yml")
    products = sum(len(c.get("products", [])) for c in companies)
    ontologies = records("ontology/*.yml")
    kpis = sum(len(r.get("metrics", [])) for r in records("kpis/*.yml"))
    integrations = sum(len(r.get("patterns", [])) for r in records("integrations/*.yml"))
    metrics = [
        ("Companies", len(companies)),
        ("Products", products),
        ("Evidence records", len(records("evidence/*.yml"))),
        ("Claims", len(records("claims/*.yml"))),
        ("Terminology records", len(records("terminology/*.yml"))),
        ("Authorities", len(records("authorities/*.yml"))),
        ("Taxonomy vocabularies", len(list(ROOT.glob("taxonomy/*.yml")))),
        ("Ontologies", len(ontologies)),
        ("Ontology entities", sum(len(o.get("entities", [])) for o in ontologies)),
        ("KPI metrics", kpis),
        ("Integration patterns", integrations),
    ]
    lines = ["# Knowledge Base Stats", "", "> Generated from canonical records. Do not edit by hand.", "", "| Metric | Count |", "|---|---:|"]
    lines.extend(f"| {name} | {count} |" for name, count in metrics)
    return "\n".join(lines) + "\n"


def gaps_markdown() -> str:
    company_gaps = []
    for company in records("companies/*/company.yml"):
        for gap in company.get("research_gaps", []) or []:
            company_gaps.append((company["name"], gap))
    term_gaps = []
    for term in records("terminology/*.yml"):
        if term.get("status") in {"candidate", "observed", "needs-research"}:
            term_gaps.append((term["preferred_term"], term["status"], len(term.get("evidence_refs", []))))
    lines = ["# Research Gaps", "", "> Generated from explicit company gaps and non-adopted terminology statuses. Do not edit by hand.", "", "## Company gaps", "", "| Company | Gap |", "|---|---|"]
    if company_gaps:
        lines.extend(f"| {company} | {gap} |" for company, gap in sorted(company_gaps))
    else:
        lines.append("| - | None |")
    lines.extend(["", "## Terminology requiring review", "", "| Term | Status | Evidence refs |", "|---|---|---:|"])
    if term_gaps:
        lines.extend(f"| {term} | {status} | {count} |" for term, status, count in sorted(term_gaps))
    else:
        lines.append("| - | None | 0 |")
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    outputs = {ROOT / "matrices/stats.md": stats_markdown(), ROOT / "matrices/research-gaps.md": gaps_markdown()}
    stale = []
    for path, content in outputs.items():
        if args.check:
            if not path.exists() or path.read_text(encoding="utf-8") != content:
                stale.append(str(path.relative_to(ROOT)))
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")
    if stale:
        raise SystemExit("Generated audit files are stale: " + ", ".join(stale))


if __name__ == "__main__":
    main()
