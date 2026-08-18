# Spotwo Competitors

Spotwo's local source of truth for intralogistics market knowledge.

This repository records competitors, products, capabilities, terminology, standards, market conventions, and the evidence behind them. Its purpose is not merely to track competitors, but to help Spotwo make consistent product, domain, UX, architecture, and engineering decisions based on established industry practice.

> Directory tree represents identity. Metadata represents classification.

## Core principles

1. **Evidence before claims** - material facts should point to captured evidence.
2. **Machine-readable first** - YAML/JSON is canonical; Markdown explains context and decisions.
3. **One entity, many facets** - geography, segment, industry, solution layer, and deployment model are metadata, not directory nesting.
4. **Terminology is a product asset** - record standards, dominant market usage, aliases, and Spotwo decisions.
5. **Time matters** - every researched fact has a verification date.
6. **Confidence is explicit** - official documentation is stronger evidence than reviews or forum posts.

## Repository map

```text
companies/       Vendor and product records
products/        Cross-vendor product records when needed
capabilities/    Canonical warehouse/intralogistics capabilities
taxonomy/        Controlled vocabularies
terminology/     Industry terms, aliases, usage, and Spotwo naming decisions
authorities/     Standards, regulations, associations, and industry guides
evidence/        Source records supporting claims
decisions/       Spotwo ADR-style product/domain/UX decisions
matrices/        Generated comparison views
schema/          JSON Schema definitions
scripts/         Validation and generation tools
.github/         CI workflows
```

## Authority order for terminology

Use this as a decision aid, not an automatic ranking:

1. Applicable regulation or law
2. Formal standard
3. Industry association / recognized guide
4. Dominant industry usage
5. Major competitor usage
6. Internal terminology

When the market term differs from a formal standard, record both and create a decision under `decisions/terminology/`.

## Evidence grades

| Grade | Source type |
|---|---|
| A | Official documentation / standard / regulation |
| B | Official vendor presentation, product page, demo, or release note |
| C | Customer case study |
| D | Analyst or reputable industry publication |
| E | Reseller / integrator material |
| F | Review, forum, social post, or unverified secondary source |

## Validation

All canonical YAML records are validated in CI against JSON Schema and repository-level referential-integrity rules.

Run locally:

```bash
python -m pip install -r requirements.txt
python scripts/validate.py
```
