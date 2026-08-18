# Spotwo Competitors

Spotwo's local source of truth for intralogistics market knowledge.

This repository records competitors, products, capabilities, terminology, standards, market conventions, claims, and the evidence behind them. Its purpose is not merely to track competitors, but to help Spotwo make consistent product, domain, UX, architecture, and engineering decisions based on established industry practice.

> Directory tree represents identity. Metadata represents classification.

## Core principles

1. **Evidence before claims** - material facts should point to captured evidence.
2. **Fact is not evidence** - reusable facts live as claims; sources live as evidence records.
3. **Machine-readable first** - YAML/JSON is canonical; Markdown explains context and decisions.
4. **One entity, many facets** - geography, segment, industry, solution layer, and deployment model are metadata, not directory nesting.
5. **Terminology is a product asset** - record standards, dominant market usage, aliases, and Spotwo decisions.
6. **Time matters** - every researched fact has a verification or observation date.
7. **Confidence is explicit** - official documentation is stronger evidence than reviews or forum posts.
8. **Unknown beats invented** - unresolved facts belong in `research_gaps`.

## Knowledge model

```text
Company / Product / Term / Authority
              |
              v
            Claim
              |
              v
           Evidence
```

## Repository map

```text
companies/       Vendor and product records
claims/          Reusable factual assertions linked to evidence
taxonomy/        Controlled vocabularies
terminology/     Industry terms, aliases, usage, and Spotwo naming decisions
authorities/     Standards, regulations, associations, and industry guides
evidence/        Source records supporting claims
decisions/       Spotwo ADR-style product/domain/UX decisions
matrices/        Generated comparison, stats, and research-gap views
schema/          JSON Schema definitions
templates/       Research and record templates
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
| C | Official customer case study |
| D | Analyst or reputable industry publication |
| E | Reseller / integrator material |
| F | Review, forum, social post, or unverified secondary source |

## Validation

Canonical records are validated in CI against JSON Schema, controlled taxonomies, evidence references, parent relationships, claim subjects, and generated-view freshness.

Run locally:

```bash
python -m pip install -r requirements.txt
python scripts/validate.py
python scripts/generate_matrices.py --check
python scripts/generate_audit.py --check
```

To refresh generated views after editing canonical records:

```bash
python scripts/generate_matrices.py
python scripts/generate_audit.py
```
