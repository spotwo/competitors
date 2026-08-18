# Product Technology Intelligence

Isolated product-level technology research for the competitor knowledge base.

This directory is intentionally separate from the existing `companies/`, `evidence/`, `matrices/`, `schema/` and generated-view flows. No root validator, generator, or CI file is changed.

## Files

- `matrix.csv` - 42 product rows across 24 companies already modeled in `companies/*/company.yml`.
- `sources.csv` - 33 primary-source records used by the matrix.

Join key:

`matrix.csv.source_refs` -> `sources.csv.id`

Product identity always reuses the existing repository identity:

`company_id/product_id`

## What the matrix separates

- primary stack;
- secondary stack;
- languages;
- frameworks/platform/runtime;
- databases with evidence scope and confidence;
- data/search/cache technologies;
- cloud;
- containers/orchestration;
- APIs/integration;
- architecture;
- verification state and overall confidence;
- unresolved research gaps.

## Evidence rules

1. Never infer a database from the vendor name.
2. Never label analytics/data-lake storage as the transactional WMS database without direct evidence.
3. Never promote one customer deployment to a universal product default.
4. Platform-level and suite-level evidence stays explicitly scoped.
5. Historical/version-specific evidence stays version-scoped.
6. Unknown beats a plausible guess.

The current source set intentionally mixes different primary-source classes: official developer/architecture docs, official source repositories, admin/install docs, API docs, product docs, customer deployment cases, partner/platform pages and license disclosures.

`verification_state=research-gap` is intentional. Those rows keep known repository products visible even when the technology stack has not yet been verified.
