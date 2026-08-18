# Contributing

## Research workflow

1. Capture the source as an `evidence/*.yml` record.
2. Add or update the canonical entity (`companies/`, `terminology/`, or `authorities/`).
3. Use controlled taxonomy values instead of inventing tags.
4. Set `last_verified` / `captured_at` to the actual research date.
5. Record unknowns in `research_gaps` rather than guessing.
6. Run `python scripts/generate_matrices.py` after changing companies or terminology.
7. Run `python scripts/validate.py`.
8. If terminology affects Spotwo product language, create an ADR under `decisions/terminology/`.

## Claims

Prefer narrowly-scoped, verifiable claims. Do not convert vendor marketing superlatives such as "best", "leading", or "most advanced" into facts.

## Dates

`last_verified` means the last date on which the linked source was checked and still supported the record. It does not mean the vendor first announced the feature.

## Geography

Keep these distinct:

- `hq_country`: legal/operating headquarters country.
- `markets`: broad regions where the vendor explicitly operates or sells.
- `served_countries`: countries with supported evidence of commercial availability or customer/service footprint.
- `local_presence_countries`: countries with supported evidence of offices, teams, or direct local presence.

## Segmentation

Company-level `segments` describes the vendor portfolio. Product-level `segments` can narrow a specific product or edition. Do not use customer size as a proxy for warehouse complexity.

## Unknowns

Do not add `unknown` to controlled taxonomies merely to satisfy a schema. Omit optional facts that are not verified and add a precise `research_gaps` item instead.
