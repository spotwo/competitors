# Contributing

## Research workflow

1. Capture the source as an `evidence/*.yml` record.
2. Add or update the canonical entity (`companies/`, `terminology/`, or `authorities/`).
3. Use controlled taxonomy values instead of inventing tags.
4. Set `last_verified` / `captured_at` to the actual research date.
5. Run `python scripts/validate.py`.
6. If terminology affects Spotwo product language, create an ADR under `decisions/terminology/`.

## Claims

Prefer narrowly-scoped, verifiable claims. Do not convert vendor marketing superlatives such as "best", "leading", or "most advanced" into facts.

## Dates

`last_verified` means the last date on which the linked source was checked and still supported the record. It does not mean the vendor first announced the feature.

## Geography

Keep these distinct:

- `hq_country`: legal/operating headquarters country.
- `markets`: broad regions where the vendor explicitly operates or sells.
- Country-level presence can be added later as evidence becomes available.

## Segmentation

`segments` describes target customer size. It is not a proxy for warehouse complexity. Use `warehouse_complexity` separately when a vendor provides enough evidence to support it.
