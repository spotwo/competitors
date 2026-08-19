# Product Technology Intelligence

Product-level technology research for the competitor knowledge base.

The source datasets stay isolated under `technology-intelligence/`, while repository validators, technology decisions, and the query layer may consume them. They remain evidence, not automatic architecture authority.

## Files

- `matrix.csv` - 42 product rows across 24 companies already modeled in `companies/*/company.yml`.
- `deployment-matrix.csv` - deployment model and hosting topology for the same 42 product rows.
- `sources.csv` - 33 primary-source records used by the technology matrix.

Join key:

`company_id/product_id`

Technology evidence join:

`matrix.csv.source_refs` -> `sources.csv.id`

Product identity always reuses the existing repository identity:

`company_id/product_id`

## Query layer

`bin/query-technology` joins product technology, deployment topology, primary-source evidence, open-source research, and scoped Spotwo technology decisions at query time. It does not copy these datasets into a second source of truth.

Examples:

```bash
bin/query-technology --database postgresql
bin/query-technology --api rest
bin/query-technology --deployment self-hosted
bin/query-technology --decision trial
bin/query-technology --company manhattan-associates
bin/query-technology --technology kubernetes --format json
bin/query-technology --verification research-gap --research-gaps
```

Filters are case-insensitive and may be repeated. Repeated filters of the same type are ANDed. Different filter types are also ANDed.

The query result has three record kinds:

- `product` - competitor product plus technology fields, deployment topology, resolved primary sources, linked Spotwo decisions, and research gaps;
- `decision` - the scoped Spotwo technology decision plus repository evidence, resolved competitor-product evidence, resolved market sources, and referenced open-source projects;
- `open-source` - one research-catalog project plus its Spotwo research metadata and any technology decisions that cite it.

Use `--kind product`, `--kind decision`, or `--kind open-source` when a caller wants only one record type. Without `--kind`, evidence relationships are traversed intentionally. For example, `--company manhattan-associates` can return both the product record and Spotwo decisions that cite that product as market evidence.

Output formats:

```bash
# Human-oriented TSV table
bin/query-technology --database postgresql

# Machine-readable envelope with query, summary, and records
bin/query-technology --database postgresql --format json

# One machine-readable record per line
bin/query-technology --decision trial --format ndjson
```

`--validate` checks cross-file joins before returning results. Repository CI runs the same join validation and query semantics tests through `bin/check`.

Important query semantics:

- a database filter searches the explicit `databases` evidence field, not free text in research gaps;
- an API filter searches product API evidence and explicitly scoped API-contract decisions;
- a deployment filter searches the deployment matrix, not generic cloud words elsewhere in a product row;
- `--decision adopt|trial|watch|reject` traverses decision links, so it can return the decision plus products/open-source projects that support it;
- resolved source objects include their source ID, publisher, type, title, URL, capture date, and supported claim;
- unknown evidence remains unknown. The query layer never converts a plausible guess into a match.

## What the technology matrix separates

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

## What the deployment matrix separates

`deployment-matrix.csv` exists because `on-premise` alone is not precise enough for warehouse software.

It records:

- canonical deployment models such as SaaS, cloud, private cloud and on-premise;
- a normalized deployment class such as cloud-service, hybrid-choice, customer-hosted or self-hosted-capable;
- hosting topology - vendor-managed cloud versus customer-controlled infrastructure;
- whether a warehouse-site installation is known, possible, unsupported or still unknown;
- whether a remote customer datacenter installation is known, possible, unsupported or still unknown;
- whether the core product requires the vendor cloud;
- whether the core can run on customer-controlled infrastructure independently of the vendor cloud;
- confidence and topology-specific research gaps.

### Important deployment distinctions

**On-premise is not a physical location.** It means the customer controls the runtime infrastructure. The server may be in the warehouse, in another building, in a central customer datacenter, or in a customer-managed private cloud. If public evidence does not identify the physical placement, the matrix keeps that distinction explicit instead of guessing.

**Self-hosted is not the same as offline.** A self-hosted WMS may be independent of the vendor's cloud while still requiring LAN connectivity between application servers, databases, handheld terminals, automation controllers and other services.

**A local device is not an on-premise core.** RF terminals, mobile apps, PLC gateways, print agents and edge adapters may run at the warehouse while the WMS itself remains a cloud SaaS product.

**WCS/WES products need extra care.** Low-latency automation control may use site-local runtimes even when the vendor markets the overall platform as cloud or SaaS. Rows with insufficient evidence explicitly keep the site runtime as unknown.

## Evidence rules

1. Never infer a database from the vendor name.
2. Never label analytics/data-lake storage as the transactional WMS database without direct evidence.
3. Never promote one customer deployment to a universal product default.
4. Platform-level and suite-level evidence stays explicitly scoped.
5. Historical/version-specific evidence stays version-scoped.
6. Unknown beats a plausible guess.
7. Never infer warehouse-site hosting merely from an `on-premise` label.
8. Never infer offline capability merely from self-hosted or on-premise deployment.
9. Never infer an on-premise WMS core merely because scanners, gateways or automation controllers run locally.

The current source set intentionally mixes different primary-source classes: official developer/architecture docs, official source repositories, admin/install docs, API docs, product docs, customer deployment cases, partner/platform pages and license disclosures.

`verification_state=research-gap` is intentional. Those rows keep known repository products visible even when the technology stack has not yet been verified.
