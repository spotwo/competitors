# Product Technology Intelligence

Isolated product-level technology research for the competitor knowledge base.

This directory is intentionally separate from the existing `companies/`, `evidence/`, `matrices/`, `schema/` and generated-view flows. No root validator, generator, or CI file is changed.

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
