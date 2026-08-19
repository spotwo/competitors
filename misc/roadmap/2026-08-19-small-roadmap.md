# Small temporary roadmap - 2026-08-19

Status: insight snapshot, not the canonical Spotwo company roadmap.

This file preserves the seven-step engineering direction that emerged after the repository governance and topology-safety work. The statuses reflect what is already true at the time this snapshot is recorded. The sequence can change as evidence changes.

## 1. Prove the protected-main ruleset with a real PR

Status: **done**.

Use an ordinary post-ruleset pull request to prove that GitHub sees all required checks and that the protected `main` flow works in practice, not only in configuration.

Result: PR #58 passed the required `validate`, `postgres-kernel-lab`, and `cleanup-guard` gates before merge.

## 2. Reconcile the long-lived research branches

Status: **done as an audit**.

Inspect `research/product-technology-matrix` and `agent/catalog-open-source-logistics-stack`, compare them with current `main`, and avoid mechanically merging stale history when the useful datasets are already canonical.

Result: the primary technology-intelligence and open-source research datasets are already present in `main`; the old branches are historical leftovers rather than missing canonical work.

## 3. Connect research into queryable evidence

Status: **in progress**.

Connect companies, products, technology intelligence, open-source projects, evidence, and sources so machine queries can answer questions such as:

- which products have verified PostgreSQL, MySQL, SQL Server, or Oracle evidence;
- which competitors expose REST/OpenAPI or other integration contracts;
- which architectural patterns repeat across strong products;
- which open-source projects support or challenge a Spotwo design choice;
- which claims remain research gaps instead of being guessed.

## 4. Build the Spotwo Technology Decision Matrix

Status: **first version done**.

Translate research into scoped `adopt`, `trial`, `watch`, and `reject` decisions with evidence, constraints, alternatives, next actions, and revisit triggers.

Result: PR #58 introduced the first machine-readable decision registry. Continue evolving it only when new evidence or executable experiments justify a change.

## 5. Prove the WMS/event kernel with a second real pipeline

Status: **planned**.

The current event-pipeline reliability machinery is heavily exercised around the inventory position projection. Add a second real consumer/pipeline and prove that registry, topology, canary, readiness, migration, recovery, and finalization are generic rather than accidentally tailored to one case.

## 6. Build a pipeline scaffolder after reuse is proven

Status: **planned after step 5**.

Once two real pipelines use the same lifecycle successfully, generate the repetitive pieces instead of copying them manually. A future command such as `bin/new-event-pipeline <name>` should create the registry contract, identities, tests, canary/readiness hooks, and deployment skeleton while preserving explicit review points.

## 7. Move from lab-grade reliability to production packaging

Status: **later**.

After the reusable kernel is proven, define production deployment profiles, secrets, PostgreSQL/NATS backup and restore, SLOs, disaster recovery, service supervision or orchestration, and any external control-plane dependency needed for failure-domain independence.

## Side experiment captured with this roadmap

The REST/OpenAPI versus gRPC comparison under `misc/api-contract-lab/` is deliberately a side experiment, not a new roadmap step. It tests one architectural question before any API technology is promoted from `trial` to `adopt`.
