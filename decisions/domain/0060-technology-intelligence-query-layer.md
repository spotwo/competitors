# ADR 0060: Technology intelligence query layer

Status: Accepted

Date: 2026-08-19

## Context

The repository now contains several evidence layers that answer different parts of a technology question:

- competitor product technology facts in `technology-intelligence/matrix.csv`;
- deployment topology in `technology-intelligence/deployment-matrix.csv`;
- primary-source claims in `technology-intelligence/sources.csv`;
- open-source research in `research/open-source/catalog.yml`;
- scoped Spotwo technology decisions in `decisions/technology/registry.yml`.

These sources are useful independently, but an agent or engineer currently has to join them manually. Manual joins make it easy to lose evidence provenance, confuse a research gap with a verified fact, or mistake market usage for a Spotwo architecture decision.

## Decision

Add a read-only query layer exposed as `bin/query-technology`.

The query layer joins canonical source files at runtime. It does not create another persisted database or duplicate source-of-truth dataset.

The result model exposes three record kinds:

1. `product` for competitor product technology, deployment, evidence, linked decisions, and research gaps;
2. `decision` for scoped Spotwo decisions and their resolved repository, market, and open-source evidence;
3. `open-source` for catalog projects and the Spotwo decisions that cite them.

Filters are semantic to the source field rather than global free-text searches. In particular:

- `--database` searches only explicit database evidence;
- `--api` searches API/integration evidence and scoped API-contract decisions;
- `--deployment` searches normalized deployment evidence;
- `--company` and `--product` can traverse into decisions that cite the matching market evidence;
- `--decision` can traverse from a Spotwo disposition into the products and open-source projects that support it;
- `--research-gaps` never turns missing evidence into a technology match.

Machine-readable JSON and NDJSON outputs preserve resolved evidence URLs and source metadata. The default table is only a concise human view of the same records.

## Evidence and authority boundary

The query layer is a read model, not an authority escalation mechanism.

A result such as:

```text
Manhattan Associates uses Kubernetes
```

is market evidence.

A result such as:

```text
Spotwo decision kubernetes-default = watch
```

is a repository architecture decision.

The first does not imply the second should become `adopt`.

Likewise, an open-source project's popularity or relevance does not create a runtime dependency.

Unknown values remain unknown. Research-gap prose is intentionally excluded from database/API technology matching so a sentence such as "PostgreSQL support is not verified" cannot accidentally become evidence that PostgreSQL is supported.

## Validation

Repository checks must verify:

- product identities are identical across technology and deployment matrices;
- source counts match source-reference lists;
- all technology source refs resolve;
- all technology-decision competitor refs resolve to known products;
- all technology-decision open-source refs resolve to known catalog projects;
- representative query semantics remain executable in tests.

## Consequences

Positive:

- agents can query research without manually opening multiple CSV/YAML files;
- every returned competitor fact can carry its primary-source evidence;
- Spotwo decisions stay visibly separate from competitor observations;
- research gaps can be queried directly instead of silently filled with guesses;
- the same interface works for humans, scripts, and future agents.

Costs:

- query semantics become a maintained repository contract;
- changes to source schemas may require query-layer updates;
- mixed record kinds require callers to use `kind` when they need a single entity class.

## Non-goals

This ADR does not introduce a search service, vector database, graph database, external index, API server, or production runtime dependency. It does not change PostgreSQL, NATS, WMS kernel behavior, or any technology disposition.
