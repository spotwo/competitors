# RFC: Evidence-backed open-source logistics research

Status: **Deferred / idea captured**

This RFC records a possible future research pass over the open-source logistics catalog. It is intentionally not an implementation commitment. The idea is to preserve a reusable prompt and expected outputs so an agent can run the work later as a dedicated research task.

## Context

`research/open-source/catalog.yml` currently acts as a machine-readable reference catalog for open-source projects and specifications relevant to Spotwo across WMS/WES/WCS, simulation, robotics, industrial connectivity, edge, optimization, routing, logistics interoperability, events, API governance, and enterprise-domain references.

The catalog is useful for prioritization, but most records are still high-level assessments. A future pass could turn it into an evidence-backed technology intelligence dataset rather than a curated list.

## Goal

Enrich every catalog entry so that Spotwo can answer, mechanically and with evidence:

- what the project or specification actually provides;
- whether Spotwo should adopt, prototype, study, watch, or reject it;
- which Spotwo capability or architecture boundary it maps to;
- which upstream version, tag, commit, documentation, or specification revision the assessment is based on;
- whether the project is healthy and actively maintained;
- what the licensing, governance, maturity, adoption, integration cost, and operational risks are;
- what alternatives should be compared before adoption.

## Proposed catalog enrichment

For each project, consider adding machine-readable fields such as:

```yaml
source:
  repository_url: https://github.com/example/project
  documentation_url: https://example.org/docs
  pinned_ref: v1.2.3
  pinned_commit: 0123456789abcdef
  last_verified_at: 2026-08-19

project:
  license: Apache-2.0
  languages: [go]
  governance: foundation
  foundation: Eclipse Foundation
  maturity: production
  maintenance_status: active
  latest_release: v1.2.3
  latest_release_at: 2026-07-10

adoption:
  production_users: []
  evidence_refs: []

spotwo:
  capabilities: []
  architecture_boundaries: []
  disposition: prototype
  use_directly: maybe
  integration_cost: medium
  operational_risk: medium
  lock_in_risk: low
  alternatives: []
  evidence_refs: []
```

The exact schema should be designed during the future task rather than treated as fixed by this example.

## Evidence model

Material claims should point to explicit evidence rather than remain prose-only assessments.

Examples:

```yaml
evidence_refs:
  - ev-open-rmf-readme
  - ev-open-rmf-fleet-adapter-docs
  - ev-open-rmf-release-2026-07
```

Prefer evidence in roughly this order:

1. official specification or standards body;
2. official repository at a pinned tag or commit;
3. official documentation;
4. official engineering article or conference material;
5. credible production-adoption evidence;
6. secondary sources only when primary sources are insufficient.

Where possible, capture immutable commit hashes or specification versions so later researchers can reproduce the assessment.

## Decision model

Every project should eventually receive one explicit disposition:

- `adopt` - use as a Spotwo contract, dependency, or supported standard;
- `prototype` - build a lab before deciding;
- `study` - architecture/domain reference only;
- `watch` - relevant, but no current work justified;
- `reject` - explicitly not recommended, with reason.

Example intended outcomes:

```text
VDA 5050        adopt as an external robot interoperability contract
GS1 EPCIS       adopt as an external event/traceability mapping
Sparkplug       prototype
OR-Tools        prototype
PLC4X           prototype
Ocava           study and reproduce the architectural pattern
OpenWMS         study the domain model
OpenWES         study only
Nakadi          reject as a new dependency, retain as architecture history
```

These are hypotheses for future verification, not decisions made by this RFC.

## Capability-to-reference map

The future research pass should generate a reverse index from Spotwo capability or architecture concern to relevant references.

Example:

```text
Simulation
  Ocava

WMS/domain model
  OpenWMS
  ERPNext
  Odoo
  OpenBoxes

WES/orchestration
  OpenWES
  Walmart Concord

PLC connectivity
  Apache PLC4X

Industrial device state
  Sparkplug / Tahu
  Eclipse Ditto
  AWS IoT Device SDK

AMR command interoperability
  VDA 5050

Multi-fleet robotics
  Open-RMF
  openTCS
  MassRobotics AMR Interop

Supply-chain visibility events
  GS1 EPCIS

Optimization
  Google OR-Tools
  Timefold Solver
  jsprit

Geospatial
  Uber H3

Road routing
  OSRM
  Valhalla
```

This map should be generated from machine-readable records rather than maintained manually if practical.

## Freshness and validation

A future validator could enforce rules such as:

- every project has a valid upstream source;
- every material recommendation has at least one evidence reference;
- every adopted/prototyped dependency records a license;
- every adopted/prototyped project has a pinned tag, revision, or commit where possible;
- archived projects cannot be marked for direct adoption without an explicit exception;
- `last_verified_at` is present;
- stale records are surfaced after a configurable time window;
- referenced evidence exists;
- alternatives reference valid catalog IDs;
- capability mappings reference canonical capability IDs where available.

The validator should report stale information rather than silently treating old research as current.

## Possible follow-up labs

The enrichment pass may recommend labs, but should not automatically implement them. Candidate labs currently include:

1. Ocava-style deterministic warehouse simulation.
2. OpenWMS domain-model comparison.
3. VDA 5050 with a simulated AMR.
4. openTCS with VDA 5050.
5. Open-RMF heterogeneous multi-fleet orchestration.
6. Apache PLC4X with simulated or real Modbus/S7 endpoints.
7. MQTT + Sparkplug device-state semantics.
8. Spotwo domain events projected to GS1 EPCIS.
9. OR-Tools warehouse and transport optimization benchmarks.

## Future agent prompt

The following prompt is intentionally stored for possible later execution:

> Deeply audit the complete Spotwo open-source research catalog in `research/open-source/catalog.yml`. Treat the existing rankings and descriptions as hypotheses, not facts. For every project and specification, use current primary upstream sources to verify scope, maintenance status, latest meaningful release or specification revision, license, governance, implementation languages, maturity, known production adoption, integration model, architecture boundaries, risks, alternatives, and direct relevance to Spotwo. Pin important evidence to immutable commit hashes, tags, release revisions, or specification versions where possible. Create reusable evidence records for material claims and link them from the project assessments. Extend the machine-readable schema only where the new fields provide durable decision value. Add explicit `adopt`, `prototype`, `study`, `watch`, or `reject` dispositions with evidence-backed rationale. Map projects bidirectionally to Spotwo capabilities and architecture layers. Identify duplicates, overlapping tools, abandoned projects, licensing concerns, and places where an industry standard should be preferred over a library. Add freshness metadata and validators so stale or unsupported assessments are detectable by CI. Generate human-readable matrices from the canonical machine-readable data. Do not adopt dependencies or build the proposed labs during this task. Finish with a prioritized list of subsequent labs and architecture decisions, and run the repository validation before opening the PR.

## Non-goals

This RFC does not:

- adopt any dependency or standard;
- declare the example dispositions final;
- start the P0 labs;
- require all possible metadata to be stored permanently;
- duplicate upstream documentation inside this repository;
- turn secondary popularity metrics into architectural evidence.

## Trigger for running this RFC

Run this research pass when one of these becomes true:

- Spotwo is about to implement one of the cataloged architecture layers;
- a technology choice needs to be made between several catalog entries;
- the catalog becomes stale enough that its rankings are no longer trustworthy;
- we want to turn the research repository into an automatically maintained technology radar;
- we explicitly decide that evidence-backed open-source research is the next research milestone.
