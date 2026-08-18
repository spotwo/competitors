# Spotwo Competitors

Spotwo's local source of truth for intralogistics market and domain knowledge.

This repository records competitors, products, capabilities, workflow decompositions, terminology, standards, market conventions, domain ontologies, KPIs, integration patterns, claims, and the evidence behind them. Its purpose is not merely to track competitors, but to help Spotwo make consistent product, domain, UX, architecture, integration, and engineering decisions based on established industry practice.

> Directory tree represents identity. Metadata represents classification.

## Core principles

1. **Evidence before claims** - material facts should point to captured evidence.
2. **Fact is not evidence** - reusable facts live as claims; sources live as evidence records.
3. **Machine-readable first** - YAML/JSON is canonical; Markdown explains context and decisions.
4. **One entity, many facets** - geography, segment, industry, solution layer, and deployment model are metadata, not directory nesting.
5. **Terminology is a product asset** - record standards, dominant market usage, aliases, and Spotwo decisions.
6. **Domain model is evidence-driven** - vendor structures are observations, not automatic Spotwo architecture.
7. **Capability is not implementation** - receiving, replenishment, picking, etc. are canonical capabilities; wave, batch, min/max, GTP, voice, and similar choices are strategies or implementation patterns where appropriate.
8. **Workflow is deeper than capability** - a workflow decomposes a capability into stages, objects, states, strategies, assignment rules, execution channels and exceptions.
9. **Time matters** - every researched fact has a verification or observation date.
10. **Confidence is explicit** - official documentation is stronger evidence than reviews or forum posts.
11. **Unknown beats invented** - unresolved facts belong in `research_gaps`.

## Knowledge model

```text
Company / Product / Term / Authority
              |
              v
            Claim
              |
              v
           Evidence

Evidence + Claims + Standards
              |
              v
 Capability / Ontology / KPI / Pattern
              |
              v
          Workflow
              |
              v
        Spotwo Decision
```

A capability answers **what operational outcome exists**. A workflow answers **how that capability becomes executable work**. Strategies and channels answer **which execution model is used**.

## Repository map

```text
companies/       Vendor and product records
capabilities/    Canonical warehouse capabilities, strategies and vendor observations
workflows/       Deep capability decompositions: stages, objects, states, vendor mappings and exceptions
claims/          Reusable factual assertions linked to evidence
taxonomy/        Controlled vocabularies
terminology/     Industry terms, aliases, usage, and Spotwo naming decisions
authorities/     Standards, regulations, associations, and industry guides
evidence/        Source records supporting claims and domain models
ontology/        Facility, location, inventory, handling-unit and process models
kpis/            Warehouse/DC KPI registry
integrations/    Integration and interoperability patterns
decisions/       Spotwo ADR-style product/domain/UX decisions
matrices/        Generated comparison, stats, domain, workflow and research views
schema/          JSON Schema definitions
templates/       Research and record templates
scripts/         Validation, query, and generation tools
.github/         CI workflows and contribution templates
```

## Current domain stance

The repository intentionally does **not** treat `ROW -> RACK -> LEVEL -> BIN` as a universal warehouse standard. Current evidence supports a more flexible model where `Location` is the canonical addressable place, while rack, aisle, bay/stack and level are optional physical or coordinate dimensions. See `decisions/domain/0003-location-model.md`.

Handling-unit identity is also separated from standardized supply-chain identity. An internal Handling Unit may have an LPN and may have an SSCC, but those identifiers are not treated as synonyms. See `decisions/domain/0004-handling-unit-identifiers.md`.

The process taxonomy currently has one canonical capability record for every defined warehouse process. Capability records distinguish the business capability from strategies such as batch/cluster/zone picking, min-max/order-based replenishment, wave versus waveless release, or human versus automated execution.

The first deep workflow is `Picking`. Its candidate Spotwo spine is:

```text
Demand Ready
  -> Release / Orchestration
  -> Pick Work Creation
  -> Assignment
  -> Sequence / Route
  -> Travel and Pick
  -> Exception Resolution
  -> Handoff
```

The canonical model uses `Pick Work Group -> Pick Task` instead of adopting a single vendor vocabulary. SAP `Warehouse Order -> Warehouse Task`, Dynamics `Work -> Work Line`, Oracle `Task -> Allocation`, NetSuite `Wave -> Pick Task`, and Infor `Assignment -> Pick Task` remain explicit mappings.

## Querying the knowledge base

```bash
# Competitor questions
python scripts/kb.py vendors --segment smb --market europe --layer wms
python scripts/kb.py vendors --country UA
python scripts/kb.py vendors --layer wes --deployment cloud
python scripts/kb.py vendors --capability picking

# Capability graph
python scripts/kb.py capability
python scripts/kb.py capability replenishment
python scripts/kb.py capability picking --vendor blue-yonder
python scripts/kb.py capability --group inbound
python scripts/kb.py capability automation --json

# Deep workflow decomposition
python scripts/kb.py workflow
python scripts/kb.py workflow picking
python scripts/kb.py workflow picking --vendor sap
python scripts/kb.py workflow picking --vendor oracle
python scripts/kb.py workflow picking --strategy cluster
python scripts/kb.py workflow picking --json

# Industry language
python scripts/kb.py term "clear height"
python scripts/kb.py term bin

# Domain ontology
python scripts/kb.py ontology
python scripts/kb.py ontology location
python scripts/kb.py ontology handling-unit --json

# Warehouse KPIs
python scripts/kb.py kpis
python scripts/kb.py kpis --category inbound
python scripts/kb.py kpis --name picking

# Integration patterns
python scripts/kb.py integrations
python scripts/kb.py integrations --category event
python scripts/kb.py integrations --protocol OPC

# Claims and evidence
python scripts/kb.py claims --subject clear-height
python scripts/kb.py evidence --subject picking-workflow
python scripts/kb.py evidence --publisher GS1

# Research backlog and live counts
python scripts/kb.py gaps
python scripts/kb.py stats
```

Add `--json` when an agent, MCP server, CI job, or another tool needs machine-readable output.

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

Canonical records are validated in CI against JSON Schema, controlled taxonomies, evidence references, parent relationships, claim subjects, capability/vendor/product relationships, workflow/company/capability relationships, nested workflow IDs, and generated-view freshness.

```bash
python -m pip install -r requirements.txt
python scripts/validate.py
python scripts/generate_matrices.py --check
python scripts/generate_audit.py --check
python scripts/generate_domain_views.py --check
python scripts/generate_workflow_views.py --check
```

Refresh generated views after editing canonical records:

```bash
python scripts/generate_matrices.py
python scripts/generate_audit.py
python scripts/generate_domain_views.py
python scripts/generate_workflow_views.py
```
