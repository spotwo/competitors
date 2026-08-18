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
7. **Capability is not implementation** - receiving, replenishment, picking, etc. are canonical capabilities; wave, batch, min/max, GTP, voice, and similar choices are strategies or implementation patterns.
8. **Workflow is deeper than capability** - a workflow decomposes a capability into stages, objects, states, strategies, execution channels, exceptions, and vendor mappings.
9. **Process state is not inventory state** - packed, loaded, receiving, held, reserved, allocated, available, and physically present are different concerns.
10. **Planning is not execution** - business need and policy create Warehouse Work; inventory changes only from explicit confirmed transactions.
11. **Time matters** - every researched fact has a verification or observation date.
12. **Confidence is explicit** - official documentation is stronger evidence than reviews or forum posts.
13. **Unknown beats invented** - unresolved facts belong in `research_gaps`.

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

A capability answers **what operational outcome exists**. A workflow answers **how that capability becomes executable work**. Strategies, policies and channels answer **which execution model is used**. Exceptions describe **how normal execution diverges without destroying the underlying business semantics**.

## Repository map

```text
companies/       Vendor and product records
capabilities/    Canonical warehouse capabilities and vendor observations
workflows/       Deep capability decompositions
claims/          Reusable factual assertions linked to evidence
taxonomy/        Controlled vocabularies
terminology/     Industry terms, aliases, usage, and Spotwo naming decisions
authorities/     Standards, regulations, associations, and industry guides
evidence/        Source records supporting claims and domain models
ontology/        Facility, location, inventory, handling-unit, process, and movement models
kpis/            Warehouse/DC KPI registry
integrations/    Integration and interoperability patterns
decisions/       Spotwo ADR-style product/domain/UX decisions
matrices/        Generated comparison, stats, workflow, domain, and research views
schema/          JSON Schema definitions
templates/       Research and record templates
scripts/         Validation, query, and generation tools
.github/         CI workflows and contribution templates
```

## Current WMS kernel stance

The current model is intentionally a **candidate kernel**, not a frozen implementation specification.

```text
Demand / Supply / Operational Need
             |
             v
     Decision / Policy Layer
             |
      +------+-------+
      |              |
      v              v
 Reservation?    Allocation
      |              |
      +------+-------+
             |
             v
        Warehouse Work
             |
             v
        Warehouse Task
             |
             v
     Execution / Exception
             |
             v
        Confirmation
             |
             v
   Inventory Movement / Adjustment
```

### Location

The repository intentionally does **not** treat `ROW -> RACK -> LEVEL -> BIN` as a universal warehouse standard. `Location` is the canonical addressable place; rack, aisle, bay/stack and level may be physical structures or coordinate dimensions. See `decisions/domain/0003-location-model.md`.

### Handling units

Handling-unit identity is separated from standardized supply-chain identity. A Handling Unit may have an LPN and may have an SSCC, but those identifiers are not synonyms. See `decisions/domain/0004-handling-unit-identifiers.md`.

### Inventory state

Inventory is modeled through orthogonal axes rather than one overloaded `inventory_status`:

```text
Stock
  +-- Physical Quantity
  +-- Reserved Quantity
  +-- Allocated Quantity
  +-- Available Quantity / Availability View
  +-- Inventory Condition
  +-- Allocation Eligibility
  +-- Inventory Hold
  +-- Owner / Lot / Serial / Location / Handling Unit
```

Availability is a derived projection. Reservation is a candidate coarse commitment to demand. Allocation is an execution-relevant source binding. See `decisions/domain/0007-inventory-state-axes.md` and `decisions/domain/0009-inventory-commitment-model.md`.

### Warehouse Work

Repeated vendor patterns support a proposed shared execution layer:

```text
Business Need
  -> Planning / Decision
  -> Warehouse Work
  -> Warehouse Task
  -> Assignment / Claim
  -> Execution
  -> Confirmation / Exception
```

Picking, Putaway, Replenishment, Relocation and Cycle Counting can reuse execution infrastructure without losing their capability-specific semantics. See `decisions/domain/0008-warehouse-work-abstraction.md`.

### Inventory Movement

Putaway, Internal Replenishment, Cross-docking and Relocation all perform inventory-context changes but have different planning reasons.

```text
Movement Intent
      |
      v
Warehouse Work / Task
      |
      v
Inventory Movement
      |
      v
Movement Confirmation
```

`Inventory Movement` is one confirmed source-to-destination inventory-context change. `Inventory Transfer` is a business intent and may orchestrate multiple movements and workflows. A tracked inter-warehouse transfer may be:

```text
Transfer Order
  -> Pick
  -> Ship
  -> In Transit
  -> Receive
  -> Putaway
```

See `decisions/domain/0010-inventory-movement-model.md`.

## Deep workflow coverage

The repository currently contains 12 deep workflow decompositions:

```text
Inbound
  Receiving
  Cross-docking
  Putaway

Inventory / storage
  Internal Replenishment
  Internal Relocation
  Cycle Counting
  Physical Inventory

Fulfillment / outbound
  Inventory Allocation and Commitment
  Picking
  Packing
  Shipping

Reverse
  Customer Returns
```

Each workflow records stages, canonical objects, candidate states, strategies, exceptions, vendor mappings, commands/events and explicit research gaps.

### Important semantic boundaries

```text
Arrival != Receipt != Putaway

Reservation != Allocation != Task Assignment

Physical Quantity != Available Quantity

Return Reason != Return Disposition != Commercial Outcome

Count Result != Accepted Count != Inventory Difference != Adjustment

Picked != Packed != Staged != Loaded != Shipped

Relocation != Transfer

Movement Intent != Inventory Movement Confirmation
```

### Picking

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

The candidate model uses `Pick Work Group -> Pick Task` instead of adopting one vendor vocabulary. See `decisions/domain/0005-picking-work-model.md`.

### Internal replenishment

```text
Trigger Evaluation
  -> Quantity Calculation
  -> Destination Selection
  -> Source Selection
  -> Work Creation
  -> Release / Assignment
  -> Pick / Move / Put
  -> Completion / Recalculation
```

Internal warehouse replenishment is separated from procurement, production supply and inter-warehouse resupply. See `decisions/domain/0006-internal-replenishment-scope.md`.

### Allocation and commitment

```text
Demand Eligible
  -> Availability Evaluation
  -> Reservation? 
  -> Source Resolution
  -> Allocation Creation
  -> Execution Handoff
  -> Commitment Resolution
```

Reservation is optional in the candidate model. Exact source identity may be deferred until execution when policy permits. See `decisions/domain/0009-inventory-commitment-model.md`.

### Cross-docking

```text
Inbound Eligibility
  -> Demand Match
  -> Cross-Dock Allocation
  -> Work Creation
  -> Move to Outbound Flow
  -> Outbound Handoff
  -> Residual / Fallback Putaway
```

Cross-docking is a demand-linked allocation and routing decision, not merely a special storage location.

### Returns

```text
Return Arrival
  -> Identification
  -> Authorization / Blind Return Resolution
  -> Return Receipt
  -> Inspection and Disposition
  -> Disposition Work
  -> Execution
  -> Return Resolution
```

Receiving a returned item does not automatically make it allocatable.

### Physical inventory

```text
Scope Definition
  -> Inventory Document
  -> Count Release
  -> Count Execution
  -> Recount / Review
  -> Difference Analysis
  -> Adjustment Posting
  -> Reconciliation / Close
```

Physical Inventory is broader audit/reconciliation scope than routine Cycle Counting.

## Querying the knowledge base

```bash
# Competitors
python scripts/kb.py vendors --segment smb --market europe --layer wms
python scripts/kb.py vendors --country UA
python scripts/kb.py vendors --capability picking

# Capabilities
python scripts/kb.py capability
python scripts/kb.py capability allocation
python scripts/kb.py capability cross-docking --vendor oracle
python scripts/kb.py capability --group inventory

# Deep workflows
python scripts/kb.py workflow
python scripts/kb.py workflow allocation
python scripts/kb.py workflow cross-docking
python scripts/kb.py workflow receiving
python scripts/kb.py workflow putaway
python scripts/kb.py workflow replenishment
python scripts/kb.py workflow relocation --vendor microsoft
python scripts/kb.py workflow picking --strategy cluster
python scripts/kb.py workflow packing
python scripts/kb.py workflow shipping
python scripts/kb.py workflow returns
python scripts/kb.py workflow cycle-counting
python scripts/kb.py workflow physical-inventory --vendor sap
python scripts/kb.py workflow allocation --json

# Domain ontology
python scripts/kb.py ontology
python scripts/kb.py ontology inventory
python scripts/kb.py ontology reservation
python scripts/kb.py ontology allocation
python scripts/kb.py ontology availability
python scripts/kb.py ontology inventory-movement
python scripts/kb.py ontology handling-unit --json

# Industry language
python scripts/kb.py term "clear height"
python scripts/kb.py term bin

# KPIs / integrations / evidence
python scripts/kb.py kpis --category inbound
python scripts/kb.py integrations --protocol OPC
python scripts/kb.py evidence --subject inventory-commitment-model
python scripts/kb.py evidence --subject inventory-movement-model
python scripts/kb.py evidence --publisher GS1

# Research backlog and live counts
python scripts/kb.py gaps
python scripts/kb.py stats
```

Add `--json` when an agent, MCP server, CI job or another tool needs machine-readable output.

`workflows/*.yml` is canonical. `matrices/workflows.md` is a generated compact comparison view. Detailed workflow data should be queried from YAML/CLI rather than duplicated into large generated Markdown files.

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

Canonical records are validated in CI against JSON Schema, controlled taxonomies, evidence references, company/product relationships, workflow references, nested workflow IDs, ontology parent/relationship references, and generated-view freshness.

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
