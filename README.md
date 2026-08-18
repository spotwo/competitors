# Spotwo Competitors

Spotwo's local source of truth for intralogistics market, domain and WMS-kernel knowledge.

This repository records competitors, products, capabilities, workflow decompositions, terminology, standards, market conventions, domain ontologies, inventory position/key semantics, inventory ledger semantics, domain events, KPIs, integration patterns, claims, and the evidence behind them. Its purpose is not merely to track competitors, but to help Spotwo make consistent product, domain, UX, architecture, integration, and engineering decisions based on established industry practice.

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
10. **Planning is not execution** - business need and policy create Warehouse Work; inventory changes only from explicit accepted transactions.
11. **Transaction is not event** - the inventory ledger is durable internal history; Domain Events communicate committed facts; EPCIS is an external visibility model.
12. **Current state is not history** - Inventory Position is optimized current state; posted Inventory Transactions are immutable history.
13. **Inventory identity means fungibility** - Inventory Key contains only dimensions that make quantities operationally non-equivalent; mutable quantities, commitments, holds, UOM and workflow state are not stock identity.
14. **Time matters** - every researched fact has a verification or observation date.
15. **Confidence is explicit** - official documentation is stronger evidence than reviews or forum posts.
16. **Unknown beats invented** - unresolved facts belong in `research_gaps`.

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

Inventory-specific kernel

Command
  -> Inventory Posting Service
       -> Inventory Transaction Ledger
       -> Inventory Position / Inventory Key
       -> Domain Event Outbox
```

A capability answers **what operational outcome exists**. A workflow answers **how that capability becomes executable work**. The inventory kernel answers **how confirmed inventory changes are identified, validated, posted, read, audited and published**.

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
ontology/        Facility, location, inventory, handling-unit, process, movement and transaction models
ledger/          Candidate inventory transaction/posting model and invariants
events/          Candidate domain-event registry, envelope and external mappings
kpis/            Warehouse/DC KPI registry
integrations/    Integration and interoperability patterns
decisions/       Spotwo ADR-style product/domain/UX/architecture decisions
matrices/        Generated comparison, stats, workflow, domain, ledger, event and research views
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
    Inventory Posting Service
      |         |          |
      v         v          v
   Ledger    Position    Outbox
                |
                v
          Inventory Key
```

### Location

The repository intentionally does **not** treat `ROW -> RACK -> LEVEL -> BIN` as a universal warehouse standard. `Location` is the canonical addressable place; rack, aisle, bay/stack and level may be physical structures or coordinate dimensions. See `decisions/domain/0003-location-model.md`.

### Handling units

Handling-unit identity is separated from standardized supply-chain identity. A Handling Unit may have an LPN and may have an SSCC, but those identifiers are not synonyms. HUs can be nested through a direct-parent containment tree. Stock inside a HU references its immediate containing HU rather than copying all HU ancestors into its Inventory Key. See `decisions/domain/0004-handling-unit-identifiers.md` and `decisions/domain/0014-inventory-key-stock-dimensions.md`.

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
  +-- Inventory Key dimensions
  +-- Serial Membership when exact tracking applies
```

Availability is a derived projection. Reservation is a candidate coarse commitment to demand. Allocation is an execution-relevant source binding. See `decisions/domain/0007-inventory-state-axes.md` and `decisions/domain/0009-inventory-commitment-model.md`.

### Inventory Key and Stock Dimensions

Inventory identity represents **fungibility and required segregation**, not every known stock attribute.

Candidate semantic key:

```text
InventoryKey
  warehouse_id
  anchor = Location XOR direct HandlingUnit
  item_id
  owner_id
  inventory_condition_id
  lot_id?
  stock_scope_id?
  attribute_set_id?
  stock_segment_id?
```

`tenant_id` participates in the physical database uniqueness boundary but is a namespace rather than a stock dimension.

Deliberately excluded from the key:

```text
serial number        -> Serial Membership
reserved/allocated   -> commitments
available            -> derived projection
hold/lock            -> restriction
UOM                   -> quantity normalized to inventory/base UOM
workflow state        -> process concern
parent HU             -> containment tree
resolved HU location  -> containment projection
```

A full-HU move updates HU placement and does not rewrite every contained stock row. Repacking part of an HU changes the direct anchor and therefore posts a normal inventory split/movement.

The candidate PostgreSQL model uses a UUIDv7 surrogate position ID plus a `UNIQUE NULLS NOT DISTINCT` semantic key and row/version locking at the affected position granularity. See `decisions/domain/0014-inventory-key-stock-dimensions.md`.

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

## Inventory transaction kernel

### Position + ledger + projection

Spotwo currently proposes a **hybrid synchronous ledger + materialized position** model rather than full event sourcing.

```text
Command
  |
  v
Inventory Posting Service
  |
  +-- validate invariants
  +-- deduplicate command
  +-- lock/version affected positions
  |
  v
ONE DATABASE TRANSACTION
  |
  +-- append InventoryTransaction + Legs
  +-- update InventoryPosition rows
  +-- append Domain Event Outbox rows
  |
 COMMIT
```

Roles are deliberately distinct:

- `InventoryPosition` - low-latency current operational balance for one semantic Inventory Key.
- `InventoryKey` - identity/fungibility dimensions for the position.
- `InventoryTransaction` - immutable audit/rebuild journal.
- `AvailabilityView` - disposable derived projection for a specific action and scope.
- `DomainEvent` - committed fact for other modules/integrations.
- `EPCIS Event` - standards-based supply-chain visibility representation when applicable.

See `decisions/domain/0011-inventory-state-ledger-projection.md` and `decisions/domain/0014-inventory-key-stock-dimensions.md`.

### Balanced quantity postings

Every accepted inventory change is represented as a typed transaction containing one or more signed legs. This is **double-entry-inspired quantity accounting**, not financial double-entry bookkeeping.

```text
Internal movement

Source Position       -10 EA
Destination Position  +10 EA
                       -----
Net physical delta       0 EA
```

```text
Condition transformation

Available stock       -10 EA
Quarantine stock      +10 EA
                       -----
Net physical delta       0 EA
```

Receipts and issues cross the modeled warehouse boundary. Adjustments are explicitly non-conserving and require reason/authority. Reservation and allocation postings change commitments, not physical quantity.

Posted transactions are immutable. Corrections use compensating transactions rather than rewriting history. See `decisions/domain/0012-balanced-inventory-postings.md`.

### Candidate inventory transaction types

```text
Physical
  receipt
  issue
  movement

Transformation
  posting-change

Commitment
  reservation
  reservation-release
  allocation
  allocation-release

Reconciliation
  adjustment-in
  adjustment-out
  count-reconciliation

Correction
  reversal
```

Canonical detail lives in `ledger/inventory.yml`; `matrices/ledger.md` is the generated review view.

### Domain events and outbox

The event model intentionally separates three layers:

```text
InventoryTransaction
  = internal durable inventory journal

DomainEvent
  = committed fact for Spotwo modules/integrations

EPCIS Event
  = standards-based supply-chain visibility representation
```

Domain Events are persisted to an outbox atomically with ledger/position changes and published at-least-once after commit. Failed publication uses bounded exponential backoff with deterministic jitter; events that exhaust their attempt budget enter PostgreSQL quarantine until an audited operator replay. Read-only telemetry classifies backlog as ready, delayed, leased, or quarantined and exports bounded JSON/Prometheus health signals. Consumers deduplicate by stable `event_id`; ordering is local to aggregates/streams where versioning exists, not global.

The executable consumer commits its Inbox receipt and local handler effect before broker ACK. The concrete Position quantity projection applies monotonic aggregate versions, durably buffers gaps, supports non-destructive bootstrap from a verified exact-version snapshot, and exposes aggregate gap health without event or Position identity labels.

The candidate envelope includes:

```text
event_id
type
source
subject
occurred_at
recorded_at
aggregate_type
aggregate_id
aggregate_version
causation_id?
correlation_id?
actor?
warehouse_id?
schema_version
data
```

CloudEvents may provide a transport envelope. GS1 EPCIS/CBV may represent suitable external visibility facts without becoming Spotwo's internal inventory ledger. See `decisions/domain/0013-inventory-domain-events.md`.

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

Inventory Position != Inventory Transaction

Inventory Key != Serial Membership

Inventory Anchor = Location XOR direct Handling Unit

Full HU Move != rewriting contained Inventory Positions

Inventory Transaction != Domain Event != EPCIS Event

Movement Intent != Inventory Movement Confirmation

Return Reason != Return Disposition != Commercial Outcome

Count Result != Accepted Count != Inventory Difference != Adjustment

Picked != Packed != Staged != Loaded != Shipped

Relocation != Transfer
```

## Querying the knowledge base

```bash
# Competitors
python scripts/kb.py vendors --segment smb --market europe --layer wms
python scripts/kb.py vendors --country UA
python scripts/kb.py vendors --capability picking

# Capabilities / workflows
python scripts/kb.py capability allocation
python scripts/kb.py capability cross-docking --vendor oracle
python scripts/kb.py workflow allocation
python scripts/kb.py workflow cross-docking
python scripts/kb.py workflow relocation --vendor microsoft
python scripts/kb.py workflow picking --strategy cluster
python scripts/kb.py workflow returns
python scripts/kb.py workflow physical-inventory --vendor sap

# Domain ontology
python scripts/kb.py ontology inventory
python scripts/kb.py ontology inventory-key
python scripts/kb.py ontology inventory-anchor
python scripts/kb.py ontology serial-membership
python scripts/kb.py ontology inventory-movement
python scripts/kb.py ontology inventory-transaction

# Inventory kernel
python scripts/kernel.py ledger
python scripts/kernel.py ledger movement
python scripts/kernel.py ledger adjustment --json
python scripts/kernel.py invariants
python scripts/kernel.py events
python scripts/kernel.py events movement
python scripts/kernel.py envelope

# Industry language / evidence
python scripts/kb.py term reservation
python scripts/kb.py term "inventory movement"
python scripts/kb.py evidence --subject inventory-key
python scripts/kb.py evidence --subject handling-unit
python scripts/kb.py evidence --publisher GS1

# Research backlog and live counts
python scripts/kb.py gaps
python scripts/kb.py stats
```

Add `--json` when an agent, MCP server, CI job or another tool needs machine-readable output.

## Evidence and standards stance

Use this as a terminology and architecture decision aid, not an automatic ranking:

1. Applicable regulation or law
2. Formal standard
3. Industry association / recognized guide
4. Dominant industry usage
5. Major competitor usage
6. Internal terminology

When the market term differs from a formal standard, record both and create a decision under `decisions/terminology/` or `decisions/domain/`.

Evidence grades:

| Grade | Source type |
|---|---|
| A | Official documentation / standard / regulation |
| B | Official vendor presentation, product page, demo, or release note |
| C | Official customer case study |
| D | Analyst or reputable industry publication |
| E | Reseller / integrator material |
| F | Review, forum, social post, or unverified secondary source |

## Validation

Canonical records are validated in CI against JSON Schema, controlled taxonomies, evidence references, company/product relationships, workflow references, nested workflow IDs, ontology parent/relationship references, ledger/event nested IDs and evidence, duplicate semantic event types, and generated-view freshness.

Under `AGENTS.md`, agent development happens on task branches. Pull-request CI provides independent verification, merge-group/pre-merge checks protect `main`, and `push: main` is only the post-merge integrity safety net.

```bash
python -m pip install -r requirements.txt
bash bin/check
```

Equivalent explicit checks:

```bash
python scripts/validate.py
python scripts/validate_kernel_semantics.py
python scripts/generate_matrices.py --check
python scripts/generate_audit.py --check
python scripts/generate_domain_views.py --check
python scripts/generate_workflow_views.py --check
python scripts/generate_kernel_views.py --check
```

Refresh generated views after editing canonical records:

```bash
python scripts/generate_matrices.py
python scripts/generate_audit.py
python scripts/generate_domain_views.py
python scripts/generate_workflow_views.py
python scripts/generate_kernel_views.py
```
