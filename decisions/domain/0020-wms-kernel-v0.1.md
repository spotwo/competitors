# ADR 0020 - Spotwo WMS Kernel v0.1 Reference Contract

Status: Accepted

Date: 2026-08-18

## Context

The repository has moved beyond disconnected warehouse research. It now contains an evidence-backed domain model, executable PostgreSQL reference kernel, concurrency tests, Golden Warehouse Scenarios, and measured smoke/standard/scale benchmark evidence.

The next risk is architectural drift: continuing to add capabilities without stating which boundaries are now intentionally stable would turn validated decisions back into an implicit candidate model.

At the same time, this repository is not a production WMS service. Freezing a reference contract must not be confused with declaring production readiness, API compatibility, deployment architecture, operational SLOs, or a complete warehouse product.

## Decision

Freeze **Spotwo WMS Kernel v0.1.0** as an evidence-backed executable **reference contract**.

The machine-readable source of the freeze is `kernel/spec.yml`. This ADR explains its intent and compatibility boundary.

```text
Capability / Business Need
          |
          v
   Planning / Decision
          |
          +----> Availability
          +----> Reservation
          +----> Allocation
          |
          v
     WarehouseWork
          |
          v
     WarehouseTask
          |
          v
 Assignment -> Execution
                  |
          +-------+--------+
          |                |
      Exception      Confirmation Bridge
          |                |
        Retry        +------+---------+
                     |                |
              Inventory Posting   CountResult / other
                     |
                     v
            InventoryTransaction
                     |
              +------+-------+
              |              |
       InventoryPosition   durable history
```

## Stable v0.1 boundaries

### Inventory identity

- `InventoryPosition` is current operational stock state; `InventoryTransaction` is immutable accepted-change history.
- `InventoryKey` represents fungibility and required segregation, not every known stock attribute.
- `InventoryAnchor = Location XOR direct HandlingUnit`.
- Serial identity is represented by `SerialMembership`, not by adding serial to the generic Inventory Key.
- A whole-HU relocation changes HU placement without rewriting every contained InventoryPosition.

### Inventory commitments

```text
Availability != Reservation != Allocation != Assignment
```

- Reservation may commit coarse capacity at exact stock-dimension scope without selecting Location/HU/Position.
- Allocation binds demand to exact InventoryPosition source capacity.
- Accepted execution consumes commitment; it does not release executed quantity back into availability.

```text
Reservation quantity
  = allocated + consumed + released + remaining

Allocation quantity
  = consumed + released + remaining
```

### Holds and eligibility

```text
InventoryCondition != InventoryHold != HoldPolicy != ActionEligibility
```

Holds are independent restrictions. Policies decide which actions they block. Physical quantity remains distinct from action eligibility.

### Warehouse execution

```text
BusinessProcess != WarehouseWork != WarehouseTask
Allocation != WorkAssignment
TaskExecution != TaskConfirmation
TaskConfirmation != InventoryTransaction
TaskException != InventoryHold
```

- WarehouseWork is the generic execution aggregate, not the business document or capability itself.
- Assignment answers who owns the work; execution channel answers how the task is presented/executed.
- Execution attempts and exceptions preserve history rather than overwriting one task status.
- Generic Work does not own inventory semantics.
- Capability-owned confirmation bridges compose Work and Inventory consequences in one transaction where atomicity is required.

### Inventory movement and reconciliation

- `MovementIntent != InventoryMovement`.
- Internal movement conserves physical quantity unless an explicit boundary crossing/transformation says otherwise.
- `CountResult` records observed evidence and does not mutate authoritative stock until explicit reconciliation.
- Reconciliation against a stale inventory snapshot is rejected.

### Ledger and event boundary

```text
InventoryTransaction != DomainEvent != EPCISEvent
```

- Inventory transaction history is the internal durable inventory journal.
- Domain Events communicate committed facts to other modules/integrations.
- EPCIS is an external supply-chain visibility representation where appropriate.

The conceptual outbox decision is part of the v0.1 architecture. Full executable outbox/event publication across the Inventory/Work bridge remains a v0.2 implementation candidate.

## Executable proof boundary

The v0.1 reference contract is backed by the PostgreSQL 18.4 lab:

```text
001 schema / inventory identity
002 function search_path hardening
003 idempotency fingerprint
004 commitment engine
005 holds and eligibility
006 warehouse work engine
007 Golden Scenario support
```

At freeze time the functional suite contains at least **43 executable tests**.

The four required Golden Scenarios are:

1. Receive -> Putaway -> Reserve -> Allocate -> Pick -> Pack -> Ship.
2. Whole-HU relocation without rewriting contained InventoryPosition identity.
3. Short Pick -> Exception -> Retry -> residual commitment consumption.
4. Cycle Count -> observed variance -> explicit reconciliation.

These scenarios are canonical regression contracts in `scenarios/golden.yml`, not merely documentation examples.

## Performance evidence at freeze

Performance evidence is observational, not an SLO.

The preserved 2026-08-18 baselines prove that the reference model was exercised at:

```text
standard: 100,000 InventoryPositions / 32 workers
scale:  1,000,000 InventoryPositions / 100 workers
```

At both sizes:

- source selection remained index-backed by `inventory_positions_item_warehouse_idx`;
- concurrent hot allocation consumed exactly physical capacity without over-allocation;
- no allocation deadlocks were observed;
- opposite-direction movement produced no client/PostgreSQL deadlocks;
- physical quantity remained conserved.

The 100-worker hot-position profile also shows substantial serialization/queueing. This is explicitly an optimization signal for later work, not a correctness failure and not a production capacity statement.

Raw provenance-rich reports live under `labs/postgres-inventory-kernel/benchmarks/baselines/`.

## Compatibility meaning

`0.1.x` may add evidence, tests, documentation, diagnostics, or implementation hardening **without changing the stable semantic boundaries above**.

A change that intentionally breaks a stable v0.1 boundary requires one of:

1. a superseding ADR plus a new minor reference-contract version; or
2. an explicit correction showing that the previous boundary was internally inconsistent or factually invalid.

The version does **not** promise stable SQL table/function APIs to external consumers. The PostgreSQL lab is a reference implementation and falsification harness, not a public production API.

## Explicit non-goals

WMS Kernel v0.1 is not:

- a production WMS application;
- a frozen Rails/service/API architecture;
- a public API compatibility promise;
- a complete planning, optimization, labor, yard, transportation, billing, or ERP system;
- a performance SLO or throughput guarantee;
- a claim that every warehouse should use one physical hierarchy or workflow strategy.

## Deferred v0.2 candidates

- executable Inventory/Work outbox and event publication;
- FEFO and source-selection policy engine;
- wildcard and hierarchical reservation scopes;
- broad hold selectors and partial-quantity holds;
- parallel task dependency graphs and automatic dispatch;
- resource qualification and equipment eligibility;
- cross-warehouse transfer orchestration;
- lot genealogy;
- row-level tenant security policies;
- partitioning and archival;
- catch-weight dual quantities;
- owner versus entitled-party/account dimension refinement;
- extreme hot-position allocation contention optimization.

## Consequences

### Positive

- Product and implementation work can now target a named, testable semantic contract instead of a moving collection of ADRs.
- Agents can mechanically determine which boundaries are stable and which topics remain research candidates.
- Future architecture changes become explicit versioned decisions.
- Performance evidence is attached to the freeze without turning noisy hosted-runner timings into contractual SLOs.

### Cost

- Changes to stable boundaries now require deliberate compatibility/version reasoning.
- The machine manifest and its referenced proof artifacts must be kept valid.
- New production architecture must still make separate decisions about APIs, deployment, observability, security and operational scale.

## Verification

```bash
bash bin/check
bash bin/check-kernel-lab
```

`bin/check` validates `kernel/spec.yml`, its ADR/artifact references, required Golden Scenario IDs, baseline evidence, and the minimum executable-test floor.
