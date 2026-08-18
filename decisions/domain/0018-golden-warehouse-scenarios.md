# ADR 0018 - Golden Warehouse Scenarios

Status: Proposed

Date: 2026-08-18

## Context

The candidate WMS kernel now has independently executable models for Inventory Position and Inventory Key, Inventory Transactions, Reservation and Allocation, Holds and Action Eligibility, and Warehouse Work / Task execution.

Those layers can each pass their own tests while still composing incorrectly. Typical integration failures include:

- Reservation becoming available again after a successful Pick;
- Allocation remaining on a source Position after physical stock moved away;
- Picked stock becoming eligible for another order before shipment;
- Work completion being mistaken for an inventory mutation;
- cycle-count evidence immediately overwriting authoritative stock;
- task retries losing the history of a partial physical outcome;
- one engine updating state while another engine fails in the same business action.

A WMS therefore needs a small set of stable end-to-end scenarios that act as executable architecture contracts.

## Decision

Introduce **Golden Warehouse Scenarios** as first-class machine-readable and executable specifications.

Canonical scenario descriptions live under `scenarios/`. PostgreSQL integration tests implement the same named scenarios and verify checkpoints after meaningful domain transitions.

A Golden Scenario is not a UI test and is not a vendor-specific workflow copy. It proves that Spotwo kernel boundaries compose correctly.

```text
Golden Scenario
  -> Preconditions
  -> Domain Actions
  -> Checkpoints
  -> Cross-layer Invariants
  -> Executable Test
```

## Initial Golden Scenarios

### 1. Receive to Ship Happy Path

```text
Receipt 100
   -> Putaway Work
   -> Reservation 20
   -> Allocation 20
   -> Pick Work
        -> consume Allocation
        -> move physical stock
        -> protect staging stock from new Allocation
   -> Packing Work
   -> Shipping Work
        -> Issue 20
```

The scenario proves that converting Reservation to Allocation does not double-subtract availability, Pick consumes rather than releases commitment, Packing does not mutate stock, and Shipping crosses the warehouse boundary through an Issue posting.

### 2. Whole Handling Unit Relocation

```text
HU at Location A
  contains Position 40 EA
       |
       v
Relocation Work
       |
       v
HU placement A -> B
```

The contained InventoryPosition keeps the same direct HU anchor and is not rewritten merely because the HU changed location.

### 3. Short Pick Recovery

```text
Reservation 10
  -> Allocation 10
  -> Pick 6
  -> SHORT_PICK Exception
  -> resolve / retry
  -> Pick remaining 4
```

The already consumed 6 units remain consumed throughout exception handling. Retrying execution must not reopen consumed Reservation capacity.

### 4. Cycle Count Reconciliation

```text
System = 100
CountResult = 97
Variance = -3

physical remains 100
        |
        v
explicit reconciliation
        |
        v
physical = 97
```

Count evidence and inventory mutation remain separate.

## Commitment consumption

Golden Scenarios expose a missing lifecycle state in the earlier Reservation / Allocation model.

Previously:

```text
Reservation quantity
  = allocated + released + remaining
```

That is insufficient after successful execution because fulfilled quantity is neither still allocated nor released back to free capacity.

Adopt the candidate identity:

```text
Reservation quantity
  = allocated + consumed + released + remaining

Allocation quantity
  = consumed + released + remaining
```

`consumed` means the commitment was used by accepted warehouse execution. It must never become free merely because a Task later retries or closes.

## Pick confirmation boundary

A Pick is both execution and an inventory-affecting domain action.

The candidate atomic boundary is:

```text
ONE DATABASE TRANSACTION
  |
  +-- consume Allocation quantity
  +-- decrement source physical quantity
  +-- increment destination physical quantity
  +-- append balanced InventoryTransaction legs
  +-- mark picked staging stock ineligible for new Allocation
  +-- record TaskConfirmation
```

The generic Work Engine does not own those inventory semantics. A capability-owned confirmation bridge composes the Inventory and Work APIs atomically.

TaskConfirmation references the capability result; it does not become the ledger itself.

## Staging eligibility boundary

The current kernel does not yet model generalized location/process availability policy. For the first Golden Scenario, picked staging stock receives an allocation-blocking position Hold that still allows shipping.

This is deliberately temporary architecture:

```text
picked stock in staging
  -> allocation blocked
  -> shipping allowed
```

A later location/process eligibility model may replace that Hold without changing the Golden Scenario invariant: picked stock cannot be allocated to unrelated demand.

## Receipt and Issue

Golden Scenarios require warehouse boundary postings in addition to internal movement.

- Receipt increases physical quantity at a Position and appends a positive physical ledger leg.
- Issue decreases physical quantity at a Position and appends a negative physical ledger leg.
- Issue must not strand exact Reservation/Allocation commitments and must respect shipping eligibility.

Neither operation is a Warehouse Task by itself. A capability Work may confirm against the accepted posting.

## Count Result and reconciliation

Introduce a candidate `InventoryCountResult` record that captures:

```text
position
system_quantity_snapshot
counted_quantity
variance
state = observed | reconciled
```

Recording the result does **not** update InventoryPosition.

Reconciliation:

1. locks the CountResult and Position;
2. rejects a stale result if Position physical quantity changed after the snapshot;
3. posts the variance as a `count_reconciliation` InventoryTransaction;
4. updates Position physical quantity;
5. marks the CountResult reconciled.

## Atomicity

A Golden Scenario distinguishes separate business actions from atomic cross-layer confirmations.

For example, Reservation creation and Pick execution are separate transactions. But once the operator confirms an executed Pick, commitment consumption, inventory movement and TaskConfirmation must commit or roll back together.

```text
operator confirms Pick
         |
         v
 capability confirmation handler
         |
         +-- Inventory kernel
         +-- Work kernel
         |
       COMMIT
```

## Checkpoints

Each Golden Scenario records explicit checkpoints such as:

```text
physical
reserved
allocated
consumed
available
Work state
Task state
ledger transaction type/delta
CountResult state
HU placement
```

Tests should assert domain quantities and relationships, not fragile row counts unless row count itself is an invariant.

## Rules

1. Golden Scenarios use public candidate kernel functions rather than direct state mutation when an API exists.
2. A successful Pick consumes commitment; it does not release fulfilled quantity.
3. Physical movement remains balanced inside one warehouse.
4. Receipt and Issue are explicit warehouse-boundary postings.
5. Work completion never implies inventory mutation unless a capability confirmation bridge posts the domain result.
6. TaskConfirmation and InventoryTransaction remain separate records.
7. A partial physical outcome survives Task Exception and retry history.
8. CountResult is evidence and does not mutate stock until explicit reconciliation.
9. Whole-HU relocation changes HU placement without rewriting contained Position identity.
10. Scenario definitions and executable test names are validated by CI.

## Consequences

- Kernel layers are tested as a composed system rather than only independently.
- Missing lifecycle concepts become visible earlier.
- Future implementation repositories can reuse these scenarios as acceptance tests.
- Refactoring internal tables remains possible if checkpoint semantics remain stable.
- A scenario failure communicates an architectural regression more clearly than a low-level unit test alone.

## Deferred

- generalized Location / Zone allocation eligibility;
- typed multi-result confirmation envelopes;
- cross-warehouse Transfer golden scenario;
- Returns and Cross-docking golden scenarios;
- lot genealogy and serial end-to-end flows;
- event/outbox assertions for every Golden Scenario;
- performance SLOs for Golden Scenario execution.

## Source of truth

Machine-readable scenarios:

- `scenarios/golden.yml`

Executable implementation:

- `labs/postgres-inventory-kernel/sql/007_golden_scenario_support.sql`
- `labs/postgres-inventory-kernel/tests/test_golden_scenarios.py`
