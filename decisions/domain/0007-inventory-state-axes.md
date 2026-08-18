# ADR 0007 - Inventory State Axes

Status: Proposed

Date: 2026-08-18

## Context

A single `inventory_status` field is too overloaded for warehouse inventory.

Observed WMS models distinguish at least these concerns:

- physical quantity currently recorded in the warehouse;
- quantity already allocated to existing demand;
- quantity available for new allocation;
- quality or business condition such as damaged or quarantine;
- operational holds or lock codes;
- process position such as received, located, packed or loaded.

Oracle WMS explicitly reports current, allocated and available quantities separately and defines allocatable versus unallocatable inventory. Infor cycle-count logic separately evaluates allocated inventory and HOLD restrictions.

## Proposed decision

Model inventory through orthogonal dimensions instead of one combinatorial state enum.

```text
Stock
 |
 +-- PhysicalQuantity
 +-- AllocatedQuantity
 +-- AvailableQuantity   <- derived by policy
 |
 +-- InventoryCondition  normal / damaged / quarantine / inspection
 +-- AllocationEligibility allocatable / unallocatable
 +-- InventoryHold       optional operational restrictions
 |
 +-- Location / HandlingUnit / Lot / Serial / Owner
```

Process state belongs primarily to work, containers and shipment/receipt objects rather than being copied into one global stock-status field.

## Rules

1. Physical presence does not imply allocation availability.
2. Allocation is a commitment against inventory, not a physical inventory state.
3. Hold or lock is a restriction and must not imply zero physical quantity.
4. Quality condition is separate from allocation state.
5. `AvailableQuantity` should be derived from physical quantity, commitments and applicable restrictions instead of stored as an independent source of truth where avoidable.
6. Packed or loaded inventory may still physically exist inside the facility while being unavailable for new allocation.
7. Inventory adjustments must preserve an auditable reason and originating process such as cycle count.

## Candidate invariant

```text
0 <= allocated_quantity <= physical_quantity
available_quantity <= physical_quantity - allocated_quantity
```

The exact availability formula remains policy-dependent because holds, process locations, lock codes, quality conditions and ownership can further reduce allocatable quantity.

## Consequences

- Fewer impossible combined states such as `damaged_allocated_on_hold_received`.
- Inventory inquiry can explain why stock is unavailable instead of exposing only a status label.
- Cycle-count adjustment can modify physical quantity without silently changing unrelated quality or allocation concepts.
- 3PL and regulated inventory can layer owner-specific and hold-specific policies without redesigning the core state machine.

## Evidence

See:

- `ontology/inventory.yml`
- `evidence/oracle-inventory-types-2026.yml`
- `evidence/infor-cycle-counting-2026.yml`
