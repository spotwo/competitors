# ADR 0009 - Inventory Commitment, Reservation, Allocation and Availability

Status: Proposed

Date: 2026-08-18

## Context

Warehouse and ERP products use the words reservation, allocation, availability and commitment with overlapping but materially different semantics.

Current evidence shows two especially useful patterns:

- Dynamics 365 can reserve inventory at different levels of a reservation hierarchy and can postpone exact location or license-plate binding until warehouse execution.
- Oracle WMS defines allocation as binding order demand to specific source inventory and can create allocations through waves, direct allocation and cross-docking.

Availability is also not one universal stored quantity. Systems calculate different views such as available physical, available for reservation, available for allocation and available for picking according to inventory dimensions, reservations, holds, process state and policy.

## Proposed decision

Use four separate Spotwo concepts:

```text
Inventory Position
      |
      +------> Availability View
      |
Demand
  |
  +------> Reservation?     # optional coarse commitment
  |             |
  |             v
  +--------> Allocation     # execution-relevant source binding
                    |
                    v
              Warehouse Work
                    |
                    v
               Consumption
```

### Availability View

A calculated projection answering a specific question under a defined scope and rule set.

Examples:

- available to reserve
- available to allocate
- available to pick
- available for transfer

It is not an independently mutable stock bucket.

### Reservation

A commitment of inventory capacity to demand that may intentionally remain less specific than the final execution source.

Example:

```text
Demand reserved at:
Site + Warehouse + Inventory Condition

Exact Location + Handling Unit
selected later by warehouse execution.
```

### Allocation

A binding of demand to inventory at sufficient specificity for downstream warehouse execution.

Example:

```text
Order Line
   |
   v
Allocation
   |
   v
Location + LPN + Lot + Quantity
```

Vendor mappings may differ. A vendor may call a source binding a reservation or may use allocation for a commercial quota concept. Integrations must map semantics, not words.

## Rules

1. `Availability` is derived from inventory state, commitments, process state and policy. Do not persist it as an authoritative independently mutable stock quantity unless it is explicitly a cache/projection.
2. `Reservation` does not require exact source identity in the Spotwo candidate model.
3. `Allocation` represents execution-relevant source binding.
4. Reservation is optional. A workflow may move directly from demand to allocation when the source can be selected immediately.
5. Reservation and allocation must not be confused with worker/task assignment.
6. Releasing a reservation or allocation restores eligibility according to the applicable availability rules; it does not create physical inventory.
7. Consuming an allocation requires an execution or inventory transaction outcome. Creating an allocation alone does not reduce physical quantity.
8. Partial execution must preserve residual demand or residual commitment explicitly.
9. Holds, inventory condition, ownership, location rules and active warehouse work may affect availability without changing physical quantity.
10. Commitment commands must be concurrency-safe and idempotent. Two simultaneous successful commitments must never promise the same exclusive quantity beyond policy limits.
11. Exact dimensional binding may be deferred until the latest point that still guarantees execution correctness.
12. Commercial allocation quotas, ATP/CTP promises and warehouse source allocation are adjacent concepts and must not be silently merged into one object.

## Candidate entities

```text
AvailabilityView
  action
  scope
  quantity
  calculated_at
  policy_version

Reservation
  id
  demand_reference
  inventory_scope
  quantity
  state

Allocation
  id
  demand_reference
  source_inventory_reference
  quantity
  state

ResidualDemand
  demand_reference
  remaining_quantity
```

## Candidate lifecycle

```text
pending
  |
  +--> reserved
  |       |
  |       v
  +----> allocated
           |
           +--> partially-consumed
           |        |
           |        v
           +----> consumed
           |
           +----> released
           |
           +----> exception
```

The state model remains candidate because some implementations may model Reservation and Allocation as different aggregates rather than states of one commitment.

## Consequences

- Warehouse source selection can evolve independently from commercial order promise logic.
- Cross-docking can reuse Allocation with an inbound source context.
- Picking can consume allocations without owning reservation policy.
- Relocation of committed stock can trigger re-reservation or source reallocation instead of corrupting demand linkage.
- Availability APIs become explicit about the action and scope being calculated.
- Agents and integrations can distinguish a quantity that physically exists from a quantity that is still promiseable.

## Open questions

- Whether Reservation and Allocation should be separate persisted aggregates or typed states under a shared `InventoryCommitment` aggregate.
- How to represent inbound/ordered inventory commitments that are not physically present yet.
- Whether soft commercial allocations belong in the same service or a separate order-promise service.
- Exact locking versus optimistic-concurrency strategy for high-volume allocation.
- How serial-number reservation and full-LPN reservation interact with delayed source binding.

## Evidence

See:

- `workflows/allocation.yml`
- `ontology/inventory.yml`
- `evidence/dynamics365-reservations-2026.yml`
- `evidence/dynamics365-onhand-availability-2026.yml`
- `evidence/oracle-allocation-model-2026.yml`
