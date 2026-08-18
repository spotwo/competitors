# ADR 0010 - Generic Inventory Movement Model

Status: Proposed

Date: 2026-08-18

## Context

Deep workflow analysis now shows the same physical inventory-change pattern inside multiple capabilities:

- Putaway moves received inventory into storage.
- Internal Replenishment moves inventory from reserve or bulk into execution locations.
- Cross-docking moves matched inbound inventory toward outbound flow.
- Relocation moves inventory between internal warehouse locations for operational reasons.
- Transfers can move inventory across warehouse or organizational boundaries.

Vendor implementations also expose generic movement concepts directly. Dynamics 365 supports Movement and Movement by Template as warehouse work. SAP EWM has internal stock-transfer warehouse tasks and ad-hoc product/HU movement. Infor WMS supports Standard Move RF including partial inventory moves and LPN splits.

Creating separate incompatible source/destination/confirmation semantics inside every capability would duplicate inventory mutation rules and make movement auditing difficult.

## Proposed decision

Introduce a generic **Inventory Movement** primitive representing one confirmed source-to-destination inventory-context change.

Keep **Movement Intent** and process-specific planning outside the primitive.

```text
Capability-specific Need
        |
        v
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

### Inventory Movement

```text
InventoryMovement
  id
  reason
  source_context
  destination_context
  requested_quantity
  confirmed_quantity
  uom
  handling_unit?
  domain_reference
  state
```

### Source / Destination Context

A context may include:

```text
warehouse
location
handling_unit
item
lot
serial
owner
condition
inventory_status
```

Only dimensions relevant to the movement need to be present.

## Movement versus Transfer

`InventoryMovement` is one inventory-context change.

`InventoryTransfer` is a business intent and may have different implementations.

### Immediate transfer

```text
Warehouse A / Location X
          |
          v
Warehouse B / Location Y
```

A system may represent this as one direct transfer or journal transaction.

### Tracked inter-warehouse transfer

```text
Transfer Order
    |
    v
Pick
    |
    v
Ship
    |
    v
In Transit
    |
    v
Receive
    |
    v
Putaway
```

This is **not** one atomic warehouse movement. The transfer orchestrates multiple inventory transitions and warehouse workflows.

## Rules

1. `Movement Intent` explains why stock should move. `Inventory Movement` records what actually moved.
2. Movement planning does not mutate authoritative inventory.
3. Inventory mutation occurs only from an accepted movement confirmation or another explicit inventory transaction.
4. Source and destination contexts must be explicit enough to reproduce the inventory change and audit it later.
5. A normal movement conserves quantity: it relocates inventory rather than creating or destroying it.
6. Quantity creation, destruction, ownership change, condition transformation or financial adjustment require explicit domain reasons and must not be hidden inside a generic move.
7. Partial confirmation records actual moved quantity and preserves `Residual Movement Need` when additional movement is still required.
8. Handling-unit movement may move all child stock implicitly only when the HU is treated as an atomic inventory container under the applicable rules.
9. Splitting a handling unit is a first-class outcome and must preserve inventory identity and traceability.
10. Existing Reservations, Allocations and Warehouse Work must be revalidated when movement changes their source inventory context.
11. `Putaway`, `Replenishment`, `Relocation`, and `Cross-docking` may share movement execution and confirmation infrastructure while keeping separate planning policies and domain invariants.
12. A cross-warehouse `Transfer` must not be forced into a single movement when the business process tracks shipment, in-transit inventory and receipt separately.
13. Movement commands must be idempotent. Replaying the same confirmed movement must never apply inventory mutation twice.
14. Movement confirmation must preserve audit identity including who or what executed the move, when, source, destination, quantity, handling unit and reason.

## Candidate lifecycle

```text
planned
  -> released
  -> in-progress
  -> partially-completed
  -> completed

or

planned/released/in-progress
  -> exception
  -> cancelled
```

The process-specific workflow may expose additional states outside the movement itself.

## Shared by capabilities

```text
Putaway
  planning: destination policy
  execution: InventoryMovement

Replenishment
  planning: need + quantity + source policy
  execution: InventoryMovement

Relocation
  planning: operational reason + destination
  execution: InventoryMovement

Cross-docking
  planning: inbound/outbound match + allocation
  execution: InventoryMovement

Transfer
  planning/orchestration: transfer document
  execution: one or more movements and possibly Shipping/Receiving
```

## Consequences

- Inventory mutation code can be centralized and heavily tested.
- Movement telemetry is comparable across warehouse capabilities.
- A single movement API can support RF, mobile, forklift, AMR, conveyor or other execution adapters.
- Capability workflows retain their business meaning without duplicating source/destination mechanics.
- Transfer orders remain free to model in-transit inventory correctly.
- Reservation and allocation integrity can be re-evaluated consistently after movement.

## Open questions

- Whether movement confirmation is itself the aggregate root or an immutable event applied to an Inventory Position aggregate.
- Whether all intra-warehouse movement should pass through Warehouse Work or simple manual movements may be confirmed atomically.
- How to lock or version source inventory during concurrent movement and allocation.
- Whether ownership/condition changes belong to `InventoryTransformation` rather than `InventoryMovement`.
- Exact handling-unit atomicity rules for mixed-owner, mixed-status, mixed-lot or partially allocated HUs.

## Evidence

See:

- `ontology/inventory-movement.yml`
- `workflows/putaway.yml`
- `workflows/replenishment.yml`
- `workflows/cross-docking.yml`
- `workflows/relocation.yml`
- `evidence/dynamics365-inventory-movement-2026.yml`
- `evidence/sap-inventory-movement-2026.yml`
- `evidence/infor-inventory-movement-2026.yml`
- `evidence/dynamics365-inventory-transfer-2026.yml`
