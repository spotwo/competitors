# ADR 0008 - Generic Warehouse Work Abstraction

Status: Proposed

Date: 2026-08-18

## Context

Deep workflow analysis now covers Picking, Internal Replenishment, Receiving, Putaway, Packing, Shipping and Cycle Counting.

Several execution-heavy capabilities repeatedly expose the same separation:

```text
Business Need / Trigger
        |
        v
Planning / Decision
        |
        v
Executable Work
        |
        v
Assignment / Claim
        |
        v
Execution
        |
        v
Confirmation / Exception
```

Vendor vocabularies differ. Examples already captured in the repository include Warehouse Order, Warehouse Task, Work, Work Line, Task, Allocation, Assignment, Pick Task, Cycle Count Work and Packing Work.

Creating a separate incompatible task engine for every Spotwo capability would duplicate scheduling, assignment, execution state, priority, device routing, exception handling and observability behavior.

## Proposed decision

Introduce a generic **Warehouse Work** abstraction for executable operational work while keeping capability-specific domain objects and payloads.

```text
WarehouseWork
  id
  capability
  reason
  priority
  state
  due_at?
  work_group_id?
  assignment_id?
  resource_requirements[]
  source_context
  destination_context
  domain_reference

WarehouseTask
  id
  warehouse_work_id
  sequence
  operation
  state
  source?
  destination?
  quantity?
  handling_unit?
  confirmation_requirements[]
```

A capability may use the generic work engine without being reduced to generic data. Picking still owns pick semantics, replenishment owns replenishment need/policy semantics, cycle counting owns count/variance semantics, and packing owns outbound-container semantics.

## Candidate common work states

```text
planned
released
assigned
in-progress
held
exception
completed
cancelled
```

Not every capability must expose every state. Capability workflows may add process states outside the work object, such as `received`, `packed`, `loaded`, `shipped`, `pending-review` or `quality-hold`.

## Rules

1. `WarehouseWork` represents executable operational intent, not the business document that created it.
2. `WarehouseTask` represents an executable operation or step. It must not replace capability-specific domain objects.
3. Work grouping is optional. Waveless or simple work must not require a fake Wave or Work Group.
4. Assignment is separate from task identity. Work may be explicitly assigned, dynamically dispatched or exposed through an eligible task pool.
5. Execution channel is orthogonal. RF, mobile, voice, light-directed, workstation and automation adapters may execute the same task semantics.
6. Confirmation records actual execution outcome. It is not merely a UI acknowledgement.
7. Exceptions are first-class domain outcomes with reason, evidence and resolution path.
8. Priority and eligibility may be recalculated while work remains executable.
9. Process-specific state remains on the relevant process/container/shipment object when it does not describe executable work itself.
10. External vendor concepts must be mapped explicitly rather than assumed to be exact synonyms of Spotwo work concepts.

## Expected shared infrastructure

A common work engine can own:

- IDs and traceability
- priority and queues
- assignment and claiming
- worker/equipment eligibility
- lifecycle timestamps
- hold/resume/cancel behavior
- execution channel adapters
- confirmation requirements
- exception envelope
- audit history
- observability and domain events
- idempotent command handling

Capability modules continue to own:

- planning logic
- business invariants
- source/destination decision rules
- inventory semantics
- domain-specific exceptions and resolutions
- downstream consequences

## Consequences

- Picking, Putaway, Replenishment and Cycle Counting can share execution infrastructure without sharing misleading business terminology.
- Future automation orchestration can target the same work model while choosing a human worker, forklift, AMR, conveyor station or other resource.
- Device UX can evolve independently from domain semantics.
- Work telemetry becomes comparable across capabilities.
- The model remains compatible with vendor integrations that use materially different task and grouping vocabularies.

## Open questions

- Whether `WarehouseWork` and `WarehouseTask` should be separate persisted aggregates or one aggregate with task children.
- Whether `Assignment` deserves a first-class aggregate for history and multi-resource execution.
- Which states are truly universal after Receiving, Packing and Shipping are sampled more deeply.
- Whether movement-heavy tasks should reference a generic `InventoryMovement` object shared by putaway, replenishment and relocation.

## Evidence

See:

- `workflows/picking.yml`
- `workflows/replenishment.yml`
- `workflows/putaway.yml`
- `workflows/cycle-counting.yml`
- `workflows/packing.yml`
- `workflows/receiving.yml`
- `workflows/shipping.yml`
- `decisions/domain/0005-picking-work-model.md`
- `decisions/domain/0006-internal-replenishment-scope.md`
