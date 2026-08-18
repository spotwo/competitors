# ADR 0017 - Warehouse Work Execution Engine

Status: Proposed

Date: 2026-08-18

## Context

ADR 0008 introduced a generic Warehouse Work abstraction after Picking, Replenishment, Receiving, Putaway, Packing, Shipping, and Cycle Counting repeatedly exposed the same execution concerns: release, priority, assignment, sequencing, execution, confirmation, exception handling, and observability.

The inventory kernel now separately models Inventory Position, Reservation, Allocation, Holds/Eligibility, Inventory Movement, and Inventory Transactions. The next boundary is executable warehouse work.

Vendor evidence reinforces that this is a reusable execution layer rather than a synonym for one process:

- Microsoft uses Work across warehouse worker operations such as picking, moving, and counting.
- SAP groups Warehouse Tasks into Warehouse Orders and assigns the work package to a resource.
- Oracle Tasks expose reusable lifecycle controls including assignment, hold, release, cancel, priority, and RF execution across multiple task types.
- Infor RF-directed tasks are dispatched to associates and inventory is updated after task completion is confirmed.

## Decision

Spotwo uses this execution spine:

```text
Business Need / Capability Object
        |
        v
Planning / Decision
        |
        v
WarehouseWork
        |
        +---- WarehouseTask #1
        +---- WarehouseTask #2
        +---- WarehouseTask #N
        |
        v
WorkAssignment
        |
        v
TaskExecution
        |
        +---- TaskConfirmation
        |
        +---- TaskException -> Resolution -> retry/new execution
```

And preserves these boundaries:

```text
Business Process != WarehouseWork
WarehouseWork != WarehouseTask
WarehouseTask != InventoryMovement
Allocation != WorkAssignment
TaskExecution != TaskConfirmation
TaskConfirmation != InventoryTransaction
TaskException != InventoryHold
```

### WarehouseWork is the aggregate root

`WarehouseWork` is the executable work package. It owns common execution state, priority, sequencing, warehouse scope, capability identity, and a reference back to the capability-owned object that created it.

Candidate fields:

```text
WarehouseWork
  id
  tenant_id
  warehouse_id
  capability
  domain_reference
  reason?
  priority
  state
  due_at?
  resource_requirements
  version
```

A Work must contain one or more ordered Warehouse Tasks before it can be released.

### WarehouseTask is an ordered executable child

A task is one executable operation within Work. It can carry capability-specific context, but the generic work engine does not reinterpret that payload.

Candidate fields:

```text
WarehouseTask
  id
  work_id
  sequence
  operation
  state
  planned_quantity?
  uom?
  source_context?
  destination_context?
  domain_payload
  requires_domain_confirmation
  confirmation_requirements
```

The v0.1 kernel uses a simple ordered sequence. Only the next incomplete task can become executable. Dependency graphs are deferred.

### Assignment is first-class history

`WorkAssignment` binds Warehouse Work to an execution resource without changing Work identity.

```text
WorkAssignment
  work_id
  resource_kind
  resource_id
  assignment_method
  state
  assigned_at
  released_at?
  completed_at?
```

The v0.1 invariant is **one active assignment per Warehouse Work**. A resource may claim specific released work; concurrent claims serialize on the Work row and only one succeeds.

Task-level multi-resource assignment is deferred until real workflows require it.

### Execution is an attempt, not task state history

`TaskExecution` records one concrete attempt to perform one task. Retrying after an exception creates a new execution attempt instead of rewriting the old one.

The execution channel is orthogonal to task semantics:

```text
rf
mobile
voice
light-directed
workstation
automation
```

The same task semantics can therefore be executed by different UX or automation adapters.

### Confirmation records actual outcome

`TaskConfirmation` is an immutable actual-execution fact, not a UI acknowledgement.

It may contain:

```text
actual_quantity?
outcome
domain_result_reference?
data
```

For tasks whose completion has a capability-owned consequence, `requires_domain_confirmation = true` and a `domain_result_reference` is mandatory.

Examples:

```text
Pick/Move task      -> InventoryTransaction / MovementConfirmation
Cycle Count task    -> CountResult
Pack task           -> PackConfirmation / HandlingUnit result
Load task           -> LoadingConfirmation
```

The generic Work engine does **not** create those domain results itself.

Production capability services must validate and create the domain consequence atomically with, or under a reliable orchestration contract around, Work confirmation. The lab only proves the generic boundary and reference requirement.

### Exceptions are first-class outcomes

A running Task may produce a `TaskException` with code, details/evidence, state, and resolution.

In v0.1:

```text
Task in_progress
   -> exception
Work in_progress
   -> exception
Execution in_progress
   -> blocked
```

Resolving with `resume` closes the exception, aborts the blocked attempt as historical evidence, returns the task to `ready`, and leaves the current assignment active. A later start creates a new execution attempt.

Capability-specific exception meaning and downstream consequences remain capability-owned.

## State model

### Warehouse Work

```text
planned
  -> released
  -> assigned
  -> in_progress
  -> exception
  -> assigned        # after resumable exception
  -> completed

planned/released/assigned
  -> cancelled       # only before execution has started
```

### Warehouse Task

```text
pending
  -> ready
  -> in_progress
  -> completed

in_progress
  -> exception
  -> ready            # after resume

pending/ready
  -> cancelled        # when parent Work is cancelled
```

### Assignment

```text
active
  -> completed
  -> released
```

### Execution attempt

```text
in_progress
  -> completed
  -> blocked
  -> aborted          # blocked attempt after exception resolution
```

## Sequencing

Releasing Work makes only the first pending Task `ready`.

Confirming Task N makes Task N+1 `ready`. Earlier tasks must be completed or cancelled before a later task can start.

This intentionally models ordered execution without inventing Waves or other grouping objects.

## Concurrency and locking

Candidate lock order:

```text
idempotency-key advisory lock
  -> WarehouseWork row
  -> WarehouseTask row
  -> active WorkAssignment row
  -> active TaskExecution row
```

Important invariants:

1. Only one active assignment exists per Work.
2. Concurrent claims for one Work cannot both succeed.
3. Only the currently assigned resource can start or confirm tasks.
4. One Task has at most one live execution attempt.
5. Task sequence cannot be skipped.
6. Final Task confirmation completes the Work and active Assignment together.
7. Historical executions, confirmations, exceptions, and assignments are append-oriented records rather than overwritten audit history.

## Idempotency

Work commands use a Work-domain command receipt rather than the Inventory Transaction ledger.

```text
same tenant + same idempotency key + same command payload
  -> return original result

same tenant + same idempotency key + different command payload
  -> reject
```

The Work engine must not create fake Inventory Transactions merely to obtain idempotency.

## Cancellation

The v0.1 engine allows cancellation only before execution starts:

```text
planned / released / assigned -> cancelled
```

Cancellation cancels remaining pending/ready tasks and releases an active assignment.

Cancellation after a task has started or been confirmed is capability-specific because compensating physical or business actions may be required.

## Vendor mapping boundary

Vendor terms are mapped, not treated as exact synonyms.

Typical mappings may be:

```text
Spotwo WarehouseWork  <-> SAP Warehouse Order
Spotwo WarehouseTask  <-> SAP Warehouse Task

Spotwo WarehouseWork  <-> Dynamics Work header
Spotwo WarehouseTask  <-> Dynamics Work line

Spotwo WarehouseWork  <-> Oracle Task in a simple one-task work package
Spotwo WarehouseTask  <-> Oracle underlying execution/allocation step where applicable
```

The mapping is integration-specific and does not redefine Spotwo semantics.

## Deliberate v0.1 boundaries

Deferred:

- resource qualifications, equipment capability, and labor-standard eligibility;
- automatic dispatch / claim-next scoring;
- task dependency graphs and parallel branches;
- multi-resource work and task-level assignment;
- work hold/resume separate from execution exceptions;
- SLA escalation and reprioritization policies;
- outbox/domain-event persistence for Work lifecycle facts;
- capability-specific atomic confirmation handlers;
- cross-work dependencies and orchestration;
- labor actuals and engineered standards.

## Executable validation

The candidate is exercised by:

- `labs/postgres-inventory-kernel/sql/006_warehouse_work_engine.sql`
- `labs/postgres-inventory-kernel/tests/test_work.py`

The PostgreSQL lab is intentionally not a production application schema. It exists to falsify state, locking, sequencing, idempotency, and boundary assumptions before they become production architecture.

## Evidence

- `evidence/dynamics365-warehouse-work-2026.yml`
- `evidence/sap-ewm-warehouse-order-work-package-2026.yml`
- `evidence/oracle-wms-task-execution-2026.yml`
- `evidence/infor-directed-task-execution-2026.yml`
