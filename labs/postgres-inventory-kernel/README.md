# PostgreSQL Inventory Kernel Lab

Executable proof-of-concept for Spotwo WMS Kernel ADR 0014 through ADR 0028.

The lab deliberately tests **database invariants and concurrency behavior**, not WMS UI or a production application architecture.

## Runtime

- PostgreSQL 18.4 (`postgres:18.4-bookworm`, pinned by multi-platform digest)
- NATS Server 2.14.5 (`nats:2.14.5`, pinned by multi-platform digest)
- Python 3.12+
- Psycopg 3.3.4
- nats-py 2.15.0
- pytest 9.1.1
- Docker Compose v2

PostgreSQL 18 is used because the target model relies on modern PostgreSQL semantics such as multicolumn `UNIQUE NULLS NOT DISTINCT`; PG18 also provides native UUIDv7 generation, although this lab keeps position/transaction IDs caller-supplied to preserve the ADR's application-generated-ID boundary.

## Run

From repository root:

```bash
bash bin/check-kernel-lab
```

The command:

1. installs the pinned lab Python dependencies;
2. selects free localhost ports and starts isolated PostgreSQL and NATS JetStream containers;
3. waits for two consecutive SQL readiness probes and a successful NATS connection;
4. applies every `sql/*.sql` migration in lexical order with `ON_ERROR_STOP=1`;
5. runs the database, publisher, and real JetStream integration tests;
6. destroys the lab database and JetStream volumes on exit.

Pin the host port explicitly when needed:

```bash
KERNEL_LAB_PORT=65432 bash bin/check-kernel-lab
```

## What is executable

### Semantic Inventory Key

`inventory_positions` uses a surrogate UUID primary key plus a semantic uniqueness constraint:

```text
tenant
warehouse
Location XOR direct HandlingUnit
item
owner
condition
lot?
stock scope?
attribute set?
stock segment?
```

`UNIQUE NULLS NOT DISTINCT` makes nullable dimensions participate in uniqueness, so two concurrent attempts to create the same fungible position converge on one row.

### Inventory Anchor

A position has exactly one immediate physical anchor:

```text
loose stock     -> Location
contained stock -> direct Handling Unit
```

A same-warehouse full-HU relocation updates the HU placement record and HU movement history without rewriting every contained Inventory Position. Repacking stock between HUs is a real inventory movement between two position keys.

### Inventory commitments

The lab separates:

```text
Availability != Reservation != Allocation != Assignment
```

A coarse Reservation commits quantity against an exact stock-dimension scope but does not select a Location, HU, or Inventory Position. An Allocation binds demand to one exact Inventory Position.

For the current exact scope, availability is derived from allocation-eligible stock:

```text
available =
  free quantity on allocation-eligible positions
  - coarse reservation remainder
```

Converting Reservation remainder into Allocation preserves availability because one commitment form decreases by the same amount that the more-specific form increases.

Reservation creation and unreserved allocation serialize through a transaction-scoped advisory lock derived from the stock-dimension scope. Position writes additionally take row locks.

### Inventory holds and action eligibility

The lab separates:

```text
Inventory Condition != Inventory Hold != Hold Policy != Action Eligibility
```

`inventory_hold_policies` define independent restrictions for allocation, movement, picking and shipping. `inventory_holds` apply one policy to either an exact stock-dimension scope or one exact Inventory Position.

Multiple active holds compose by logical OR:

```text
eligible(action) =
  no active matching hold policy blocks(action)
```

A hold is active only after `starts_at`, before optional `expires_at`, and until explicit release. Applying/releasing a hold does not change physical quantity and is recorded through `hold_apply` / `hold_release` InventoryTransactions.

Allocation-blocking holds remove affected positions from current availability. Allocation increases are protected by a database trigger so neither the high-level commitment function nor the low-level allocation primitive can bypass eligibility.

Movement eligibility is independent. Quantity movements check source and target stock, while whole-HU relocation walks the contained HU subtree so moving a pallet cannot bypass a movement hold on stock inside it.

Hold apply/release uses the same exact-scope advisory lock as Reservation and Allocation. A hold therefore serializes with new commitments without silently rewriting a Reservation or Allocation that already committed first.

### Position-level commitments

`allocated_qty` and `reserved_qty` on the position represent only commitments already bound to that exact Inventory Key. The low-level allocation function also respects coarse Reservation capacity and active allocation holds.

### Balanced physical movements

`transfer_inventory_quantity()` locks the semantic stock scope, evaluates movement eligibility, then locks source and target positions in deterministic ID order, validates non-anchor identity, posts source/target quantity updates, and writes two balanced transaction legs.

### Serial exclusivity

Exact serial tracking uses `inventory_serial_memberships`; a tenant/serial can have only one live position membership. Serial identity is not a generic Inventory Key column.

### Warehouse Work execution

The lab now separates execution concerns from inventory and capability documents:

```text
Business Process != WarehouseWork
WarehouseWork != WarehouseTask
Allocation != WorkAssignment
TaskExecution != TaskConfirmation
TaskConfirmation != InventoryTransaction
```

`warehouse_works` is the aggregate root. Ordered `warehouse_tasks` are executable children. Releasing work makes only the first task ready; each confirmation unlocks the next task.

One active `warehouse_work_assignment` is allowed per Work in v0.1. Concurrent claim attempts serialize on the Work row so only one resource wins. The assigned resource must own every execution attempt.

`warehouse_task_executions` are attempts rather than overwritten state history. A task exception blocks the active attempt; resolving it with `resume` aborts that historical attempt, returns the task to `ready`, and a later start creates a new execution attempt.

Execution channel is orthogonal to task semantics and is persisted per attempt, so RF, mobile, voice, workstation, light-directed, or automation adapters can execute the same generic task contract.

For capability-affecting tasks, `requires_domain_confirmation = true` requires a `domain_result_reference` before the task can be completed. The generic Work engine therefore cannot silently stand in for an Inventory Transaction, Count Result, Pack Confirmation, or other capability-owned consequence.

Work commands use their own tenant-scoped idempotency receipt table. The Inventory Transaction ledger is not abused as a generic command-deduplication store.

### Retry safety

Inventory posting commands carry a tenant-scoped idempotency key bound to a canonical request fingerprint. Work lifecycle commands use the same retry-safety principle in their own domain receipt table. An exact retry returns the already-applied result; reuse of the same key for a different payload is rejected.

## Executable scenarios

| Test | Invariant demonstrated |
|---|---|
| merge race | concurrent semantic-key creation produces one position |
| double allocation | competing commitments cannot over-allocate one position |
| serial double assignment | one exact serial cannot occupy two live positions |
| partial split | source `-Q` and target `+Q` conserve physical quantity |
| same-HU relocation | moving a whole HU does not rewrite contained position rows |
| repack | moving part of stock HU A -> HU B is a real position split |
| deadlock ordering | opposite concurrent transfers lock the same pair in deterministic order |
| idempotent retry | repeating one logical allocation does not double-post it |
| idempotency collision | reusing one key for a different payload is rejected |
| coarse reservation | reservation reduces scope availability without touching position `reserved_qty` |
| reservation conversion | Reservation -> Allocation preserves global availability |
| reserved-capacity protection | unreserved allocation cannot steal coarse-reserved stock |
| reservation race | concurrent reservations cannot overcommit one stock scope |
| multi-position allocation | one Reservation can allocate across multiple Inventory Positions |
| allocation release | active Reservation regains remainder rather than free capacity |
| closed reservation | releasing a later Allocation cannot reopen a released Reservation |
| reservation retry | reservation idempotency replays exact payload and rejects collision |
| scope allocation hold | held stock remains physical but leaves allocation availability |
| movement-only hold | policy can allow allocation while rejecting movement |
| position hold | one held position is excluded while another remains eligible |
| composed holds | multiple active hold policies combine restrictions by OR |
| hold release | explicit release restores eligibility without rewriting stock |
| hold expiration | expired restriction remains auditable but stops blocking actions |
| reservation plus later hold | existing Reservation survives while held position becomes unusable for new Allocation |
| HU hold enforcement | whole-HU relocation cannot bypass held inventory inside the HU subtree |
| hold retry | apply command is idempotent and rejects a changed payload under the same key |
| hold/reservation race | hold and reservation serialize on the same stock-scope lock |
| work release | only the first ordered Warehouse Task becomes ready |
| work queue priority | released work is ranked by priority/due time without inventing a Wave |
| concurrent work claim | only one resource can own the active Work assignment |
| execution ownership | only the assigned resource can start a task; channel remains orthogonal |
| task sequence | later tasks cannot start early and confirmation unlocks the next task |
| work completion | final task confirmation completes Work and its active Assignment together |
| domain confirmation boundary | inventory/capability-affecting task requires an external domain result reference |
| exception retry | resolved exception preserves the blocked attempt and creates a new execution attempt on retry |
| pre-start cancellation | cancelling unstarted work releases assignment and cancels remaining tasks |
| late cancellation rejection | generic cancellation cannot bypass capability compensation after execution starts |
| work command retry | exact command replay is idempotent and changed payload under the same key is rejected |
| JetStream PUB ACK | Outbox ACK happens only after the real transport confirms stream acceptance |
| JetStream dedupe | retrying the same stable `event_id` uses `Nats-Msg-Id` and appends one stream record |
| durable pull redelivery | an unacknowledged message is redelivered and then explicitly ACKed |
| broker outage isolation | NATS failure leaves committed Inventory and durable Outbox facts intact |
| bounded Outbox retry | exponential delay is capped and deterministically jittered per event attempt |
| poison-event quarantine | a permanently failing event stops consuming publisher capacity at its attempt limit |
| audited operator replay | replay requires operator identity/reason and preserves the previous failure state in an audit row |
| Outbox state telemetry | every unpublished event is exactly ready, delayed, leased, or quarantined |
| backlog health | oldest ready age, attempt buckets, and bounded alert codes render as JSON or Prometheus |
| safe Outbox retention | only old published events move in bounded transactional archive batches |
| durable event dedupe | producer idempotency and replay audit survive live Outbox retention |
| atomic Inbox effect | consumer receipt and handler SQL commit in one PostgreSQL transaction |
| consumer ACK uncertainty | JetStream redelivery after ACK loss skips the already-committed handler |
| consumer failure redelivery | failed handler SQL rolls back its receipt and succeeds on redelivery |
| explicit consumer provisioning | runtime refuses to create a missing durable with accidental defaults |

## Deliberate simplifications

This is a kernel lab, not the production schema. It intentionally omits or simplifies:

- wildcard/overlapping reservation scopes;
- broad hold selectors such as entire Location, HU or Lot across otherwise-different stock scopes;
- partial-quantity holds and overlap accounting;
- order-type exceptions that explicitly permit selected hold codes;
- automatic holds at receipt and hold review workflows;
- separate available-to-reserve versus available-to-allocate projections;
- reservation expiration and priority;
- FEFO/source-selection policy;
- resource qualification and equipment eligibility;
- automatic dispatch / claim-next scoring;
- task dependency graphs and parallel execution branches;
- multi-resource work and task-level assignment;
- work hold/resume separate from execution exceptions;
- capability-specific atomic confirmation handlers beyond the domain-result-reference contract;
- outbox/domain-event persistence for Work lifecycle facts;
- cross-work dependencies and orchestration;
- labor actuals and engineered standards;
- cross-warehouse transfer orchestration;
- lot genealogy;
- HU-cycle prevention beyond the immediate self-parent check;
- row-level security / tenant policies;
- physical archive partitioning, cold export, and destructive purge;
- consumer poison-message quarantine, DLQ review, and Inbox cleanup;
- strict per-aggregate consumer lanes and version-gap recovery;
- catch-weight dual quantities;
- production UUIDv7 generation policy;
- performance benchmarks and `EXPLAIN (ANALYZE, BUFFERS)` tuning.

These are subsequent layers after the inventory, commitment, eligibility, Work lifecycle, and concurrency semantics prove sound.

## Source of truth

The conceptual decisions are:

- `decisions/domain/0014-inventory-key-stock-dimensions.md`
- `decisions/domain/0015-inventory-commitment-engine.md`
- `decisions/domain/0016-inventory-holds-eligibility.md`
- `decisions/domain/0017-warehouse-work-execution-engine.md`
- `decisions/domain/0018-golden-warehouse-scenarios.md`
- `decisions/domain/0019-postgresql-performance-contention-lab.md`
- `decisions/domain/0020-wms-kernel-v0.1.md`
- `decisions/domain/0021-transactional-outbox-domain-events.md`
- `decisions/domain/0022-outbox-publisher-runtime.md`
- `decisions/domain/0023-domain-event-transport-selection.md`
- `decisions/domain/0024-nats-jetstream-transport-adapter.md`
- `decisions/domain/0025-outbox-retry-quarantine.md`
- `decisions/domain/0026-outbox-operational-telemetry.md`
- `decisions/domain/0027-outbox-retention-archive.md`
- `decisions/domain/0028-inbox-consumer-runtime.md`

The lab exists to falsify or strengthen those candidate models with executable PostgreSQL and transport behavior.
