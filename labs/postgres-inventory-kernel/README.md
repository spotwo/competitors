# PostgreSQL Inventory Kernel Lab

Executable proof-of-concept for ADR 0014, ADR 0015, ADR 0016, and the inventory transaction kernel.

The lab deliberately tests **database invariants and concurrency behavior**, not WMS UI or a production application architecture.

## Runtime

- PostgreSQL 18.4 (`postgres:18.4-bookworm`, pinned by multi-platform digest)
- Python 3.12+
- Psycopg 3.3.4
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
2. selects a free localhost port and starts an isolated PostgreSQL container there;
3. waits for two consecutive SQL readiness probes through the migration execution path;
4. applies every `sql/*.sql` migration in lexical order with `ON_ERROR_STOP=1`;
5. runs the concurrency/invariant tests;
6. destroys the lab database volume on exit.

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

### Retry safety

Inventory posting commands carry a tenant-scoped idempotency key bound to a canonical request fingerprint. An exact retry returns the already-posted result instead of applying the mutation twice; reuse of the same key for a different payload is rejected.

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
- Warehouse Work creation and assignment;
- outbox/domain-event persistence;
- cross-warehouse transfer orchestration;
- lot genealogy;
- HU-cycle prevention beyond the immediate self-parent check;
- row-level security / tenant policies;
- partitioning and archival;
- catch-weight dual quantities;
- production UUIDv7 generation policy;
- performance benchmarks and `EXPLAIN (ANALYZE, BUFFERS)` tuning.

These are subsequent layers after the commitment, eligibility and concurrency semantics prove sound.

## Source of truth

The conceptual decisions are:

- `decisions/domain/0014-inventory-key-stock-dimensions.md`
- `decisions/domain/0015-inventory-commitment-engine.md`
- `decisions/domain/0016-inventory-holds-eligibility.md`

The lab exists to falsify or strengthen those candidate models with executable PostgreSQL behavior.
