# PostgreSQL Inventory Kernel Lab

Executable proof-of-concept for ADR 0014 and the inventory transaction kernel.

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
2. starts an isolated PostgreSQL container on port `55432` by default;
3. applies `sql/001_schema.sql` with `ON_ERROR_STOP=1`;
4. runs the concurrency/invariant tests;
5. destroys the lab database volume on exit.

Override the host port when needed:

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

### Position-level commitments

`allocated_qty` and `reserved_qty` on the position represent only commitments already bound to that exact Inventory Key. The allocation function locks the position and prevents competing allocations from exceeding physical quantity.

### Balanced physical movements

`transfer_inventory_quantity()` locks source and target positions in deterministic ID order, validates non-anchor identity, posts source/target quantity updates, and writes two balanced transaction legs.

### Serial exclusivity

Exact serial tracking uses `inventory_serial_memberships`; a tenant/serial can have only one live position membership. Serial identity is not a generic Inventory Key column.

### Retry safety

Inventory posting commands carry a tenant-scoped idempotency key. A retry returns the already-posted transaction instead of applying the quantity delta a second time.

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

## Deliberate simplifications

This is a kernel lab, not the production schema. It intentionally omits or simplifies:

- reservation aggregates above exact-position granularity;
- outbox/domain-event persistence;
- cross-warehouse transfer orchestration;
- lot genealogy;
- HU-cycle prevention beyond the immediate self-parent check;
- row-level security / tenant policies;
- partitioning and archival;
- catch-weight dual quantities;
- production UUIDv7 generation policy;
- performance benchmarks and `EXPLAIN (ANALYZE, BUFFERS)` tuning.

These are next-layer concerns after the concurrency semantics prove sound.

## Source of truth

The conceptual decision remains `decisions/domain/0014-inventory-key-stock-dimensions.md`. This lab exists to falsify or strengthen that candidate model with executable PostgreSQL behavior.
